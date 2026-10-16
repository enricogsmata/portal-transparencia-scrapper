import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import iconv from 'iconv-lite';
import csvParser from 'csv-parser';
import { sql } from 'drizzle-orm';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import { Pep } from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY, extractMetadataFromFileName, ReferenceMetadata, normalize } from '../shared.js';

const logger = new_logger(import.meta.url);
const BATCH_SIZE = 2000;

type PepRow = typeof Pep.$inferInsert;

async function persistData(data: PepRow[]) {
    const log = logger.forMethod('persistData');
    try {
        await bronze_db.insert(Pep).values(data).onDuplicateKeyUpdate({
            set: {
                cpf: sql`values(${Pep.cpf})`,
                nomePep: sql`values(${Pep.nomePep})`,
                siglaFuncao: sql`values(${Pep.siglaFuncao})`,
                descricaoFuncao: sql`values(${Pep.descricaoFuncao})`,
                nivelFuncao: sql`values(${Pep.nivelFuncao})`,
                nomeOrgao: sql`values(${Pep.nomeOrgao})`,
                dataInicioExercicio: sql`values(${Pep.dataInicioExercicio})`,
                dataFimExercicio: sql`values(${Pep.dataFimExercicio})`,
                dataFimCarencia: sql`values(${Pep.dataFimCarencia})`,
                exercicioReferencia: sql`values(${Pep.exercicioReferencia})`,
                mesReferencia: sql`values(${Pep.mesReferencia})`,
                diaReferencia: sql`values(${Pep.diaReferencia})`,
                dadosAdicionais: sql`values(${Pep.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de PEP`);
        throw error;
    }
}

interface HeaderRule {
    key: string;
    keywords?: string[];
    matchExact?: string[];
    require?: string[];
}

const PEP_HEADER_RULES: HeaderRule[] = [
    { key: 'cpf', matchExact: ['CPF'] },
    { key: 'nomePep', matchExact: ['NOME PEP', 'NOMEPEP'] },

    // Função
    { key: 'siglaFuncao', matchExact: ['SIGLA FUNCAO', 'SIGLAFUNCAO'] },
    { key: 'descricaoFuncao', matchExact: ['DESCRICAO FUNCAO', 'DESCRICAOFUNCAO'] },
    { key: 'nivelFuncao', matchExact: ['NIVEL FUNCAO', 'NIVELFUNCAO'] },

    // Órgão e Datas
    { key: 'nomeOrgao', matchExact: ['NOME ORGAO', 'NOMEORGAO'] },
    { key: 'dataInicioExercicio', matchExact: ['DATA INICIO EXERCICIO', 'DATAINICIOEXERCICIO'] },
    { key: 'dataFimExercicio', matchExact: ['DATA FIM EXERCICIO', 'DATAFIMEXERCICIO'] },
    { key: 'dataFimCarencia', matchExact: ['DATA FIM CARENCIA', 'DATAFIMCARENCIA'] },
];

// Helper genérico para resolução das chaves
function resolveHeaderKey(header: string, rules: HeaderRule[]): string {
    if (!header) return '';
    const cleanHeader = normalize(header);

    // Remove os espaços do cabeçalho limpo para ignorar o agrupamento feito pela remoção do _
    const groupedHeader = cleanHeader.replace(/\s+/g, '');

    // 1º Passo: Busca por correspondência exata para evitar colisão
    for (const rule of rules) {
        if (rule.matchExact) {
            const match = rule.matchExact.some(exact => exact.replace(/\s+/g, '') === groupedHeader);
            if (match) return rule.key;
        }
    }

    // 2º Passo: Busca por palavras-chave (Fallback seguro)
    for (const rule of rules) {
        if (rule.keywords) {
            const hasKeyword = rule.keywords.some(kw => groupedHeader.includes(kw.replace(/\s+/g, '')));
            if (hasKeyword) {
                if (rule.require) {
                    const hasRequired = rule.require.some(req => groupedHeader.includes(req.replace(/\s+/g, '')));
                    if (hasRequired) return rule.key;
                } else {
                    return rule.key;
                }
            }
        }
    }

    return cleanHeader;
}

export async function processCsvStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processCsvStream');
    let batch: PepRow[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => resolveHeaderKey(header, PEP_HEADER_RULES),
            }));

        for await (const data of parser) {
            batch.push({
                ...data,
                ...metadata,
            });

            if (batch.length >= BATCH_SIZE) {
                await persistData(batch);
                totalProcessed += batch.length;
                batch = [];
            }
        }

        if (batch.length > 0) {
            await persistData(batch);
            totalProcessed += batch.length;
        }

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento de PEP concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream de PEP');
        throw error;
    }
}

export async function extractAndProcessZips(moduleName: string): Promise<number> {
    const log = logger.forMethod('extractAndProcessZips');
    const dirPath = path.join(ZIP_FILES_DIRECTORY, moduleName);

    if (!fs.existsSync(dirPath)) {
        const error = new Error(`Diretório não encontrado: ${dirPath}`);
        log.error({ data: error }, 'Falha na leitura do diretório');
        throw error;
    }

    const files = fs.readdirSync(dirPath).filter(file => file.endsWith('.zip'));
    let totalRowsAllFiles = 0;

    for (const file of files) {
        const zipPath = path.join(dirPath, file);
        const metadata = extractMetadataFromFileName(file);
        log.info({ data: { zipPath, metadata } }, 'Iniciando extração do arquivo ZIP');

        const zipRowsCount = await new Promise<number>((resolve, reject) => {
            let fileTotal = 0;
            let pendingEntries = 0;
            let isOpen = true;

            yauzl.open(zipPath, { lazyEntries: true }, (err, zipfile) => {
                if (err || !zipfile) {
                    log.error({ data: err }, 'Erro ao abrir arquivo ZIP');
                    return reject(err);
                }

                zipfile.readEntry();

                zipfile.on('entry', (entry: yauzl.Entry) => {
                    if (/\/$/.test(entry.fileName) || !entry.fileName.toLowerCase().endsWith('.csv')) {
                        zipfile.readEntry();
                        return;
                    }

                    pendingEntries++;
                    zipfile.openReadStream(entry, async (errStream, readStream) => {
                        if (errStream || !readStream) {
                            log.error({ data: errStream }, 'Erro ao ler arquivo dentro do ZIP');
                            return reject(errStream);
                        }

                        try {
                            const upperName = entry.fileName.toUpperCase();
                            let rowsCount = 0;

                            if (upperName.includes('PEP')) {
                                rowsCount = await processCsvStream(readStream, metadata);
                            } else {
                                log.info({ data: { fileName: entry.fileName } }, 'Arquivo CSV ignorado');
                                readStream.resume();
                            }

                            fileTotal += rowsCount;
                        } catch (streamErr) {
                            reject(streamErr);
                        } finally {
                            pendingEntries--;
                            if (!isOpen && pendingEntries === 0) resolve(fileTotal);
                            zipfile.readEntry();
                        }
                    });
                });

                zipfile.on('end', () => {
                    isOpen = false;
                    if (pendingEntries === 0) resolve(fileTotal);
                });

                zipfile.on('error', (zipErr) => {
                    log.error({ data: zipErr }, 'Erro durante leitura do ZIP');
                    reject(zipErr);
                });
            });
        });

        totalRowsAllFiles += zipRowsCount;
    }

    return totalRowsAllFiles;
}

export async function runPepIngestion() {
    const log = logger.forMethod('runPepIngestion');
    const moduleName = 'pep';

    try {
        log.info({ data: { moduleName } }, 'Iniciando ingestão do módulo');
        const totalRows = await extractAndProcessZips(moduleName);
        log.info({ data: { totalRows } }, 'Ingestão concluída com sucesso');
        return;
    } catch (error) {
        log.error({ data: error }, `Erro fatal ao processar módulo ${moduleName}`);
        throw error;
    }
}