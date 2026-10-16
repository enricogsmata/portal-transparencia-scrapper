import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import iconv from 'iconv-lite';
import csvParser from 'csv-parser';
import { sql } from 'drizzle-orm';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import { Ceaf } from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY, extractMetadataFromFileName, ReferenceMetadata, normalize } from '../shared.js';

const logger = new_logger(import.meta.url);
const BATCH_SIZE = 2000;

type CeafRow = typeof Ceaf.$inferInsert;

async function persistData(data: CeafRow[]) {
    const log = logger.forMethod('persistData');
    try {
        await bronze_db.insert(Ceaf).values(data).onDuplicateKeyUpdate({
            set: {
                cadastro: sql`values(${Ceaf.cadastro})`,
                codigoSancao: sql`values(${Ceaf.codigoSancao})`,
                tipoPessoa: sql`values(${Ceaf.tipoPessoa})`,
                cpfCnpjSancionado: sql`values(${Ceaf.cpfCnpjSancionado})`,
                nomeSancionado: sql`values(${Ceaf.nomeSancionado})`,
                categoriaSancao: sql`values(${Ceaf.categoriaSancao})`,
                numeroDocumento: sql`values(${Ceaf.numeroDocumento})`,
                numeroProcesso: sql`values(${Ceaf.numeroProcesso})`,
                dataInicioSancao: sql`values(${Ceaf.dataInicioSancao})`,
                dataFinalSancao: sql`values(${Ceaf.dataFinalSancao})`,
                dataPublicacao: sql`values(${Ceaf.dataPublicacao})`,
                publicacao: sql`values(${Ceaf.publicacao})`,
                detalhamentoMeioPublicacao: sql`values(${Ceaf.detalhamentoMeioPublicacao})`,
                dataTransitoJulgado: sql`values(${Ceaf.dataTransitoJulgado})`,
                abrangenciaSancao: sql`values(${Ceaf.abrangenciaSancao})`,
                cargoEfetivo: sql`values(${Ceaf.cargoEfetivo})`,
                funcaoCargoConfianca: sql`values(${Ceaf.funcaoCargoConfianca})`,
                orgaoLotacao: sql`values(${Ceaf.orgaoLotacao})`,
                orgaoSancionador: sql`values(${Ceaf.orgaoSancionador})`,
                ufOrgaoSancionador: sql`values(${Ceaf.ufOrgaoSancionador})`,
                esferaOrgaoSancionador: sql`values(${Ceaf.esferaOrgaoSancionador})`,
                fundamentacaoLegal: sql`values(${Ceaf.fundamentacaoLegal})`,
                dataOrigemInformacao: sql`values(${Ceaf.dataOrigemInformacao})`,
                origemInformacoes: sql`values(${Ceaf.origemInformacoes})`,
                observacoes: sql`values(${Ceaf.observacoes})`,
                exercicioReferencia: sql`values(${Ceaf.exercicioReferencia})`,
                mesReferencia: sql`values(${Ceaf.mesReferencia})`,
                diaReferencia: sql`values(${Ceaf.diaReferencia})`,
                dadosAdicionais: sql`values(${Ceaf.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de CEAF`);
        throw error;
    }
}

interface HeaderRule {
    key: string;
    keywords?: string[];
    matchExact?: string[];
    require?: string[];
}

const CEAF_HEADER_RULES: HeaderRule[] = [
    { key: 'cadastro', matchExact: ['CADASTRO'] },
    { key: 'codigoSancao', keywords: ['SANCAO'], require: ['CODIGO'] },
    { key: 'tipoPessoa', keywords: ['TIPO DE PESSOA', 'TIPO PESSOA'] },
    { key: 'cpfCnpjSancionado', keywords: ['SANCIONADO'], require: ['CPF', 'CNPJ'] },
    { key: 'nomeSancionado', keywords: ['NOME DO SANCIONADO', 'NOME SANCIONADO'] },
    { key: 'categoriaSancao', keywords: ['CATEGORIA DA SANCAO', 'CATEGORIA SANCAO'] },
    { key: 'numeroDocumento', keywords: ['DOCUMENTO'], require: ['NUMERO'] },
    { key: 'numeroProcesso', keywords: ['PROCESSO', 'NUMERO DO PROCESSO'] },

    // Datas
    { key: 'dataInicioSancao', keywords: ['DATA INICIO SANCAO'], require: ['DATA', 'INICIO'] },
    { key: 'dataFinalSancao', keywords: ['DATA FINAL SANCAO'], require: ['DATA', 'FINAL'] },
    { key: 'dataPublicacao', keywords: ['PUBLICACAO'], require: ['DATA'] },
    { key: 'publicacao', matchExact: ['PUBLICACAO'] },
    { key: 'detalhamentoMeioPublicacao', keywords: ['DETALHAMENTO'] },
    { key: 'dataTransitoJulgado', keywords: ['TRANSITO EM JULGADO', 'TRANSITO JULGADO'] },

    { key: 'abrangenciaSancao', keywords: ['ABRANGENCIA', 'ABRAGENCIA'] },

    // Cargo e Lotação do Servidor Expulso
    { key: 'cargoEfetivo', keywords: ['CARGO EFETIVO'] },
    { key: 'funcaoCargoConfianca', keywords: ['FUNCAO OU CARGO DE CONFIANCA', 'CARGO DE CONFIANCA', 'FUNCAO CARGO CONFIANCA'] },
    { key: 'orgaoLotacao', keywords: ['ORGAO DE LOTACAO', 'ORGAO LOTACAO', 'LOTACAO'] },

    // Órgão Sancionador
    { key: 'ufOrgaoSancionador', keywords: ['ORGAO SANCIONADOR'], require: ['UF'] },
    { key: 'esferaOrgaoSancionador', keywords: ['ORGAO SANCIONADOR'], require: ['ESFERA'] },
    { key: 'orgaoSancionador', keywords: ['ORGAO SANCIONADOR'] },

    { key: 'fundamentacaoLegal', keywords: ['FUNDAMENTACAO LEGAL'] },
    { key: 'dataOrigemInformacao', keywords: ['ORIGEM INFORMACAO', 'ORIGEM DA INFORMACAO'], require: ['DATA'] },
    { key: 'origemInformacoes', keywords: ['ORIGEM INFORMACAO', 'ORIGEM INFORMACOES'] },
    { key: 'observacoes', keywords: ['OBSERVACOES', 'OBSERVACAO'] },
];

// Helper genérico para resolução das chaves
function resolveHeaderKey(header: string, rules: HeaderRule[]): string {
    if (!header) return '';
    const cleanHeader = normalize(header);

    for (const rule of rules) {
        if (rule.matchExact && rule.matchExact.includes(cleanHeader)) {
            return rule.key;
        }

        const hasKeyword = rule.keywords && rule.keywords.some(kw => cleanHeader.includes(kw));

        if (hasKeyword) {
            if (rule.require) {
                const hasRequired = rule.require.some(req => cleanHeader.includes(req));
                if (hasRequired) return rule.key;
            } else {
                return rule.key;
            }
        }
    }

    return cleanHeader;
}

export async function processCsvStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processCsvStream');
    let batch: CeafRow[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => resolveHeaderKey(header, CEAF_HEADER_RULES),
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

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento do CEAF concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream do CEAF');
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

                            if (upperName.includes('CEAF') || upperName.includes('EXPULSOES') || upperName.includes('EXPULSÕES')) {
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

export async function runCeafIngestion() {
    const log = logger.forMethod('runCeafIngestion');
    const moduleName = 'ceaf';

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