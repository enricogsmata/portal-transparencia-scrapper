import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import csvParser from 'csv-parser';
import iconv from 'iconv-lite';
import { sql } from 'drizzle-orm';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import { Cnep } from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY, extractMetadataFromFileName, ReferenceMetadata, normalize } from '../shared.js';

const logger = new_logger(import.meta.url);
const BATCH_SIZE = 2000;

type CnepRow = typeof Cnep.$inferInsert;

async function persistData(data: CnepRow[]) {
    const log = logger.forMethod('persistData');
    try {
        await bronze_db.insert(Cnep).values(data).onDuplicateKeyUpdate({
            set: {
                cadastro: sql`values(${Cnep.cadastro})`,
                codigoSancao: sql`values(${Cnep.codigoSancao})`,
                tipoPessoa: sql`values(${Cnep.tipoPessoa})`,
                cpfCnpjSancionado: sql`values(${Cnep.cpfCnpjSancionado})`,
                nomeSancionado: sql`values(${Cnep.nomeSancionado})`,
                nomeInformadoOrgaoSancionador: sql`values(${Cnep.nomeInformadoOrgaoSancionador})`,
                razaoSocialCadastroReceita: sql`values(${Cnep.razaoSocialCadastroReceita})`,
                nomeFantasiaCadastroReceita: sql`values(${Cnep.nomeFantasiaCadastroReceita})`,
                numeroProcesso: sql`values(${Cnep.numeroProcesso})`,
                categoriaSancao: sql`values(${Cnep.categoriaSancao})`,
                valorMulta: sql`values(${Cnep.valorMulta})`,
                dataInicioSancao: sql`values(${Cnep.dataInicioSancao})`,
                dataFinalSancao: sql`values(${Cnep.dataFinalSancao})`,
                dataPublicacao: sql`values(${Cnep.dataPublicacao})`,
                publicacao: sql`values(${Cnep.publicacao})`,
                detalhamentoMeioPublicacao: sql`values(${Cnep.detalhamentoMeioPublicacao})`,
                dataTransitoJulgado: sql`values(${Cnep.dataTransitoJulgado})`,
                abrangenciaSancao: sql`values(${Cnep.abrangenciaSancao})`,
                orgaoSancionador: sql`values(${Cnep.orgaoSancionador})`,
                ufOrgaoSancionador: sql`values(${Cnep.ufOrgaoSancionador})`,
                esferaOrgaoSancionador: sql`values(${Cnep.esferaOrgaoSancionador})`,
                fundamentacaoLegal: sql`values(${Cnep.fundamentacaoLegal})`,
                dataOrigemInformacao: sql`values(${Cnep.dataOrigemInformacao})`,
                origemInformacoes: sql`values(${Cnep.origemInformacoes})`,
                observacoes: sql`values(${Cnep.observacoes})`,
                exercicioReferencia: sql`values(${Cnep.exercicioReferencia})`,
                mesReferencia: sql`values(${Cnep.mesReferencia})`,
                diaReferencia: sql`values(${Cnep.diaReferencia})`,
                dadosAdicionais: sql`values(${Cnep.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de CNEP`);
        throw error;
    }
}

interface HeaderRule {
    key: string;
    keywords?: string[];
    matchExact?: string[];
    require?: string[];
}

const CNEP_HEADER_RULES: HeaderRule[] = [
    { key: 'cadastro', matchExact: ['CADASTRO'] },
    { key: 'codigoSancao', keywords: ['SANCAO'], require: ['CODIGO'] },
    { key: 'tipoPessoa', keywords: ['TIPO DE PESSOA', 'TIPO PESSOA'] },
    { key: 'cpfCnpjSancionado', keywords: ['SANCIONADO'], require: ['CPF', 'CNPJ'] },
    { key: 'nomeSancionado', keywords: ['NOME DO SANCIONADO', 'NOME SANCIONADO'] },
    { key: 'nomeInformadoOrgaoSancionador', keywords: ['INFORMADO PELO ORGAO'] },
    { key: 'razaoSocialCadastroReceita', keywords: ['RAZAO SOCIAL'] },
    { key: 'nomeFantasiaCadastroReceita', keywords: ['NOME FANTASIA'] },
    { key: 'numeroProcesso', keywords: ['PROCESSO', 'NUMERO DO PROCESSO'] },
    { key: 'categoriaSancao', keywords: ['CATEGORIA DA SANCAO', 'CATEGORIA SANCAO'] },
    { key: 'valorMulta', keywords: ['VALOR DA MULTA', 'VALOR MULTA'] },

    // Datas
    { key: 'dataInicioSancao', keywords: ['INICIO SANCAO'], require: ['DATA', 'INICIO'] },
    { key: 'dataFinalSancao', keywords: ['FINAL SANCAO'], require: ['DATA', 'FINAL'] },
    { key: 'dataPublicacao', keywords: ['PUBLICACAO'], require: ['DATA'] },
    { key: 'publicacao', matchExact: ['PUBLICACAO'] },
    { key: 'detalhamentoMeioPublicacao', keywords: ['DETALHAMENTO'] },
    { key: 'dataTransitoJulgado', keywords: ['TRANSITO EM JULGADO', 'TRANSITO JULGADO'] },

    { key: 'abrangenciaSancao', keywords: ['ABRANGENCIA', 'ABRAGENCIA'] },

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
    let batch: CnepRow[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => resolveHeaderKey(header, CNEP_HEADER_RULES),
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

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento do CNEP concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream do CNEP');
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

                            if (upperName.includes('CNEP')) {
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

export async function runCnepIngestion() {
    const log = logger.forMethod('runCnepIngestion');
    const moduleName = 'cnep';

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