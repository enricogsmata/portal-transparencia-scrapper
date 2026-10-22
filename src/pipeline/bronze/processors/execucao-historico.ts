import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import iconv from 'iconv-lite';
import csvParser from 'csv-parser';
import { sql } from 'drizzle-orm';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import { ExecucaoPorNatureza, ExecucaoPorPrograma } from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY, extractMetadataFromFileName, ReferenceMetadata, normalize } from '../shared.js';

const logger = new_logger(import.meta.url);
const BATCH_SIZE = 2000;

async function persistData(schema: any, data: any[], logPrefix: string) {
    const log = logger.forMethod(`persistData:${logPrefix}`);
    try {
        const columns = Object.keys(data[0] || {});
        const updateSet: Record<string, any> = {};
        for (const col of columns) {
            if (col !== 'id') {
                updateSet[col] = sql`values(${schema[col]})`;
            }
        }

        await bronze_db.insert(schema).values(data).onDuplicateKeyUpdate({
            set: updateSet,
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de ${logPrefix}`);
        throw error;
    }
}

interface HeaderRule {
    key: string;
    keywords?: string[];
    matchExact?: string[];
    require?: string[];
}

const EXECUCAO_NATUREZA_HEADER_RULES: HeaderRule[] = [
    { key: 'codigoOrgaoSuperior', matchExact: ['CODIGO ORGAO SUPERIOR'] },
    { key: 'nomeOrgaoSuperior', matchExact: ['NOME ORGAO SUPERIOR'] },
    { key: 'codigoOrgao', matchExact: ['CODIGO ORGAO', 'CODIGO ORGAO'] },
    { key: 'nomeOrgao', matchExact: ['NOME ORGAO'] },
    { key: 'codigoUnidadeOrcamentaria', matchExact: ['CODIGO UNIDADE ORCAMENTARIA'] },
    { key: 'nomeUnidadeOrcamentaria', matchExact: ['NOME UNIDADE ORCAMENTARIA'] },
    { key: 'mesExecucao', matchExact: ['MES EXECUCAO', 'MES DE EXECUCAO'] },
    { key: 'anoExecucao', matchExact: ['ANO EXECUCAO', 'ANO DA EXECUCAO'] },
    { key: 'codigoNaturezaDespesa', matchExact: ['CODIGO DA NATUREZA DA DESPESA', 'CODIGO NATUREZA DESPESA'] },
    { key: 'descricaoNaturezaDespesa', matchExact: ['DESCRICAO DA NATUREZA DA DESPESA', 'DESCRICAO NATUREZA DESPESA'] },
    { key: 'valorLiquidado', matchExact: ['VALOR LIQUIDADO'] },
    { key: 'valorPago', matchExact: ['VALOR PAGO'] },
];

const EXECUCAO_PROGRAMA_HEADER_RULES: HeaderRule[] = [
    { key: 'codigoOrgaoSuperior', matchExact: ['CODIGO ORGAO SUPERIOR'] },
    { key: 'nomeOrgaoSuperior', matchExact: ['NOME ORGAO SUPERIOR'] },
    { key: 'codigoOrgao', matchExact: ['CODIGO ORGAO', 'CODIGO ORGAO'] },
    { key: 'nomeOrgao', matchExact: ['NOME ORGAO'] },
    { key: 'codigoUnidadeOrcamentaria', matchExact: ['CODIGO UNIDADE ORCAMENTARIA'] },
    { key: 'nomeUnidadeOrcamentaria', matchExact: ['NOME UNIDADE ORCAMENTARIA'] },
    { key: 'mesExecucao', matchExact: ['MES DE EXECUCAO', 'MES EXECUCAO'] },
    { key: 'anoExecucao', matchExact: ['ANO DA EXECUCAO', 'ANO EXECUCAO'] },
    { key: 'codigoPrograma', matchExact: ['CODIGO DO PROGRAMA', 'CODIGO PROGRAMA'] },
    { key: 'nomePrograma', matchExact: ['NOME DO PROGRAMA', 'NOME PROGRAMA'] },
    { key: 'valorAtualizado', matchExact: ['VALOR ATUALIZADO'] },
    { key: 'valorLiquidado', matchExact: ['VALOR LIQUIDADO'] },
    { key: 'valorPago', matchExact: ['VALOR PAGO'] },
];

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

async function processGenericStream(
    stream: NodeJS.ReadableStream,
    schema: any,
    rules: HeaderRule[],
    logPrefix: string,
    metadata: ReferenceMetadata
): Promise<number> {
    const log = logger.forMethod(`processGenericStream:${logPrefix}`);
    let batch: any[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => resolveHeaderKey(header, rules),
            }));

        for await (const data of parser) {
            batch.push({
                ...data,
                ...metadata,
            });

            if (batch.length >= BATCH_SIZE) {
                await persistData(schema, batch, logPrefix);
                totalProcessed += batch.length;
                batch = [];
            }
        }

        if (batch.length > 0) {
            await persistData(schema, batch, logPrefix);
            totalProcessed += batch.length;
        }

        log.info({ data: { rowsProcessed: totalProcessed } }, `Processamento de ${logPrefix} concluído`);
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, `Erro ao processar stream de ${logPrefix}`);
        throw error;
    }
}

export async function extractAndProcessZips(moduleName: string): Promise<number> {
    const log = logger.forMethod('extractAndProcessZips');
    let dirPath = path.join(ZIP_FILES_DIRECTORY, 'historico-', moduleName);

    if (!fs.existsSync(dirPath)) {
        const fallbacks = [
            path.join(ZIP_FILES_DIRECTORY, 'execucao-historico'),
            path.join(ZIP_FILES_DIRECTORY, 'execucao-despesa-historico'),
            path.join(ZIP_FILES_DIRECTORY, 'despesas-execucao-historico')
        ];
        const matchedFallback = fallbacks.find(fb => fs.existsSync(fb));
        if (matchedFallback) {
            dirPath = matchedFallback;
        } else {
            const error = new Error(`Diretório não encontrado: ${dirPath}`);
            log.error({ data: error }, 'Falha na leitura do diretório');
            throw error;
        }
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
                            const upperFileName = entry.fileName.toUpperCase();
                            let rowsCount = 0;

                            if (upperFileName.includes('NATUREZA')) {
                                rowsCount = await processGenericStream(
                                    readStream,
                                    ExecucaoPorNatureza,
                                    EXECUCAO_NATUREZA_HEADER_RULES,
                                    'ExecucaoPorNatureza',
                                    metadata
                                );
                            } else if (upperFileName.includes('PROGRAMA')) {
                                rowsCount = await processGenericStream(
                                    readStream,
                                    ExecucaoPorPrograma,
                                    EXECUCAO_PROGRAMA_HEADER_RULES,
                                    'ExecucaoPorPrograma',
                                    metadata
                                );
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

export async function runExecucaoHistoricoIngestion(moduleName: string = 'execucao-historico') {
    const log = logger.forMethod('runExecucaoHistoricoIngestion');

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