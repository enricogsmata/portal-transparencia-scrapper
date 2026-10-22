import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import iconv from 'iconv-lite';
import csvParser from 'csv-parser';
import { sql } from 'drizzle-orm';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import { ContratosHistorico } from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY, extractMetadataFromFileName, ReferenceMetadata, normalize } from '../shared.js';

const logger = new_logger(import.meta.url);
const BATCH_SIZE = 2000;

type ContratosHistoricoRow = typeof ContratosHistorico.$inferInsert;

async function persistData(data: ContratosHistoricoRow[]) {
    const log = logger.forMethod('persistData');
    try {
        const columns = Object.keys(data[0] || {}) as (keyof typeof ContratosHistorico._.columns)[];
        const updateSet: Record<string, any> = {};
        for (const col of columns) {
            if (col !== 'id') {
                updateSet[col] = sql`values(${ContratosHistorico[col]})`;
            }
        }

        await bronze_db.insert(ContratosHistorico).values(data).onDuplicateKeyUpdate({
            set: updateSet,
        });
    } catch (error: any) {
        log.fatal({ data: error }, 'Falha na persistência de ContratosHistorico (Histórico)');
        throw error;
    }
}

interface HeaderRule {
    key: string;
    keywords?: string[];
    matchExact?: string[];
    require?: string[];
}

const CONTRATOS_HEADER_RULES: HeaderRule[] = [
    { key: 'codigoOrgaoSuperior', matchExact: ['CODIGO ORGAO SUPERIOR'] },
    { key: 'nomeOrgaoSuperior', matchExact: ['NOME ORGAO SUPERIOR'] },
    { key: 'codigoOrgao', matchExact: ['CODIGO ORGAO'] },
    { key: 'nomeOrgao', matchExact: ['NOME ORGAO'] },
    { key: 'codigoUnidadeGestora', matchExact: ['CODIGO UNIDADE GESTORA'] },
    { key: 'nomeUnidadeGestora', matchExact: ['NOME UNIDADE GESTORA'] },
    { key: 'numeroContrato', matchExact: ['NUMERO CONTRATO'] },
    { key: 'numeroProcesso', matchExact: ['NUMERO PROCESSO'] },
    { key: 'numeroLicitacao', matchExact: ['NUMERO LICITACAO'] },
    { key: 'modalidadeLicitacao', matchExact: ['MODALIDADE LICITACAO'] },
    { key: 'situacaoContrato', matchExact: ['SITUACAO CONTRATO'] },
    { key: 'cpfCnpj', matchExact: ['CPF OU CNPJ', 'CPF CNPJ', 'CPF/CNPJ'] },
    { key: 'nomeEmpresa', matchExact: ['NOME EMPRESA'] },
    { key: 'objeto', matchExact: ['OBJETO'] },
    { key: 'inicioVigencia', matchExact: ['INICIO VIGENCIA'] },
    { key: 'fimVigencia', matchExact: ['FIM VIGENCIA'] },
    { key: 'valorContrato', matchExact: ['VALOR CONTRATO'] },
    { key: 'fundamentoLegal', matchExact: ['FUNDAMENTO LEGAL'] },
    { key: 'dataPublicacaoDou', matchExact: ['DATA PUBLICACAO DOU'] },
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

export async function processCsvStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processCsvStream');
    let batch: ContratosHistoricoRow[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => resolveHeaderKey(header, CONTRATOS_HEADER_RULES),
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

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento de ContratosHistorico (Histórico) concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream de ContratosHistorico (Histórico)');
        throw error;
    }
}

export async function extractAndProcessZips(moduleName: string): Promise<number> {
    const log = logger.forMethod('extractAndProcessZips');
    let dirPath = path.join(ZIP_FILES_DIRECTORY, 'historico-', moduleName);

    if (!fs.existsSync(dirPath)) {
        const fallback = path.join(ZIP_FILES_DIRECTORY, 'contratos');
        if (fs.existsSync(fallback)) {
            dirPath = fallback;
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

                            if (upperFileName.includes('CONTRATO') || upperFileName.includes('CONTRATOS')) {
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

export async function runContratosHistoricoIngestion(moduleName: string = 'contratos-historico') {
    const log = logger.forMethod('runContratosHistoricoHistoricoIngestion');

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