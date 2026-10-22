import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import iconv from 'iconv-lite';
import csvParser from 'csv-parser';
import { sql } from 'drizzle-orm';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import { DiariasEPassagens } from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY, extractMetadataFromFileName, ReferenceMetadata, normalize } from '../shared.js';

const logger = new_logger(import.meta.url);
const BATCH_SIZE = 2000;

type DiariasEPassagensRow = typeof DiariasEPassagens.$inferInsert;

async function persistData(data: DiariasEPassagensRow[]) {
    const log = logger.forMethod('persistData');
    try {
        const columns = Object.keys(data[0] || {}) as (keyof typeof DiariasEPassagens._.columns)[];
        const updateSet: Record<string, any> = {};
        for (const col of columns) {
            if (col !== 'id') {
                updateSet[col] = sql`values(${DiariasEPassagens[col]})`;
            }
        }

        await bronze_db.insert(DiariasEPassagens).values(data).onDuplicateKeyUpdate({
            set: updateSet,
        });
    } catch (error: any) {
        log.fatal({ data: error }, 'Falha na persistência de DiariasEPassagens');
        throw error;
    }
}

interface HeaderRule {
    key: string;
    keywords?: string[];
    matchExact?: string[];
    require?: string[];
}

const DIARIAS_PASSAGENS_HEADER_RULES: HeaderRule[] = [
    { key: 'codigoOrgaoSuperior', matchExact: ['CODIGO ORGAO SUPERIOR'] },
    { key: 'nomeOrgaoSuperior', matchExact: ['NOME ORGAO SUPERIOR'] },
    { key: 'codigoOrgao', matchExact: ['CODIGO ORGAO'] },
    { key: 'nomeOrgao', matchExact: ['NOME ORGAO'] },
    { key: 'codigoUnidadeGestora', matchExact: ['CODIGO UNIDADE GESTORA'] },
    { key: 'nomeUnidadeGestora', matchExact: ['NOME UNIDADE GESTORA'] },
    { key: 'nomeServidor', matchExact: ['NOME SERVIDOR'] },
    { key: 'cargo', matchExact: ['CARGO'] },
    { key: 'dataInicioViagem', matchExact: ['DATA INICIO VIAGEM'] },
    { key: 'dataTerminoViagem', matchExact: ['DATA TERMINO VIAGEM'] },
    { key: 'motivo', matchExact: ['MOTIVO'] },
    { key: 'valorTotal', matchExact: ['VALOR TOTAL'] },
    { key: 'inicioTrecho', matchExact: ['INICIO TRECHO'] },
    { key: 'terminoTrecho', matchExact: ['TERMINO TRECHO'] },
    { key: 'municipioOrigem', matchExact: ['MUNICIPIO ORIGEM'] },
    { key: 'ufOrigem', matchExact: ['UF ORIGEM'] },
    { key: 'municipioDestino', matchExact: ['MUNICIPIO DESTINO'] },
    { key: 'ufDestino', matchExact: ['UF DESTINO'] },
    { key: 'numeroDiarias', matchExact: ['NUMERO DIARIAS'] },
    { key: 'valorDiarias', matchExact: ['VALOR DIARIAS'] },
    { key: 'meioTransporte', matchExact: ['MEIO DE TRANSPORTE'] },
    { key: 'categoriaPassagem', matchExact: ['CATEGORIA PASSAGEM'] },
    { key: 'valorPassagem', matchExact: ['VALOR PASSAGEM'] },
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
    let batch: DiariasEPassagensRow[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => resolveHeaderKey(header, DIARIAS_PASSAGENS_HEADER_RULES),
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

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento de Diárias e Passagens concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream de Diárias e Passagens');
        throw error;
    }
}

export async function extractAndProcessZips(moduleName: string): Promise<number> {
    const log = logger.forMethod('extractAndProcessZips');
    let dirPath = path.join(ZIP_FILES_DIRECTORY, moduleName.startsWith('historico-') ? moduleName : `historico-${moduleName}`);

    if (!fs.existsSync(dirPath)) {
        const fallback = path.join(ZIP_FILES_DIRECTORY, moduleName);
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

                            if (upperFileName.includes('DIARIAS') || upperFileName.includes('PASSAGENS')) {
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

export async function runDiariasEPassagensIngestion(moduleName: string = 'historico-diarias-e-passagens') {
    const log = logger.forMethod('runDiariasEPassagensIngestion');

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