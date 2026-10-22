import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import iconv from 'iconv-lite';
import csvParser from 'csv-parser';
import { sql } from 'drizzle-orm';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import { LicitacoesHistorico } from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY, extractMetadataFromFileName, ReferenceMetadata, normalize } from '../shared.js';

const logger = new_logger(import.meta.url);
const BATCH_SIZE = 2000;

type LicitacoesHistoricoRow = typeof LicitacoesHistorico.$inferInsert;

async function persistData(data: LicitacoesHistoricoRow[]) {
    const log = logger.forMethod('persistData');
    try {
        const columns = Object.keys(data[0] || {}) as (keyof typeof LicitacoesHistorico._.columns)[];
        const updateSet: Record<string, any> = {};
        for (const col of columns) {
            if (col !== 'id') {
                updateSet[col] = sql`values(${LicitacoesHistorico[col]})`;
            }
        }

        await bronze_db.insert(LicitacoesHistorico).values(data).onDuplicateKeyUpdate({
            set: updateSet,
        });
    } catch (error: any) {
        log.fatal({ data: error }, 'Falha na persistência de Licitações (Histórico)');
        throw error;
    }
}

interface HeaderRule {
    key: string;
    keywords?: string[];
    matchExact?: string[];
    require?: string[];
}

const LICITACOES_HEADER_RULES: HeaderRule[] = [
    { key: 'codigoOrgaoSuperior', matchExact: ['CODIGO ORGAO SUPERIOR'] },
    { key: 'nomeOrgaoSuperior', matchExact: ['NOME ORGAO SUPERIOR'] },
    { key: 'codigoOrgao', matchExact: ['CODIGO ORGAO'] },
    { key: 'nomeOrgao', matchExact: ['NOME ORGAO'] },
    { key: 'codigoUnidadeGestora', matchExact: ['CODIGO UNIDADE GESTORA'] },
    { key: 'nomeUnidadeGestora', matchExact: ['NOME UNIDADE GESTORA'] },
    { key: 'numeroLicitacao', matchExact: ['NUMERO LICITACAO'] },
    { key: 'numeroProcesso', matchExact: ['NUMERO PROCESSO'] },
    { key: 'modalidadeLicitacao', matchExact: ['MODALIDADE LICITACAO'] },
    { key: 'situacaoLicitacao', matchExact: ['SITUACAO LICITACAO'] },
    { key: 'objeto', matchExact: ['OBJETO'] },
    { key: 'dataHoraAbertura', matchExact: ['DATA E HORA DE ABERTURA', 'DATA HORA ABERTURA', 'DATA ABERTURA'] },
    { key: 'localAbertura', matchExact: ['LOCAL DE ABERTURA', 'LOCAL ABERTURA'] },
    { key: 'responsavelLicitacao', matchExact: ['RESPONSAVEL PELA LICITACAO', 'RESPONSAVEL LICITACAO'] },
    { key: 'numeroItem', matchExact: ['NUMERO ITEM'] },
    { key: 'descricaoItem', matchExact: ['DESCRICAO ITEM'] },
    { key: 'quantidadeItem', matchExact: ['QUANTIDADE ITEM'] },
    { key: 'valorItem', matchExact: ['VALOR ITEM'] },
    { key: 'cnpjVencedor', matchExact: ['CNPJ VENCEDOR'] },
    { key: 'nomeVencedor', matchExact: ['NOME VENCEDOR'] },
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
    let batch: LicitacoesHistoricoRow[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => resolveHeaderKey(header, LICITACOES_HEADER_RULES),
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

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento de Licitações (Histórico) concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream de Licitações (Histórico)');
        throw error;
    }
}

export async function extractAndProcessZips(moduleName: string): Promise<number> {
    const log = logger.forMethod('extractAndProcessZips');
    let dirPath = path.join(ZIP_FILES_DIRECTORY, 'historico-', moduleName);

    if (!fs.existsSync(dirPath)) {
        const fallback = path.join(ZIP_FILES_DIRECTORY, 'licitacoes');
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

                            if (upperFileName.includes('LICITACAO') || upperFileName.includes('LICITACOES')) {
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

export async function runLicitacoesHistoricoIngestion(moduleName: string = 'licitacoes-historico') {
    const log = logger.forMethod('runLicitacoesHistoricoHistoricoIngestion');

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