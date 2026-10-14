import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import csvParser from 'csv-parser';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import { CartaoPagamentoCpgf } from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY } from '../shared.js';

const logger = new_logger(import.meta.url);

type CartaoPagamentoCpgfRow = typeof CartaoPagamentoCpgf.$inferInsert;

async function persistData(data: CartaoPagamentoCpgfRow[]) {
    const log = logger.forMethod('persistData');
    try {
        await bronze_db.insert(CartaoPagamentoCpgf).values(data);
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência`);
        throw error;
    }
}

export async function processCsvStream(stream: NodeJS.ReadableStream): Promise<number> {
    const log = logger.forMethod('processCsvStream');
    const BATCH_SIZE = 2000;
    let batch: CartaoPagamentoCpgfRow[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase();
                
                if (h.includes('ÓRGÃO SUPERIOR') || h.includes('ORGAO SUPERIOR')) {
                    if (h.includes('CÓDIGO') || h.includes('CODIGO')) return 'codigoOrgaoSuperior';
                    if (h.includes('NOME')) return 'nomeOrgaoSuperior';
                }
                
                if (h.includes('ÓRGÃO SUBORDINADO') || h.includes('ORGAO SUBORDINADO')) {
                    if (h.includes('CÓDIGO') || h.includes('CODIGO')) return 'codigoOrgaoSubordinado';
                    if (h.includes('NOME')) return 'nomeOrgaoSubordinado';
                }
                
                if (h.includes('UNIDADE GESTORA')) {
                    if (h.includes('CÓDIGO') || h.includes('CODIGO')) return 'codigoUnidadeGestora';
                    if (h.includes('NOME')) return 'nomeUnidadeGestora';
                }
                
                if (h.includes('ANO EXTRATO')) return 'anoExtrato';
                if (h.includes('MÊS EXTRATO') || h.includes('MES EXTRATO')) return 'mesExtrato';
                
                if (h.includes('CPF PORTADOR')) return 'cpfPortador';
                if (h.includes('NOME PORTADOR')) return 'nomePortador';
                
                if (h.includes('DATA TRANSAÇÃO') || h.includes('DATA TRANSACAO')) return 'dataTransacao';
                if (h.includes('VALOR TRANSAÇÃO') || h.includes('VALOR TRANSACAO')) return 'valorTransacao';
                if (h === 'TRANSAÇÃO' || h === 'TRANSACAO') return 'transacao';
                
                if (h.includes('NOME FAVORECIDO')) return 'nomeFavorecido';
                if (h.includes('CNPJ') || h.includes('CPF DO FAVORECIDO') || h.includes('CPF FAVORECIDO')) return 'documentoFavorecido';
                
                return header.trim();
            },
        }));

        for await (const data of parser) {
            batch.push(data);

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

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento do CSV concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream do CSV');
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
        log.info({ data: { zipPath } }, 'Iniciando extração do arquivo ZIP');

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
                            const rowsCount = await processCsvStream(readStream);
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

export async function runCartaoPagamentoCpgfIngestion() {
    const log = logger.forMethod('runCartaoPagamentoCpgfIngestion');
    const moduleName = 'cartao-pagamento';

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