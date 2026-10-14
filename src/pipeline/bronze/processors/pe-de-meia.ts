import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import csvParser from 'csv-parser';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import { PeDeMeia } from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY } from '../shared.js';

const logger = new_logger(import.meta.url);

type PeDeMeiaRow = typeof PeDeMeia.$inferInsert;

async function persistData(data: PeDeMeiaRow[]) {
    const log = logger.forMethod('persistData');
    try {
        await bronze_db.insert(PeDeMeia).values(data);
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência`);
        throw error;
    }
}

export async function processCsvStream(stream: NodeJS.ReadableStream): Promise<number> {
    const log = logger.forMethod('processCsvStream');
    const BATCH_SIZE = 2000;
    let batch: PeDeMeiaRow[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase();
                if (h.includes('MÊS FOLHA') || h.includes('MES FOLHA')) return 'mesFolha';
                if (h.includes('MÊS REFER') || h.includes('MES REFER')) return 'mesReferencia';
                if (h.includes('DATA DE PAGAMENTO') || h.includes('DATA PAGAMENTO')) return 'dataPagamento';
                if (h === 'UF' || h.includes('UF ')) return 'uf';
                if (h.includes('CÓDIGO DO MUNIC') || h.includes('CODIGO DO MUNIC') || h.includes('CÓDIGO MUNIC') || h.includes('CODIGO MUNIC')) return 'codigoMunicipio';
                if (h.includes('NOME DO MUNIC') || h.includes('NOME MUNIC')) return 'nomeMunicipio';
                if (h.includes('TIPO DE PARCELA') || h.includes('TIPO PARCELA')) return 'tipoParcela';
                if (h.includes('PERÍODO DA PARCELA') || h.includes('PERIODO DA PARCELA') || h.includes('PERÍODO PARCELA')) return 'periodoParcela';
                if (h.includes('ETAPA DE ENSINO') || h.includes('ETAPA ENSINO')) return 'etapaEnsino';
                if (h.includes('CPF DO BENEFICI') || h.includes('CPF BENEFICI')) return 'cpfBeneficiario';
                if (h.includes('NIS DO BENEFICI') || h.includes('NIS BENEFICI')) return 'nisBeneficiario';
                if (h.includes('NOME DO BENEFICI') || h.includes('NOME BENEFICI')) return 'nomeBeneficiario';
                if (h.includes('CPF DO RESPONS') || h.includes('CPF RESPONS')) return 'cpfResponsavel';
                if (h.includes('NIS DO RESPONS') || h.includes('NIS RESPONS')) return 'nisResponsavel';
                if (h.includes('NOME DO RESPONS') || h.includes('NOME RESPONS')) return 'nomeResponsavel';
                if (h.includes('VALOR PARCELA')) return 'valorParcela';
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

export async function runPeDeMeiaIngestion() {
    const log = logger.forMethod('runPeDeMeiaIngestion');
    const moduleName = 'pe-de-meia';

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