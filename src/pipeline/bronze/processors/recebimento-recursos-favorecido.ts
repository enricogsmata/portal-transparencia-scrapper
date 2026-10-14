import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import csvParser from 'csv-parser';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import { RecebimentoRecursosFavorecido } from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY } from '../shared.js';

const logger = new_logger(import.meta.url);

type RecebimentoRecursosFavorecidoRow = typeof RecebimentoRecursosFavorecido.$inferInsert;

async function persistData(data: RecebimentoRecursosFavorecidoRow[]) {
    const log = logger.forMethod('persistData');
    try {
        await bronze_db.insert(RecebimentoRecursosFavorecido).values(data);
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência`);
        throw error;
    }
}

export async function processCsvStream(stream: NodeJS.ReadableStream): Promise<number> {
    const log = logger.forMethod('processCsvStream');
    const BATCH_SIZE = 2000;
    let batch: RecebimentoRecursosFavorecidoRow[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase().trim();
                
                if (h.includes('CÓDIGO FAVORECIDO') || h.includes('CODIGO FAVORECIDO')) return 'codigoFavorecido';
                if (h.includes('NOME FAVORECIDO')) return 'nomeFavorecido';
                if (h.includes('SIGLA UF')) return 'siglaUf';
                if (h.includes('NOME MUNICÍPIO') || h.includes('NOME MUNICIPIO')) return 'nomeMunicipio';
                if (h.includes('CÓDIGO ÓRGÃO SUPERIOR') || h.includes('CODIGO ORGAO SUPERIOR')) return 'codigoOrgaoSuperior';
                if (h.includes('NOME ÓRGÃO SUPERIOR') || h.includes('NOME ORGAO SUPERIOR')) return 'nomeOrgaoSuperior';
                if (h.includes('CÓDIGO ÓRGÃO') || h.includes('CODIGO ORGAO')) return 'codigoOrgao';
                if (h.includes('NOME ÓRGÃO') || h.includes('NOME ORGAO')) return 'nomeOrgao';
                if (h.includes('CÓDIGO UNIDADE GESTORA') || h.includes('CODIGO UNIDADE GESTORA')) return 'codigoUnidadeGestora';
                if (h.includes('NOME UNIDADE GESTORA')) return 'nomeUnidadeGestora';
                if (h.includes('ANO E MÊS DO LANÇAMENTO') || h.includes('ANO E MES DO LANCAMENTO')) return 'anoMesLancamento';
                if (h.includes('VALOR RECEBIDO')) return 'valorRecebido';
                
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

export async function runRecebimentoRecursosFavorecidoIngestion() {
    const log = logger.forMethod('runRecebimentoRecursosFavorecidoIngestion');
    const moduleName = 'recebimento-recursos-favorecido';

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