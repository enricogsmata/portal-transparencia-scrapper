import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import csvParser from 'csv-parser';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import { AuxilioBrasil } from '../../../database/bronze_schema.js';
import { extractMetadataFromFileName, ReferenceMetadata, ZIP_FILES_DIRECTORY } from '../shared.js';
import { sql } from 'drizzle-orm';

const logger = new_logger(import.meta.url);

type AuxilioBrasilRow = typeof AuxilioBrasil.$inferInsert;

async function persistData(data: AuxilioBrasilRow[]) {
    const log = logger.forMethod('persistData');
    try {
        await bronze_db
            .insert(AuxilioBrasil)
            .values(data)
            .onDuplicateKeyUpdate({
                set: {
                    mesCompetencia: sql`values(${AuxilioBrasil.mesCompetencia})`,
                    mesReferencia: sql`values(${AuxilioBrasil.mesReferencia})`,
                    uf: sql`values(${AuxilioBrasil.uf})`,
                    codigoMunicipioSiafi: sql`values(${AuxilioBrasil.codigoMunicipioSiafi})`,
                    nomeMunicipio: sql`values(${AuxilioBrasil.nomeMunicipio})`,
                    cpfFavorecido: sql`values(${AuxilioBrasil.cpfFavorecido})`,
                    nisFavorecido: sql`values(${AuxilioBrasil.nisFavorecido})`,
                    nomeFavorecido: sql`values(${AuxilioBrasil.nomeFavorecido})`,
                    valorParcela: sql`values(${AuxilioBrasil.valorParcela})`,
                    exercicioReferencia: sql`values(${AuxilioBrasil.exercicioReferencia})`,
                },
            });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência`);
        throw error;
    }
}

export async function processCsvStream(
    stream: NodeJS.ReadableStream,
    metadata: ReferenceMetadata
): Promise<number> {
    const log = logger.forMethod('processCsvStream');
    const BATCH_SIZE = 3000;
    let batch: AuxilioBrasilRow[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase();
                if (h.includes('COMPET')) return 'mesCompetencia';
                if (h.includes('REFER')) return 'mesReferencia';
                if (h.includes('UF')) return 'uf';
                if (h.includes('SIAFI')) return 'codigoMunicipioSiafi';
                if (h.includes('NOME MUNIC')) return 'nomeMunicipio';
                if (h.includes('CPF FAVORECIDO')) return 'cpfFavorecido';
                if (h.includes('NIS FAVORECIDO')) return 'nisFavorecido';
                if (h.includes('NOME FAVORECIDO')) return 'nomeFavorecido';
                if (h.includes('VALOR PARCELA')) return 'valorParcela';
                return header.trim();
            },
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
                            const rowsCount = await processCsvStream(readStream, metadata);
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

export async function runAuxilioBrasilIngestion() {
    const log = logger.forMethod('runAuxilioBrasilIngestion');
    const moduleName = 'auxilio-brasil';

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