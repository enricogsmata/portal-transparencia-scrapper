import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import csvParser from 'csv-parser';
import { sql } from 'drizzle-orm';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import { Bpc } from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY, extractMetadataFromFileName, ReferenceMetadata } from '../shared.js';

const logger = new_logger(import.meta.url);

type BpcRow = typeof Bpc.$inferInsert;

async function persistData(data: BpcRow[]) {
    const log = logger.forMethod('persistData');
    try {
        await bronze_db.insert(Bpc).values(data).onDuplicateKeyUpdate({
            set: {
                mesCompetencia: sql`values(${Bpc.mesCompetencia})`,
                mesReferencia: sql`values(${Bpc.mesReferencia})`,
                uf: sql`values(${Bpc.uf})`,
                codigoMunicipioSiafi: sql`values(${Bpc.codigoMunicipioSiafi})`,
                nomeMunicipio: sql`values(${Bpc.nomeMunicipio})`,
                nisBeneficiario: sql`values(${Bpc.nisBeneficiario})`,
                cpfBeneficiario: sql`values(${Bpc.cpfBeneficiario})`,
                nomeBeneficiario: sql`values(${Bpc.nomeBeneficiario})`,
                nisRepresentanteLegal: sql`values(${Bpc.nisRepresentanteLegal})`,
                cpfRepresentanteLegal: sql`values(${Bpc.cpfRepresentanteLegal})`,
                nomeRepresentanteLegal: sql`values(${Bpc.nomeRepresentanteLegal})`,
                numeroBeneficio: sql`values(${Bpc.numeroBeneficio})`,
                concedidoJudicialmente: sql`values(${Bpc.concedidoJudicialmente})`,
                valorParcela: sql`values(${Bpc.valorParcela})`,
                exercicioReferencia: sql`values(${Bpc.exercicioReferencia})`,
                diaReferencia: sql`values(${Bpc.diaReferencia})`,
                dadosAdicionais: sql`values(${Bpc.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência`);
        throw error;
    }
}

export async function processCsvStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processCsvStream');
    const BATCH_SIZE = 2000;
    let batch: BpcRow[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase();
                if (h.includes('COMPET')) return 'mesCompetencia';
                if (h.includes('REFER')) return 'mesReferencia';
                if (h === 'UF' || h.includes('UF ')) return 'uf';
                if (h.includes('SIAFI') && (h.includes('CÓDIGO') || h.includes('CODIGO'))) return 'codigoMunicipioSiafi';
                if (h.includes('NOME MUNIC')) return 'nomeMunicipio';
                if (h.includes('NIS BENEFICI')) return 'nisBeneficiario';
                if (h.includes('CPF BENEFICI')) return 'cpfBeneficiario';
                if (h.includes('NOME BENEFICI')) return 'nomeBeneficiario';
                if (h.includes('NIS REPRESENTANTE')) return 'nisRepresentanteLegal';
                if (h.includes('CPF REPRESENTANTE')) return 'cpfRepresentanteLegal';
                if (h.includes('NOME REPRESENTANTE')) return 'nomeRepresentanteLegal';
                if (h.includes('NÚMERO BENEF') || h.includes('NUMERO BENEF')) return 'numeroBeneficio';
                if (h.includes('JUDICIALMENTE')) return 'concedidoJudicialmente';
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

export async function runBpcIngestion() {
    const log = logger.forMethod('runBpcIngestion');
    const moduleName = 'bpc';

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