import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import csvParser from 'csv-parser';
import { sql } from 'drizzle-orm';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import { AuxilioEmergencial } from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY, extractMetadataFromFileName, ReferenceMetadata } from '../shared.js';

const logger = new_logger(import.meta.url);

type AuxilioEmergencialRow = typeof AuxilioEmergencial.$inferInsert;

async function persistData(data: AuxilioEmergencialRow[]) {
    const log = logger.forMethod('persistData');
    try {
        await bronze_db.insert(AuxilioEmergencial).values(data).onDuplicateKeyUpdate({
            set: {
                mesDisponibilizacao: sql`values(${AuxilioEmergencial.mesDisponibilizacao})`,
                uf: sql`values(${AuxilioEmergencial.uf})`,
                codigoMunicipioIbge: sql`values(${AuxilioEmergencial.codigoMunicipioIbge})`,
                nomeMunicipio: sql`values(${AuxilioEmergencial.nomeMunicipio})`,
                nisBeneficiario: sql`values(${AuxilioEmergencial.nisBeneficiario})`,
                cpfBeneficiario: sql`values(${AuxilioEmergencial.cpfBeneficiario})`,
                nomeBeneficiario: sql`values(${AuxilioEmergencial.nomeBeneficiario})`,
                nisResponsavel: sql`values(${AuxilioEmergencial.nisResponsavel})`,
                cpfResponsavel: sql`values(${AuxilioEmergencial.cpfResponsavel})`,
                nomeResponsavel: sql`values(${AuxilioEmergencial.nomeResponsavel})`,
                enquadramento: sql`values(${AuxilioEmergencial.enquadramento})`,
                parcela: sql`values(${AuxilioEmergencial.parcela})`,
                observacao: sql`values(${AuxilioEmergencial.observacao})`,
                valorBeneficio: sql`values(${AuxilioEmergencial.valorBeneficio})`,
                exercicioReferencia: sql`values(${AuxilioEmergencial.exercicioReferencia})`,
                mesReferencia: sql`values(${AuxilioEmergencial.mesReferencia})`,
                diaReferencia: sql`values(${AuxilioEmergencial.diaReferencia})`,
                dadosAdicionais: sql`values(${AuxilioEmergencial.dadosAdicionais})`,
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
    let batch: AuxilioEmergencialRow[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase();
                if (h.includes('DISPONIBIL')) return 'mesDisponibilizacao';
                if (h.includes('UF')) return 'uf';
                if (h.includes('IBGE')) return 'codigoMunicipioIbge';
                if (h.includes('NOME MUNIC')) return 'nomeMunicipio';
                if (h.includes('NIS BENEFICI')) return 'nisBeneficiario';
                if (h.includes('CPF BENEFICI')) return 'cpfBeneficiario';
                if (h.includes('NOME BENEFICI')) return 'nomeBeneficiario';
                if (h.includes('NIS RESPONS')) return 'nisResponsavel';
                if (h.includes('CPF RESPONS')) return 'cpfResponsavel';
                if (h.includes('NOME RESPONS')) return 'nomeResponsavel';
                if (h.includes('ENQUADRAMENTO')) return 'enquadramento';
                if (h.includes('PARCELA')) return 'parcela';
                if (h.includes('OBSERV')) return 'observacao';
                if (h.includes('VALOR BENEF')) return 'valorBeneficio';
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

export async function runAuxilioEmergencialIngestion() {
    const log = logger.forMethod('runAuxilioEmergencialIngestion');
    const moduleName = 'auxilio-emergencial';

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