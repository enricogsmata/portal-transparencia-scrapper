import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import csvParser from 'csv-parser';
import { sql } from 'drizzle-orm';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import { BolsaFamiliaPagamentos } from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY, extractMetadataFromFileName, ReferenceMetadata } from '../shared.js';

const logger = new_logger(import.meta.url);

type BolsaFamiliaPagamentosRow = typeof BolsaFamiliaPagamentos.$inferInsert;

async function persistData(data: BolsaFamiliaPagamentosRow[]) {
    const log = logger.forMethod('persistData');
    try {
        await bronze_db.insert(BolsaFamiliaPagamentos).values(data).onDuplicateKeyUpdate({
            set: {
                anoMesReferencia: sql`values(${BolsaFamiliaPagamentos.anoMesReferencia})`,
                anoMesCompetencia: sql`values(${BolsaFamiliaPagamentos.anoMesCompetencia})`,
                uf: sql`values(${BolsaFamiliaPagamentos.uf})`,
                codigoMunicipioSiafi: sql`values(${BolsaFamiliaPagamentos.codigoMunicipioSiafi})`,
                nomeMunicipioSiafi: sql`values(${BolsaFamiliaPagamentos.nomeMunicipioSiafi})`,
                cpfBeneficiario: sql`values(${BolsaFamiliaPagamentos.cpfBeneficiario})`,
                nisBeneficiario: sql`values(${BolsaFamiliaPagamentos.nisBeneficiario})`,
                nomeBeneficiario: sql`values(${BolsaFamiliaPagamentos.nomeBeneficiario})`,
                valorParcela: sql`values(${BolsaFamiliaPagamentos.valorParcela})`,
                exercicioReferencia: sql`values(${BolsaFamiliaPagamentos.exercicioReferencia})`,
                mesReferencia: sql`values(${BolsaFamiliaPagamentos.mesReferencia})`,
                diaReferencia: sql`values(${BolsaFamiliaPagamentos.diaReferencia})`,
                dadosAdicionais: sql`values(${BolsaFamiliaPagamentos.dadosAdicionais})`,
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
    let batch: BolsaFamiliaPagamentosRow[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase();
                if (h.includes('REFER')) return 'anoMesReferencia';
                if (h.includes('COMPET')) return 'anoMesCompetencia';
                if (h.includes('UF')) return 'uf';
                if (h.includes('CÓDIGO MUNIC') || h.includes('CODIGO MUNIC')) return 'codigoMunicipioSiafi';
                if (h.includes('NOME MUNIC')) return 'nomeMunicipioSiafi';
                if (h.includes('CPF BENEFICI')) return 'cpfBeneficiario';
                if (h.includes('NIS BENEFICI')) return 'nisBeneficiario';
                if (h.includes('NOME BENEFICI')) return 'nomeBeneficiario';
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

export async function runBolsaPagamentosFamiliaIngestion() {
    const log = logger.forMethod('runBolsaPagamentosFamiliaIngestion');
    const moduleName = 'bolsa-familia';

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