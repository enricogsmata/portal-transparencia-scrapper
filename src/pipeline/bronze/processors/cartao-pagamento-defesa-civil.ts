import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import csvParser from 'csv-parser';
import { sql } from 'drizzle-orm';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import { CartaoPagamentoDefesaCivil } from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY, extractMetadataFromFileName, ReferenceMetadata } from '../shared.js';

const logger = new_logger(import.meta.url);

type CartaoPagamentoDefesaCivilRow = typeof CartaoPagamentoDefesaCivil.$inferInsert;

async function persistData(data: CartaoPagamentoDefesaCivilRow[]) {
    const log = logger.forMethod('persistData');
    try {
        await bronze_db.insert(CartaoPagamentoDefesaCivil).values(data).onDuplicateKeyUpdate({
            set: {
                uf: sql`values(${CartaoPagamentoDefesaCivil.uf})`,
                executorDespesa: sql`values(${CartaoPagamentoDefesaCivil.executorDespesa})`,
                codigoConvenente: sql`values(${CartaoPagamentoDefesaCivil.codigoConvenente})`,
                nomeConvenente: sql`values(${CartaoPagamentoDefesaCivil.nomeConvenente})`,
                repasse: sql`values(${CartaoPagamentoDefesaCivil.repasse})`,
                numeroConvenio: sql`values(${CartaoPagamentoDefesaCivil.numeroConvenio})`,
                anoExtrato: sql`values(${CartaoPagamentoDefesaCivil.anoExtrato})`,
                mesExtrato: sql`values(${CartaoPagamentoDefesaCivil.mesExtrato})`,
                cpfPortador: sql`values(${CartaoPagamentoDefesaCivil.cpfPortador})`,
                nomePortador: sql`values(${CartaoPagamentoDefesaCivil.nomePortador})`,
                transacao: sql`values(${CartaoPagamentoDefesaCivil.transacao})`,
                dataTransacao: sql`values(${CartaoPagamentoDefesaCivil.dataTransacao})`,
                documentoFavorecido: sql`values(${CartaoPagamentoDefesaCivil.documentoFavorecido})`,
                nomeFavorecido: sql`values(${CartaoPagamentoDefesaCivil.nomeFavorecido})`,
                valorTransacao: sql`values(${CartaoPagamentoDefesaCivil.valorTransacao})`,
                exercicioReferencia: sql`values(${CartaoPagamentoDefesaCivil.exercicioReferencia})`,
                mesReferencia: sql`values(${CartaoPagamentoDefesaCivil.mesReferencia})`,
                diaReferencia: sql`values(${CartaoPagamentoDefesaCivil.diaReferencia})`,
                dadosAdicionais: sql`values(${CartaoPagamentoDefesaCivil.dadosAdicionais})`,
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
    let batch: CartaoPagamentoDefesaCivilRow[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase();

                if (h === 'UF' || h.includes('UF ')) return 'uf';
                if (h.includes('EXECUTOR DESPESA')) return 'executorDespesa';
                if (h.includes('CÓDIGO CONVENENTE') || h.includes('CODIGO CONVENENTE')) return 'codigoConvenente';
                if (h.includes('NOME CONVENENTE')) return 'nomeConvenente';
                if (h.includes('REPASSE')) return 'repasse';
                if (h.includes('NÚMERO CONVÊNIO') || h.includes('NUMERO CONVENIO')) return 'numeroConvenio';
                if (h.includes('ANO EXTRATO')) return 'anoExtrato';
                if (h.includes('MÊS EXTRATO') || h.includes('MES EXTRATO')) return 'mesExtrato';
                if (h.includes('CPF PORTADOR')) return 'cpfPortador';
                if (h.includes('NOME PORTADOR')) return 'nomePortador';
                if (h === 'TRANSAÇÃO' || h === 'TRANSACAO') return 'transacao';
                if (h.includes('DATA TRANSAÇÃO') || h.includes('DATA TRANSACAO')) return 'dataTransacao';
                if (h.includes('CNPJ') || h.includes('CPF DO FAVORECIDO') || h.includes('CPF FAVORECIDO')) return 'documentoFavorecido';
                if (h.includes('NOME DO FAVORECIDO') || h.includes('NOME FAVORECIDO')) return 'nomeFavorecido';
                if (h.includes('VALOR TRANSAÇÃO') || h.includes('VALOR TRANSACAO')) return 'valorTransacao';

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

export async function runCartaoPagamentoDefesaCivilIngestion() {
    const log = logger.forMethod('runCartaoPagamentoDefesaCivilIngestion');
    const moduleName = 'cpdc';

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