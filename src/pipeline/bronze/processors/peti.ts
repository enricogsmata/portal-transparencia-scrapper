import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import iconv from 'iconv-lite';
import csvParser from 'csv-parser';
import { normalize } from '../shared.js'; 
import { sql } from 'drizzle-orm';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import { Peti } from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY, extractMetadataFromFileName, ReferenceMetadata } from '../shared.js';

const logger = new_logger(import.meta.url);

type PetiRow = typeof Peti.$inferInsert;

async function persistData(data: PetiRow[]) {
    const log = logger.forMethod('persistData');
    try {
        await bronze_db.insert(Peti).values(data).onDuplicateKeyUpdate({
            set: {
                anoMesReferencia: sql`values(${Peti.anoMesReferencia})`,
                uf: sql`values(${Peti.uf})`,
                codigoMunicipioSiafi: sql`values(${Peti.codigoMunicipioSiafi})`,
                nomeMunicipioSiafi: sql`values(${Peti.nomeMunicipioSiafi})`,
                nisBeneficiario: sql`values(${Peti.nisBeneficiario})`,
                nomeBeneficiario: sql`values(${Peti.nomeBeneficiario})`,
                situacao: sql`values(${Peti.situacao})`,
                valorParcela: sql`values(${Peti.valorParcela})`,
                exercicioReferencia: sql`values(${Peti.exercicioReferencia})`,
                mesReferencia: sql`values(${Peti.mesReferencia})`,
                diaReferencia: sql`values(${Peti.diaReferencia})`,
                dadosAdicionais: sql`values(${Peti.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência`);
        throw error;
    }
}

// Rules específicas para a tabela do PETI
const PETI_HEADER_RULES = [
    { key: 'anoMesReferencia', keywords: ['REFER', 'MES REFER'] },
    { key: 'uf', matchExact: ['UF'] },
    { key: 'codigoMunicipioSiafi', keywords: ['SIAFI'], require: ['CODIGO', 'CÓDIGO'] },
    { key: 'nomeMunicipioSiafi', keywords: ['SIAFI'], require: ['NOME'] },
    { key: 'nisBeneficiario', keywords: ['NIS'] },
    { key: 'nomeBeneficiario', keywords: ['NOME BENEFICI', 'NOME FAVORECIDO'] },
    { key: 'situacao', keywords: ['SITUA'] },
    { key: 'valorParcela', keywords: ['VALOR PARCELA', 'VALOR BENEFICIO'] }
];

export async function processCsvStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processCsvStream');
    const BATCH_SIZE = 2000;
    let batch: PetiRow[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => {
                    if (!header) return null;

                    const cleanHeader = normalize(header);

                    for (const rule of PETI_HEADER_RULES) {
                        // Verifica correspondência exata
                        if (rule.matchExact && rule.matchExact.includes(cleanHeader)) {
                            return rule.key;
                        }

                        // Verifica keywords principais
                        const hasKeyword = rule.keywords && rule.keywords.some(kw => cleanHeader.includes(kw));

                        if (hasKeyword) {
                            // Se a regra exigir termos adicionais (ex: SIAFI + NOME)
                            if (rule.require) {
                                const hasRequired = rule.require.some(req => cleanHeader.includes(req));
                                if (hasRequired) return rule.key;
                            } else {
                                return rule.key;
                            }
                        }
                    }

                    return cleanHeader;
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

export async function runPetiIngestion() {
    const log = logger.forMethod('runPetiIngestion');
    const moduleName = 'peti';

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