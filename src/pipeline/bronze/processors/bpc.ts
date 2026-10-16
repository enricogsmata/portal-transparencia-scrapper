import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import iconv from 'iconv-lite';
import csvParser from 'csv-parser';
import { normalize } from '../shared.js';
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

// Mapeamento de regras para o BPC
const BPC_HEADER_RULES = [
    { key: 'mesCompetencia', keywords: ['COMPET'] },
    { key: 'mesReferencia', keywords: ['REFER', 'MES REFER'] },
    { key: 'uf', matchExact: ['UF'] },
    { key: 'codigoMunicipioSiafi', keywords: ['SIAFI'], require: ['CODIGO'] },
    { key: 'nomeMunicipio', keywords: ['NOME MUNIC', 'NOME DO MUNIC'] },

    // Desambiguação Beneficiário vs Representante Legal
    { key: 'nisBeneficiario', keywords: ['NIS'], require: ['BENEFICI'] },
    { key: 'cpfBeneficiario', keywords: ['CPF'], require: ['BENEFICI'] },
    { key: 'nomeBeneficiario', keywords: ['NOME'], require: ['BENEFICI'] },

    { key: 'nisRepresentanteLegal', keywords: ['NIS'], require: ['REPRESENTANTE'] },
    { key: 'cpfRepresentanteLegal', keywords: ['CPF'], require: ['REPRESENTANTE'] },
    { key: 'nomeRepresentanteLegal', keywords: ['NOME'], require: ['REPRESENTANTE'] },

    { key: 'numeroBeneficio', keywords: ['NUMERO BENEF', 'NUMERO DO BENEF'] },
    { key: 'concedidoJudicialmente', keywords: ['JUDICIALMENTE', 'CONCEDIDO JUDICIAL'] },
    { key: 'valorParcela', keywords: ['VALOR PARCELA', 'VALOR BENEFICIO'] }
];

export async function processCsvStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processCsvStream');
    const BATCH_SIZE = 2000;
    let batch: BpcRow[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => {
                    if (!header) return null;

                    const cleanHeader = normalize(header);

                    for (const rule of BPC_HEADER_RULES) {
                        // Correspondência exata (ex: UF)
                        if (rule.matchExact && rule.matchExact.includes(cleanHeader)) {
                            return rule.key;
                        }

                        // Correspondência parcial
                        const hasKeyword = rule.keywords && rule.keywords.some(kw => cleanHeader.includes(kw));

                        if (hasKeyword) {
                            // Desambiguação de campos compostos (ex: BENEFICIÁRIO vs REPRESENTANTE)
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