import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import csvParser from 'csv-parser';
import { sql } from 'drizzle-orm';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import { SeguroDefeso } from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY, extractMetadataFromFileName, ReferenceMetadata, normalize } from '../shared.js';
import iconv from 'iconv-lite';

const logger = new_logger(import.meta.url);

type SeguroDefesoRow = typeof SeguroDefeso.$inferInsert;

async function persistData(data: SeguroDefesoRow[]) {
    const log = logger.forMethod('persistData');
    try {
        await bronze_db.insert(SeguroDefeso).values(data).onDuplicateKeyUpdate({
            set: {
                anoMesReferencia: sql`values(${SeguroDefeso.anoMesReferencia})`,
                uf: sql`values(${SeguroDefeso.uf})`,
                codigoSiafiMunicipio: sql`values(${SeguroDefeso.codigoSiafiMunicipio})`,
                nomeMunicipio: sql`values(${SeguroDefeso.nomeMunicipio})`,
                cpfFavorecido: sql`values(${SeguroDefeso.cpfFavorecido})`,
                nisFavorecido: sql`values(${SeguroDefeso.nisFavorecido})`,
                rgpFavorecido: sql`values(${SeguroDefeso.rgpFavorecido})`,
                nomeFavorecido: sql`values(${SeguroDefeso.nomeFavorecido})`,
                valorParcela: sql`values(${SeguroDefeso.valorParcela})`,
                exercicioReferencia: sql`values(${SeguroDefeso.exercicioReferencia})`,
                mesReferencia: sql`values(${SeguroDefeso.mesReferencia})`,
                diaReferencia: sql`values(${SeguroDefeso.diaReferencia})`,
                dadosAdicionais: sql`values(${SeguroDefeso.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência`);
        throw error;
    }
}

// Mapeamento de regras do Seguro Defeso
const SEGURO_DEFESO_HEADER_RULES = [
    { key: 'anoMesReferencia', keywords: ['REFER', 'MES REFER'] },
    { key: 'uf', matchExact: ['UF'] },
    { key: 'codigoSiafiMunicipio', keywords: ['SIAFI', 'CODIGO MUNIC'] },
    { key: 'nomeMunicipio', keywords: ['NOME DO MUNIC', 'NOME MUNIC'] },
    { key: 'cpfFavorecido', keywords: ['CPF'] },
    { key: 'nisFavorecido', keywords: ['NIS'] },
    { key: 'rgpFavorecido', keywords: ['RGP'] },
    { key: 'nomeFavorecido', keywords: ['NOME FAVORECIDO', 'NOME BENEFICI'] },
    { key: 'valorParcela', keywords: ['VALOR PARCELA', 'VALOR BENEFICIO'] }
];

export async function processCsvStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processCsvStream');
    const BATCH_SIZE = 2000;
    let batch: SeguroDefesoRow[] = [];
    let totalProcessed = 0;

    try {
        // Intercepta a stream convertendo o encoding de ISO-8859-1 (latin1) para UTF-8 limpo
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => {
                    if (!header) return null;

                    const cleanHeader = normalize(header);

                    for (const rule of SEGURO_DEFESO_HEADER_RULES) {
                        if (rule.matchExact && rule.matchExact.includes(cleanHeader)) {
                            return rule.key;
                        }
                        if (rule.keywords && rule.keywords.some(kw => cleanHeader.includes(kw))) {
                            return rule.key;
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

export async function runSeguroDefesoIngestion() {
    const log = logger.forMethod('runSeguroDefesoIngestion');
    const moduleName = 'seguro-defeso';

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