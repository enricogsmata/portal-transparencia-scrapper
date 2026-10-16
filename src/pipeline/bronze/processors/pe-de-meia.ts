import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import iconv from 'iconv-lite';
import csvParser from 'csv-parser';
import { normalize } from '../shared.js';
import { sql } from 'drizzle-orm';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import { PeDeMeia } from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY, extractMetadataFromFileName, ReferenceMetadata } from '../shared.js';

const logger = new_logger(import.meta.url);

type PeDeMeiaRow = typeof PeDeMeia.$inferInsert;

async function persistData(data: PeDeMeiaRow[]) {
    const log = logger.forMethod('persistData');
    try {
        await bronze_db.insert(PeDeMeia).values(data).onDuplicateKeyUpdate({
            set: {
                mesFolha: sql`values(${PeDeMeia.mesFolha})`,
                mesReferencia: sql`values(${PeDeMeia.mesReferencia})`,
                dataPagamento: sql`values(${PeDeMeia.dataPagamento})`,
                uf: sql`values(${PeDeMeia.uf})`,
                codigoMunicipio: sql`values(${PeDeMeia.codigoMunicipio})`,
                nomeMunicipio: sql`values(${PeDeMeia.nomeMunicipio})`,
                tipoParcela: sql`values(${PeDeMeia.tipoParcela})`,
                periodoParcela: sql`values(${PeDeMeia.periodoParcela})`,
                etapaEnsino: sql`values(${PeDeMeia.etapaEnsino})`,
                cpfBeneficiario: sql`values(${PeDeMeia.cpfBeneficiario})`,
                nisBeneficiario: sql`values(${PeDeMeia.nisBeneficiario})`,
                nomeBeneficiario: sql`values(${PeDeMeia.nomeBeneficiario})`,
                cpfResponsavel: sql`values(${PeDeMeia.cpfResponsavel})`,
                nisResponsavel: sql`values(${PeDeMeia.nisResponsavel})`,
                nomeResponsavel: sql`values(${PeDeMeia.nomeResponsavel})`,
                valorParcela: sql`values(${PeDeMeia.valorParcela})`,
                exercicioReferencia: sql`values(${PeDeMeia.exercicioReferencia})`,
                diaReferencia: sql`values(${PeDeMeia.diaReferencia})`,
                dadosAdicionais: sql`values(${PeDeMeia.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência`);
        throw error;
    }
}

// Mapeamento de regras para o Pé-de-Meia
const PE_DE_MEIA_HEADER_RULES = [
    { key: 'mesFolha', keywords: ['MES FOLHA'] },
    { key: 'mesReferencia', keywords: ['MES REFER'] },
    { key: 'dataPagamento', keywords: ['DATA PAGAMENTO', 'DATA DO PAGAMENTO'] },
    { key: 'uf', matchExact: ['UF'] },
    { key: 'codigoMunicipio', keywords: ['CODIGO MUNIC', 'CODIGO DO MUNIC'] },
    { key: 'nomeMunicipio', keywords: ['NOME DO MUNIC', 'NOME MUNIC'] },
    { key: 'tipoParcela', keywords: ['TIPO PARCELA', 'TIPO INCENTIVO'] },
    { key: 'periodoParcela', keywords: ['PERIODO INCENTIVO', 'PERIODO DO INCENTIVO'] },
    { key: 'etapaEnsino', keywords: ['ETAPA ENSINO', 'ETAPA DE ENSINO'] },

    // Regras de Beneficiário (Keywords primárias + require para desambiguar do Responsável)
    { key: 'cpfBeneficiario', keywords: ['CPF'], require: ['BENEFICI'] },
    { key: 'nisBeneficiario', keywords: ['NIS'], require: ['BENEFICI'] },
    { key: 'nomeBeneficiario', keywords: ['NOME'], require: ['BENEFICI'] },

    // Regras de Responsável
    { key: 'cpfResponsavel', keywords: ['CPF'], require: ['RESPONS'] },
    { key: 'nisResponsavel', keywords: ['NIS'], require: ['RESPONS'] },
    { key: 'nomeResponsavel', keywords: ['NOME'], require: ['RESPONS'] },

    { key: 'valorParcela', keywords: ['VALOR PARCELA', 'VALOR BENEFICIO'] }
];

export async function processCsvStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processCsvStream');
    const BATCH_SIZE = 2000;
    let batch: PeDeMeiaRow[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => {
                    if (!header) return null;

                    const cleanHeader = normalize(header);

                    for (const rule of PE_DE_MEIA_HEADER_RULES) {
                        // Correspondência exata (ex: UF)
                        if (rule.matchExact && rule.matchExact.includes(cleanHeader)) {
                            return rule.key;
                        }

                        // Correspondência parcial
                        const hasKeyword = rule.keywords && rule.keywords.some(kw => cleanHeader.includes(kw));

                        if (hasKeyword) {
                            // Desambiguação de campos compostos (ex: CPF BENEFICIÁRIO vs CPF RESPONSÁVEL)
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