import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import iconv from 'iconv-lite';
import csvParser from 'csv-parser';
import { normalize } from '../shared.js';
import { sql } from 'drizzle-orm';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import { RecebimentoRecursosFavorecido } from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY, extractMetadataFromFileName, ReferenceMetadata } from '../shared.js';

const logger = new_logger(import.meta.url);

type RecebimentoRecursosFavorecidoRow = typeof RecebimentoRecursosFavorecido.$inferInsert;

async function persistData(data: RecebimentoRecursosFavorecidoRow[]) {
    const log = logger.forMethod('persistData');
    try {
        await bronze_db.insert(RecebimentoRecursosFavorecido).values(data).onDuplicateKeyUpdate({
            set: {
                codigoFavorecido: sql`values(${RecebimentoRecursosFavorecido.codigoFavorecido})`,
                nomeFavorecido: sql`values(${RecebimentoRecursosFavorecido.nomeFavorecido})`,
                siglaUf: sql`values(${RecebimentoRecursosFavorecido.siglaUf})`,
                nomeMunicipio: sql`values(${RecebimentoRecursosFavorecido.nomeMunicipio})`,
                codigoOrgaoSuperior: sql`values(${RecebimentoRecursosFavorecido.codigoOrgaoSuperior})`,
                nomeOrgaoSuperior: sql`values(${RecebimentoRecursosFavorecido.nomeOrgaoSuperior})`,
                codigoOrgao: sql`values(${RecebimentoRecursosFavorecido.codigoOrgao})`,
                nomeOrgao: sql`values(${RecebimentoRecursosFavorecido.nomeOrgao})`,
                codigoUnidadeGestora: sql`values(${RecebimentoRecursosFavorecido.codigoUnidadeGestora})`,
                nomeUnidadeGestora: sql`values(${RecebimentoRecursosFavorecido.nomeUnidadeGestora})`,
                anoMesLancamento: sql`values(${RecebimentoRecursosFavorecido.anoMesLancamento})`,
                valorRecebido: sql`values(${RecebimentoRecursosFavorecido.valorRecebido})`,
                exercicioReferencia: sql`values(${RecebimentoRecursosFavorecido.exercicioReferencia})`,
                mesReferencia: sql`values(${RecebimentoRecursosFavorecido.mesReferencia})`,
                diaReferencia: sql`values(${RecebimentoRecursosFavorecido.diaReferencia})`,
                dadosAdicionais: sql`values(${RecebimentoRecursosFavorecido.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência`);
        throw error;
    }
}

interface HeaderRule {
    key: string;
    keywords?: string[];
    matchExact?: string[];
    require?: string[];
}

const RECEBIMENTO_RECURSOS_FAVORECIDO_HEADER_RULES: HeaderRule[] = [
    // Favorecido e Localização
    { key: 'codigoFavorecido', keywords: ['FAVORECIDO'], require: ['CODIGO'] },
    { key: 'nomeFavorecido', keywords: ['FAVORECIDO'], require: ['NOME'] },
    { key: 'siglaUf', keywords: ['SIGLA UF'], matchExact: ['UF'] },
    { key: 'nomeMunicipio', keywords: ['MUNICIPIO'], require: ['NOME'] },

    // Órgãos
    { key: 'codigoOrgaoSuperior', keywords: ['ORGAO SUPERIOR'], require: ['CODIGO'] },
    { key: 'nomeOrgaoSuperior', keywords: ['ORGAO SUPERIOR'], require: ['NOME'] },

    { key: 'codigoOrgao', keywords: ['CODIGO ORGAO', 'CODIGO DO ORGAO'] },
    { key: 'nomeOrgao', keywords: ['NOME ORGAO', 'NOME DO ORGAO'] },

    // Unidade Gestora
    { key: 'codigoUnidadeGestora', keywords: ['UNIDADE GESTORA'], require: ['CODIGO'] },
    { key: 'nomeUnidadeGestora', keywords: ['UNIDADE GESTORA'], require: ['NOME'] },

    // Período e Valor
    { key: 'anoMesLancamento', keywords: ['ANO E MES', 'ANO MES DO LANCAMENTO', 'LANCAMENTO'] },
    { key: 'valorRecebido', keywords: ['VALOR RECEBIDO'] },
];

export async function processCsvStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processCsvStream');
    const BATCH_SIZE = 2000;
    let batch: RecebimentoRecursosFavorecidoRow[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => {
                    if (!header) return null;

                    const cleanHeader = normalize(header);

                    for (const rule of RECEBIMENTO_RECURSOS_FAVORECIDO_HEADER_RULES) {
                        if (rule.matchExact && rule.matchExact.includes(cleanHeader)) {
                            return rule.key;
                        }

                        const hasKeyword = rule.keywords && rule.keywords.some(kw => cleanHeader.includes(kw));

                        if (hasKeyword) {
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

export async function runRecebimentoRecursosFavorecidoIngestion() {
    const log = logger.forMethod('runRecebimentoRecursosFavorecidoIngestion');
    const moduleName = 'despesas-favorecidos';

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