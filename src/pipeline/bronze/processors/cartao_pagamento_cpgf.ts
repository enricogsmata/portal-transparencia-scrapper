import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import iconv from 'iconv-lite';
import csvParser from 'csv-parser';
import { normalize } from '../shared.js';
import { sql } from 'drizzle-orm';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import { CartaoPagamentoCpgf } from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY, extractMetadataFromFileName, ReferenceMetadata } from '../shared.js';

const logger = new_logger(import.meta.url);

type CartaoPagamentoCpgfRow = typeof CartaoPagamentoCpgf.$inferInsert;

async function persistData(data: CartaoPagamentoCpgfRow[]) {
    const log = logger.forMethod('persistData');
    try {
        await bronze_db.insert(CartaoPagamentoCpgf).values(data).onDuplicateKeyUpdate({
            set: {
                codigoOrgaoSuperior: sql`values(${CartaoPagamentoCpgf.codigoOrgaoSuperior})`,
                nomeOrgaoSuperior: sql`values(${CartaoPagamentoCpgf.nomeOrgaoSuperior})`,
                codigoOrgaoSubordinado: sql`values(${CartaoPagamentoCpgf.codigoOrgaoSubordinado})`,
                nomeOrgaoSubordinado: sql`values(${CartaoPagamentoCpgf.nomeOrgaoSubordinado})`,
                codigoUnidadeGestora: sql`values(${CartaoPagamentoCpgf.codigoUnidadeGestora})`,
                nomeUnidadeGestora: sql`values(${CartaoPagamentoCpgf.nomeUnidadeGestora})`,
                anoExtrato: sql`values(${CartaoPagamentoCpgf.anoExtrato})`,
                mesExtrato: sql`values(${CartaoPagamentoCpgf.mesExtrato})`,
                cpfPortador: sql`values(${CartaoPagamentoCpgf.cpfPortador})`,
                nomePortador: sql`values(${CartaoPagamentoCpgf.nomePortador})`,
                dataTransacao: sql`values(${CartaoPagamentoCpgf.dataTransacao})`,
                valorTransacao: sql`values(${CartaoPagamentoCpgf.valorTransacao})`,
                transacao: sql`values(${CartaoPagamentoCpgf.transacao})`,
                nomeFavorecido: sql`values(${CartaoPagamentoCpgf.nomeFavorecido})`,
                documentoFavorecido: sql`values(${CartaoPagamentoCpgf.documentoFavorecido})`,
                exercicioReferencia: sql`values(${CartaoPagamentoCpgf.exercicioReferencia})`,
                mesReferencia: sql`values(${CartaoPagamentoCpgf.mesReferencia})`,
                diaReferencia: sql`values(${CartaoPagamentoCpgf.diaReferencia})`,
                dadosAdicionais: sql`values(${CartaoPagamentoCpgf.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência`);
        throw error;
    }
}

// Mapeamento de regras para o CPGF (Cartão Corporativo)
const CPGF_HEADER_RULES = [
    // 1. Mais específicos primeiro (Órgão Superior e Unidade Gestora)
    { key: 'codigoOrgaoSuperior', keywords: ['ORGAO SUPERIOR'], require: ['CODIGO'] },
    { key: 'nomeOrgaoSuperior', keywords: ['ORGAO SUPERIOR'], require: ['NOME'] },

    { key: 'codigoOrgaoSubordinado', keywords: ['ORGAO SUBORDINADO'], require: ['CODIGO'] },
    { key: 'nomeOrgaoSubordinado', keywords: ['ORGAO SUBORDINADO'], require: ['NOME'] },

    { key: 'codigoUnidadeGestora', keywords: ['UNIDADE GESTORA'], require: ['CODIGO'] },
    { key: 'nomeUnidadeGestora', keywords: ['UNIDADE GESTORA'], require: ['NOME'] },

    // 2. Órgão genérico (CÓDIGO ÓRGÃO / NOME ÓRGÃO)
    // Se não caiu em "ORGAO SUPERIOR" acima, cairá aqui e mapeará para o órgão subordinado/executante!
    { key: 'codigoOrgaoSubordinado', keywords: ['CODIGO ORGAO', 'CODIGO DO ORGAO'] },
    { key: 'nomeOrgaoSubordinado', keywords: ['NOME ORGAO', 'NOME DO ORGAO'] },

    // Extrato
    { key: 'anoExtrato', keywords: ['ANO EXTRATO'] },
    { key: 'mesExtrato', keywords: ['MES EXTRATO'] },

    // Portador
    { key: 'cpfPortador', keywords: ['CPF PORTADOR'] },
    { key: 'nomePortador', keywords: ['NOME PORTADOR'] },

    // Transação (A ordem importa: DATA e VALOR antes de TRANSAÇÃO isolado)
    { key: 'dataTransacao', keywords: ['DATA TRANSACAO', 'DATA DA TRANSACAO'] },
    { key: 'valorTransacao', keywords: ['VALOR TRANSACAO', 'VALOR DA TRANSACAO'] },
    { key: 'transacao', matchExact: ['TRANSACAO', 'TIPO DE TRANSACAO', 'TIPO TRANSACAO'] },

    // Favorecido (Cobre "CNPJ OU CPF FAVORECIDO" visível na imagem)
    { key: 'nomeFavorecido', keywords: ['NOME FAVORECIDO', 'NOME DO FAVORECIDO'] },
    { key: 'documentoFavorecido', keywords: ['CNPJ OU CPF FAVORECIDO', 'CPF DO FAVORECIDO', 'CPF FAVORECIDO', 'CNPJ FAVORECIDO'] }
];
export async function processCsvStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processCsvStream');
    const BATCH_SIZE = 2000;
    let batch: CartaoPagamentoCpgfRow[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => {
                    if (!header) return null;

                    const cleanHeader = normalize(header);

                    for (const rule of CPGF_HEADER_RULES) {
                        // Correspondência exata
                        if (rule.matchExact && rule.matchExact.includes(cleanHeader)) {
                            return rule.key;
                        }

                        // Correspondência por keywords
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

export async function runCartaoPagamentoCpgfIngestion() {
    const log = logger.forMethod('runCartaoPagamentoCpgfIngestion');
    const moduleName = 'cpgf';

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