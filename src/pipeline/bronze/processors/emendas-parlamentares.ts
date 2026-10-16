import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import csvParser from 'csv-parser';
import iconv from 'iconv-lite';
import { sql } from 'drizzle-orm';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import {
    EmendasParlamentares,
    EmendasParlamentaresConvenios,
    EmendasParlamentaresFavorecido
} from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY, extractMetadataFromFileName, ReferenceMetadata, normalize } from '../shared.js';

const logger = new_logger(import.meta.url);

const BATCH_SIZE = 2000;

async function persistEmendas(data: (typeof EmendasParlamentares.$inferInsert)[]) {
    const log = logger.forMethod('persistEmendas');
    try {
        await bronze_db.insert(EmendasParlamentares).values(data).onDuplicateKeyUpdate({
            set: {
                codigoEmenda: sql`values(${EmendasParlamentares.codigoEmenda})`,
                anoEmenda: sql`values(${EmendasParlamentares.anoEmenda})`,
                tipoEmenda: sql`values(${EmendasParlamentares.tipoEmenda})`,
                codigoAutorEmenda: sql`values(${EmendasParlamentares.codigoAutorEmenda})`,
                nomeAutorEmenda: sql`values(${EmendasParlamentares.nomeAutorEmenda})`,
                numeroEmenda: sql`values(${EmendasParlamentares.numeroEmenda})`,
                localidadeGasto: sql`values(${EmendasParlamentares.localidadeGasto})`,
                codigoMunicipioIbge: sql`values(${EmendasParlamentares.codigoMunicipioIbge})`,
                municipio: sql`values(${EmendasParlamentares.municipio})`,
                codigoUfIbge: sql`values(${EmendasParlamentares.codigoUfIbge})`,
                uf: sql`values(${EmendasParlamentares.uf})`,
                regiao: sql`values(${EmendasParlamentares.regiao})`,
                codigoFuncao: sql`values(${EmendasParlamentares.codigoFuncao})`,
                nomeFuncao: sql`values(${EmendasParlamentares.nomeFuncao})`,
                codigoSubfuncao: sql`values(${EmendasParlamentares.codigoSubfuncao})`,
                nomeSubfuncao: sql`values(${EmendasParlamentares.nomeSubfuncao})`,
                codigoPrograma: sql`values(${EmendasParlamentares.codigoPrograma})`,
                nomePrograma: sql`values(${EmendasParlamentares.nomePrograma})`,
                codigoAcao: sql`values(${EmendasParlamentares.codigoAcao})`,
                nomeAcao: sql`values(${EmendasParlamentares.nomeAcao})`,
                codigoPlanoOrcamentario: sql`values(${EmendasParlamentares.codigoPlanoOrcamentario})`,
                nomePlanoOrcamentario: sql`values(${EmendasParlamentares.nomePlanoOrcamentario})`,
                valorEmpenhado: sql`values(${EmendasParlamentares.valorEmpenhado})`,
                valorLiquidado: sql`values(${EmendasParlamentares.valorLiquidado})`,
                valorPago: sql`values(${EmendasParlamentares.valorPago})`,
                valorRestosAPagarInscritos: sql`values(${EmendasParlamentares.valorRestosAPagarInscritos})`,
                valorRestosAPagarCancelados: sql`values(${EmendasParlamentares.valorRestosAPagarCancelados})`,
                valorRestosAPagarPagos: sql`values(${EmendasParlamentares.valorRestosAPagarPagos})`,
                exercicioReferencia: sql`values(${EmendasParlamentares.exercicioReferencia})`,
                mesReferencia: sql`values(${EmendasParlamentares.mesReferencia})`,
                diaReferencia: sql`values(${EmendasParlamentares.diaReferencia})`,
                dadosAdicionais: sql`values(${EmendasParlamentares.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Emendas Parlamentares`);
        throw error;
    }
}

async function persistConvenios(data: (typeof EmendasParlamentaresConvenios.$inferInsert)[]) {
    const log = logger.forMethod('persistConvenios');
    try {
        await bronze_db.insert(EmendasParlamentaresConvenios).values(data).onDuplicateKeyUpdate({
            set: {
                codigoEmenda: sql`values(${EmendasParlamentaresConvenios.codigoEmenda})`,
                codigoFuncao: sql`values(${EmendasParlamentaresConvenios.codigoFuncao})`,
                nomeFuncao: sql`values(${EmendasParlamentaresConvenios.nomeFuncao})`,
                codigoSubfuncao: sql`values(${EmendasParlamentaresConvenios.codigoSubfuncao})`,
                nomeSubfuncao: sql`values(${EmendasParlamentaresConvenios.nomeSubfuncao})`,
                localidadeGasto: sql`values(${EmendasParlamentaresConvenios.localidadeGasto})`,
                tipoEmenda: sql`values(${EmendasParlamentaresConvenios.tipoEmenda})`,
                dataPublicacaoConvenio: sql`values(${EmendasParlamentaresConvenios.dataPublicacaoConvenio})`,
                convenente: sql`values(${EmendasParlamentaresConvenios.convenente})`,
                objetoConvenio: sql`values(${EmendasParlamentaresConvenios.objetoConvenio})`,
                numeroConvenio: sql`values(${EmendasParlamentaresConvenios.numeroConvenio})`,
                valorConvenio: sql`values(${EmendasParlamentaresConvenios.valorConvenio})`,
                exercicioReferencia: sql`values(${EmendasParlamentaresConvenios.exercicioReferencia})`,
                mesReferencia: sql`values(${EmendasParlamentaresConvenios.mesReferencia})`,
                diaReferencia: sql`values(${EmendasParlamentaresConvenios.diaReferencia})`,
                dadosAdicionais: sql`values(${EmendasParlamentaresConvenios.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Convênios de Emendas`);
        throw error;
    }
}

async function persistFavorecidos(data: (typeof EmendasParlamentaresFavorecido.$inferInsert)[]) {
    const log = logger.forMethod('persistFavorecidos');
    try {
        await bronze_db.insert(EmendasParlamentaresFavorecido).values(data).onDuplicateKeyUpdate({
            set: {
                codigoEmenda: sql`values(${EmendasParlamentaresFavorecido.codigoEmenda})`,
                codigoAutorEmenda: sql`values(${EmendasParlamentaresFavorecido.codigoAutorEmenda})`,
                nomeAutorEmenda: sql`values(${EmendasParlamentaresFavorecido.nomeAutorEmenda})`,
                numeroEmenda: sql`values(${EmendasParlamentaresFavorecido.numeroEmenda})`,
                tipoEmenda: sql`values(${EmendasParlamentaresFavorecido.tipoEmenda})`,
                anoMes: sql`values(${EmendasParlamentaresFavorecido.anoMes})`,
                codigoFavorecido: sql`values(${EmendasParlamentaresFavorecido.codigoFavorecido})`,
                favorecido: sql`values(${EmendasParlamentaresFavorecido.favorecido})`,
                naturezaJuridica: sql`values(${EmendasParlamentaresFavorecido.naturezaJuridica})`,
                tipoFavorecido: sql`values(${EmendasParlamentaresFavorecido.tipoFavorecido})`,
                ufFavorecido: sql`values(${EmendasParlamentaresFavorecido.ufFavorecido})`,
                municipioFavorecido: sql`values(${EmendasParlamentaresFavorecido.municipioFavorecido})`,
                valorRecebido: sql`values(${EmendasParlamentaresFavorecido.valorRecebido})`,
                exercicioReferencia: sql`values(${EmendasParlamentaresFavorecido.exercicioReferencia})`,
                mesReferencia: sql`values(${EmendasParlamentaresFavorecido.mesReferencia})`,
                diaReferencia: sql`values(${EmendasParlamentaresFavorecido.diaReferencia})`,
                dadosAdicionais: sql`values(${EmendasParlamentaresFavorecido.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Favorecidos de Emendas`);
        throw error;
    }
}

interface HeaderRule {
    key: string;
    keywords?: string[];
    matchExact?: string[];
    require?: string[];
}

const EMENDAS_HEADER_RULES: HeaderRule[] = [
    { key: 'codigoAutorEmenda', keywords: ['AUTOR DA EMENDA'], require: ['CODIGO'] },
    { key: 'codigoEmenda', keywords: ['EMENDA'], require: ['CODIGO'] },
    { key: 'anoEmenda', keywords: ['EMENDA'], require: ['ANO'] },
    { key: 'tipoEmenda', keywords: ['EMENDA'], require: ['TIPO'] },
    { key: 'nomeAutorEmenda', keywords: ['AUTOR'], require: ['NOME'] },
    { key: 'numeroEmenda', keywords: ['EMENDA'], require: ['NUMERO'] },
    { key: 'localidadeGasto', keywords: ['LOCALIDADE DE APLICACAO', 'LOCALIDADE APLICACAO'] },
    { key: 'codigoMunicipioIbge', keywords: ['MUNICIPIO IBGE'], require: ['CODIGO'] },
    { key: 'municipio', matchExact: ['MUNICIPIO'] },
    { key: 'codigoUfIbge', keywords: ['UF IBGE'], require: ['CODIGO'] },
    { key: 'uf', matchExact: ['UF'] },
    { key: 'regiao', matchExact: ['REGIAO'] },

    // Estrutura Orçamentária (SUBFUNÇÃO antes de FUNÇÃO)
    { key: 'codigoSubfuncao', keywords: ['SUBFUNCAO'], require: ['CODIGO'] },
    { key: 'nomeSubfuncao', keywords: ['SUBFUNCAO'], require: ['NOME'] },
    { key: 'codigoFuncao', keywords: ['FUNCAO'], require: ['CODIGO'] },
    { key: 'nomeFuncao', keywords: ['FUNCAO'], require: ['NOME'] },
    { key: 'codigoPrograma', keywords: ['PROGRAMA'], require: ['CODIGO'] },
    { key: 'nomePrograma', keywords: ['PROGRAMA'], require: ['NOME'] },
    { key: 'codigoAcao', keywords: ['ACAO'], require: ['CODIGO'] },
    { key: 'nomeAcao', keywords: ['ACAO'], require: ['NOME'] },
    { key: 'codigoPlanoOrcamentario', keywords: ['PLANO ORCAMENTARIO'], require: ['CODIGO'] },
    { key: 'nomePlanoOrcamentario', keywords: ['PLANO ORCAMENTARIO'], require: ['NOME'] },

    // Valores e Restos a Pagar
    { key: 'valorEmpenhado', keywords: ['VALOR EMPENHADO'] },
    { key: 'valorLiquidado', keywords: ['VALOR LIQUIDADO'] },
    { key: 'valorPago', keywords: ['VALOR PAGO'] },
    { key: 'valorRestosAPagarInscritos', keywords: ['RESTOS A PAGAR INSCRITOS'] },
    { key: 'valorRestosAPagarCancelados', keywords: ['RESTOS A PAGAR CANCELADO', 'RESTOS A PAGAR CANCELADOS'] },
    { key: 'valorRestosAPagarPagos', keywords: ['RESTOS A PAGAR PAGOS'] },
];

const EMENDAS_CONVENIOS_HEADER_RULES: HeaderRule[] = [
    { key: 'codigoEmenda', keywords: ['EMENDA'], require: ['CODIGO'] },
    { key: 'codigoSubfuncao', keywords: ['SUBFUNCAO'], require: ['CODIGO'] },
    { key: 'nomeSubfuncao', keywords: ['SUBFUNCAO'], require: ['NOME'] },
    { key: 'codigoFuncao', keywords: ['FUNCAO'], require: ['CODIGO'] },
    { key: 'nomeFuncao', keywords: ['FUNCAO'], require: ['NOME'] },
    { key: 'localidadeGasto', keywords: ['LOCALIDADE DO GASTO', 'LOCALIDADE GASTO'] },
    { key: 'tipoEmenda', keywords: ['EMENDA'], require: ['TIPO'] },
    { key: 'dataPublicacaoConvenio', keywords: ['PUBLICACAO CONVENIO'] },
    { key: 'convenente', matchExact: ['CONVENENTE', 'NOME CONVENENTE'] },
    { key: 'objetoConvenio', keywords: ['OBJETO CONVENIO', 'OBJETO DO CONVENIO'] },
    { key: 'numeroConvenio', keywords: ['CONVENIO'], require: ['NUMERO'] },
    { key: 'valorConvenio', keywords: ['CONVENIO'], require: ['VALOR'] },
];

const EMENDAS_FAVORECIDOS_HEADER_RULES: HeaderRule[] = [
    { key: 'codigoAutorEmenda', keywords: ['AUTOR DA EMENDA'], require: ['CODIGO'] },
    { key: 'codigoEmenda', keywords: ['EMENDA'], require: ['CODIGO'] },
    { key: 'nomeAutorEmenda', keywords: ['AUTOR'], require: ['NOME'] },
    { key: 'numeroEmenda', keywords: ['EMENDA'], require: ['NUMERO'] },
    { key: 'tipoEmenda', keywords: ['EMENDA'], require: ['TIPO'] },
    { key: 'anoMes', keywords: ['ANO MES', 'ANO / MES', 'ANO/MES', 'ANOMES'] },
    { key: 'codigoFavorecido', keywords: ['FAVORECIDO'], require: ['CODIGO'] },
    { key: 'favorecido', matchExact: ['FAVORECIDO', 'NOME FAVORECIDO'] },
    { key: 'naturezaJuridica', keywords: ['NATUREZA JURIDICA'] },
    { key: 'tipoFavorecido', keywords: ['FAVORECIDO'], require: ['TIPO'] },
    { key: 'ufFavorecido', keywords: ['FAVORECIDO'], require: ['UF'] },
    { key: 'municipioFavorecido', keywords: ['FAVORECIDO'], require: ['MUNICIPIO'] },
    { key: 'valorRecebido', keywords: ['VALOR RECEBIDO'] },
];

// Helper genérico reutilizável
function resolveHeaderKey(header: string, rules: HeaderRule[]): string {
    if (!header) return '';
    const cleanHeader = normalize(header);

    for (const rule of rules) {
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
}

async function processEmendasStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processEmendasStream');
    let batch: (typeof EmendasParlamentares.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => resolveHeaderKey(header, EMENDAS_HEADER_RULES),
            }));

        for await (const data of parser) {
            batch.push({
                ...data,
                ...metadata,
            });
            if (batch.length >= BATCH_SIZE) {
                await persistEmendas(batch);
                totalProcessed += batch.length;
                batch = [];
            }
        }
        if (batch.length > 0) {
            await persistEmendas(batch);
            totalProcessed += batch.length;
        }

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento de Emendas concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream de Emendas');
        throw error;
    }
}

async function processConveniosStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processConveniosStream');
    let batch: (typeof EmendasParlamentaresConvenios.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => resolveHeaderKey(header, EMENDAS_CONVENIOS_HEADER_RULES),
            }));

        for await (const data of parser) {
            batch.push({
                ...data,
                ...metadata,
            });
            if (batch.length >= BATCH_SIZE) {
                await persistConvenios(batch);
                totalProcessed += batch.length;
                batch = [];
            }
        }
        if (batch.length > 0) {
            await persistConvenios(batch);
            totalProcessed += batch.length;
        }

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento de Convênios de Emendas concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream de Convênios de Emendas');
        throw error;
    }
}

async function processFavorecidosStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processFavorecidosStream');
    let batch: (typeof EmendasParlamentaresFavorecido.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => resolveHeaderKey(header, EMENDAS_FAVORECIDOS_HEADER_RULES),
            }));

        for await (const data of parser) {
            batch.push({
                ...data,
                ...metadata,
            });
            if (batch.length >= BATCH_SIZE) {
                await persistFavorecidos(batch);
                totalProcessed += batch.length;
                batch = [];
            }
        }
        if (batch.length > 0) {
            await persistFavorecidos(batch);
            totalProcessed += batch.length;
        }

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento de Favorecidos de Emendas concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream de Favorecidos de Emendas');
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
                            const upperName = entry.fileName.toUpperCase();
                            let rowsCount = 0;

                            const streamEndPromise = new Promise<void>((resolveStream, rejectStream) => {
                                readStream.on('end', resolveStream);
                                readStream.on('error', rejectStream);
                            });

                            if (upperName.includes('CONVENIOS')) {
                                rowsCount = await processConveniosStream(readStream, metadata);
                            } else if (upperName.includes('FAVORECIDO')) {
                                rowsCount = await processFavorecidosStream(readStream, metadata);
                            } else if (upperName.includes('EMENDASPARLAMENTARES.CSV') || upperName.match(/_EMENDAS\.CSV$/)) {
                                rowsCount = await processEmendasStream(readStream, metadata);
                            } else {
                                log.info({ data: { fileName: entry.fileName } }, 'Arquivo CSV ignorado');
                                readStream.resume();
                            }

                            await streamEndPromise;

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

export async function runEmendasParlamentaresIngestion() {
    const log = logger.forMethod('runEmendasParlamentaresIngestion');
    const moduleName = 'emendas-parlamentares';

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