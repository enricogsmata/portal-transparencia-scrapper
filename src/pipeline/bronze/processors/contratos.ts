import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import iconv from 'iconv-lite';
import csvParser from 'csv-parser';
import { sql } from 'drizzle-orm';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import {
    ContratosCompras,
    ContratosItemCompra,
    ContratosTermoAditivo,
    ContratosApostilamento
} from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY, extractMetadataFromFileName, ReferenceMetadata, normalize } from '../shared.js';

const logger = new_logger(import.meta.url);

const BATCH_SIZE = 2000;

async function persistCompras(data: (typeof ContratosCompras.$inferInsert)[]) {
    const log = logger.forMethod('persistCompras');
    try {
        await bronze_db.insert(ContratosCompras).values(data).onDuplicateKeyUpdate({
            set: {
                numeroContrato: sql`values(${ContratosCompras.numeroContrato})`,
                objeto: sql`values(${ContratosCompras.objeto})`,
                fundamentoLegal: sql`values(${ContratosCompras.fundamentoLegal})`,
                modalidadeCompra: sql`values(${ContratosCompras.modalidadeCompra})`,
                situacaoContrato: sql`values(${ContratosCompras.situacaoContrato})`,
                codigoOrgaoSuperior: sql`values(${ContratosCompras.codigoOrgaoSuperior})`,
                nomeOrgaoSuperior: sql`values(${ContratosCompras.nomeOrgaoSuperior})`,
                codigoOrgao: sql`values(${ContratosCompras.codigoOrgao})`,
                nomeOrgao: sql`values(${ContratosCompras.nomeOrgao})`,
                codigoUg: sql`values(${ContratosCompras.codigoUg})`,
                nomeUg: sql`values(${ContratosCompras.nomeUg})`,
                dataAssinaturaContrato: sql`values(${ContratosCompras.dataAssinaturaContrato})`,
                dataPublicacaoDou: sql`values(${ContratosCompras.dataPublicacaoDou})`,
                dataInicioVigencia: sql`values(${ContratosCompras.dataInicioVigencia})`,
                dataFimVigencia: sql`values(${ContratosCompras.dataFimVigencia})`,
                codigoContratado: sql`values(${ContratosCompras.codigoContratado})`,
                nomeContratado: sql`values(${ContratosCompras.nomeContratado})`,
                valorInicialCompra: sql`values(${ContratosCompras.valorInicialCompra})`,
                valorFinalCompra: sql`values(${ContratosCompras.valorFinalCompra})`,
                numeroLicitacao: sql`values(${ContratosCompras.numeroLicitacao})`,
                codigoUgLicitacao: sql`values(${ContratosCompras.codigoUgLicitacao})`,
                nomeUgLicitacao: sql`values(${ContratosCompras.nomeUgLicitacao})`,
                codigoModalidadeCompraLicitacao: sql`values(${ContratosCompras.codigoModalidadeCompraLicitacao})`,
                modalidadeCompraLicitacao: sql`values(${ContratosCompras.modalidadeCompraLicitacao})`,
                exercicioReferencia: sql`values(${ContratosCompras.exercicioReferencia})`,
                mesReferencia: sql`values(${ContratosCompras.mesReferencia})`,
                diaReferencia: sql`values(${ContratosCompras.diaReferencia})`,
                dadosAdicionais: sql`values(${ContratosCompras.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Compras`);
        throw error;
    }
}

async function persistItemCompra(data: (typeof ContratosItemCompra.$inferInsert)[]) {
    const log = logger.forMethod('persistItemCompra');
    try {
        await bronze_db.insert(ContratosItemCompra).values(data).onDuplicateKeyUpdate({
            set: {
                numeroContrato: sql`values(${ContratosItemCompra.numeroContrato})`,
                codigoOrgao: sql`values(${ContratosItemCompra.codigoOrgao})`,
                nomeOrgao: sql`values(${ContratosItemCompra.nomeOrgao})`,
                codigoUg: sql`values(${ContratosItemCompra.codigoUg})`,
                nomeUg: sql`values(${ContratosItemCompra.nomeUg})`,
                codigoItemCompra: sql`values(${ContratosItemCompra.codigoItemCompra})`,
                descricaoItemCompra: sql`values(${ContratosItemCompra.descricaoItemCompra})`,
                quantidadeItem: sql`values(${ContratosItemCompra.quantidadeItem})`,
                valorItem: sql`values(${ContratosItemCompra.valorItem})`,
                exercicioReferencia: sql`values(${ContratosItemCompra.exercicioReferencia})`,
                mesReferencia: sql`values(${ContratosItemCompra.mesReferencia})`,
                diaReferencia: sql`values(${ContratosItemCompra.diaReferencia})`,
                dadosAdicionais: sql`values(${ContratosItemCompra.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Item Compra`);
        throw error;
    }
}

async function persistTermoAditivo(data: (typeof ContratosTermoAditivo.$inferInsert)[]) {
    const log = logger.forMethod('persistTermoAditivo');
    try {
        await bronze_db.insert(ContratosTermoAditivo).values(data).onDuplicateKeyUpdate({
            set: {
                numeroContrato: sql`values(${ContratosTermoAditivo.numeroContrato})`,
                codigoOrgaoSuperior: sql`values(${ContratosTermoAditivo.codigoOrgaoSuperior})`,
                nomeOrgaoSuperior: sql`values(${ContratosTermoAditivo.nomeOrgaoSuperior})`,
                codigoOrgao: sql`values(${ContratosTermoAditivo.codigoOrgao})`,
                nomeOrgao: sql`values(${ContratosTermoAditivo.nomeOrgao})`,
                codigoUg: sql`values(${ContratosTermoAditivo.codigoUg})`,
                nomeUg: sql`values(${ContratosTermoAditivo.nomeUg})`,
                numeroTermoAditivo: sql`values(${ContratosTermoAditivo.numeroTermoAditivo})`,
                dataPublicacao: sql`values(${ContratosTermoAditivo.dataPublicacao})`,
                objeto: sql`values(${ContratosTermoAditivo.objeto})`,
                exercicioReferencia: sql`values(${ContratosTermoAditivo.exercicioReferencia})`,
                mesReferencia: sql`values(${ContratosTermoAditivo.mesReferencia})`,
                diaReferencia: sql`values(${ContratosTermoAditivo.diaReferencia})`,
                dadosAdicionais: sql`values(${ContratosTermoAditivo.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Termo Aditivo`);
        throw error;
    }
}

async function persistApostilamento(data: (typeof ContratosApostilamento.$inferInsert)[]) {
    const log = logger.forMethod('persistApostilamento');
    try {
        await bronze_db.insert(ContratosApostilamento).values(data).onDuplicateKeyUpdate({
            set: {
                numeroContrato: sql`values(${ContratosApostilamento.numeroContrato})`,
                codigoOrgaoSuperior: sql`values(${ContratosApostilamento.codigoOrgaoSuperior})`,
                nomeOrgaoSuperior: sql`values(${ContratosApostilamento.nomeOrgaoSuperior})`,
                codigoOrgao: sql`values(${ContratosApostilamento.codigoOrgao})`,
                nomeOrgao: sql`values(${ContratosApostilamento.nomeOrgao})`,
                codigoUg: sql`values(${ContratosApostilamento.codigoUg})`,
                nomeUg: sql`values(${ContratosApostilamento.nomeUg})`,
                numeroApostilamento: sql`values(${ContratosApostilamento.numeroApostilamento})`,
                descricao: sql`values(${ContratosApostilamento.descricao})`,
                valorApostilamento: sql`values(${ContratosApostilamento.valorApostilamento})`,
                dataInclusao: sql`values(${ContratosApostilamento.dataInclusao})`,
                situacaoApostilamento: sql`values(${ContratosApostilamento.situacaoApostilamento})`,
                exercicioReferencia: sql`values(${ContratosApostilamento.exercicioReferencia})`,
                mesReferencia: sql`values(${ContratosApostilamento.mesReferencia})`,
                diaReferencia: sql`values(${ContratosApostilamento.diaReferencia})`,
                dadosAdicionais: sql`values(${ContratosApostilamento.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Apostilamento`);
        throw error;
    }
}

interface HeaderRule {
    key: string;
    keywords?: string[];
    matchExact?: string[];
    require?: string[];
}

// Regras Comuns do Módulo de Contratos
const COMMON_CONTRATOS_RULES: HeaderRule[] = [
    { key: 'numeroContrato', keywords: ['CONTRATO'], require: ['NUMERO'] },
    
    // Órgãos (Superior antes do Genérico)
    { key: 'codigoOrgaoSuperior', keywords: ['ORGAO SUPERIOR'], require: ['CODIGO'] },
    { key: 'nomeOrgaoSuperior', keywords: ['ORGAO SUPERIOR'], require: ['NOME'] },
    { key: 'codigoOrgao', keywords: ['ORGAO'], require: ['CODIGO'] },
    { key: 'nomeOrgao', keywords: ['ORGAO'], require: ['NOME'] },
    
    // Unidade Gestora
    { key: 'codigoUg', keywords: ['UG'], require: ['CODIGO'] },
    { key: 'nomeUg', keywords: ['UG'], require: ['NOME'] },
];

const COMPRAS_MAIN_RULES: HeaderRule[] = [
    { key: 'codigoModalidadeCompraLicitacao', keywords: ['MODALIDADE COMPRA LICITACAO'], require: ['CODIGO'] },
    { key: 'modalidadeCompraLicitacao', keywords: ['MODALIDADE COMPRA LICITACAO'] },
    { key: 'objeto', matchExact: ['OBJETO', 'OBJETO DO CONTRATO'] },
    { key: 'fundamentoLegal', keywords: ['FUNDAMENTO LEGAL'] },
    { key: 'modalidadeCompra', keywords: ['MODALIDADE COMPRA', 'MODALIDADE DE COMPRA'] },
    { key: 'situacaoContrato', keywords: ['SITUACAO CONTRATO', 'SITUACAO DO CONTRATO'] },
    
    // Datas
    { key: 'dataAssinaturaContrato', keywords: ['ASSINATURA CONTRATO'] },
    { key: 'dataPublicacaoDou', keywords: ['PUBLICACAO DOU'] },
    { key: 'dataInicioVigencia', keywords: ['INICIO VIGENCIA', 'INICIO DA VIGENCIA'] },
    { key: 'dataFimVigencia', keywords: ['FIM VIGENCIA', 'FIM DA VIGENCIA'] },
    
    // Contratado
    { key: 'codigoContratado', keywords: ['CONTRATADO'], require: ['CODIGO'] },
    { key: 'nomeContratado', keywords: ['CONTRATADO'], require: ['NOME'] },
    
    // Valores
    { key: 'valorInicialCompra', keywords: ['VALOR INICIAL'] },
    { key: 'valorFinalCompra', keywords: ['VALOR FINAL'] },
    
    // Dados da Licitação associada
    { key: 'numeroLicitacao', keywords: ['LICITACAO'], require: ['NUMERO'] },
    { key: 'codigoUgLicitacao', keywords: ['UG LICITACAO'], require: ['CODIGO'] },
    { key: 'nomeUgLicitacao', keywords: ['UG LICITACAO'], require: ['NOME'] },
];

const ITEM_COMPRA_RULES: HeaderRule[] = [
    { key: 'codigoItemCompra', keywords: ['ITEM COMPRA', 'ITEM DE COMPRA'], require: ['CODIGO'] },
    { key: 'descricaoItemCompra', keywords: ['ITEM COMPRA', 'ITEM DE COMPRA'], require: ['DESCRICAO'] },
    { key: 'quantidadeItem', keywords: ['QUANTIDADE ITEM', 'QUANTIDADE'] },
    { key: 'valorItem', keywords: ['VALOR ITEM', 'VALOR DO ITEM'] },
];

const TERMO_ADITIVO_RULES: HeaderRule[] = [
    { key: 'numeroTermoAditivo', keywords: ['TERMO ADITIVO'], require: ['NUMERO'] },
    { key: 'dataPublicacao', keywords: ['DATA PUBLICACAO', 'PUBLICACAO'] },
    { key: 'objeto', matchExact: ['OBJETO', 'OBJETO ADITIVO'] },
];

const APOSTILAMENTO_RULES: HeaderRule[] = [
    { key: 'numeroApostilamento', keywords: ['APOSTILAMENTO'], require: ['NUMERO'] },
    { key: 'descricao', matchExact: ['DESCRICAO', 'DESCRICAO APOSTILAMENTO'] },
    { key: 'valorApostilamento', keywords: ['VALOR APOSTILAMENTO', 'VALOR'] },
    { key: 'dataInclusao', keywords: ['INCLUSAO', 'DATA DE INCLUSAO'] },
    { key: 'situacaoApostilamento', keywords: ['SITUACAO APOSTILAMENTO', 'SITUACAO'] },
];

// Helper genérico para buscar nas regras específicas + comuns de Contratos
function resolveContratosHeaderKey(header: string, specificRules: HeaderRule[]): string {
    if (!header) return '';
    const cleanHeader = normalize(header);

    // 1. Tenta casar primeiro nas regras específicas da sub-tabela
    for (const rule of specificRules) {
        if (rule.matchExact && rule.matchExact.includes(cleanHeader)) return rule.key;
        const hasKeyword = rule.keywords && rule.keywords.some(kw => cleanHeader.includes(kw));
        if (hasKeyword) {
            if (rule.require) {
                if (rule.require.some(req => cleanHeader.includes(req))) return rule.key;
            } else {
                return rule.key;
            }
        }
    }

    // 2. Se não casou, tenta as regras comuns de Contratos
    for (const rule of COMMON_CONTRATOS_RULES) {
        if (rule.matchExact && rule.matchExact.includes(cleanHeader)) return rule.key;
        const hasKeyword = rule.keywords && rule.keywords.some(kw => cleanHeader.includes(kw));
        if (hasKeyword) {
            if (rule.require) {
                if (rule.require.some(req => cleanHeader.includes(req))) return rule.key;
            } else {
                return rule.key;
            }
        }
    }

    return cleanHeader;
}

async function processComprasStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processComprasStream');
    let batch: (typeof ContratosCompras.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => resolveContratosHeaderKey(header, COMPRAS_MAIN_RULES),
            }));

        for await (const data of parser) {
            batch.push({
                ...data,
                ...metadata,
            });
            if (batch.length >= BATCH_SIZE) {
                await persistCompras(batch);
                totalProcessed += batch.length;
                batch = [];
            }
        }
        if (batch.length > 0) {
            await persistCompras(batch);
            totalProcessed += batch.length;
        }
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream de Compras');
        throw error;
    }
}

async function processItemCompraStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processItemCompraStream');
    let batch: (typeof ContratosItemCompra.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => resolveContratosHeaderKey(header, ITEM_COMPRA_RULES),
            }));

        for await (const data of parser) {
            batch.push({
                ...data,
                ...metadata,
            });
            if (batch.length >= BATCH_SIZE) {
                await persistItemCompra(batch);
                totalProcessed += batch.length;
                batch = [];
            }
        }
        if (batch.length > 0) {
            await persistItemCompra(batch);
            totalProcessed += batch.length;
        }
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream de Item Compra');
        throw error;
    }
}

async function processTermoAditivoStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processTermoAditivoStream');
    let batch: (typeof ContratosTermoAditivo.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => resolveContratosHeaderKey(header, TERMO_ADITIVO_RULES),
            }));

        for await (const data of parser) {
            batch.push({
                ...data,
                ...metadata,
            });
            if (batch.length >= BATCH_SIZE) {
                await persistTermoAditivo(batch);
                totalProcessed += batch.length;
                batch = [];
            }
        }
        if (batch.length > 0) {
            await persistTermoAditivo(batch);
            totalProcessed += batch.length;
        }
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream de Termo Aditivo');
        throw error;
    }
}

async function processApostilamentoStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processApostilamentoStream');
    let batch: (typeof ContratosApostilamento.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => resolveContratosHeaderKey(header, APOSTILAMENTO_RULES),
            }));

        for await (const data of parser) {
            batch.push({
                ...data,
                ...metadata,
            });
            if (batch.length >= BATCH_SIZE) {
                await persistApostilamento(batch);
                totalProcessed += batch.length;
                batch = [];
            }
        }
        if (batch.length > 0) {
            await persistApostilamento(batch);
            totalProcessed += batch.length;
        }
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream de Apostilamento');
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

                            if (upperName.includes('APOSTILAMENTO')) {
                                rowsCount = await processApostilamentoStream(readStream, metadata);
                            } else if (upperName.includes('TERMOADITIVO') || upperName.includes('TERMO_ADITIVO')) {
                                rowsCount = await processTermoAditivoStream(readStream, metadata);
                            } else if (upperName.includes('ITEMCOMPRA') || upperName.includes('ITEM_COMPRA') || upperName.includes('ITEM_DE_COMPRA')) {
                                rowsCount = await processItemCompraStream(readStream, metadata);
                            } else if (upperName.includes('COMPRAS')) {
                                rowsCount = await processComprasStream(readStream, metadata);
                            } else {
                                log.info({ data: { fileName: entry.fileName } }, 'Arquivo CSV ignorado');
                                readStream.resume();
                            }

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

export async function runContratosIngestion() {
    const log = logger.forMethod('runContratosIngestion');
    const moduleName = 'compras';

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