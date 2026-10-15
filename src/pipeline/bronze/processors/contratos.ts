import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
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
import { ZIP_FILES_DIRECTORY, extractMetadataFromFileName, ReferenceMetadata } from '../shared.js';

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

async function processComprasStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processComprasStream');
    let batch: (typeof ContratosCompras.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase().trim();
                if (h.includes('NÚMERO DO CONTRATO') || h.includes('NUMERO DO CONTRATO')) return 'numeroContrato';
                if (h === 'OBJETO') return 'objeto';
                if (h.includes('FUNDAMENTO LEGAL')) return 'fundamentoLegal';
                if (h === 'MODALIDADE DE COMPRA' || h === 'MODALIDADE COMPRA') return 'modalidadeCompra';
                if (h.includes('SITUAÇÃO CONTRATO') || h.includes('SITUACAO CONTRATO')) return 'situacaoContrato';
                if (h.includes('CÓDIGO DO ÓRGÃO SUPERIOR') || h.includes('CODIGO DO ORGAO SUPERIOR') || h.includes('CÓDIGO ÓRGÃO SUPERIOR') || h.includes('CODIGO ORGAO SUPERIOR')) return 'codigoOrgaoSuperior';
                if (h.includes('NOME ÓRGÃO SUPERIOR') || h.includes('NOME ORGAO SUPERIOR')) return 'nomeOrgaoSuperior';
                if (h.includes('CÓDIGO ÓRGÃO') || h.includes('CODIGO ORGAO')) return 'codigoOrgao';
                if (h.includes('NOME ÓRGÃO') || h.includes('NOME ORGAO')) return 'nomeOrgao';
                if (h === 'CÓDIGO UG' || h === 'CODIGO UG') return 'codigoUg';
                if (h === 'NOME UG') return 'nomeUg';
                if (h.includes('DATA ASSINATURA CONTRATO')) return 'dataAssinaturaContrato';
                if (h.includes('DATA PUBLICAÇÃO DOU') || h.includes('DATA PUBLICACAO DOU')) return 'dataPublicacaoDou';
                if (h.includes('DATA INÍCIO DA VIGÊNCIA') || h.includes('DATA INICIO DA VIGENCIA') || h.includes('DATA INÍCIO VIGÊNCIA')) return 'dataInicioVigencia';
                if (h.includes('DATA FIM DA VIGÊNCIA') || h.includes('DATA FIM DA VIGENCIA') || h.includes('DATA FIM VIGÊNCIA')) return 'dataFimVigencia';
                if (h.includes('CÓDIGO CONTRATADO') || h.includes('CODIGO CONTRATADO')) return 'codigoContratado';
                if (h.includes('NOME CONTRATADO')) return 'nomeContratado';
                if (h.includes('VALOR INICIAL DA COMPRA')) return 'valorInicialCompra';
                if (h.includes('VALOR FINAL DA COMPRA')) return 'valorFinalCompra';
                if (h.includes('NÚMERO DA LICITAÇÃO') || h.includes('NUMERO DA LICITACAO') || h.includes('NÚMERO LICITAÇÃO') || h.includes('NUMERO LICITACAO')) return 'numeroLicitacao';
                if (h.includes('CÓDIGO UG LICITAÇÃO') || h.includes('CODIGO UG LICITACAO')) return 'codigoUgLicitacao';
                if (h.includes('NOME UG LICITAÇÃO') || h.includes('NOME UG LICITACAO')) return 'nomeUgLicitacao';
                if (h.includes('CÓDIGO MODALIDADE COMPRA LICITAÇÃO') || h.includes('CODIGO MODALIDADE COMPRA LICITACAO')) return 'codigoModalidadeCompraLicitacao';
                if (h.includes('MODALIDADE COMPRA LICITAÇÃO') || h.includes('MODALIDADE COMPRA LICITACAO')) return 'modalidadeCompraLicitacao';
                return h;
            },
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
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase().trim();
                if (h.includes('NÚMERO CONTRATO') || h.includes('NUMERO CONTRATO') || h.includes('NÚMERO DO CONTRATO')) return 'numeroContrato';
                if (h.includes('CÓDIGO ÓRGÃO') || h.includes('CODIGO ORGAO')) return 'codigoOrgao';
                if (h.includes('NOME ÓRGÃO') || h.includes('NOME ORGAO')) return 'nomeOrgao';
                if (h === 'CÓDIGO UG' || h === 'CODIGO UG') return 'codigoUg';
                if (h === 'NOME UG') return 'nomeUg';
                if (h.includes('CÓDIGO ITEM COMPRA') || h.includes('CODIGO ITEM COMPRA')) return 'codigoItemCompra';
                if (h.includes('DESCRIÇÃO ITEM COMPRA') || h.includes('DESCRICAO ITEM COMPRA')) return 'descricaoItemCompra';
                if (h.includes('QUANTIDADE ITEM')) return 'quantidadeItem';
                if (h.includes('VALOR ITEM')) return 'valorItem';
                return h;
            },
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
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase().trim();
                if (h.includes('NÚMERO CONTRATO') || h.includes('NUMERO CONTRATO') || h.includes('NÚMERO DO CONTRATO')) return 'numeroContrato';
                if (h.includes('CÓDIGO DO ÓRGÃO SUPERIOR') || h.includes('CODIGO DO ORGAO SUPERIOR') || h.includes('CÓDIGO ÓRGÃO SUPERIOR') || h.includes('CODIGO ORGAO SUPERIOR')) return 'codigoOrgaoSuperior';
                if (h.includes('NOME ÓRGÃO SUPERIOR') || h.includes('NOME ORGAO SUPERIOR')) return 'nomeOrgaoSuperior';
                if (h.includes('CÓDIGO ÓRGÃO') || h.includes('CODIGO ORGAO')) return 'codigoOrgao';
                if (h.includes('NOME ÓRGÃO') || h.includes('NOME ORGAO')) return 'nomeOrgao';
                if (h === 'CÓDIGO UG' || h === 'CODIGO UG') return 'codigoUg';
                if (h === 'NOME UG') return 'nomeUg';
                if (h.includes('NÚMERO TERMO ADITIVO') || h.includes('NUMERO TERMO ADITIVO')) return 'numeroTermoAditivo';
                if (h.includes('DATA PUBLICAÇÃO') || h.includes('DATA PUBLICACAO')) return 'dataPublicacao';
                if (h === 'OBJETO') return 'objeto';
                return h;
            },
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
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase().trim();
                if (h.includes('NÚMERO CONTRATO') || h.includes('NUMERO CONTRATO') || h.includes('NÚMERO DO CONTRATO')) return 'numeroContrato';
                if (h.includes('CÓDIGO DO ÓRGÃO SUPERIOR') || h.includes('CODIGO DO ORGAO SUPERIOR') || h.includes('CÓDIGO ÓRGÃO SUPERIOR') || h.includes('CODIGO ORGAO SUPERIOR')) return 'codigoOrgaoSuperior';
                if (h.includes('NOME ÓRGÃO SUPERIOR') || h.includes('NOME ORGAO SUPERIOR')) return 'nomeOrgaoSuperior';
                if (h.includes('CÓDIGO ÓRGÃO') || h.includes('CODIGO ORGAO')) return 'codigoOrgao';
                if (h.includes('NOME ÓRGÃO') || h.includes('NOME ORGAO')) return 'nomeOrgao';
                if (h === 'CÓDIGO UG' || h === 'CODIGO UG') return 'codigoUg';
                if (h === 'NOME UG') return 'nomeUg';
                if (h.includes('NÚMERO APOSTILAMENTO') || h.includes('NUMERO APOSTILAMENTO')) return 'numeroApostilamento';
                if (h === 'DESCRIÇÃO' || h === 'DESCRICAO') return 'descricao';
                if (h.includes('VALOR APOSTILAMENTO')) return 'valorApostilamento';
                if (h.includes('DATA DE INCLUSÃO') || h.includes('DATA DE INCLUSAO') || h.includes('DATA INCLUSÃO')) return 'dataInclusao';
                if (h.includes('SITUAÇÃO APOSTILAMENTO') || h.includes('SITUACAO APOSTILAMENTO')) return 'situacaoApostilamento';
                return h;
            },
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