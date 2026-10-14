// src/modules/emendas-parlamentares/processor.ts
import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import csvParser from 'csv-parser';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import {
    EmendasParlamentares,
    EmendasParlamentaresConvenios,
    EmendasParlamentaresFavorecido
} from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY } from '../shared.js';

const logger = new_logger(import.meta.url);

const BATCH_SIZE = 2000;

async function persistEmendas(data: (typeof EmendasParlamentares.$inferInsert)[]) {
    const log = logger.forMethod('persistEmendas');
    try {
        await bronze_db.insert(EmendasParlamentares).values(data);
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Emendas Parlamentares`);
        throw error;
    }
}

async function persistConvenios(data: (typeof EmendasParlamentaresConvenios.$inferInsert)[]) {
    const log = logger.forMethod('persistConvenios');
    try {
        await bronze_db.insert(EmendasParlamentaresConvenios).values(data);
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Convênios de Emendas`);
        throw error;
    }
}

async function persistFavorecidos(data: (typeof EmendasParlamentaresFavorecido.$inferInsert)[]) {
    const log = logger.forMethod('persistFavorecidos');
    try {
        await bronze_db.insert(EmendasParlamentaresFavorecido).values(data);
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Favorecidos de Emendas`);
        throw error;
    }
}

async function processEmendasStream(stream: NodeJS.ReadableStream): Promise<number> {
    const log = logger.forMethod('processEmendasStream');
    let batch: (typeof EmendasParlamentares.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase().trim();
                if (h.includes('CÓDIGO DA EMENDA') || h.includes('CODIGO DA EMENDA')) return 'codigoEmenda';
                if (h.includes('ANO DA EMENDA')) return 'anoEmenda';
                if (h.includes('TIPO DA EMENDA')) return 'tipoEmenda';
                if (h.includes('CÓDIGO DO AUTOR') || h.includes('CODIGO DO AUTOR')) return 'codigoAutorEmenda';
                if (h.includes('NOME DO AUTOR')) return 'nomeAutorEmenda';
                if (h.includes('NÚMERO DA EMENDA') || h.includes('NUMERO DA EMENDA')) return 'numeroEmenda';
                if (h.includes('LOCALIDADE DO GASTO')) return 'localidadeGasto';
                if (h.includes('CÓDIGO MUNICÍPIO IBGE') || h.includes('CODIGO MUNICIPIO IBGE')) return 'codigoMunicipioIbge';
                if (h === 'MUNICÍPIO' || h === 'MUNICIPIO') return 'municipio';
                if (h.includes('CÓDIGO UF IBGE') || h.includes('CODIGO UF IBGE')) return 'codigoUfIbge';
                if (h === 'UF') return 'uf';
                if (h === 'REGIÃO' || h === 'REGIAO') return 'regiao';
                if (h.includes('CÓDIGO FUNÇÃO') || h.includes('CODIGO FUNCAO')) return 'codigoFuncao';
                if (h.includes('NOME FUNÇÃO') || h.includes('NOME FUNCAO')) return 'nomeFuncao';
                if (h.includes('CÓDIGO SUBFUNÇÃO') || h.includes('CODIGO SUBFUNCAO')) return 'codigoSubfuncao';
                if (h.includes('NOME SUBFUNÇÃO') || h.includes('NOME SUBFUNCAO')) return 'nomeSubfuncao';
                if (h.includes('CÓDIGO PROGRAMA') || h.includes('CODIGO PROGRAMA')) return 'codigoPrograma';
                if (h.includes('NOME PROGRAMA')) return 'nomePrograma';
                if (h.includes('CÓDIGO AÇÃO') || h.includes('CODIGO ACAO')) return 'codigoAcao';
                if (h.includes('NOME AÇÃO') || h.includes('NOME ACAO')) return 'nomeAcao';
                if (h.includes('CÓDIGO PLANO ORÇAMENTÁRIO') || h.includes('CODIGO PLANO ORCAMENTARIO')) return 'codigoPlanoOrcamentario';
                if (h.includes('NOME PLANO ORÇAMENTÁRIO') || h.includes('NOME PLANO ORCAMENTARIO')) return 'nomePlanoOrcamentario';
                if (h.includes('VALOR EMPENHADO')) return 'valorEmpenhado';
                if (h.includes('VALOR LIQUIDADO')) return 'valorLiquidado';
                if (h.includes('VALOR PAGO')) return 'valorPago';
                if (h.includes('RESTOS A PAGAR INSCRITOS')) return 'valorRestosAPagarInscritos';
                if (h.includes('RESTOS A PAGAR CANCELADOS')) return 'valorRestosAPagarCancelados';
                if (h.includes('RESTOS A PAGAR PAGOS')) return 'valorRestosAPagarPagos';
                return h;
            },
        }));

        for await (const data of parser) {
            batch.push(data);
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

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream');
        throw error;
    }
}

async function processConveniosStream(stream: NodeJS.ReadableStream): Promise<number> {
    const log = logger.forMethod('processConveniosStream');
    let batch: (typeof EmendasParlamentaresConvenios.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase().trim();
                if (h.includes('CÓDIGO DA EMENDA') || h.includes('CODIGO DA EMENDA')) return 'codigoEmenda';
                if (h.includes('CÓDIGO FUNÇÃO') || h.includes('CODIGO FUNCAO')) return 'codigoFuncao';
                if (h.includes('NOME FUNÇÃO') || h.includes('NOME FUNCAO')) return 'nomeFuncao';
                if (h.includes('CÓDIGO SUBFUNÇÃO') || h.includes('CODIGO SUBFUNCAO')) return 'codigoSubfuncao';
                if (h.includes('NOME SUBFUNÇÃO') || h.includes('NOME SUBFUNCAO')) return 'nomeSubfuncao';
                if (h.includes('LOCALIDADE DO GASTO')) return 'localidadeGasto';
                if (h.includes('TIPO DE EMENDA')) return 'tipoEmenda';
                if (h.includes('DATA PUBLICAÇÃO CONVÊNIO') || h.includes('DATA PUBLICACAO CONVENIO')) return 'dataPublicacaoConvenio';
                if (h === 'CONVENENTE') return 'convenente';
                if (h.includes('OBJETO CONVÊNIO') || h.includes('OBJETO CONVENIO')) return 'objetoConvenio';
                if (h.includes('NÚMERO CONVÊNIO') || h.includes('NUMERO CONVENIO')) return 'numeroConvenio';
                if (h.includes('VALOR CONVÊNIO') || h.includes('VALOR CONVENIO')) return 'valorConvenio';
                return h;
            },
        }));

        for await (const data of parser) {
            batch.push(data);
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

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream');
        throw error;
    }
}

async function processFavorecidosStream(stream: NodeJS.ReadableStream): Promise<number> {
    const log = logger.forMethod('processFavorecidosStream');
    let batch: (typeof EmendasParlamentaresFavorecido.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase().trim();
                if (h.includes('CÓDIGO DA EMENDA') || h.includes('CODIGO DA EMENDA')) return 'codigoEmenda';
                if (h.includes('CÓDIGO DO AUTOR') || h.includes('CODIGO DO AUTOR')) return 'codigoAutorEmenda';
                if (h.includes('NOME DO AUTOR')) return 'nomeAutorEmenda';
                if (h.includes('NÚMERO DA EMENDA') || h.includes('NUMERO DA EMENDA')) return 'numeroEmenda';
                if (h.includes('TIPO DE EMENDA')) return 'tipoEmenda';
                if (h.includes('ANO/MÊS') || h.includes('ANO/MES') || h.includes('ANO / MÊS')) return 'anoMes';
                if (h.includes('CÓDIGO DO FAVORECIDO') || h.includes('CODIGO DO FAVORECIDO')) return 'codigoFavorecido';
                if (h === 'FAVORECIDO') return 'favorecido';
                if (h.includes('NATUREZA JURÍDICA') || h.includes('NATUREZA JURIDICA')) return 'naturezaJuridica';
                if (h.includes('TIPO FAVORECIDO')) return 'tipoFavorecido';
                if (h.includes('UF FAVORECIDO')) return 'ufFavorecido';
                if (h.includes('MUNICÍPIO FAVORECIDO') || h.includes('MUNICIPIO FAVORECIDO')) return 'municipioFavorecido';
                if (h.includes('VALOR RECEBIDO')) return 'valorRecebido';
                return h;
            },
        }));

        for await (const data of parser) {
            batch.push(data);
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

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream');
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
        log.info({ data: { zipPath } }, 'Iniciando extração do arquivo ZIP');

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

                            if (upperName.includes('CONVENIOS')) {
                                rowsCount = await processConveniosStream(readStream);
                            } else if (upperName.includes('FAVORECIDO')) {
                                rowsCount = await processFavorecidosStream(readStream);
                            } else if (upperName.includes('EMENDASPARLAMENTARES.CSV') || upperName.match(/_EMENDAS\.CSV$/)) {
                                rowsCount = await processEmendasStream(readStream);
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