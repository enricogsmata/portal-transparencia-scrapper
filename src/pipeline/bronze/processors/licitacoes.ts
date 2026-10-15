import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import csvParser from 'csv-parser';
import { sql } from 'drizzle-orm';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import {
    LicitacaoEmpenhosRelacionados,
    LicitacaoItem,
    Licitacao,
    LicitacaoParticipantes
} from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY, extractMetadataFromFileName, ReferenceMetadata } from '../shared.js';

const logger = new_logger(import.meta.url);

const BATCH_SIZE = 2000;

async function persistEmpenhosRelacionados(data: (typeof LicitacaoEmpenhosRelacionados.$inferInsert)[]) {
    const log = logger.forMethod('persistEmpenhosRelacionados');
    try {
        await bronze_db.insert(LicitacaoEmpenhosRelacionados).values(data).onDuplicateKeyUpdate({
            set: {
                numeroLicitacao: sql`values(${LicitacaoEmpenhosRelacionados.numeroLicitacao})`,
                codigoUg: sql`values(${LicitacaoEmpenhosRelacionados.codigoUg})`,
                nomeUg: sql`values(${LicitacaoEmpenhosRelacionados.nomeUg})`,
                codigoModalidadeCompra: sql`values(${LicitacaoEmpenhosRelacionados.codigoModalidadeCompra})`,
                modalidadeCompra: sql`values(${LicitacaoEmpenhosRelacionados.modalidadeCompra})`,
                numeroProcesso: sql`values(${LicitacaoEmpenhosRelacionados.numeroProcesso})`,
                codigoEmpenho: sql`values(${LicitacaoEmpenhosRelacionados.codigoEmpenho})`,
                dataEmissaoEmpenho: sql`values(${LicitacaoEmpenhosRelacionados.dataEmissaoEmpenho})`,
                observacaoEmpenho: sql`values(${LicitacaoEmpenhosRelacionados.observacaoEmpenho})`,
                valorEmpenho: sql`values(${LicitacaoEmpenhosRelacionados.valorEmpenho})`,
                exercicioReferencia: sql`values(${LicitacaoEmpenhosRelacionados.exercicioReferencia})`,
                mesReferencia: sql`values(${LicitacaoEmpenhosRelacionados.mesReferencia})`,
                diaReferencia: sql`values(${LicitacaoEmpenhosRelacionados.diaReferencia})`,
                dadosAdicionais: sql`values(${LicitacaoEmpenhosRelacionados.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Empenhos Relacionados`);
        throw error;
    }
}

async function persistItemLicitacao(data: (typeof LicitacaoItem.$inferInsert)[]) {
    const log = logger.forMethod('persistItemLicitacao');
    try {
        await bronze_db.insert(LicitacaoItem).values(data).onDuplicateKeyUpdate({
            set: {
                numeroLicitacao: sql`values(${LicitacaoItem.numeroLicitacao})`,
                codigoUg: sql`values(${LicitacaoItem.codigoUg})`,
                nomeUg: sql`values(${LicitacaoItem.nomeUg})`,
                codigoModalidadeCompra: sql`values(${LicitacaoItem.codigoModalidadeCompra})`,
                modalidadeCompra: sql`values(${LicitacaoItem.modalidadeCompra})`,
                numeroProcesso: sql`values(${LicitacaoItem.numeroProcesso})`,
                codigoOrgao: sql`values(${LicitacaoItem.codigoOrgao})`,
                nomeOrgao: sql`values(${LicitacaoItem.nomeOrgao})`,
                codigoItemCompra: sql`values(${LicitacaoItem.codigoItemCompra})`,
                descricaoItemCompra: sql`values(${LicitacaoItem.descricaoItemCompra})`,
                cnpjParticipante: sql`values(${LicitacaoItem.cnpjParticipante})`,
                nomeParticipante: sql`values(${LicitacaoItem.nomeParticipante})`,
                flagVencedor: sql`values(${LicitacaoItem.flagVencedor})`,
                exercicioReferencia: sql`values(${LicitacaoItem.exercicioReferencia})`,
                mesReferencia: sql`values(${LicitacaoItem.mesReferencia})`,
                diaReferencia: sql`values(${LicitacaoItem.diaReferencia})`,
                dadosAdicionais: sql`values(${LicitacaoItem.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Item Licitação`);
        throw error;
    }
}

async function persistLicitacao(data: (typeof Licitacao.$inferInsert)[]) {
    const log = logger.forMethod('persistLicitacao');
    try {
        await bronze_db.insert(Licitacao).values(data).onDuplicateKeyUpdate({
            set: {
                numeroLicitacao: sql`values(${Licitacao.numeroLicitacao})`,
                codigoUg: sql`values(${Licitacao.codigoUg})`,
                nomeUg: sql`values(${Licitacao.nomeUg})`,
                codigoModalidadeCompra: sql`values(${Licitacao.codigoModalidadeCompra})`,
                modalidadeCompra: sql`values(${Licitacao.modalidadeCompra})`,
                numeroProcesso: sql`values(${Licitacao.numeroProcesso})`,
                objeto: sql`values(${Licitacao.objeto})`,
                situacaoLicitacao: sql`values(${Licitacao.situacaoLicitacao})`,
                codigoOrgaoSuperior: sql`values(${Licitacao.codigoOrgaoSuperior})`,
                nomeOrgaoSuperior: sql`values(${Licitacao.nomeOrgaoSuperior})`,
                codigoOrgao: sql`values(${Licitacao.codigoOrgao})`,
                nomeOrgao: sql`values(${Licitacao.nomeOrgao})`,
                ufMunicipio: sql`values(${Licitacao.ufMunicipio})`,
                dataResultadoCompra: sql`values(${Licitacao.dataResultadoCompra})`,
                dataAbertura: sql`values(${Licitacao.dataAbertura})`,
                valorLicitacao: sql`values(${Licitacao.valorLicitacao})`,
                exercicioReferencia: sql`values(${Licitacao.exercicioReferencia})`,
                mesReferencia: sql`values(${Licitacao.mesReferencia})`,
                diaReferencia: sql`values(${Licitacao.diaReferencia})`,
                dadosAdicionais: sql`values(${Licitacao.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Licitação`);
        throw error;
    }
}

async function persistParticipantes(data: (typeof LicitacaoParticipantes.$inferInsert)[]) {
    const log = logger.forMethod('persistParticipantes');
    try {
        await bronze_db.insert(LicitacaoParticipantes).values(data).onDuplicateKeyUpdate({
            set: {
                numeroLicitacao: sql`values(${LicitacaoParticipantes.numeroLicitacao})`,
                codigoUg: sql`values(${LicitacaoParticipantes.codigoUg})`,
                nomeUg: sql`values(${LicitacaoParticipantes.nomeUg})`,
                codigoModalidadeCompra: sql`values(${LicitacaoParticipantes.codigoModalidadeCompra})`,
                modalidadeCompra: sql`values(${LicitacaoParticipantes.modalidadeCompra})`,
                numeroProcesso: sql`values(${LicitacaoParticipantes.numeroProcesso})`,
                codigoOrgao: sql`values(${LicitacaoParticipantes.codigoOrgao})`,
                nomeOrgao: sql`values(${LicitacaoParticipantes.nomeOrgao})`,
                codigoItemCompra: sql`values(${LicitacaoParticipantes.codigoItemCompra})`,
                descricaoItemCompra: sql`values(${LicitacaoParticipantes.descricaoItemCompra})`,
                cnpjParticipante: sql`values(${LicitacaoParticipantes.cnpjParticipante})`,
                nomeParticipante: sql`values(${LicitacaoParticipantes.nomeParticipante})`,
                flagVencedor: sql`values(${LicitacaoParticipantes.flagVencedor})`,
                exercicioReferencia: sql`values(${LicitacaoParticipantes.exercicioReferencia})`,
                mesReferencia: sql`values(${LicitacaoParticipantes.mesReferencia})`,
                diaReferencia: sql`values(${LicitacaoParticipantes.diaReferencia})`,
                dadosAdicionais: sql`values(${LicitacaoParticipantes.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Participantes da Licitação`);
        throw error;
    }
}

async function processEmpenhosRelacionadosStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processEmpenhosRelacionadosStream');
    let batch: (typeof LicitacaoEmpenhosRelacionados.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase().trim();
                if (h.includes('NÚMERO LICITAÇÃO') || h.includes('NUMERO LICITACAO')) return 'numeroLicitacao';
                if (h.includes('CÓDIGO UG') || h.includes('CODIGO UG')) return 'codigoUg';
                if (h.includes('NOME UG')) return 'nomeUg';
                if (h.includes('CÓDIGO MODALIDADE DE COMPRA') || h.includes('CODIGO MODALIDADE DE COMPRA') || h.includes('CÓDIGO MODALIDADE COMPRA')) return 'codigoModalidadeCompra';
                if (h.includes('MODALIDADE COMPRA') || h.includes('MODALIDADE DE COMPRA')) return 'modalidadeCompra';
                if (h.includes('NÚMERO PROCESSO') || h.includes('NUMERO PROCESSO')) return 'numeroProcesso';
                if (h.includes('CÓDIGO EMPENHO') || h.includes('CODIGO EMPENHO')) return 'codigoEmpenho';
                if (h.includes('DATA EMISSÃO EMPENHO') || h.includes('DATA EMISSAO EMPENHO')) return 'dataEmissaoEmpenho';
                if (h.includes('OBSERVAÇÃO EMPENHO') || h.includes('OBSERVACAO EMPENHO')) return 'observacaoEmpenho';
                if (h.includes('VALOR EMPENHO')) return 'valorEmpenho';
                return h;
            },
        }));

        for await (const data of parser) {
            batch.push({
                ...data,
                ...metadata,
            });
            if (batch.length >= BATCH_SIZE) {
                await persistEmpenhosRelacionados(batch);
                totalProcessed += batch.length;
                batch = [];
            }
        }
        if (batch.length > 0) {
            await persistEmpenhosRelacionados(batch);
            totalProcessed += batch.length;
        }
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream');
        throw error;
    }
}

async function processItemLicitacaoStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processItemLicitacaoStream');
    let batch: (typeof LicitacaoItem.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase().trim();
                if (h.includes('NÚMERO LICITAÇÃO') || h.includes('NUMERO LICITACAO')) return 'numeroLicitacao';
                if (h.includes('CÓDIGO UG') || h.includes('CODIGO UG')) return 'codigoUg';
                if (h.includes('NOME UG')) return 'nomeUg';
                if (h.includes('CÓDIGO MODALIDADE DE COMPRA') || h.includes('CODIGO MODALIDADE DE COMPRA') || h.includes('CÓDIGO MODALIDADE COMPRA')) return 'codigoModalidadeCompra';
                if (h.includes('MODALIDADE COMPRA') || h.includes('MODALIDADE DE COMPRA')) return 'modalidadeCompra';
                if (h.includes('NÚMERO PROCESSO') || h.includes('NUMERO PROCESSO')) return 'numeroProcesso';
                if (h.includes('CÓDIGO ÓRGÃO') || h.includes('CODIGO ORGAO')) return 'codigoOrgao';
                if (h.includes('NOME ÓRGÃO') || h.includes('NOME ORGAO')) return 'nomeOrgao';
                if (h.includes('CÓDIGO ITEM COMPRA') || h.includes('CODIGO ITEM COMPRA')) return 'codigoItemCompra';
                if (h.includes('DESCRIÇÃO ITEM COMPRA') || h.includes('DESCRICAO ITEM COMPRA')) return 'descricaoItemCompra';
                if (h.includes('CNPJ PARTICIPANTE') || h.includes('CÓDIGO VENCEDOR') || h.includes('CODIGO VENCEDOR')) return 'cnpjParticipante';
                if (h.includes('NOME PARTICIPANTE') || h.includes('NOME VENCEDOR')) return 'nomeParticipante';
                if (h.includes('FLAG VENCEDOR')) return 'flagVencedor';
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
                await persistItemLicitacao(batch);
                totalProcessed += batch.length;
                batch = [];
            }
        }
        if (batch.length > 0) {
            await persistItemLicitacao(batch);
            totalProcessed += batch.length;
        }
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream');
        throw error;
    }
}

async function processLicitacaoStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processLicitacaoStream');
    let batch: (typeof Licitacao.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase().trim();
                if (h.includes('NÚMERO LICITAÇÃO') || h.includes('NUMERO LICITACAO')) return 'numeroLicitacao';
                if (h.includes('CÓDIGO UG') || h.includes('CODIGO UG')) return 'codigoUg';
                if (h.includes('NOME UG')) return 'nomeUg';
                if (h.includes('CÓDIGO MODALIDADE DE COMPRA') || h.includes('CODIGO MODALIDADE DE COMPRA') || h.includes('CÓDIGO MODALIDADE COMPRA')) return 'codigoModalidadeCompra';
                if (h.includes('MODALIDADE COMPRA') || h.includes('MODALIDADE DE COMPRA')) return 'modalidadeCompra';
                if (h.includes('NÚMERO PROCESSO') || h.includes('NUMERO PROCESSO')) return 'numeroProcesso';
                if (h === 'OBJETO') return 'objeto';
                if (h.includes('SITUAÇÃO LICITAÇÃO') || h.includes('SITUACAO LICITACAO')) return 'situacaoLicitacao';
                if (h.includes('CÓDIGO DO ÓRGÃO SUPERIOR') || h.includes('CODIGO DO ORGAO SUPERIOR') || h.includes('CÓDIGO ÓRGÃO SUPERIOR')) return 'codigoOrgaoSuperior';
                if (h.includes('NOME ÓRGÃO SUPERIOR') || h.includes('NOME ORGAO SUPERIOR')) return 'nomeOrgaoSuperior';
                if (h.includes('CÓDIGO ÓRGÃO') || h.includes('CODIGO ORGAO')) return 'codigoOrgao';
                if (h.includes('NOME ÓRGÃO') || h.includes('NOME ORGAO')) return 'nomeOrgao';
                if (h.includes('UF/MUNICÍPIO') || h.includes('UF/MUNICIPIO')) return 'ufMunicipio';
                if (h.includes('DATA RESULTADO COMPRA')) return 'dataResultadoCompra';
                if (h.includes('DATA ABERTURA')) return 'dataAbertura';
                if (h.includes('VALOR LICITAÇÃO') || h.includes('VALOR LICITACAO')) return 'valorLicitacao';
                return h;
            },
        }));

        for await (const data of parser) {
            batch.push({
                ...data,
                ...metadata,
            });
            if (batch.length >= BATCH_SIZE) {
                await persistLicitacao(batch);
                totalProcessed += batch.length;
                batch = [];
            }
        }
        if (batch.length > 0) {
            await persistLicitacao(batch);
            totalProcessed += batch.length;
        }
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream');
        throw error;
    }
}

async function processParticipantesStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processParticipantesStream');
    let batch: (typeof LicitacaoParticipantes.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase().trim();
                if (h.includes('NÚMERO LICITAÇÃO') || h.includes('NUMERO LICITACAO')) return 'numeroLicitacao';
                if (h.includes('CÓDIGO UG') || h.includes('CODIGO UG')) return 'codigoUg';
                if (h.includes('NOME UG')) return 'nomeUg';
                if (h.includes('CÓDIGO MODALIDADE DE COMPRA') || h.includes('CODIGO MODALIDADE DE COMPRA') || h.includes('CÓDIGO MODALIDADE COMPRA')) return 'codigoModalidadeCompra';
                if (h.includes('MODALIDADE COMPRA') || h.includes('MODALIDADE DE COMPRA')) return 'modalidadeCompra';
                if (h.includes('NÚMERO PROCESSO') || h.includes('NUMERO PROCESSO')) return 'numeroProcesso';
                if (h.includes('CÓDIGO ÓRGÃO') || h.includes('CODIGO ORGAO')) return 'codigoOrgao';
                if (h.includes('NOME ÓRGÃO') || h.includes('NOME ORGAO')) return 'nomeOrgao';
                if (h.includes('CÓDIGO ITEM COMPRA') || h.includes('CODIGO ITEM COMPRA')) return 'codigoItemCompra';
                if (h.includes('DESCRIÇÃO ITEM COMPRA') || h.includes('DESCRICAO ITEM COMPRA')) return 'descricaoItemCompra';
                if (h.includes('CNPJ PARTICIPANTE')) return 'cnpjParticipante';
                if (h.includes('NOME PARTICIPANTE')) return 'nomeParticipante';
                if (h.includes('FLAG VENCEDOR')) return 'flagVencedor';
                return h;
            },
        }));

        for await (const data of parser) {
            batch.push({
                ...data,
                ...metadata,
            });
            if (batch.length >= BATCH_SIZE) {
                await persistParticipantes(batch);
                totalProcessed += batch.length;
                batch = [];
            }
        }
        if (batch.length > 0) {
            await persistParticipantes(batch);
            totalProcessed += batch.length;
        }
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

                            if (upperName.includes('EMPENHOSRELACIONADOS') || upperName.includes('EMPENHOS_RELACIONADOS')) {
                                rowsCount = await processEmpenhosRelacionadosStream(readStream, metadata);
                            } else if (upperName.includes('ITEMLICITACAO') || upperName.includes('ITEM_LICITACAO') || upperName.includes('ITEMLICITAÇÃO')) {
                                rowsCount = await processItemLicitacaoStream(readStream, metadata);
                            } else if (upperName.includes('PARTICIPANTESLICITACAO') || upperName.includes('PARTICIPANTES_LICITACAO') || upperName.includes('PARTICIPANTESLICITAÇÃO')) {
                                rowsCount = await processParticipantesStream(readStream, metadata);
                            } else if (upperName.includes('LICITACAO') || upperName.includes('LICITAÇÃO')) {
                                rowsCount = await processLicitacaoStream(readStream, metadata);
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

export async function runLicitacoesIngestion() {
    const log = logger.forMethod('runLicitacoesIngestion');
    const moduleName = 'licitacoes';

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