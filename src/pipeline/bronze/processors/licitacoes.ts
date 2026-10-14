import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import csvParser from 'csv-parser';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import {
    LicitacaoEmpenhosRelacionados,
    LicitacaoItem,
    Licitacao,
    LicitacaoParticipantes
} from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY } from '../shared.js';

const logger = new_logger(import.meta.url);

const BATCH_SIZE = 2000;

async function persistEmpenhosRelacionados(data: (typeof LicitacaoEmpenhosRelacionados.$inferInsert)[]) {
    const log = logger.forMethod('persistEmpenhosRelacionados');
    try {
        await bronze_db.insert(LicitacaoEmpenhosRelacionados).values(data);
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Empenhos Relacionados`);
        throw error;
    }
}

async function persistItemLicitacao(data: (typeof LicitacaoItem.$inferInsert)[]) {
    const log = logger.forMethod('persistItemLicitacao');
    try {
        await bronze_db.insert(LicitacaoItem).values(data);
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Item Licitação`);
        throw error;
    }
}

async function persistLicitacao(data: (typeof Licitacao.$inferInsert)[]) {
    const log = logger.forMethod('persistLicitacao');
    try {
        await bronze_db.insert(Licitacao).values(data);
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Licitação`);
        throw error;
    }
}

async function persistParticipantes(data: (typeof LicitacaoParticipantes.$inferInsert)[]) {
    const log = logger.forMethod('persistParticipantes');
    try {
        await bronze_db.insert(LicitacaoParticipantes).values(data);
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Participantes da Licitação`);
        throw error;
    }
}

async function processEmpenhosRelacionadosStream(stream: NodeJS.ReadableStream): Promise<number> {
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
            batch.push(data);
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

async function processItemLicitacaoStream(stream: NodeJS.ReadableStream): Promise<number> {
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
                // Para o arquivo ItemLicitação
                if (h.includes('QUANTIDADE ITEM')) return 'quantidadeItem'; // Adicionado, embora não esteja no schema final, para evitar erro se houver
                if (h.includes('VALOR ITEM')) return 'valorItem';           // Adicionado, embora não esteja no schema final, para evitar erro se houver

                return h;
            },
        }));

        for await (const data of parser) {
            batch.push(data);
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

async function processLicitacaoStream(stream: NodeJS.ReadableStream): Promise<number> {
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
            batch.push(data);
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

async function processParticipantesStream(stream: NodeJS.ReadableStream): Promise<number> {
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
            batch.push(data);
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

                            if (upperName.includes('EMPENHOSRELACIONADOS') || upperName.includes('EMPENHOS_RELACIONADOS')) {
                                rowsCount = await processEmpenhosRelacionadosStream(readStream);
                            } else if (upperName.includes('ITEMLICITACAO') || upperName.includes('ITEM_LICITACAO') || upperName.includes('ITEMLICITAÇÃO')) {
                                rowsCount = await processItemLicitacaoStream(readStream);
                            } else if (upperName.includes('PARTICIPANTESLICITACAO') || upperName.includes('PARTICIPANTES_LICITACAO') || upperName.includes('PARTICIPANTESLICITAÇÃO')) {
                                rowsCount = await processParticipantesStream(readStream);
                            } else if (upperName.includes('LICITACAO') || upperName.includes('LICITAÇÃO')) {
                                rowsCount = await processLicitacaoStream(readStream);
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