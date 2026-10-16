import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import csvParser from 'csv-parser';
import iconv from 'iconv-lite';
import { sql } from 'drizzle-orm';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import {
    LicitacaoEmpenhosRelacionados,
    LicitacaoItem,
    Licitacao,
    LicitacaoParticipantes
} from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY, extractMetadataFromFileName, ReferenceMetadata, normalize } from '../shared.js';

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
                uf: sql`values(${Licitacao.uf})`,
                municipio: sql`values(${Licitacao.municipio})`,
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

interface HeaderRule {
    key: string;
    keywords?: string[];
    matchExact?: string[];
    require?: string[];
}


// Regras Comuns do Módulo de Licitações
const COMMON_LICITACAO_RULES: HeaderRule[] = [
    { key: 'numeroLicitacao', keywords: ['NUMERO LICITACAO', 'NUMERO DA LICITACAO'] },
    { key: 'codigoUg', keywords: ['UG'], require: ['CODIGO'] },
    { key: 'nomeUg', keywords: ['UG'], require: ['NOME'] },
    { key: 'codigoModalidadeCompra', keywords: ['MODALIDADE COMPRA', 'MODALIDADE DE COMPRA'], require: ['CODIGO'] },
    { key: 'modalidadeCompra', keywords: ['MODALIDADE COMPRA', 'MODALIDADE DE COMPRA'] },
    { key: 'numeroProcesso', keywords: ['PROCESSO', 'NUMERO PROCESSO'] },

    // Órgãos (Superior antes do Genérico)
    { key: 'codigoOrgaoSuperior', keywords: ['ORGAO SUPERIOR'], require: ['CODIGO'] },
    { key: 'nomeOrgaoSuperior', keywords: ['ORGAO SUPERIOR'], require: ['NOME'] },
    { key: 'codigoOrgao', keywords: ['ORGAO'], require: ['CODIGO'] },
    { key: 'nomeOrgao', keywords: ['ORGAO'], require: ['NOME'] },
];

const EMPENHOS_RELACIONADOS_RULES: HeaderRule[] = [
    { key: 'codigoEmpenho', keywords: ['EMPENHO'], require: ['CODIGO'] },
    { key: 'dataEmissaoEmpenho', keywords: ['EMISSÃO EMPENHO', 'EMISSAO EMPENHO', 'DATA EMPENHO'] },
    { key: 'observacaoEmpenho', keywords: ['OBSERVACAO EMPENHO', 'OBSERVACAO'] },
    { key: 'valorEmpenho', keywords: ['VALOR EMPENHO'] },
];

const ITEM_LICITACAO_RULES: HeaderRule[] = [
    { key: 'codigoItemCompra', keywords: ['ITEM COMPRA', 'ITEM DE COMPRA'], require: ['CODIGO'] },
    { key: 'descricaoItemCompra', keywords: ['DESCRICAO'], require: ['DESCRICAO'] },
    { key: 'flagVencedor', keywords: ['CODIGO VENCEDOR'], require: ['CODIGO'] },
    { key: 'cnpjParticipante', keywords: ['CNPJ PARTICIPANTE', 'PARTICIPANTE', 'VENCEDOR'], require: ['CNPJ', 'CODIGO'] },
    { key: 'nomeParticipante', keywords: ['NOME PARTICIPANTE', 'PARTICIPANTE', 'VENCEDOR'], require: ['NOME'] },
    { key: 'quantidadeItem', keywords: ['QUANTIDADE ITEM', 'QUANTIDADE'] },
    { key: 'valorItem', keywords: ['VALOR ITEM', 'VALOR DO ITEM'] },
];

const LICITACAO_MAIN_RULES: HeaderRule[] = [
    { key: 'objeto', matchExact: ['OBJETO', 'OBJETO LICITACAO'] },
    { key: 'situacaoLicitacao', keywords: ['SITUACAO LICITACAO', 'SITUACAO DA LICITACAO'] },
    { key: 'uf', matchExact: ['UF'] },
    { key: 'municipio', keywords: ['MUNICIPIO'] },
    { key: 'dataResultadoCompra', keywords: ['RESULTADO COMPRA', 'RESULTADO DA COMPRA'] },
    { key: 'dataAbertura', keywords: ['DATA ABERTURA', 'ABERTURA'] },
    { key: 'valorLicitacao', keywords: ['VALOR LICITACAO', 'VALOR DA LICITACAO'] },
];

const PARTICIPANTES_RULES: HeaderRule[] = [
    { key: 'codigoItemCompra', keywords: ['ITEM COMPRA', 'ITEM DE COMPRA'], require: ['CODIGO'] },
    { key: 'descricaoItemCompra', keywords: ['ITEM COMPRA', 'ITEM DE COMPRA'], require: ['DESCRICAO'] },
    { key: 'cnpjParticipante', keywords: ['CNPJ PARTICIPANTE', 'CNPJ DO PARTICIPANTE', 'CODIGO PARTICIPANTE'] },
    { key: 'nomeParticipante', keywords: ['NOME PARTICIPANTE', 'NOME DO PARTICIPANTE'] },
    { key: 'flagVencedor', keywords: ['FLAG VENCEDOR', 'VENCEDOR'] },
];

// Helper genérico para buscar nas regras específicas + comuns
function resolveLicitacaoHeaderKey(header: string, specificRules: HeaderRule[]): string {
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

    // 2. Se não casou, tenta as regras comuns de Licitações
    for (const rule of COMMON_LICITACAO_RULES) {
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

async function processEmpenhosRelacionadosStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processEmpenhosRelacionadosStream');
    let batch: (typeof LicitacaoEmpenhosRelacionados.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => resolveLicitacaoHeaderKey(header, EMPENHOS_RELACIONADOS_RULES),
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

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento de Empenhos Relacionados concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream de Empenhos Relacionados');
        throw error;
    }
}

async function processItemLicitacaoStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processItemLicitacaoStream');
    let batch: (typeof LicitacaoItem.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => resolveLicitacaoHeaderKey(header, ITEM_LICITACAO_RULES),
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

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento de Itens da Licitação concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream de Itens da Licitação');
        throw error;
    }
}

async function processLicitacaoStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processLicitacaoStream');
    let batch: (typeof Licitacao.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => resolveLicitacaoHeaderKey(header, LICITACAO_MAIN_RULES),
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

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento de Licitações concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream de Licitações');
        throw error;
    }
}

async function processParticipantesStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processParticipantesStream');
    let batch: (typeof LicitacaoParticipantes.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => resolveLicitacaoHeaderKey(header, PARTICIPANTES_RULES),
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

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento de Participantes concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream de Participantes');
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