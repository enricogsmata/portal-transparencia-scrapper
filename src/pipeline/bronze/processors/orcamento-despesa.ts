import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import csvParser from 'csv-parser';
import iconv from 'iconv-lite';
import { sql } from 'drizzle-orm';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import { OrcamentoDespesa } from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY, extractMetadataFromFileName, ReferenceMetadata, normalize } from '../shared.js';

const logger = new_logger(import.meta.url);
const BATCH_SIZE = 2000;

type OrcamentoDespesaRow = typeof OrcamentoDespesa.$inferInsert;

async function persistData(data: OrcamentoDespesaRow[]) {
    const log = logger.forMethod('persistData');
    try {
        await bronze_db.insert(OrcamentoDespesa).values(data).onDuplicateKeyUpdate({
            set: {
                exercicio: sql`values(${OrcamentoDespesa.exercicio})`,
                codigoOrgaoSuperior: sql`values(${OrcamentoDespesa.codigoOrgaoSuperior})`,
                nomeOrgaoSuperior: sql`values(${OrcamentoDespesa.nomeOrgaoSuperior})`,
                codigoOrgaoSubordinado: sql`values(${OrcamentoDespesa.codigoOrgaoSubordinado})`,
                nomeOrgaoSubordinado: sql`values(${OrcamentoDespesa.nomeOrgaoSubordinado})`,
                codigoUnidadeOrcamentaria: sql`values(${OrcamentoDespesa.codigoUnidadeOrcamentaria})`,
                nomeUnidadeOrcamentaria: sql`values(${OrcamentoDespesa.nomeUnidadeOrcamentaria})`,
                codigoFuncao: sql`values(${OrcamentoDespesa.codigoFuncao})`,
                nomeFuncao: sql`values(${OrcamentoDespesa.nomeFuncao})`,
                codigoSubfuncao: sql`values(${OrcamentoDespesa.codigoSubfuncao})`,
                nomeSubfuncao: sql`values(${OrcamentoDespesa.nomeSubfuncao})`,
                codigoProgramaOrcamentario: sql`values(${OrcamentoDespesa.codigoProgramaOrcamentario})`,
                nomeProgramaOrcamentario: sql`values(${OrcamentoDespesa.nomeProgramaOrcamentario})`,
                codigoAcao: sql`values(${OrcamentoDespesa.codigoAcao})`,
                nomeAcao: sql`values(${OrcamentoDespesa.nomeAcao})`,
                codigoCategoriaEconomica: sql`values(${OrcamentoDespesa.codigoCategoriaEconomica})`,
                nomeCategoriaEconomica: sql`values(${OrcamentoDespesa.nomeCategoriaEconomica})`,
                codigoGrupoDespesa: sql`values(${OrcamentoDespesa.codigoGrupoDespesa})`,
                nomeGrupoDespesa: sql`values(${OrcamentoDespesa.nomeGrupoDespesa})`,
                codigoElementoDespesa: sql`values(${OrcamentoDespesa.codigoElementoDespesa})`,
                nomeElementoDespesa: sql`values(${OrcamentoDespesa.nomeElementoDespesa})`,
                orcamentoInicial: sql`values(${OrcamentoDespesa.orcamentoInicial})`,
                orcamentoAtualizado: sql`values(${OrcamentoDespesa.orcamentoAtualizado})`,
                orcamentoEmpenhado: sql`values(${OrcamentoDespesa.orcamentoEmpenhado})`,
                orcamentoRealizado: sql`values(${OrcamentoDespesa.orcamentoRealizado})`,
                percentualRealizado: sql`values(${OrcamentoDespesa.percentualRealizado})`,
                exercicioReferencia: sql`values(${OrcamentoDespesa.exercicioReferencia})`,
                mesReferencia: sql`values(${OrcamentoDespesa.mesReferencia})`,
                diaReferencia: sql`values(${OrcamentoDespesa.diaReferencia})`,
                dadosAdicionais: sql`values(${OrcamentoDespesa.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Orçamento Despesa`);
        throw error;
    }
}

interface HeaderRule {
    key: string;
    keywords?: string[];
    matchExact?: string[];
    require?: string[];
}

const ORCAMENTO_DESPESA_HEADER_RULES: HeaderRule[] = [
    { key: 'exercicio', matchExact: ['EXERCICIO'] },
    { key: 'percentualRealizado', keywords: ['REALIZADO DO ORCAMENTO'] },

    // Órgãos
    { key: 'codigoOrgaoSuperior', keywords: ['ORGAO SUPERIOR'], require: ['CODIGO'] },
    { key: 'nomeOrgaoSuperior', keywords: ['ORGAO SUPERIOR'], require: ['NOME'] },
    { key: 'codigoOrgaoSubordinado', keywords: ['ORGAO SUBORDINADO'], require: ['CODIGO'] },
    { key: 'nomeOrgaoSubordinado', keywords: ['ORGAO SUBORDINADO'], require: ['NOME'] },

    // Estrutura Orçamentária (SUBFUNÇÃO antes de FUNÇÃO)
    { key: 'codigoUnidadeOrcamentaria', keywords: ['UNIDADE ORCAMENTARIA'], require: ['CODIGO'] },
    { key: 'nomeUnidadeOrcamentaria', keywords: ['UNIDADE ORCAMENTARIA'], require: ['NOME'] },
    { key: 'codigoSubfuncao', keywords: ['SUBFUNCAO'], require: ['CODIGO'] },
    { key: 'nomeSubfuncao', keywords: ['SUBFUNCAO'], require: ['NOME'] },
    { key: 'codigoFuncao', keywords: ['FUNCAO'], require: ['CODIGO'] },
    { key: 'nomeFuncao', keywords: ['FUNCAO'], require: ['NOME'] },
    { key: 'codigoProgramaOrcamentario', keywords: ['PROGRAMA ORCAMENTARIO'], require: ['CODIGO'] },
    { key: 'nomeProgramaOrcamentario', keywords: ['PROGRAMA ORCAMENTARIO'], require: ['NOME'] },
    { key: 'codigoAcao', keywords: ['ACAO'], require: ['CODIGO'] },
    { key: 'nomeAcao', keywords: ['ACAO'], require: ['NOME'] },
    { key: 'codigoPlanoOrcamentario', keywords: ['PLANO ORCAMENTARIO'], require: ['CODIGO'] },
    { key: 'planoOrcamentario', keywords: ['PLANO ORCAMENTARIO'] },

    // Classificação de Despesa
    { key: 'codigoCategoriaEconomica', keywords: ['CATEGORIA ECONOMICA'], require: ['CODIGO'] },
    { key: 'nomeCategoriaEconomica', keywords: ['CATEGORIA ECONOMICA'], require: ['NOME'] },
    { key: 'codigoGrupoDespesa', keywords: ['GRUPO DE DESPESA', 'GRUPO DESPESA'], require: ['CODIGO'] },
    { key: 'nomeGrupoDespesa', keywords: ['GRUPO DE DESPESA', 'GRUPO DESPESA'], require: ['NOME'] },
    { key: 'codigoElementoDespesa', keywords: ['ELEMENTO DE DESPESA', 'ELEMENTO DESPESA'], require: ['CODIGO'] },
    { key: 'nomeElementoDespesa', keywords: ['ELEMENTO DE DESPESA', 'ELEMENTO DESPESA'], require: ['NOME'] },

    // Valores Orçamentários
    { key: 'orcamentoInicial', keywords: ['ORCAMENTO INICIAL'] },
    { key: 'orcamentoAtualizado', keywords: ['ORCAMENTO ATUALIZADO'] },
    { key: 'orcamentoEmpenhado', keywords: ['ORCAMENTO EMPENHADO'] },
    { key: 'orcamentoRealizado', keywords: ['ORCAMENTO REALIZADO'] },
];

// Helper genérico para busca de chave
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

export async function processCsvStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processCsvStream');
    let batch: OrcamentoDespesaRow[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => resolveHeaderKey(header, ORCAMENTO_DESPESA_HEADER_RULES),
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

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento do Orçamento da Despesa concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream do Orçamento da Despesa');
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

                            if (upperName.includes('ORCAMENTO') || upperName.includes('ORÇAMENTO')) {
                                rowsCount = await processCsvStream(readStream, metadata);
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

export async function runOrcamentoDespesaIngestion() {
    const log = logger.forMethod('runOrcamentoDespesaIngestion');
    const moduleName = 'orcamento-despesa';

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