import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import iconv from 'iconv-lite';
import csvParser from 'csv-parser';
import { normalize } from '../shared.js';
import { sql } from 'drizzle-orm';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import { ExecucaoDespesa } from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY, extractMetadataFromFileName, ReferenceMetadata } from '../shared.js';

const logger = new_logger(import.meta.url);

type ExecucaoDespesaRow = typeof ExecucaoDespesa.$inferInsert;

async function persistData(data: ExecucaoDespesaRow[]) {
    const log = logger.forMethod('persistData');
    try {
        await bronze_db.insert(ExecucaoDespesa).values(data).onDuplicateKeyUpdate({
            set: {
                anoMesLancamento: sql`values(${ExecucaoDespesa.anoMesLancamento})`,
                codigoOrgaoSuperior: sql`values(${ExecucaoDespesa.codigoOrgaoSuperior})`,
                nomeOrgaoSuperior: sql`values(${ExecucaoDespesa.nomeOrgaoSuperior})`,
                codigoOrgaoSubordinado: sql`values(${ExecucaoDespesa.codigoOrgaoSubordinado})`,
                nomeOrgaoSubordinado: sql`values(${ExecucaoDespesa.nomeOrgaoSubordinado})`,
                codigoUnidadeGestora: sql`values(${ExecucaoDespesa.codigoUnidadeGestora})`,
                nomeUnidadeGestora: sql`values(${ExecucaoDespesa.nomeUnidadeGestora})`,
                codigoGestao: sql`values(${ExecucaoDespesa.codigoGestao})`,
                nomeGestao: sql`values(${ExecucaoDespesa.nomeGestao})`,
                codigoUnidadeOrcamentaria: sql`values(${ExecucaoDespesa.codigoUnidadeOrcamentaria})`,
                nomeUnidadeOrcamentaria: sql`values(${ExecucaoDespesa.nomeUnidadeOrcamentaria})`,
                codigoFuncao: sql`values(${ExecucaoDespesa.codigoFuncao})`,
                nomeFuncao: sql`values(${ExecucaoDespesa.nomeFuncao})`,
                codigoSubfuncao: sql`values(${ExecucaoDespesa.codigoSubfuncao})`,
                nomeSubfuncao: sql`values(${ExecucaoDespesa.nomeSubfuncao})`,
                codigoProgramaOrcamentario: sql`values(${ExecucaoDespesa.codigoProgramaOrcamentario})`,
                nomeProgramaOrcamentario: sql`values(${ExecucaoDespesa.nomeProgramaOrcamentario})`,
                codigoAcao: sql`values(${ExecucaoDespesa.codigoAcao})`,
                nomeAcao: sql`values(${ExecucaoDespesa.nomeAcao})`,
                codigoPlanoOrcamentario: sql`values(${ExecucaoDespesa.codigoPlanoOrcamentario})`,
                planoOrcamentario: sql`values(${ExecucaoDespesa.planoOrcamentario})`,
                codigoProgramaGoverno: sql`values(${ExecucaoDespesa.codigoProgramaGoverno})`,
                nomeProgramaGoverno: sql`values(${ExecucaoDespesa.nomeProgramaGoverno})`,
                uf: sql`values(${ExecucaoDespesa.uf})`,
                municipio: sql`values(${ExecucaoDespesa.municipio})`,
                codigoSubtitulo: sql`values(${ExecucaoDespesa.codigoSubtitulo})`,
                nomeSubtitulo: sql`values(${ExecucaoDespesa.nomeSubtitulo})`,
                codigoLocalizador: sql`values(${ExecucaoDespesa.codigoLocalizador})`,
                nomeLocalizador: sql`values(${ExecucaoDespesa.nomeLocalizador})`,
                siglaLocalizador: sql`values(${ExecucaoDespesa.siglaLocalizador})`,
                descricaoComplementarLocalizador: sql`values(${ExecucaoDespesa.descricaoComplementarLocalizador})`,
                codigoAutorEmenda: sql`values(${ExecucaoDespesa.codigoAutorEmenda})`,
                nomeAutorEmenda: sql`values(${ExecucaoDespesa.nomeAutorEmenda})`,
                codigoCategoriaEconomica: sql`values(${ExecucaoDespesa.codigoCategoriaEconomica})`,
                nomeCategoriaEconomica: sql`values(${ExecucaoDespesa.nomeCategoriaEconomica})`,
                codigoGrupoDespesa: sql`values(${ExecucaoDespesa.codigoGrupoDespesa})`,
                nomeGrupoDespesa: sql`values(${ExecucaoDespesa.nomeGrupoDespesa})`,
                codigoElementoDespesa: sql`values(${ExecucaoDespesa.codigoElementoDespesa})`,
                nomeElementoDespesa: sql`values(${ExecucaoDespesa.nomeElementoDespesa})`,
                codigoModalidadeDespesa: sql`values(${ExecucaoDespesa.codigoModalidadeDespesa})`,
                modalidadeDespesa: sql`values(${ExecucaoDespesa.modalidadeDespesa})`,
                valorEmpenhado: sql`values(${ExecucaoDespesa.valorEmpenhado})`,
                valorLiquidado: sql`values(${ExecucaoDespesa.valorLiquidado})`,
                valorPago: sql`values(${ExecucaoDespesa.valorPago})`,
                valorRestosAPagarInscritos: sql`values(${ExecucaoDespesa.valorRestosAPagarInscritos})`,
                valorRestosAPagarCancelado: sql`values(${ExecucaoDespesa.valorRestosAPagarCancelado})`,
                valorRestosAPagarPagos: sql`values(${ExecucaoDespesa.valorRestosAPagarPagos})`,
                exercicioReferencia: sql`values(${ExecucaoDespesa.exercicioReferencia})`,
                mesReferencia: sql`values(${ExecucaoDespesa.mesReferencia})`,
                diaReferencia: sql`values(${ExecucaoDespesa.diaReferencia})`,
                dadosAdicionais: sql`values(${ExecucaoDespesa.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência`);
        throw error;
    }
}

// Rules map para ExecucaoDespesa (Despesas Execução)
const EXECUCAO_DESPESA_HEADER_RULES = [
    { key: 'anoMesLancamento', keywords: ['ANO E MES', 'ANO MES'] },

    // Órgãos, UG ken Gestão
    { key: 'codigoOrgaoSuperior', keywords: ['ORGAO SUPERIOR'], require: ['CODIGO'] },
    { key: 'nomeOrgaoSuperior', keywords: ['ORGAO SUPERIOR'], require: ['NOME'] },
    { key: 'codigoOrgaoSubordinado', keywords: ['ORGAO SUBORDINADO'], require: ['CODIGO'] },
    { key: 'nomeOrgaoSubordinado', keywords: ['ORGAO SUBORDINADO'], require: ['NOME'] },
    { key: 'codigoUnidadeGestora', keywords: ['UNIDADE GESTORA'], require: ['CODIGO'] },
    { key: 'nomeUnidadeGestora', keywords: ['UNIDADE GESTORA'], require: ['NOME'] },
    { key: 'codigoGestao', keywords: ['GESTAO'], require: ['CODIGO'] },
    { key: 'nomeGestao', keywords: ['GESTAO'], require: ['NOME'] },

    // Estrutura Orçamentária
    { key: 'codigoUnidadeOrcamentaria', keywords: ['UNIDADE ORCAMENTARIA'], require: ['CODIGO'] },
    { key: 'nomeUnidadeOrcamentaria', keywords: ['UNIDADE ORCAMENTARIA'], require: ['NOME'] },
    { key: 'codigoSubfuncao', keywords: ['SUBFUNCAO', 'SUBFUCAO'], require: ['CODIGO'] },
    { key: 'nomeSubfuncao', keywords: ['SUBFUNCAO', 'SUBFUCAO'], require: ['NOME'] },
    { key: 'codigoFuncao', keywords: ['FUNCAO'], require: ['CODIGO'] },
    { key: 'nomeFuncao', keywords: ['FUNCAO'], require: ['NOME'] },
    { key: 'codigoProgramaOrcamentario', keywords: ['PROGRAMA ORCAMENTARIO'], require: ['CODIGO'] },
    { key: 'nomeProgramaOrcamentario', keywords: ['PROGRAMA ORCAMENTARIO'], require: ['NOME'] },
    { key: 'codigoAcao', keywords: ['ACAO'], require: ['CODIGO'] },
    { key: 'nomeAcao', keywords: ['ACAO'], require: ['NOME'] },
    { key: 'codigoPlanoOrcamentario', keywords: ['PLANO ORCAMENTARIO'], require: ['CODIGO'] },
    { key: 'planoOrcamentario', keywords: ['PLANO ORCAMENTARIO'] },
    { key: 'codigoProgramaGoverno', keywords: ['PROGRAMA GOVERNO'], require: ['CODIGO'] },
    { key: 'nomeProgramaGoverno', keywords: ['PROGRAMA GOVERNO'], require: ['NOME'] },

    // Localização ken Emendas
    { key: 'uf', matchExact: ['UF'] },
    { key: 'municipio', keywords: ['MUNICIPIO'] },
    { key: 'codigoSubtitulo', keywords: ['SUBTITULO'], require: ['CODIGO'] },
    { key: 'nomeSubtitulo', keywords: ['SUBTITULO'], require: ['NOME'] },
    { key: 'codigoLocalizador', keywords: ['LOCALIZADOR'], require: ['CODIGO'] },
    { key: 'nomeLocalizador', keywords: ['LOCALIZADOR'], require: ['NOME'] },
    { key: 'siglaLocalizador', keywords: ['LOCALIZADOR'], require: ['SIGLA'] },
    { key: 'descricaoComplementarLocalizador', keywords: ['DESCRICAO COMPLEMENTAR'] },
    { key: 'codigoAutorEmenda', keywords: ['AUTOR EMENDA'], require: ['CODIGO'] },
    { key: 'nomeAutorEmenda', keywords: ['AUTOR EMENDA'], require: ['NOME'] },

    // Classificação da Despesa
    { key: 'codigoCategoriaEconomica', keywords: ['CATEGORIA ECONOMICA'], require: ['CODIGO'] },
    { key: 'nomeCategoriaEconomica', keywords: ['CATEGORIA ECONOMICA'], require: ['NOME'] },
    { key: 'codigoGrupoDespesa', keywords: ['GRUPO DE DESPESA', 'GRUPO DESPESA'], require: ['CODIGO'] },
    { key: 'nomeGrupoDespesa', keywords: ['GRUPO DE DESPESA', 'GRUPO DESPESA'], require: ['NOME'] },
    { key: 'codigoElementoDespesa', keywords: ['ELEMENTO DE DESPESA', 'ELEMENTO DESPESA'], require: ['CODIGO'] },
    { key: 'nomeElementoDespesa', keywords: ['ELEMENTO DE DESPESA', 'ELEMENTO DESPESA'], require: ['NOME'] },
    { key: 'codigoModalidadeDespesa', keywords: ['MODALIDADE DA DESPESA', 'MODALIDADE DESPESA'], require: ['CODIGO'] },
    { key: 'modalidadeDespesa', keywords: ['MODALIDADE DA DESPESA', 'MODALIDADE DESPESA'] },

    // Valores
    { key: 'valorEmpenhado', keywords: ['VALOR EMPENHADO'] },
    { key: 'valorLiquidado', keywords: ['VALOR LIQUIDADO'] },
    { key: 'valorPago', keywords: ['VALOR PAGO'] },
    { key: 'valorRestosAPagarInscritos', keywords: ['RESTOS A PAGAR INSCRITOS'] },
    { key: 'valorRestosAPagarCancelado', keywords: ['RESTOS A PAGAR CANCELADO'] },
    { key: 'valorRestosAPagarPagos', keywords: ['RESTOS A PAGAR PAGOS'] }
];

export async function processCsvStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processCsvStream');
    const BATCH_SIZE = 2000;
    let batch: ExecucaoDespesaRow[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => {
                    if (!header) return null;

                    const cleanHeader = normalize(header);

                    for (const rule of EXECUCAO_DESPESA_HEADER_RULES) {
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

export async function runExecucaoDespesaIngestion() {
    const log = logger.forMethod('runExecucaoDespesaIngestion');
    const moduleName = 'despesas-execucao';

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