import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import csvParser from 'csv-parser';
import iconv from 'iconv-lite';
import { sql } from 'drizzle-orm';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import { ApoiamentosEmendasParlamentares } from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY, extractMetadataFromFileName, ReferenceMetadata, normalize } from '../shared.js';

const logger = new_logger(import.meta.url);

type ApoiamentosEmendasParlamentaresRow = typeof ApoiamentosEmendasParlamentares.$inferInsert;

async function persistData(data: ApoiamentosEmendasParlamentaresRow[]) {
    const log = logger.forMethod('persistData');
    try {
        await bronze_db.insert(ApoiamentosEmendasParlamentares).values(data).onDuplicateKeyUpdate({
            set: {
                codigoApoiador: sql`values(${ApoiamentosEmendasParlamentares.codigoApoiador})`,
                apoiador: sql`values(${ApoiamentosEmendasParlamentares.apoiador})`,
                dataApoio: sql`values(${ApoiamentosEmendasParlamentares.dataApoio})`,
                dataRetiradaApoio: sql`values(${ApoiamentosEmendasParlamentares.dataRetiradaApoio})`,
                empenho: sql`values(${ApoiamentosEmendasParlamentares.empenho})`,
                dataUltimaMovimentacaoEmpenho: sql`values(${ApoiamentosEmendasParlamentares.dataUltimaMovimentacaoEmpenho})`,
                codigoFavorecido: sql`values(${ApoiamentosEmendasParlamentares.codigoFavorecido})`,
                favorecido: sql`values(${ApoiamentosEmendasParlamentares.favorecido})`,
                tipoFavorecido: sql`values(${ApoiamentosEmendasParlamentares.tipoFavorecido})`,
                ufFavorecido: sql`values(${ApoiamentosEmendasParlamentares.ufFavorecido})`,
                municipioFavorecido: sql`values(${ApoiamentosEmendasParlamentares.municipioFavorecido})`,
                codigoEmenda: sql`values(${ApoiamentosEmendasParlamentares.codigoEmenda})`,
                tipoEmenda: sql`values(${ApoiamentosEmendasParlamentares.tipoEmenda})`,
                anoEmenda: sql`values(${ApoiamentosEmendasParlamentares.anoEmenda})`,
                codigoUg: sql`values(${ApoiamentosEmendasParlamentares.codigoUg})`,
                ug: sql`values(${ApoiamentosEmendasParlamentares.ug})`,
                codigoUnidadeOrcamentaria: sql`values(${ApoiamentosEmendasParlamentares.codigoUnidadeOrcamentaria})`,
                unidadeOrcamentaria: sql`values(${ApoiamentosEmendasParlamentares.unidadeOrcamentaria})`,
                codigoOrgaoSiafi: sql`values(${ApoiamentosEmendasParlamentares.codigoOrgaoSiafi})`,
                orgao: sql`values(${ApoiamentosEmendasParlamentares.orgao})`,
                codigoOrgaoSuperiorSiafi: sql`values(${ApoiamentosEmendasParlamentares.codigoOrgaoSuperiorSiafi})`,
                orgaoSuperior: sql`values(${ApoiamentosEmendasParlamentares.orgaoSuperior})`,
                codigoAcao: sql`values(${ApoiamentosEmendasParlamentares.codigoAcao})`,
                acao: sql`values(${ApoiamentosEmendasParlamentares.acao})`,
                valorEmpenhado: sql`values(${ApoiamentosEmendasParlamentares.valorEmpenhado})`,
                valorCancelado: sql`values(${ApoiamentosEmendasParlamentares.valorCancelado})`,
                valorPago: sql`values(${ApoiamentosEmendasParlamentares.valorPago})`,
                exercicioReferencia: sql`values(${ApoiamentosEmendasParlamentares.exercicioReferencia})`,
                mesReferencia: sql`values(${ApoiamentosEmendasParlamentares.mesReferencia})`,
                diaReferencia: sql`values(${ApoiamentosEmendasParlamentares.diaReferencia})`,
                dadosAdicionais: sql`values(${ApoiamentosEmendasParlamentares.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência`);
        throw error;
    }
}
interface HeaderRule {
    key: string;
    keywords?: string[];
    matchExact?: string[];
    require?: string[];
}

const APOIAMENTOS_EMENDAS_HEADER_RULES: HeaderRule[] = [
    // Apoiador e Apoio
    { key: 'codigoApoiador', keywords: ['APOIADOR'], require: ['CODIGO'] },
    { key: 'apoiador', matchExact: ['APOIADOR', 'NOME APOIADOR'] },
    { key: 'dataApoio', keywords: ['DATA DO APOIO', 'DATA APOIO'] },
    { key: 'dataRetiradaApoio', keywords: ['DATA RETIRADA DO APOIO', 'RETIRADA APOIO'] },

    // Empenho e Movimentação
    { key: 'dataUltimaMovimentacaoEmpenho', keywords: ['MOVIMENTACAO'] },
    { key: 'empenho', matchExact: ['EMPENHO', 'NUMERO EMPENHO'] },

    // Favorecido
    { key: 'codigoFavorecido', keywords: ['FAVORECIDO'], require: ['CODIGO'] },
    { key: 'tipoFavorecido', keywords: ['FAVORECIDO'], require: ['TIPO'] },
    { key: 'ufFavorecido', keywords: ['FAVORECIDO'], require: ['UF'] },
    { key: 'municipioFavorecido', keywords: ['FAVORECIDO'], require: ['MUNICIPIO'] },
    { key: 'favorecido', matchExact: ['FAVORECIDO', 'NOME FAVORECIDO'] },

    // Emenda
    { key: 'codigoEmenda', keywords: ['EMENDA'], require: ['CODIGO'] },
    { key: 'tipoEmenda', keywords: ['EMENDA'], require: ['TIPO'] },
    { key: 'anoEmenda', keywords: ['EMENDA'], require: ['ANO'] },

    // Unidades e Órgãos (Órgão Superior antes de Órgão genérico)
    { key: 'codigoUg', keywords: ['UG'], require: ['CODIGO'] },
    { key: 'ug', matchExact: ['UG'] },
    { key: 'codigoUnidadeOrcamentaria', keywords: ['UNIDADE ORCAMENTARIA'], require: ['CODIGO'] },
    { key: 'unidadeOrcamentaria', keywords: ['UNIDADE ORCAMENTARIA'] },

    { key: 'codigoOrgaoSuperiorSiafi', keywords: ['ORGAO SUPERIOR'], require: ['CODIGO'] },
    { key: 'orgaoSuperior', keywords: ['ORGAO SUPERIOR'] },
    { key: 'codigoOrgaoSiafi', keywords: ['ORGAO SIAFI', 'ORGAO'], require: ['CODIGO'] },
    { key: 'orgao', matchExact: ['ORGAO'] },

    // Estrutura Orçamentária e Valores
    { key: 'codigoAcao', keywords: ['ACAO'], require: ['CODIGO'] },
    { key: 'acao', matchExact: ['ACAO'] },
    { key: 'valorEmpenhado', keywords: ['VALOR EMPENHADO'] },
    { key: 'valorCancelado', keywords: ['VALOR CANCELADO'] },
    { key: 'valorPago', keywords: ['VALOR PAGO'] },
];

export async function processCsvStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processCsvStream');
    const BATCH_SIZE = 2000;
    let batch: ApoiamentosEmendasParlamentaresRow[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => {
                    if (!header) return null;

                    const cleanHeader = normalize(header);

                    for (const rule of APOIAMENTOS_EMENDAS_HEADER_RULES) {
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

export async function runApoiamentosEmendasParlamentaresIngestion() {
    const log = logger.forMethod('runApoiamentosEmendasParlamentaresIngestion');
    const moduleName = 'apoiamento-emendas-parlamentares-documentos';

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