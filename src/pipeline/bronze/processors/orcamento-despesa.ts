import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import csvParser from 'csv-parser';
import { sql } from 'drizzle-orm';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import { OrcamentoDespesa } from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY, extractMetadataFromFileName, ReferenceMetadata } from '../shared.js';

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
                codigoPlanoOrcamentario: sql`values(${OrcamentoDespesa.codigoPlanoOrcamentario})`,
                planoOrcamentario: sql`values(${OrcamentoDespesa.planoOrcamentario})`,
                codigoProgramaGoverno: sql`values(${OrcamentoDespesa.codigoProgramaGoverno})`,
                nomeProgramaGoverno: sql`values(${OrcamentoDespesa.nomeProgramaGoverno})`,
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

export async function processCsvStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processCsvStream');
    let batch: OrcamentoDespesaRow[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase().trim();

                if (h === 'EXERCÍCIO' || h === 'EXERCICIO') return 'exercicio';
                if (h.includes('CÓDIGO ÓRGÃO SUPERIOR') || h.includes('CODIGO ORGAO SUPERIOR')) return 'codigoOrgaoSuperior';
                if (h.includes('NOME ÓRGÃO SUPERIOR') || h.includes('NOME ORGAO SUPERIOR')) return 'nomeOrgaoSuperior';
                if (h.includes('CÓDIGO ÓRGÃO SUBORDINADO') || h.includes('CODIGO ORGAO SUBORDINADO')) return 'codigoOrgaoSubordinado';
                if (h.includes('NOME ÓRGÃO SUBORDINADO') || h.includes('NOME ORGAO SUBORDINADO')) return 'nomeOrgaoSubordinado';
                if (h.includes('CÓDIGO UNIDADE ORÇAMENTÁRIA') || h.includes('CODIGO UNIDADE ORCAMENTARIA')) return 'codigoUnidadeOrcamentaria';
                if (h.includes('NOME UNIDADE ORÇAMENTÁRIA') || h.includes('NOME UNIDADE ORCAMENTARIA')) return 'nomeUnidadeOrcamentaria';
                if (h.includes('CÓDIGO FUNÇÃO') || h.includes('CODIGO FUNCAO')) return 'codigoFuncao';
                if (h.includes('NOME FUNÇÃO') || h.includes('NOME FUNCAO')) return 'nomeFuncao';
                if (h.includes('CÓDIGO SUBFUNÇÃO') || h.includes('CODIGO SUBFUNCAO')) return 'codigoSubfuncao';
                if (h.includes('NOME SUBFUNÇÃO') || h.includes('NOME SUBFUNCAO')) return 'nomeSubfuncao';
                if (h.includes('CÓDIGO PROGRAMA ORÇAMENTÁRIO') || h.includes('CODIGO PROGRAMA ORCAMENTARIO')) return 'codigoProgramaOrcamentario';
                if (h.includes('NOME PROGRAMA ORÇAMENTÁRIO') || h.includes('NOME PROGRAMA ORCAMENTARIO')) return 'nomeProgramaOrcamentario';
                if (h.includes('CÓDIGO AÇÃO') || h.includes('CODIGO ACAO')) return 'codigoAcao';
                if (h.includes('NOME AÇÃO') || h.includes('NOME ACAO')) return 'nomeAcao';
                if (h.includes('CÓDIGO PLANO ORÇAMENTÁRIO') || h.includes('CODIGO PLANO ORCAMENTARIO')) return 'codigoPlanoOrcamentario';
                if (h === 'PLANO ORÇAMENTÁRIO' || h === 'PLANO ORCAMENTARIO') return 'planoOrcamentario';
                if (h.includes('CÓDIGO PROGRAMA GOVERNO') || h.includes('CODIGO PROGRAMA GOVERNO')) return 'codigoProgramaGoverno';
                if (h.includes('NOME PROGRAMA GOVERNO')) return 'nomeProgramaGoverno';
                if (h.includes('CÓDIGO CATEGORIA ECONÔMICA') || h.includes('CODIGO CATEGORIA ECONOMICA')) return 'codigoCategoriaEconomica';
                if (h.includes('NOME CATEGORIA ECONÔMICA') || h.includes('NOME CATEGORIA ECONOMICA')) return 'nomeCategoriaEconomica';
                if (h.includes('CÓDIGO GRUPO DE DESPESA') || h.includes('CODIGO GRUPO DE DESPESA')) return 'codigoGrupoDespesa';
                if (h.includes('NOME GRUPO DE DESPESA')) return 'nomeGrupoDespesa';
                if (h.includes('CÓDIGO ELEMENTO DE DESPESA') || h.includes('CODIGO ELEMENTO DE DESPESA')) return 'codigoElementoDespesa';
                if (h.includes('NOME ELEMENTO DE DESPESA')) return 'nomeElementoDespesa';
                if (h.includes('ORÇAMENTO INICIAL') || h.includes('ORCAMENTO INICIAL')) return 'orcamentoInicial';
                if (h.includes('ORÇAMENTO ATUALIZADO') || h.includes('ORCAMENTO ATUALIZADO')) return 'orcamentoAtualizado';
                if (h.includes('ORÇAMENTO EMPENHADO') || h.includes('ORCAMENTO EMPENHADO')) return 'orcamentoEmpenhado';
                if (h.includes('ORÇAMENTO REALIZADO') || h.includes('ORCAMENTO REALIZADO')) return 'orcamentoRealizado';
                if (h.includes('% REALIZADO DO ORÇAMENTO') || h.includes('% REALIZADO DO ORCAMENTO')) return 'percentualRealizado';

                return h;
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