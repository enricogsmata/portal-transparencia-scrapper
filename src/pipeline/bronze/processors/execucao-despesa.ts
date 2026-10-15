import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import csvParser from 'csv-parser';
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

export async function processCsvStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processCsvStream');
    const BATCH_SIZE = 2000;
    let batch: ExecucaoDespesaRow[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase();

                if (h.includes('ANO E MÊS') || h.includes('ANO E MES')) return 'anoMesLancamento';
                if (h.includes('CÓDIGO ÓRGÃO SUPERIOR') || h.includes('CODIGO ORGAO SUPERIOR')) return 'codigoOrgaoSuperior';
                if (h === 'NOME ÓRGÃO SUPERIOR' || h === 'NOME ORGAO SUPERIOR') return 'nomeOrgaoSuperior';
                if (h.includes('CÓDIGO ÓRGÃO SUBORDINADO') || h.includes('CODIGO ORGAO SUBORDINADO')) return 'codigoOrgaoSubordinado';
                if (h === 'NOME ÓRGÃO SUBORDINADO' || h === 'NOME ORGAO SUBORDINADO') return 'nomeOrgaoSubordinado';
                if (h.includes('CÓDIGO UNIDADE GESTORA') || h.includes('CODIGO UNIDADE GESTORA')) return 'codigoUnidadeGestora';
                if (h === 'NOME UNIDADE GESTORA') return 'nomeUnidadeGestora';
                if (h.includes('CÓDIGO GESTÃO') || h.includes('CODIGO GESTAO')) return 'codigoGestao';
                if (h === 'NOME GESTÃO' || h === 'NOME GESTAO') return 'nomeGestao';
                if (h.includes('CÓDIGO UNIDADE ORÇAMENTÁRIA') || h.includes('CODIGO UNIDADE ORCAMENTARIA')) return 'codigoUnidadeOrcamentaria';
                if (h === 'NOME UNIDADE ORÇAMENTÁRIA' || h === 'NOME UNIDADE ORCAMENTARIA') return 'nomeUnidadeOrcamentaria';
                if (h.includes('CÓDIGO FUNÇÃO') || h.includes('CODIGO FUNCAO')) return 'codigoFuncao';
                if (h === 'NOME FUNÇÃO' || h === 'NOME FUNCAO') return 'nomeFuncao';
                if (h.includes('CÓDIGO SUBFUNÇÃO') || h.includes('CODIGO SUBFUNCAO')) return 'codigoSubfuncao';
                if (h === 'NOME SUBFUNÇÃO' || h === 'NOME SUBFUNCAO') return 'nomeSubfuncao';
                if (h.includes('CÓDIGO PROGRAMA ORÇAMENTÁRIO') || h.includes('CODIGO PROGRAMA ORCAMENTARIO')) return 'codigoProgramaOrcamentario';
                if (h === 'NOME PROGRAMA ORÇAMENTÁRIO' || h === 'NOME PROGRAMA ORCAMENTARIO') return 'nomeProgramaOrcamentario';
                if (h.includes('CÓDIGO AÇÃO') || h.includes('CODIGO ACAO')) return 'codigoAcao';
                if (h === 'NOME AÇÃO' || h === 'NOME ACAO') return 'nomeAcao';
                if (h.includes('CÓDIGO PLANO ORÇAMENTÁRIO') || h.includes('CODIGO PLANO ORCAMENTARIO')) return 'codigoPlanoOrcamentario';
                if (h === 'PLANO ORÇAMENTÁRIO' || h === 'PLANO ORCAMENTARIO') return 'planoOrcamentario';
                if (h.includes('CÓDIGO PROGRAMA GOVERNO') || h.includes('CODIGO PROGRAMA GOVERNO')) return 'codigoProgramaGoverno';
                if (h === 'NOME PROGRAMA GOVERNO') return 'nomeProgramaGoverno';
                if (h === 'UF' || h.includes('UF ')) return 'uf';
                if (h === 'MUNICÍPIO' || h === 'MUNICIPIO') return 'municipio';
                if (h.includes('CÓDIGO SUBTÍTULO') || h.includes('CODIGO SUBTITULO')) return 'codigoSubtitulo';
                if (h === 'NOME SUBTÍTULO' || h === 'NOME SUBTITULO') return 'nomeSubtitulo';
                if (h.includes('CÓDIGO LOCALIZADOR') || h.includes('CODIGO LOCALIZADOR')) return 'codigoLocalizador';
                if (h === 'NOME LOCALIZADOR') return 'nomeLocalizador';
                if (h === 'SIGLA LOCALIZADOR') return 'siglaLocalizador';
                if (h.includes('DESCRIÇÃO COMPLEMENTAR') || h.includes('DESCRICAO COMPLEMENTAR')) return 'descricaoComplementarLocalizador';
                if (h.includes('CÓDIGO AUTOR EMENDA') || h.includes('CODIGO AUTOR EMENDA')) return 'codigoAutorEmenda';
                if (h === 'NOME AUTOR EMENDA') return 'nomeAutorEmenda';
                if (h.includes('CÓDIGO CATEGORIA ECONÔMICA') || h.includes('CODIGO CATEGORIA ECONOMICA')) return 'codigoCategoriaEconomica';
                if (h === 'NOME CATEGORIA ECONÔMICA' || h === 'NOME CATEGORIA ECONOMICA') return 'nomeCategoriaEconomica';
                if (h.includes('CÓDIGO GRUPO DE DESPESA') || h.includes('CODIGO GRUPO DE DESPESA')) return 'codigoGrupoDespesa';
                if (h === 'NOME GRUPO DE DESPESA') return 'nomeGrupoDespesa';
                if (h.includes('CÓDIGO ELEMENTO DE DESPESA') || h.includes('CODIGO ELEMENTO DE DESPESA')) return 'codigoElementoDespesa';
                if (h === 'NOME ELEMENTO DE DESPESA') return 'nomeElementoDespesa';
                if (h.includes('CÓDIGO MODALIDADE DA DESPESA') || h.includes('CODIGO MODALIDADE DA DESPESA')) return 'codigoModalidadeDespesa';
                if (h === 'MODALIDADE DA DESPESA') return 'modalidadeDespesa';
                if (h.includes('VALOR EMPENHADO')) return 'valorEmpenhado';
                if (h.includes('VALOR LIQUIDADO')) return 'valorLiquidado';
                if (h.includes('VALOR PAGO')) return 'valorPago';
                if (h.includes('RESTOS A PAGAR INSCRITOS')) return 'valorRestosAPagarInscritos';
                if (h.includes('RESTOS A PAGAR CANCELADO')) return 'valorRestosAPagarCancelado';
                if (h.includes('RESTOS A PAGAR PAGOS')) return 'valorRestosAPagarPagos';

                return header.trim();
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