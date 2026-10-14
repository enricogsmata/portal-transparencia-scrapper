// src/modules/emendas-parlamentares-documentos-despesa/processor.ts
import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import csvParser from 'csv-parser';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import { EmendasParlamentaresDocumentosDespesa } from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY } from '../shared.js';

const logger = new_logger(import.meta.url);

type EmendasParlamentaresDocumentosDespesaRow = typeof EmendasParlamentaresDocumentosDespesa.$inferInsert;

async function persistData(data: EmendasParlamentaresDocumentosDespesaRow[]) {
    const log = logger.forMethod('persistData');
    try {
        await bronze_db.insert(EmendasParlamentaresDocumentosDespesa).values(data);
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência`);
        throw error;
    }
}

export async function processCsvStream(stream: NodeJS.ReadableStream): Promise<number> {
    const log = logger.forMethod('processCsvStream');
    const BATCH_SIZE = 2000;
    let batch: EmendasParlamentaresDocumentosDespesaRow[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase().trim();
                
                if (h.includes('CÓDIGO DA EMENDA') || h.includes('CODIGO DA EMENDA')) return 'codigoEmenda';
                if (h === 'ANO DA EMENDA') return 'anoEmenda';
                if (h.includes('CÓDIGO DO AUTOR') || h.includes('CODIGO DO AUTOR')) return 'codigoAutorEmenda';
                if (h.includes('NOME DO AUTOR')) return 'nomeAutorEmenda';
                if (h.includes('NÚMERO DA EMENDA') || h.includes('NUMERO DA EMENDA')) return 'numeroEmenda';
                if (h === 'CÓDIGO FUNÇÃO' || h === 'CODIGO FUNCAO') return 'codigoFuncao';
                if (h === 'VALOR EMPENHADO') return 'valorEmpenhado';
                if (h === 'VALOR PAGO') return 'valorPago';
                if (h === 'TIPO DE EMENDA') return 'tipoEmenda';
                if (h === 'DATA DOCUMENTO') return 'dataDocumento';
                if (h === 'CÓDIGO DOCUMENTO' || h === 'CODIGO DOCUMENTO') return 'codigoDocumento';
                if (h.includes('LOCALIDADE DE APLICAÇÃO') || h.includes('LOCALIDADE DE APLICACAO')) return 'localidadeAplicacaoRecurso';
                if (h.includes('UF DE APLICAÇÃO') || h.includes('UF DE APLICACAO')) return 'ufAplicacaoRecurso';
                if (h.includes('MUNICÍPIO DE APLICAÇÃO') || h.includes('MUNICIPIO DE APLICACAO')) return 'municipioAplicacaoRecurso';
                if (h.includes('CÓDIGO IBGE DO MUNICÍPIO') || h.includes('CODIGO IBGE DO MUNICIPIO')) return 'codigoIbgeMunicipioAplicacaoRecurso';
                if (h === 'FASE DA DESPESA') return 'faseDespesa';
                if (h === 'CÓDIGO FAVORECIDO' || h === 'CODIGO FAVORECIDO') return 'codigoFavorecido';
                if (h === 'FAVORECIDO') return 'favorecido';
                if (h === 'TIPO FAVORECIDO') return 'tipoFavorecido';
                if (h === 'UF FAVORECIDO') return 'ufFavorecido';
                if (h === 'MUNICÍPIO FAVORECIDO' || h === 'MUNICIPIO FAVORECIDO') return 'municipioFavorecido';
                if (h === 'CÓDIGO UG' || h === 'CODIGO UG') return 'codigoUg';
                if (h === 'UG') return 'ug';
                if (h === 'CÓDIGO UNIDADE ORÇAMENTÁRIA' || h === 'CODIGO UNIDADE ORCAMENTARIA') return 'codigoUnidadeOrcamentaria';
                if (h === 'UNIDADE ORÇAMENTÁRIA' || h === 'UNIDADE ORCAMENTARIA') return 'unidadeOrcamentaria';
                if (h === 'CÓDIGO ÓRGÃO SIAFI' || h === 'CODIGO ORGAO SIAFI') return 'codigoOrgaoSiafi';
                if (h === 'ÓRGÃO' || h === 'ORGAO') return 'orgao';
                if (h === 'CÓDIGO ÓRGÃO SUPERIOR SIAFI' || h === 'CODIGO ORGAO SUPERIOR SIAFI') return 'codigoOrgaoSuperiorSiafi';
                if (h === 'ÓRGÃO SUPERIOR' || h === 'ORGAO SUPERIOR') return 'orgaoSuperior';
                if (h === 'CÓDIGO GRUPO DESPESA' || h === 'CODIGO GRUPO DESPESA') return 'codigoGrupoDespesa';
                if (h === 'GRUPO DESPESA') return 'grupoDespesa';
                if (h === 'CÓDIGO ELEMENTO DESPESA' || h === 'CODIGO ELEMENTO DESPESA') return 'codigoElementoDespesa';
                if (h === 'ELEMENTO DESPESA') return 'elementoDespesa';
                if (h === 'CÓDIGO MODALIDADE APLICAÇÃO DESPESA' || h === 'CODIGO MODALIDADE APLICACAO DESPESA') return 'codigoModalidadeAplicacaoDespesa';
                if (h === 'MODALIDADE APLICAÇÃO DESPESA' || h === 'MODALIDADE APLICACAO DESPESA') return 'modalidadeAplicacaoDespesa';
                if (h === 'CÓDIGO PLANO ORÇAMENTÁRIO' || h === 'CODIGO PLANO ORCAMENTARIO') return 'codigoPlanoOrcamentario';
                if (h === 'PLANO ORÇAMENTÁRIO' || h === 'PLANO ORCAMENTARIO') return 'planoOrcamentario';
                if (h === 'FUNÇÃO' || h === 'FUNCAO') return 'funcao';
                if (h === 'CÓDIGO SUBFUNÇÃO' || h === 'CODIGO SUBFUNCAO') return 'codigoSubfuncao';
                if (h === 'SUBFUNÇÃO' || h === 'SUBFUNCAO') return 'subfuncao';
                if (h === 'CÓDIGO PROGRAMA' || h === 'CODIGO PROGRAMA') return 'codigoPrograma';
                if (h === 'PROGRAMA') return 'programa';
                if (h === 'CÓDIGO AÇÃO' || h === 'CODIGO ACAO') return 'codigoAcao';
                if (h === 'AÇÃO' || h === 'ACAO') return 'acao';
                if (h === 'LINGUAGEM CIDADÃ' || h === 'LINGUAGEM CIDADA') return 'linguagemCidada';
                if (h === 'CÓDIGO SUBTÍTULO (LOCALIZADOR)' || h === 'CODIGO SUBTITULO (LOCALIZADOR)') return 'codigoSubtituloLocalizador';
                if (h === 'SUBTÍTULO (LOCALIZADOR)' || h === 'SUBTITULO (LOCALIZADOR)') return 'subtituloLocalizador';
                if (h === 'POSSUI CONVÊNIO?' || h === 'POSSUI CONVENIO?') return 'possuiConvenio';
                
                return header.trim();
            },
        }));

        for await (const data of parser) {
            batch.push(data);

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
                            const rowsCount = await processCsvStream(readStream);
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

export async function runEmendasParlamentaresDocumentosDespesaIngestion() {
    const log = logger.forMethod('runEmendasParlamentaresDocumentosDespesaIngestion');
    const moduleName = 'emendas-parlamentares-documentos-despesa';

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