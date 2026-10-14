import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import csvParser from 'csv-parser';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import { 
    DespesasEmpenho, 
    DespesasLiquidacao, 
    DespesasPagamento 
} from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY } from '../shared.js';

const logger = new_logger(import.meta.url);
const BATCH_SIZE = 2000;

async function persistEmpenho(data: (typeof DespesasEmpenho.$inferInsert)[]) {
    const log = logger.forMethod('persistEmpenho');
    try {
        await bronze_db.insert(DespesasEmpenho).values(data);
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Despesas Empenho`);
        throw error;
    }
}

async function persistLiquidacao(data: (typeof DespesasLiquidacao.$inferInsert)[]) {
    const log = logger.forMethod('persistLiquidacao');
    try {
        await bronze_db.insert(DespesasLiquidacao).values(data);
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Despesas Liquidação`);
        throw error;
    }
}

async function persistPagamento(data: (typeof DespesasPagamento.$inferInsert)[]) {
    const log = logger.forMethod('persistPagamento');
    try {
        await bronze_db.insert(DespesasPagamento).values(data);
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Despesas Pagamento`);
        throw error;
    }
}

function mapCommonHeaders(h: string): string | null {
    if (h === 'DATA EMISSÃO' || h === 'DATA EMISSAO') return 'dataEmissao';
    if (h === 'CÓDIGO TIPO DOCUMENTO' || h === 'CODIGO TIPO DOCUMENTO') return 'codigoTipoDocumento';
    if (h === 'TIPO DOCUMENTO') return 'tipoDocumento';
    if (h === 'CÓDIGO ÓRGÃO SUPERIOR' || h === 'CODIGO ORGAO SUPERIOR') return 'codigoOrgaoSuperior';
    if (h === 'ÓRGÃO SUPERIOR' || h === 'ORGAO SUPERIOR') return 'orgaoSuperior';
    if (h === 'CÓDIGO ÓRGÃO' || h === 'CODIGO ORGAO') return 'codigoOrgao';
    if (h === 'ÓRGÃO' || h === 'ORGAO') return 'orgao';
    if (h === 'CÓDIGO UNIDADE GESTORA' || h === 'CODIGO UNIDADE GESTORA') return 'codigoUnidadeGestora';
    if (h === 'UNIDADE GESTORA') return 'unidadeGestora';
    if (h === 'CÓDIGO GESTÃO' || h === 'CODIGO GESTAO') return 'codigoGestao';
    if (h === 'GESTÃO' || h === 'GESTAO') return 'gestao';
    if (h === 'CÓDIGO FAVORECIDO' || h === 'CODIGO FAVORECIDO') return 'codigoFavorecido';
    if (h === 'FAVORECIDO') return 'favorecido';
    if (h === 'OBSERVAÇÃO' || h === 'OBSERVACAO') return 'observacao';
    if (h === 'CÓDIGO CATEGORIA DE DESPESA' || h === 'CODIGO CATEGORIA DE DESPESA') return 'codigoCategoriaDespesa';
    if (h === 'CATEGORIA DE DESPESA') return 'categoriaDespesa';
    if (h === 'CÓDIGO GRUPO DE DESPESA' || h === 'CODIGO GRUPO DE DESPESA') return 'codigoGrupoDespesa';
    if (h === 'GRUPO DE DESPESA') return 'grupoDespesa';
    if (h === 'CÓDIGO MODALIDADE DE APLICAÇÃO' || h === 'CODIGO MODALIDADE DE APLICACAO') return 'codigoModalidadeAplicacao';
    if (h === 'MODALIDADE DE APLICAÇÃO' || h === 'MODALIDADE DE APLICACAO') return 'modalidadeAplicacao';
    if (h === 'CÓDIGO ELEMENTO DE DESPESA' || h === 'CODIGO ELEMENTO DE DESPESA') return 'codigoElementoDespesa';
    if (h === 'ELEMENTO DE DESPESA') return 'elementoDespesa';
    if (h === 'CÓDIGO PLANO ORÇAMENTÁRIO' || h === 'CODIGO PLANO ORCAMENTARIO') return 'codigoPlanoOrcamentario';
    if (h === 'PLANO ORÇAMENTÁRIO' || h === 'PLANO ORCAMENTARIO') return 'planoOrcamentario';
    if (h === 'CÓDIGO PROGRAMA GOVERNO' || h === 'CODIGO PROGRAMA GOVERNO') return 'codigoProgramaGoverno';
    if (h === 'NOME PROGRAMA GOVERNO') return 'nomeProgramaGoverno';
    if (h === 'PROCESSO') return 'processo';
    if (h === 'VALOR UTILIZADO NA CONVERSÃO' || h === 'VALOR UTILIZADO NA CONVERSAO') return 'valorUtilizadoConversao';
    return null;
}

async function processEmpenhoStream(stream: NodeJS.ReadableStream): Promise<number> {
    const log = logger.forMethod('processEmpenhoStream');
    let batch: (typeof DespesasEmpenho.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase().trim();
                const common = mapCommonHeaders(h);
                if (common) return common;
                
                if (h === 'ID EMPENHO') return 'idEmpenho';
                if (h === 'CÓDIGO EMPENHO' || h === 'CODIGO EMPENHO') return 'codigoEmpenho';
                if (h === 'CÓDIGO EMPENHO RESUMIDO' || h === 'CODIGO EMPENHO RESUMIDO') return 'codigoEmpenhoResumido';
                if (h === 'TIPO EMPENHO') return 'tipoEmpenho';
                if (h === 'ESPÉCIE EMPENHO' || h === 'ESPECIE EMPENHO') return 'especieEmpenho';
                if (h === 'CÓDIGO ESFERA ORÇAMENTÁRIA' || h === 'CODIGO ESFERA ORCAMENTARIA') return 'codigoEsferaOrcamentaria';
                if (h === 'ESFERA ORÇAMENTÁRIA' || h === 'ESFERA ORCAMENTARIA') return 'esferaOrcamentaria';
                if (h === 'CÓDIGO TIPO CRÉDITO' || h === 'CODIGO TIPO CREDITO') return 'codigoTipoCredito';
                if (h === 'TIPO CRÉDITO' || h === 'TIPO CREDITO') return 'tipoCredito';
                if (h === 'CÓDIGO GRUPO FONTE RECURSO' || h === 'CODIGO GRUPO FONTE RECURSO') return 'codigoGrupoFonteRecurso';
                if (h === 'GRUPO FONTE RECURSO') return 'grupoFonteRecurso';
                if (h === 'CÓDIGO FONTE RECURSO' || h === 'CODIGO FONTE RECURSO') return 'codigoFonteRecurso';
                if (h === 'FONTE RECURSO') return 'fonteRecurso';
                if (h === 'CÓDIGO UNIDADE ORÇAMENTÁRIA' || h === 'CODIGO UNIDADE ORCAMENTARIA') return 'codigoUnidadeOrcamentaria';
                if (h === 'UNIDADE ORÇAMENTÁRIA' || h === 'UNIDADE ORCAMENTARIA') return 'unidadeOrcamentaria';
                if (h === 'CÓDIGO FUNÇÃO' || h === 'CODIGO FUNCAO') return 'codigoFuncao';
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
                if (h === 'AUTOR EMENDA') return 'autorEmenda';
                if (h === 'MODALIDADE DE LICITAÇÃO' || h === 'MODALIDADE DE LICITACAO') return 'modalidadeLicitacao';
                if (h === 'INCISO') return 'inciso';
                if (h === 'AMPARO') return 'amparo';
                if (h === 'REFERÊNCIA DE DISPENSA OU INEXIGIBILIDADE' || h === 'REFERENCIA DE DISPENSA OU INEXIGIBILIDADE') return 'referenciaDispensaInexigibilidade';
                if (h === 'CÓDIGO CONVÊNIO' || h === 'CODIGO CONVENIO') return 'codigoConvenio';
                if (h === 'CONTRATO DE REPASSE / TERMO DE PARCERIA / OUTROS') return 'contratoRepasseTermoParceriaOutros';
                if (h === 'VALOR ORIGINAL DO EMPENHO') return 'valorOriginalEmpenho';
                if (h === 'VALOR DO EMPENHO CONVERTIDO PRA R$' || h === 'VALOR DO EMPENHO CONVERTIDO PARA R$') return 'valorEmpenhoConvertido';
                
                return h;
            },
        }));

        for await (const data of parser) {
            batch.push(data);
            if (batch.length >= BATCH_SIZE) {
                await persistEmpenho(batch);
                totalProcessed += batch.length;
                batch = [];
            }
        }
        if (batch.length > 0) {
            await persistEmpenho(batch);
            totalProcessed += batch.length;
        }

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento do arquivo Despesas Empenho concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream do arquivo Despesas Empenho');
        throw error;
    }
}

async function processLiquidacaoStream(stream: NodeJS.ReadableStream): Promise<number> {
    const log = logger.forMethod('processLiquidacaoStream');
    let batch: (typeof DespesasLiquidacao.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase().trim();
                const common = mapCommonHeaders(h);
                if (common) return common;
                
                if (h === 'CÓDIGO LIQUIDAÇÃO' || h === 'CODIGO LIQUIDACAO') return 'codigoLiquidacao';
                if (h === 'CÓDIGO LIQUIDAÇÃO RESUMIDO' || h === 'CODIGO LIQUIDACAO RESUMIDO') return 'codigoLiquidacaoResumido';
                
                return h;
            },
        }));

        for await (const data of parser) {
            batch.push(data);
            if (batch.length >= BATCH_SIZE) {
                await persistLiquidacao(batch);
                totalProcessed += batch.length;
                batch = [];
            }
        }
        if (batch.length > 0) {
            await persistLiquidacao(batch);
            totalProcessed += batch.length;
        }

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento do arquivo Despesas Liquidação concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream do arquivo Despesas Liquidação');
        throw error;
    }
}

async function processPagamentoStream(stream: NodeJS.ReadableStream): Promise<number> {
    const log = logger.forMethod('processPagamentoStream');
    let batch: (typeof DespesasPagamento.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase().trim();
                const common = mapCommonHeaders(h);
                if (common) return common;
                
                if (h === 'CÓDIGO PAGAMENTO' || h === 'CODIGO PAGAMENTO') return 'codigoPagamento';
                if (h === 'CÓDIGO PAGAMENTO RESUMIDO' || h === 'CODIGO PAGAMENTO RESUMIDO') return 'codigoPagamentoResumido';
                if (h === 'TIPO OB') return 'tipoOb';
                if (h === 'EXTRAORÇAMENTÁRIO' || h === 'EXTRAORCAMENTARIO') return 'extraorcamentario';
                if (h === 'VALOR ORIGINAL DO PAGAMENTO') return 'valorOriginalPagamento';
                if (h === 'VALOR DO PAGAMENTO CONVERTIDO PRA R$' || h === 'VALOR DO PAGAMENTO CONVERTIDO PARA R$') return 'valorPagamentoConvertido';

                return h;
            },
        }));

        for await (const data of parser) {
            batch.push(data);
            if (batch.length >= BATCH_SIZE) {
                await persistPagamento(batch);
                totalProcessed += batch.length;
                batch = [];
            }
        }
        if (batch.length > 0) {
            await persistPagamento(batch);
            totalProcessed += batch.length;
        }

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento do arquivo Despesas Pagamento concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream do arquivo Despesas Pagamento');
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
                            
                            if (upperName.includes('_EMPENHO.CSV') && !upperName.includes('ITEM') && !upperName.includes('IMPACTADOS')) {
                                rowsCount = await processEmpenhoStream(readStream);
                            } else if (upperName.includes('_LIQUIDACAO.CSV') && !upperName.includes('IMPACTADOS')) {
                                rowsCount = await processLiquidacaoStream(readStream);
                            } else if (upperName.includes('_PAGAMENTO.CSV') && !upperName.includes('IMPACTADOS') && !upperName.includes('FAVORECIDOS') && !upperName.includes('LISTA')) {
                                rowsCount = await processPagamentoStream(readStream);
                            } else {
                                log.info({ data: { fileName: entry.fileName } }, 'Arquivo CSV ignorado por não ser o arquivo primário da etapa processada nesta sprint');
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

export async function runDespesasIngestion() {
    const log = logger.forMethod('runDespesasIngestion');
    const moduleName = 'despesas';

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