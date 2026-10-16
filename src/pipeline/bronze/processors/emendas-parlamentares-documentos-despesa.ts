import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import csvParser from 'csv-parser';
import iconv from 'iconv-lite';
import { sql } from 'drizzle-orm';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import { EmendasParlamentaresDocumentosDespesa } from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY, extractMetadataFromFileName, ReferenceMetadata, normalize } from '../shared.js';

const logger = new_logger(import.meta.url);

type EmendasParlamentaresDocumentosDespesaRow = typeof EmendasParlamentaresDocumentosDespesa.$inferInsert;

async function persistData(data: EmendasParlamentaresDocumentosDespesaRow[]) {
    const log = logger.forMethod('persistData');
    try {
        await bronze_db.insert(EmendasParlamentaresDocumentosDespesa).values(data).onDuplicateKeyUpdate({
            set: {
                codigoEmenda: sql`values(${EmendasParlamentaresDocumentosDespesa.codigoEmenda})`,
                anoEmenda: sql`values(${EmendasParlamentaresDocumentosDespesa.anoEmenda})`,
                codigoAutorEmenda: sql`values(${EmendasParlamentaresDocumentosDespesa.codigoAutorEmenda})`,
                nomeAutorEmenda: sql`values(${EmendasParlamentaresDocumentosDespesa.nomeAutorEmenda})`,
                numeroEmenda: sql`values(${EmendasParlamentaresDocumentosDespesa.numeroEmenda})`,
                codigoFuncao: sql`values(${EmendasParlamentaresDocumentosDespesa.codigoFuncao})`,
                valorEmpenhado: sql`values(${EmendasParlamentaresDocumentosDespesa.valorEmpenhado})`,
                valorPago: sql`values(${EmendasParlamentaresDocumentosDespesa.valorPago})`,
                tipoEmenda: sql`values(${EmendasParlamentaresDocumentosDespesa.tipoEmenda})`,
                dataDocumento: sql`values(${EmendasParlamentaresDocumentosDespesa.dataDocumento})`,
                codigoDocumento: sql`values(${EmendasParlamentaresDocumentosDespesa.codigoDocumento})`,
                localidadeAplicacaoRecurso: sql`values(${EmendasParlamentaresDocumentosDespesa.localidadeAplicacaoRecurso})`,
                ufAplicacaoRecurso: sql`values(${EmendasParlamentaresDocumentosDespesa.ufAplicacaoRecurso})`,
                municipioAplicacaoRecurso: sql`values(${EmendasParlamentaresDocumentosDespesa.municipioAplicacaoRecurso})`,
                codigoIbgeMunicipioAplicacaoRecurso: sql`values(${EmendasParlamentaresDocumentosDespesa.codigoIbgeMunicipioAplicacaoRecurso})`,
                faseDespesa: sql`values(${EmendasParlamentaresDocumentosDespesa.faseDespesa})`,
                codigoFavorecido: sql`values(${EmendasParlamentaresDocumentosDespesa.codigoFavorecido})`,
                favorecido: sql`values(${EmendasParlamentaresDocumentosDespesa.favorecido})`,
                tipoFavorecido: sql`values(${EmendasParlamentaresDocumentosDespesa.tipoFavorecido})`,
                ufFavorecido: sql`values(${EmendasParlamentaresDocumentosDespesa.ufFavorecido})`,
                municipioFavorecido: sql`values(${EmendasParlamentaresDocumentosDespesa.municipioFavorecido})`,
                codigoUg: sql`values(${EmendasParlamentaresDocumentosDespesa.codigoUg})`,
                ug: sql`values(${EmendasParlamentaresDocumentosDespesa.ug})`,
                codigoUnidadeOrcamentaria: sql`values(${EmendasParlamentaresDocumentosDespesa.codigoUnidadeOrcamentaria})`,
                unidadeOrcamentaria: sql`values(${EmendasParlamentaresDocumentosDespesa.codigoUnidadeOrcamentaria})`,
                codigoOrgaoSiafi: sql`values(${EmendasParlamentaresDocumentosDespesa.codigoOrgaoSiafi})`,
                orgao: sql`values(${EmendasParlamentaresDocumentosDespesa.orgao})`,
                codigoOrgaoSuperiorSiafi: sql`values(${EmendasParlamentaresDocumentosDespesa.codigoOrgaoSuperiorSiafi})`,
                orgaoSuperior: sql`values(${EmendasParlamentaresDocumentosDespesa.orgaoSuperior})`,
                codigoGrupoDespesa: sql`values(${EmendasParlamentaresDocumentosDespesa.codigoGrupoDespesa})`,
                grupoDespesa: sql`values(${EmendasParlamentaresDocumentosDespesa.grupoDespesa})`,
                codigoElementoDespesa: sql`values(${EmendasParlamentaresDocumentosDespesa.codigoElementoDespesa})`,
                elementoDespesa: sql`values(${EmendasParlamentaresDocumentosDespesa.elementoDespesa})`,
                codigoModalidadeAplicacaoDespesa: sql`values(${EmendasParlamentaresDocumentosDespesa.codigoModalidadeAplicacaoDespesa})`,
                modalidadeAplicacaoDespesa: sql`values(${EmendasParlamentaresDocumentosDespesa.modalidadeAplicacaoDespesa})`,
                codigoPlanoOrcamentario: sql`values(${EmendasParlamentaresDocumentosDespesa.codigoPlanoOrcamentario})`,
                planoOrcamentario: sql`values(${EmendasParlamentaresDocumentosDespesa.planoOrcamentario})`,
                funcao: sql`values(${EmendasParlamentaresDocumentosDespesa.funcao})`,
                codigoSubfuncao: sql`values(${EmendasParlamentaresDocumentosDespesa.codigoSubfuncao})`,
                subfuncao: sql`values(${EmendasParlamentaresDocumentosDespesa.subfuncao})`,
                codigoPrograma: sql`values(${EmendasParlamentaresDocumentosDespesa.codigoPrograma})`,
                programa: sql`values(${EmendasParlamentaresDocumentosDespesa.programa})`,
                codigoAcao: sql`values(${EmendasParlamentaresDocumentosDespesa.codigoAcao})`,
                acao: sql`values(${EmendasParlamentaresDocumentosDespesa.acao})`,
                linguagemCidada: sql`values(${EmendasParlamentaresDocumentosDespesa.linguagemCidada})`,
                codigoSubtituloLocalizador: sql`values(${EmendasParlamentaresDocumentosDespesa.codigoSubtituloLocalizador})`,
                subtituloLocalizador: sql`values(${EmendasParlamentaresDocumentosDespesa.subtituloLocalizador})`,
                possuiConvenio: sql`values(${EmendasParlamentaresDocumentosDespesa.possuiConvenio})`,
                exercicioReferencia: sql`values(${EmendasParlamentaresDocumentosDespesa.exercicioReferencia})`,
                mesReferencia: sql`values(${EmendasParlamentaresDocumentosDespesa.mesReferencia})`,
                diaReferencia: sql`values(${EmendasParlamentaresDocumentosDespesa.diaReferencia})`,
                dadosAdicionais: sql`values(${EmendasParlamentaresDocumentosDespesa.dadosAdicionais})`,
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

const DOCUMENTOS_DESPESA_EMENDAS_HEADER_RULES: HeaderRule[] = [
    // Emenda e Autor
    { key: 'codigoAutorEmenda', keywords: ['AUTOR DA EMENDA'], require: ['CODIGO'] },
    { key: 'codigoEmenda', keywords: ['EMENDA'], require: ['CODIGO'] },
    { key: 'anoEmenda', keywords: ['EMENDA'], require: ['ANO'] },
    { key: 'nomeAutorEmenda', keywords: ['AUTOR'], require: ['NOME'] },
    { key: 'numeroEmenda', keywords: ['EMENDA'], require: ['NUMERO'] },
    { key: 'tipoEmenda', keywords: ['EMENDA'], require: ['TIPO'] },

    // Documento e Valores
    { key: 'dataDocumento', keywords: ['DATA DOCUMENTO'] },
    { key: 'codigoDocumento', keywords: ['DOCUMENTO'], require: ['CODIGO'] },
    { key: 'faseDespesa', keywords: ['FASE DA DESPESA', 'FASE DESPESA'] },
    { key: 'valorEmpenhado', keywords: ['VALOR EMPENHADO'] },
    { key: 'valorPago', keywords: ['VALOR PAGO'] },

    // Aplicação do Recurso
    { key: 'localidadeAplicacaoRecurso', keywords: ['LOCALIDADE DE APLICACAO', 'LOCALIDADE APLICACAO'] },
    { key: 'ufAplicacaoRecurso', keywords: ['UF DE APLICACAO', 'UF APLICACAO'] },
    { key: 'codigoIbgeMunicipioAplicacaoRecurso', keywords: ['MUNICIPIO'], require: ['IBGE'] },
    { key: 'municipioAplicacaoRecurso', keywords: ['MUNICIPIO DE APLICACAO', 'MUNICIPIO APLICACAO'] },

    // Favorecido
    { key: 'codigoFavorecido', keywords: ['FAVORECIDO'], require: ['CODIGO'] },
    { key: 'tipoFavorecido', keywords: ['FAVORECIDO'], require: ['TIPO'] },
    { key: 'ufFavorecido', keywords: ['FAVORECIDO'], require: ['UF'] },
    { key: 'municipioFavorecido', keywords: ['FAVORECIDO'], require: ['MUNICIPIO'] },
    { key: 'favorecido', matchExact: ['FAVORECIDO', 'NOME FAVORECIDO'] },

    // Unidades e Órgãos (A ordem importa: Órgão Superior antes de Órgão genérico)
    { key: 'codigoOrgaoSuperiorSiafi', keywords: ['ORGAO SUPERIOR'], require: ['CODIGO'] },
    { key: 'orgaoSuperior', keywords: ['ORGAO SUPERIOR'] },
    { key: 'codigoOrgaoSiafi', keywords: ['ORGAO SIAFI', 'ORGAO'], require: ['CODIGO'] },
    { key: 'orgao', matchExact: ['ORGAO'] },

    { key: 'codigoUg', keywords: ['UG'], require: ['CODIGO'] },
    { key: 'ug', matchExact: ['UG'] },
    { key: 'codigoUnidadeOrcamentaria', keywords: ['UNIDADE ORCAMENTARIA'], require: ['CODIGO'] },
    { key: 'unidadeOrcamentaria', keywords: ['UNIDADE ORCAMENTARIA'] },

    // Estrutura Orçamentária e Classificação de Despesa
    { key: 'codigoGrupoDespesa', keywords: ['GRUPO DESPESA', 'GRUPO DE DESPESA'], require: ['CODIGO'] },
    { key: 'grupoDespesa', keywords: ['GRUPO DESPESA', 'GRUPO DE DESPESA'] },
    { key: 'codigoElementoDespesa', keywords: ['ELEMENTO DESPESA', 'ELEMENTO DE DESPESA'], require: ['CODIGO'] },
    { key: 'elementoDespesa', keywords: ['ELEMENTO DESPESA', 'ELEMENTO DE DESPESA'] },
    { key: 'codigoModalidadeAplicacaoDespesa', keywords: ['MODALIDADE APLICACAO'], require: ['CODIGO'] },
    { key: 'modalidadeAplicacaoDespesa', keywords: ['MODALIDADE APLICACAO'] },
    { key: 'codigoPlanoOrcamentario', keywords: ['PLANO ORCAMENTARIO'], require: ['CODIGO'] },
    { key: 'planoOrcamentario', keywords: ['PLANO ORCAMENTARIO'] },

    // SUBFUNÇÃO antes de FUNÇÃO para evitar falsos positivos
    { key: 'codigoSubfuncao', keywords: ['SUBFUNCAO'], require: ['CODIGO'] },
    { key: 'subfuncao', keywords: ['SUBFUNCAO'] },
    { key: 'codigoFuncao', keywords: ['FUNCAO'], require: ['CODIGO'] },
    { key: 'funcao', matchExact: ['FUNCAO'] },

    { key: 'codigoPrograma', keywords: ['PROGRAMA'], require: ['CODIGO'] },
    { key: 'programa', matchExact: ['PROGRAMA'] },
    { key: 'codigoAcao', keywords: ['ACAO'], require: ['CODIGO'] },
    { key: 'acao', matchExact: ['ACAO'] },

    { key: 'linguagemCidada', keywords: ['LINGUAGEM CIDADA'] },
    { key: 'codigoSubtituloLocalizador', keywords: ['SUBTITULO LOCALIZADOR', 'SUBTITULO (LOCALIZADOR)'], require: ['CODIGO'] },
    { key: 'subtituloLocalizador', keywords: ['SUBTITULO LOCALIZADOR', 'SUBTITULO (LOCALIZADOR)'] },
    { key: 'possuiConvenio', keywords: ['POSSUI CONVENIO'] },
];

export async function processCsvStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processCsvStream');
    const BATCH_SIZE = 2000;
    let batch: EmendasParlamentaresDocumentosDespesaRow[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => {
                    if (!header) return null;

                    const cleanHeader = normalize(header);

                    for (const rule of DOCUMENTOS_DESPESA_EMENDAS_HEADER_RULES) {
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

export async function runEmendasParlamentaresDocumentosDespesaIngestion() {
    const log = logger.forMethod('runEmendasParlamentaresDocumentosDespesaIngestion');
    const moduleName = 'emendas-parlamentares-documentos';

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