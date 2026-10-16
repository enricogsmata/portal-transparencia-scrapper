import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import iconv from 'iconv-lite';
import csvParser from 'csv-parser';
import { normalize } from '../shared.js';
import { sql } from 'drizzle-orm';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import { RecursosTransferidos } from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY, extractMetadataFromFileName, ReferenceMetadata } from '../shared.js';

const logger = new_logger(import.meta.url);

type RecursosTransferidosRow = typeof RecursosTransferidos.$inferInsert;

async function persistData(data: RecursosTransferidosRow[]) {
    const log = logger.forMethod('persistData');
    try {
        await bronze_db.insert(RecursosTransferidos).values(data).onDuplicateKeyUpdate({
            set: {
                anoMes: sql`values(${RecursosTransferidos.anoMes})`,
                tipoTransferencia: sql`values(${RecursosTransferidos.tipoTransferencia})`,
                tipoFavorecido: sql`values(${RecursosTransferidos.tipoFavorecido})`,
                uf: sql`values(${RecursosTransferidos.uf})`,
                codigoMunicipioSiafi: sql`values(${RecursosTransferidos.codigoMunicipioSiafi})`,
                nomeMunicipio: sql`values(${RecursosTransferidos.nomeMunicipio})`,
                codigoOrgaoSiafi: sql`values(${RecursosTransferidos.codigoOrgaoSiafi})`,
                nomeOrgao: sql`values(${RecursosTransferidos.nomeOrgao})`,
                codigoUnidadeGestora: sql`values(${RecursosTransferidos.codigoUnidadeGestora})`,
                nomeUnidadeGestora: sql`values(${RecursosTransferidos.nomeUnidadeGestora})`,
                codigoFuncao: sql`values(${RecursosTransferidos.codigoFuncao})`,
                nomeFuncao: sql`values(${RecursosTransferidos.nomeFuncao})`,
                codigoSubfuncao: sql`values(${RecursosTransferidos.codigoSubfuncao})`,
                nomeSubfuncao: sql`values(${RecursosTransferidos.nomeSubfuncao})`,
                codigoPrograma: sql`values(${RecursosTransferidos.codigoPrograma})`,
                nomePrograma: sql`values(${RecursosTransferidos.nomePrograma})`,
                codigoAcao: sql`values(${RecursosTransferidos.codigoAcao})`,
                nomeAcao: sql`values(${RecursosTransferidos.nomeAcao})`,
                linguagemCidada: sql`values(${RecursosTransferidos.linguagemCidada})`,
                codigoGrupoDespesa: sql`values(${RecursosTransferidos.codigoGrupoDespesa})`,
                grupoDespesa: sql`values(${RecursosTransferidos.grupoDespesa})`,
                codigoModalidadeAplicacaoDespesa: sql`values(${RecursosTransferidos.codigoModalidadeAplicacaoDespesa})`,
                nomeModalidadeAplicacaoDespesa: sql`values(${RecursosTransferidos.nomeModalidadeAplicacaoDespesa})`,
                codigoElementoDespesa: sql`values(${RecursosTransferidos.codigoElementoDespesa})`,
                nomeElementoDespesa: sql`values(${RecursosTransferidos.nomeElementoDespesa})`,
                codigoPlanoOrcamentario: sql`values(${RecursosTransferidos.codigoPlanoOrcamentario})`,
                nomePlanoOrcamentario: sql`values(${RecursosTransferidos.nomePlanoOrcamentario})`,
                codigoSubtitulo: sql`values(${RecursosTransferidos.codigoSubtitulo})`,
                nomeSubtitulo: sql`values(${RecursosTransferidos.nomeSubtitulo})`,
                codigoLocalizador: sql`values(${RecursosTransferidos.codigoLocalizador})`,
                nomeLocalizador: sql`values(${RecursosTransferidos.nomeLocalizador})`,
                siglaLocalizador: sql`values(${RecursosTransferidos.siglaLocalizador})`,
                descricaoComplementarLocalizador: sql`values(${RecursosTransferidos.descricaoComplementarLocalizador})`,
                codigoFavorecido: sql`values(${RecursosTransferidos.codigoFavorecido})`,
                nomeFavorecido: sql`values(${RecursosTransferidos.nomeFavorecido})`,
                valorTransferido: sql`values(${RecursosTransferidos.valorTransferido})`,
                exercicioReferencia: sql`values(${RecursosTransferidos.exercicioReferencia})`,
                mesReferencia: sql`values(${RecursosTransferidos.mesReferencia})`,
                diaReferencia: sql`values(${RecursosTransferidos.diaReferencia})`,
                dadosAdicionais: sql`values(${RecursosTransferidos.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência`);
        throw error;
    }
}

// Mapeamento de regras para Recursos Transferidos
const RECURSOS_TRANSFERIDOS_HEADER_RULES = [
    { key: 'anoMes', keywords: ['ANO MES', 'ANO / MES'] },
    { key: 'tipoTransferencia', keywords: ['TIPO TRANSFERENCIA'] },
    { key: 'tipoFavorecido', keywords: ['TIPO FAVORECIDO'] },
    { key: 'uf', matchExact: ['UF'] },
    { key: 'codigoMunicipioSiafi', keywords: ['MUNICIPIO SIAFI', 'MUNICIPIO'], require: ['CODIGO'] },
    { key: 'nomeMunicipio', keywords: ['MUNICIPIO SIAFI', 'MUNICIPIO'], require: ['NOME'] },
    { key: 'codigoOrgaoSiafi', keywords: ['ORGAO SIAFI', 'ORGAO'], require: ['CODIGO'] },
    { key: 'nomeOrgao', keywords: ['ORGAO SIAFI', 'ORGAO'], require: ['NOME'] },
    { key: 'codigoUnidadeGestora', keywords: ['UNIDADE GESTORA'], require: ['CODIGO'] },
    { key: 'nomeUnidadeGestora', keywords: ['UNIDADE GESTORA'], require: ['NOME'] },

    // Estrutura Orçamentária
    { key: 'codigoSubfuncao', keywords: ['SUBFUNCAO'], require: ['CODIGO'] },
    { key: 'nomeSubfuncao', keywords: ['SUBFUNCAO'], require: ['NOME'] },
    { key: 'codigoFuncao', keywords: ['FUNCAO'], require: ['CODIGO'] },
    { key: 'nomeFuncao', keywords: ['FUNCAO'], require: ['NOME'] },
    { key: 'codigoPrograma', keywords: ['PROGRAMA'], require: ['CODIGO'] },
    { key: 'nomePrograma', keywords: ['PROGRAMA'], require: ['NOME'] },

    { key: 'codigoAcao', matchExact: ['CODIGO ACAO', 'ACAO'] },
    { key: 'nomeAcao', matchExact: ['NOME ACAO'] },

    { key: 'linguagemCidada', keywords: ['LINGUAGEM CIDADA'] },

    // Classificação da Despesa
    { key: 'codigoGrupoDespesa', keywords: ['GRUPO DESPESA', 'GRUPO DE DESPESA'], require: ['CODIGO'] },
    { key: 'grupoDespesa', keywords: ['GRUPO DESPESA', 'GRUPO DE DESPESA'], require: ['NOME'] },
    { key: 'codigoModalidadeAplicacaoDespesa', keywords: ['MODALIDADE APLICACAO'], require: ['CODIGO'] },
    { key: 'nomeModalidadeAplicacaoDespesa', keywords: ['MODALIDADE APLICACAO'], require: ['NOME'] },
    { key: 'codigoElementoDespesa', keywords: ['ELEMENTO DESPESA'], require: ['CODIGO'] },
    { key: 'nomeElementoDespesa', keywords: ['ELEMENTO DESPESA'], require: ['NOME'] },
    { key: 'codigoPlanoOrcamentario', keywords: ['PLANO ORCAMENTARIO'], require: ['CODIGO'] },
    { key: 'nomePlanoOrcamentario', keywords: ['PLANO ORCAMENTARIO'], require: ['NOME'] },

    // Localização
    { key: 'codigoSubtitulo', keywords: ['SUBTITULO'], require: ['CODIGO'] },
    { key: 'nomeSubtitulo', keywords: ['SUBTITULO'], require: ['NOME'] },
    { key: 'codigoLocalizador', keywords: ['LOCALIZADOR'], require: ['CODIGO'] },
    { key: 'nomeLocalizador', keywords: ['LOCALIZADOR'], require: ['NOME'] },
    { key: 'siglaLocalizador', keywords: ['LOCALIZADOR'], require: ['SIGLA'] },
    { key: 'descricaoComplementarLocalizador', keywords: ['DESCRICAO COMPLEMENTAR'] },

    // Favorecido e Valores
    { key: 'codigoFavorecido', keywords: ['FAVORECIDO'], require: ['CODIGO'] },
    { key: 'nomeFavorecido', keywords: ['FAVORECIDO'], require: ['NOME'] },
    { key: 'valorTransferido', keywords: ['VALOR TRANSFERIDO'] }
];

export async function processCsvStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processCsvStream');
    const BATCH_SIZE = 2000;
    let batch: RecursosTransferidosRow[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => {
                    if (!header) return null;

                    const cleanHeader = normalize(header);
                    log.info(`Clean Header: ${cleanHeader}`)

                    for (const rule of RECURSOS_TRANSFERIDOS_HEADER_RULES) {
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

export async function runRecursosTransferidosIngestion() {
    const log = logger.forMethod('runRecursosTransferidosIngestion');
    const moduleName = 'transferencias';

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