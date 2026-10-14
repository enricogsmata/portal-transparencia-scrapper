import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import csvParser from 'csv-parser';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import { RecursosTransferidos } from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY } from '../shared.js';

const logger = new_logger(import.meta.url);

type RecursosTransferidosRow = typeof RecursosTransferidos.$inferInsert;

async function persistData(data: RecursosTransferidosRow[]) {
    const log = logger.forMethod('persistData');
    try {
        await bronze_db.insert(RecursosTransferidos).values(data);
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência`);
        throw error;
    }
}

export async function processCsvStream(stream: NodeJS.ReadableStream): Promise<number> {
    const log = logger.forMethod('processCsvStream');
    const BATCH_SIZE = 2000;
    let batch: RecursosTransferidosRow[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase().trim();
                
                if (h.includes('ANO / MÊS') || h.includes('ANO / MES')) return 'anoMes';
                if (h.includes('TIPO TRANSFERÊNCIA') || h.includes('TIPO TRANSFERENCIA')) return 'tipoTransferencia';
                if (h.includes('TIPO FAVORECIDO')) return 'tipoFavorecido';
                if (h === 'UF' || h.includes('UF ')) return 'uf';
                if (h.includes('CÓDIGO MUNICÍPIO SIAFI') || h.includes('CODIGO MUNICIPIO SIAFI')) return 'codigoMunicipioSiafi';
                if (h.includes('NOME MUNICÍPIO') || h.includes('NOME MUNICIPIO')) return 'nomeMunicipio';
                if (h.includes('CÓDIGO ÓRGÃO SIAFI') || h.includes('CODIGO ORGAO SIAFI')) return 'codigoOrgaoSiafi';
                if (h.includes('NOME ÓRGÃO') || h.includes('NOME ORGAO')) return 'nomeOrgao';
                if (h.includes('CÓDIGO UNIDADE GESTORA') || h.includes('CODIGO UNIDADE GESTORA')) return 'codigoUnidadeGestora';
                if (h.includes('NOME UNIDADE GESTORA')) return 'nomeUnidadeGestora';
                if (h.includes('CÓDIGO FUNÇÃO') || h.includes('CODIGO FUNCAO')) return 'codigoFuncao';
                if (h.includes('NOME FUNÇÃO') || h.includes('NOME FUNCAO')) return 'nomeFuncao';
                if (h.includes('CÓDIGO SUBFUNÇÃO') || h.includes('CODIGO SUBFUNCAO')) return 'codigoSubfuncao';
                if (h.includes('NOME SUBFUNÇÃO') || h.includes('NOME SUBFUNCAO')) return 'nomeSubfuncao';
                if (h.includes('CÓDIGO PROGRAMA') || h.includes('CODIGO PROGRAMA')) return 'codigoPrograma';
                if (h.includes('NOME PROGRAMA')) return 'nomePrograma';
                if (h.includes('CÓDIGO AÇÃO') || h.includes('CODIGO ACAO')) return 'codigoAcao';
                if (h.includes('NOME AÇÃO') || h.includes('NOME ACAO')) return 'nomeAcao';
                if (h.includes('LINGUAGEM CIDADÃ') || h.includes('LINGUAGEM CIDADA')) return 'linguagemCidada';
                if (h.includes('CÓDIGO GRUPO DESPESA') || h.includes('CODIGO GRUPO DESPESA')) return 'codigoGrupoDespesa';
                if (h.includes('GRUPO DE DESPESA')) return 'grupoDespesa';
                if (h.includes('CÓDIGO MODALIDADE APLICAÇÃO') || h.includes('CODIGO MODALIDADE APLICACAO')) return 'codigoModalidadeAplicacaoDespesa';
                if (h.includes('NOME MODALIDADE APLICAÇÃO') || h.includes('NOME MODALIDADE APLICACAO')) return 'nomeModalidadeAplicacaoDespesa';
                if (h.includes('CÓDIGO ELEMENTO DESPESA') || h.includes('CODIGO ELEMENTO DESPESA')) return 'codigoElementoDespesa';
                if (h.includes('NOME ELEMENTO DESPESA')) return 'nomeElementoDespesa';
                if (h.includes('CÓDIGO PLANO ORÇAMENTÁRIO') || h.includes('CODIGO PLANO ORCAMENTARIO')) return 'codigoPlanoOrcamentario';
                if (h.includes('NOME PLANO ORÇAMENTÁRIO') || h.includes('NOME PLANO ORCAMENTARIO')) return 'nomePlanoOrcamentario';
                if (h.includes('CÓDIGO SUBTÍTULO') || h.includes('CODIGO SUBTITULO')) return 'codigoSubtitulo';
                if (h.includes('NOME SUBTÍTULO') || h.includes('NOME SUBTITULO')) return 'nomeSubtitulo';
                if (h.includes('CÓDIGO LOCALIZADOR') || h.includes('CODIGO LOCALIZADOR')) return 'codigoLocalizador';
                if (h.includes('NOME LOCALIZADOR')) return 'nomeLocalizador';
                if (h.includes('SIGLA LOCALIZADOR')) return 'siglaLocalizador';
                if (h.includes('DESCRIÇÃO COMPLEMENTAR') || h.includes('DESCRICAO COMPLEMENTAR')) return 'descricaoComplementarLocalizador';
                if (h.includes('CÓDIGO FAVORECIDO') || h.includes('CODIGO FAVORECIDO')) return 'codigoFavorecido';
                if (h.includes('NOME FAVORECIDO')) return 'nomeFavorecido';
                if (h.includes('VALOR TRANSFERIDO')) return 'valorTransferido';
                
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

export async function runRecursosTransferidosIngestion() {
    const log = logger.forMethod('runRecursosTransferidosIngestion');
    const moduleName = 'recursos-transferidos';

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