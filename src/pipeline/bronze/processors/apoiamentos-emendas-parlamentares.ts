import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import csvParser from 'csv-parser';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import { ApoiamentosEmendasParlamentares } from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY } from '../shared.js';

const logger = new_logger(import.meta.url);

type ApoiamentosEmendasParlamentaresRow = typeof ApoiamentosEmendasParlamentares.$inferInsert;

async function persistData(data: ApoiamentosEmendasParlamentaresRow[]) {
    const log = logger.forMethod('persistData');
    try {
        await bronze_db.insert(ApoiamentosEmendasParlamentares).values(data);
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência`);
        throw error;
    }
}

export async function processCsvStream(stream: NodeJS.ReadableStream): Promise<number> {
    const log = logger.forMethod('processCsvStream');
    const BATCH_SIZE = 2000;
    let batch: ApoiamentosEmendasParlamentaresRow[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase().trim();
                
                if (h.includes('CÓDIGO APOIADOR') || h.includes('CODIGO APOIADOR')) return 'codigoApoiador';
                if (h === 'APOIADOR') return 'apoiador';
                if (h === 'DATA DO APOIO') return 'dataApoio';
                if (h.includes('DATA RETIRADA DO APOIO')) return 'dataRetiradaApoio';
                if (h === 'EMPENHO') return 'empenho';
                if (h.includes('DATA ÚLTIMA MOVIMENTAÇÃO') || h.includes('DATA ULTIMA MOVIMENTACAO')) return 'dataUltimaMovimentacaoEmpenho';
                if (h.includes('CÓDIGO FAVORECIDO') || h.includes('CODIGO FAVORECIDO')) return 'codigoFavorecido';
                if (h === 'FAVORECIDO') return 'favorecido';
                if (h === 'TIPO FAVORECIDO') return 'tipoFavorecido';
                if (h === 'UF FAVORECIDO') return 'ufFavorecido';
                if (h.includes('MUNICÍPIO FAVORECIDO') || h.includes('MUNICIPIO FAVORECIDO')) return 'municipioFavorecido';
                if (h.includes('CÓDIGO DA EMENDA') || h.includes('CODIGO DA EMENDA')) return 'codigoEmenda';
                if (h.includes('TIPO DE EMENDA')) return 'tipoEmenda';
                if (h === 'ANO DA EMENDA') return 'anoEmenda';
                if (h === 'CÓDIGO UG' || h === 'CODIGO UG') return 'codigoUg';
                if (h === 'UG') return 'ug';
                if (h === 'CÓDIGO UNIDADE ORÇAMENTÁRIA' || h === 'CODIGO UNIDADE ORCAMENTARIA') return 'codigoUnidadeOrcamentaria';
                if (h === 'UNIDADE ORÇAMENTÁRIA' || h === 'UNIDADE ORCAMENTARIA') return 'unidadeOrcamentaria';
                if (h === 'CÓDIGO ÓRGÃO SIAFI' || h === 'CODIGO ORGAO SIAFI') return 'codigoOrgaoSiafi';
                if (h === 'ÓRGÃO' || h === 'ORGAO') return 'orgao';
                if (h === 'CÓDIGO ÓRGÃO SUPERIOR SIAFI' || h === 'CODIGO ORGAO SUPERIOR SIAFI') return 'codigoOrgaoSuperiorSiafi';
                if (h === 'ÓRGÃO SUPERIOR' || h === 'ORGAO SUPERIOR') return 'orgaoSuperior';
                if (h === 'CÓDIGO AÇÃO' || h === 'CODIGO ACAO') return 'codigoAcao';
                if (h === 'AÇÃO' || h === 'ACAO') return 'acao';
                if (h === 'VALOR EMPENHADO') return 'valorEmpenhado';
                if (h === 'VALOR CANCELADO') return 'valorCancelado';
                if (h === 'VALOR PAGO') return 'valorPago';
                
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