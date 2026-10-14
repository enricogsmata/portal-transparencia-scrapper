import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import csvParser from 'csv-parser';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import { Pep } from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY } from '../shared.js';

const logger = new_logger(import.meta.url);
const BATCH_SIZE = 2000;

type PepRow = typeof Pep.$inferInsert;

async function persistData(data: PepRow[]) {
    const log = logger.forMethod('persistData');
    try {
        await bronze_db.insert(Pep).values(data);
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de PEP`);
        throw error;
    }
}

export async function processCsvStream(stream: NodeJS.ReadableStream): Promise<number> {
    const log = logger.forMethod('processCsvStream');
    let batch: PepRow[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase().trim();
                
                if (h === 'CPF') return 'cpf';
                if (h.includes('NOME_PEP') || h === 'NOME PEP' || h === 'NOME_PEP') return 'nomePep';
                if (h.includes('SIGLA_FUNÇÃO') || h.includes('SIGLA FUNCAO') || h.includes('SIGLA_FUNCAO') || h.includes('SIGLA FUNÇÃO')) return 'siglaFuncao';
                if (h.includes('DESCRIÇÃO_FUNÇÃO') || h.includes('DESCRICAO FUNCAO') || h.includes('DESCRICAO_FUNCAO') || h.includes('DESCRIÇÃO FUNÇÃO')) return 'descricaoFuncao';
                if (h.includes('NÍVEL_FUNÇÃO') || h.includes('NIVEL FUNCAO') || h.includes('NIVEL_FUNCAO') || h.includes('NÍVEL FUNÇÃO')) return 'nivelFuncao';
                if (h.includes('NOME_ÓRGÃO') || h.includes('NOME ORGAO') || h.includes('NOME_ORGAO') || h.includes('NOME ÓRGÃO')) return 'nomeOrgao';
                if (h.includes('DATA_INÍCIO_EXERCÍCIO') || h.includes('DATA INICIO EXERCICIO') || h.includes('DATA_INICIO_EXERCICIO') || h.includes('DATA INÍCIO EXERCÍCIO')) return 'dataInicioExercicio';
                if (h.includes('DATA_FIM_EXERCÍCIO') || h.includes('DATA FIM EXERCICIO') || h.includes('DATA_FIM_EXERCICIO') || h.includes('DATA FIM EXERCÍCIO')) return 'dataFimExercicio';
                if (h.includes('DATA_FIM_CARÊNCIA') || h.includes('DATA FIM CARENCIA') || h.includes('DATA_FIM_CARENCIA') || h.includes('DATA FIM CARÊNCIA')) return 'dataFimCarencia';
                
                return h;
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
                            const upperName = entry.fileName.toUpperCase();
                            let rowsCount = 0;
                            
                            if (upperName.includes('PEP')) {
                                rowsCount = await processCsvStream(readStream);
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

export async function runPepIngestion() {
    const log = logger.forMethod('runPepIngestion');
    const moduleName = 'pep';

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