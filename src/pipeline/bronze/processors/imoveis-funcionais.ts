import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import csvParser from 'csv-parser';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import { 
    ImoveisFuncionais, 
    ImoveisFuncionaisPermissionarios 
} from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY } from '../shared.js';

const logger = new_logger(import.meta.url);

const BATCH_SIZE = 2000;

async function persistImoveisFuncionais(data: (typeof ImoveisFuncionais.$inferInsert)[]) {
    const log = logger.forMethod('persistImoveisFuncionais');
    try {
        await bronze_db.insert(ImoveisFuncionais).values(data);
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Imóveis Funcionais`);
        throw error;
    }
}

async function persistPermissionarios(data: (typeof ImoveisFuncionaisPermissionarios.$inferInsert)[]) {
    const log = logger.forMethod('persistPermissionarios');
    try {
        await bronze_db.insert(ImoveisFuncionaisPermissionarios).values(data);
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Permissionários de Imóveis Funcionais`);
        throw error;
    }
}

async function processImoveisFuncionaisStream(stream: NodeJS.ReadableStream): Promise<number> {
    const log = logger.forMethod('processImoveisFuncionaisStream');
    let batch: (typeof ImoveisFuncionais.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase().trim();
                if (h.includes('ENDEREÇO') || h.includes('ENDERECO')) return 'endereco';
                if (h.includes('ÓRGÃO RESPONSÁVEL') || h.includes('ORGAO RESPONSAVEL')) return 'orgaoResponsavel';
                if (h.includes('SITUAÇÃO') || h.includes('SITUACAO')) return 'situacao';
                return h;
            },
        }));

        for await (const data of parser) {
            batch.push(data);
            if (batch.length >= BATCH_SIZE) {
                await persistImoveisFuncionais(batch);
                totalProcessed += batch.length;
                batch = [];
            }
        }
        if (batch.length > 0) {
            await persistImoveisFuncionais(batch);
            totalProcessed += batch.length;
        }

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream');
        throw error;
    }
}

async function processPermissionariosStream(stream: NodeJS.ReadableStream): Promise<number> {
    const log = logger.forMethod('processPermissionariosStream');
    let batch: (typeof ImoveisFuncionaisPermissionarios.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase().trim();
                if (h.includes('NOME PERMISSIONÁRIO') || h.includes('NOME PERMISSIONARIO')) return 'nomePermissionario';
                if (h === 'CPF') return 'cpf';
                if (h.includes('CARGOFUNÇÃO') || h.includes('CARGOFUNCAO') || h.includes('CARGO/FUNÇÃO') || h.includes('CARGO/FUNCAO')) return 'cargoFuncao';
                if (h.includes('ORGAOEXERCICIO') || h.includes('ÓRGÃO EXERCÍCIO') || h.includes('ORGAO EXERCICIO')) return 'orgaoExercicio';
                if (h.includes('DATAOCUPACAO') || h.includes('DATA OCUPAÇÃO') || h.includes('DATA OCUPACAO')) return 'dataOcupacao';
                return h;
            },
        }));

        for await (const data of parser) {
            batch.push(data);
            if (batch.length >= BATCH_SIZE) {
                await persistPermissionarios(batch);
                totalProcessed += batch.length;
                batch = [];
            }
        }
        if (batch.length > 0) {
            await persistPermissionarios(batch);
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
                            
                            if (upperName.includes('PERMISSIONARIOS') || upperName.includes('PERMISSIONÁRIOS')) {
                                rowsCount = await processPermissionariosStream(readStream);
                            } else if (upperName.includes('IMOVEIS') || upperName.includes('IMÓVEIS')) {
                                rowsCount = await processImoveisFuncionaisStream(readStream);
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

export async function runImoveisFuncionaisIngestion() {
    const log = logger.forMethod('runImoveisFuncionaisIngestion');
    const moduleName = 'imoveis-funcionais';

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