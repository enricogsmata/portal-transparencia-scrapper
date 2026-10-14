import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import csvParser from 'csv-parser';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import { 
    FavorecidosPjCnpj, 
    FavorecidosPjCnae, 
    FavorecidosPjNaturezaJuridica 
} from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY } from '../shared.js';

const logger = new_logger(import.meta.url);

const BATCH_SIZE = 2000;

async function persistCnpj(data: (typeof FavorecidosPjCnpj.$inferInsert)[]) {
    const log = logger.forMethod('persistCnpj');
    try {
        await bronze_db.insert(FavorecidosPjCnpj).values(data);
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de CNPJ`);
        throw error;
    }
}

async function persistCnae(data: (typeof FavorecidosPjCnae.$inferInsert)[]) {
    const log = logger.forMethod('persistCnae');
    try {
        await bronze_db.insert(FavorecidosPjCnae).values(data);
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de CNAE`);
        throw error;
    }
}

async function persistNaturezaJuridica(data: (typeof FavorecidosPjNaturezaJuridica.$inferInsert)[]) {
    const log = logger.forMethod('persistNaturezaJuridica');
    try {
        await bronze_db.insert(FavorecidosPjNaturezaJuridica).values(data);
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Natureza Jurídica`);
        throw error;
    }
}

async function processCnpjStream(stream: NodeJS.ReadableStream): Promise<number> {
    const log = logger.forMethod('processCnpjStream');
    let batch: (typeof FavorecidosPjCnpj.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase().trim();
                if (h === 'CNPJ') return 'cnpj';
                if (h === 'RAZAOSOCIAL') return 'razaoSocial';
                if (h === 'NOMEFANTASIA') return 'nomeFantasia';
                if (h === 'COD_CNAE') return 'codigoCnae';
                if (h === 'COD_NATJURIDICA') return 'codigoNaturezaJuridica';
                if (h === 'TIPO_PESSOA') return 'tipoPessoa';
                if (h === 'LOGRADOURO') return 'logradouro';
                if (h === 'NUMERO') return 'numero';
                if (h === 'COMPLEMENTO') return 'complemento';
                if (h === 'CEP') return 'cep';
                if (h === 'BAIRRO') return 'bairro';
                if (h === 'MUNICIPIO') return 'municipio';
                if (h === 'UF') return 'uf';
                return h;
            },
        }));

        for await (const data of parser) {
            batch.push(data);
            if (batch.length >= BATCH_SIZE) {
                await persistCnpj(batch);
                totalProcessed += batch.length;
                batch = [];
            }
        }
        if (batch.length > 0) {
            await persistCnpj(batch);
            totalProcessed += batch.length;
        }

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento do arquivo CNPJ concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream do arquivo CNPJ');
        throw error;
    }
}

async function processCnaeStream(stream: NodeJS.ReadableStream): Promise<number> {
    const log = logger.forMethod('processCnaeStream');
    let batch: (typeof FavorecidosPjCnae.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase().trim();
                if (h === 'COD_SECAO') return 'codigoSecao';
                if (h === 'DESC_SECAO') return 'descricaoSecao';
                if (h === 'COD_SUBCLASSE') return 'codigoSubclasse';
                if (h === 'DESC_SUBCLASSE') return 'descricaoSubclasse';
                return h;
            },
        }));

        for await (const data of parser) {
            batch.push(data);
            if (batch.length >= BATCH_SIZE) {
                await persistCnae(batch);
                totalProcessed += batch.length;
                batch = [];
            }
        }
        if (batch.length > 0) {
            await persistCnae(batch);
            totalProcessed += batch.length;
        }

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento do arquivo CNAE concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream do arquivo CNAE');
        throw error;
    }
}

async function processNaturezaJuridicaStream(stream: NodeJS.ReadableStream): Promise<number> {
    const log = logger.forMethod('processNaturezaJuridicaStream');
    let batch: (typeof FavorecidosPjNaturezaJuridica.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase().trim();
                if (h === 'COD_NATJURIDICA') return 'codigoNaturezaJuridica';
                if (h === 'DESC_NATJURIDICA') return 'descricaoNaturezaJuridica';
                if (h === 'COD_TIPO_NATJURIDICA') return 'codigoTipoNaturezaJuridica';
                if (h === 'DESC_TIPO_NATJURIDICA') return 'descricaoTipoNaturezaJuridica';
                return h;
            },
        }));

        for await (const data of parser) {
            batch.push(data);
            if (batch.length >= BATCH_SIZE) {
                await persistNaturezaJuridica(batch);
                totalProcessed += batch.length;
                batch = [];
            }
        }
        if (batch.length > 0) {
            await persistNaturezaJuridica(batch);
            totalProcessed += batch.length;
        }

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento do arquivo Natureza Jurídica concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream do arquivo Natureza Jurídica');
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
                            
                            if (upperName.includes('CNAE')) {
                                rowsCount = await processCnaeStream(readStream);
                            } else if (upperName.includes('CNPJ')) {
                                rowsCount = await processCnpjStream(readStream);
                            } else if (upperName.includes('NATUREZAJURIDICA')) {
                                rowsCount = await processNaturezaJuridicaStream(readStream);
                            } else {
                                log.info({ data: { fileName: entry.fileName } }, 'Arquivo CSV ignorado pois não corresponde aos padrões');
                                readStream.resume(); // Esgota o stream para liberar memória
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

export async function runFavorecidosPjIngestion() {
    const log = logger.forMethod('runFavorecidosPjIngestion');
    const moduleName = 'favorecidos-pj';

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