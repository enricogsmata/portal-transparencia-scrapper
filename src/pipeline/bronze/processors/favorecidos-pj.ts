import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import iconv from 'iconv-lite';
import csvParser from 'csv-parser';
import { normalize } from '../shared.js';
import { sql } from 'drizzle-orm';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import {
    FavorecidosPjCnpj,
    FavorecidosPjCnae,
    FavorecidosPjNaturezaJuridica
} from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY, extractMetadataFromFileName, ReferenceMetadata } from '../shared.js';

const logger = new_logger(import.meta.url);

const BATCH_SIZE = 2000;

async function persistCnpj(data: (typeof FavorecidosPjCnpj.$inferInsert)[]) {
    const log = logger.forMethod('persistCnpj');
    try {
        await bronze_db.insert(FavorecidosPjCnpj).values(data).onDuplicateKeyUpdate({
            set: {
                cnpj: sql`values(${FavorecidosPjCnpj.cnpj})`,
                razaoSocial: sql`values(${FavorecidosPjCnpj.razaoSocial})`,
                nomeFantasia: sql`values(${FavorecidosPjCnpj.nomeFantasia})`,
                codigoCnae: sql`values(${FavorecidosPjCnpj.codigoCnae})`,
                codigoNaturezaJuridica: sql`values(${FavorecidosPjCnpj.codigoNaturezaJuridica})`,
                tipoPessoa: sql`values(${FavorecidosPjCnpj.tipoPessoa})`,
                logradouro: sql`values(${FavorecidosPjCnpj.logradouro})`,
                numero: sql`values(${FavorecidosPjCnpj.numero})`,
                complemento: sql`values(${FavorecidosPjCnpj.complemento})`,
                cep: sql`values(${FavorecidosPjCnpj.cep})`,
                bairro: sql`values(${FavorecidosPjCnpj.bairro})`,
                municipio: sql`values(${FavorecidosPjCnpj.municipio})`,
                uf: sql`values(${FavorecidosPjCnpj.uf})`,
                exercicioReferencia: sql`values(${FavorecidosPjCnpj.exercicioReferencia})`,
                mesReferencia: sql`values(${FavorecidosPjCnpj.mesReferencia})`,
                diaReferencia: sql`values(${FavorecidosPjCnpj.diaReferencia})`,
                dadosAdicionais: sql`values(${FavorecidosPjCnpj.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de CNPJ`);
        throw error;
    }
}

async function persistCnae(data: (typeof FavorecidosPjCnae.$inferInsert)[]) {
    const log = logger.forMethod('persistCnae');
    try {
        await bronze_db.insert(FavorecidosPjCnae).values(data).onDuplicateKeyUpdate({
            set: {
                codigoSecao: sql`values(${FavorecidosPjCnae.codigoSecao})`,
                descricaoSecao: sql`values(${FavorecidosPjCnae.descricaoSecao})`,
                codigoSubclasse: sql`values(${FavorecidosPjCnae.codigoSubclasse})`,
                descricaoSubclasse: sql`values(${FavorecidosPjCnae.descricaoSubclasse})`,
                exercicioReferencia: sql`values(${FavorecidosPjCnae.exercicioReferencia})`,
                mesReferencia: sql`values(${FavorecidosPjCnae.mesReferencia})`,
                diaReferencia: sql`values(${FavorecidosPjCnae.diaReferencia})`,
                dadosAdicionais: sql`values(${FavorecidosPjCnae.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de CNAE`);
        throw error;
    }
}

async function persistNaturezaJuridica(data: (typeof FavorecidosPjNaturezaJuridica.$inferInsert)[]) {
    const log = logger.forMethod('persistNaturezaJuridica');
    try {
        await bronze_db.insert(FavorecidosPjNaturezaJuridica).values(data).onDuplicateKeyUpdate({
            set: {
                codigoNaturezaJuridica: sql`values(${FavorecidosPjNaturezaJuridica.codigoNaturezaJuridica})`,
                descricaoNaturezaJuridica: sql`values(${FavorecidosPjNaturezaJuridica.descricaoNaturezaJuridica})`,
                codigoTipoNaturezaJuridica: sql`values(${FavorecidosPjNaturezaJuridica.codigoTipoNaturezaJuridica})`,
                descricaoTipoNaturezaJuridica: sql`values(${FavorecidosPjNaturezaJuridica.descricaoTipoNaturezaJuridica})`,
                exercicioReferencia: sql`values(${FavorecidosPjNaturezaJuridica.exercicioReferencia})`,
                mesReferencia: sql`values(${FavorecidosPjNaturezaJuridica.mesReferencia})`,
                diaReferencia: sql`values(${FavorecidosPjNaturezaJuridica.diaReferencia})`,
                dadosAdicionais: sql`values(${FavorecidosPjNaturezaJuridica.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Natureza Jurídica`);
        throw error;
    }
}

const CNPJ_HEADER_RULES = [
    { key: 'cnpj', matchExact: ['CNPJ'] },
    { key: 'razaoSocial', keywords: ['RAZAOSOCIAL', 'RAZAO SOCIAL'] },
    { key: 'nomeFantasia', keywords: ['NOMEFANTASIA', 'NOME FANTASIA'] },
    { key: 'codigoCnae', keywords: ['CODCNAE', 'COD CNAE', 'CNAE'] },
    { key: 'codigoNaturezaJuridica', keywords: ['CODNATJURIDICA', 'COD NATJURIDICA', 'NATUREZA JURIDICA'] },
    { key: 'tipoPessoa', keywords: ['TIPOPESSOA', 'TIPO PESSOA'] },
    { key: 'logradouro', matchExact: ['LOGRADOURO'] },
    { key: 'numero', matchExact: ['NUMERO'] },
    { key: 'complemento', matchExact: ['COMPLEMENTO'] },
    { key: 'cep', matchExact: ['CEP'] },
    { key: 'bairro', matchExact: ['BAIRRO'] },
    { key: 'municipio', keywords: ['MUNICIPIO'] },
    { key: 'uf', matchExact: ['UF'] },
];

const CNAE_HEADER_RULES = [
    { key: 'codigoSecao', keywords: ['CODSECAO', 'COD SECAO'] },
    { key: 'descricaoSecao', keywords: ['DESCSECAO', 'DESC SECAO'] },
    { key: 'codigoSubclasse', keywords: ['CODSUBCLASSE', 'COD SUBCLASSE'] },
    { key: 'descricaoSubclasse', keywords: ['DESCSUBCLASSE', 'DESC SUBCLASSE'] },
];

const NATUREZA_JURIDICA_HEADER_RULES = [
    { key: 'codigoTipoNaturezaJuridica', keywords: ['CODTIPONATJURIDICA', 'COD TIPO NATJURIDICA'] },
    { key: 'descricaoTipoNaturezaJuridica', keywords: ['DESCTIPONATJURIDICA', 'DESC TIPO NATJURIDICA'] },
    { key: 'codigoNaturezaJuridica', keywords: ['CODNATJURIDICA', 'COD NATJURIDICA'] },
    { key: 'descricaoNaturezaJuridica', keywords: ['DESCNATJURIDICA', 'DESC NATJURIDICA'] },
];

// Helper genérico para buscar no array de regras
function resolveHeaderKey(header: string, rules: typeof CNPJ_HEADER_RULES): string {
    if (!header) return '';
    const cleanHeader = normalize(header);

    for (const rule of rules) {
        if (rule.matchExact && rule.matchExact.includes(cleanHeader)) {
            return rule.key;
        }
        if (rule.keywords && rule.keywords.some(kw => cleanHeader.includes(kw))) {
            return rule.key;
        }
    }

    return cleanHeader;
}

async function processCnpjStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processCnpjStream');
    let batch: (typeof FavorecidosPjCnpj.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => resolveHeaderKey(header, CNPJ_HEADER_RULES),
            }));

        for await (const data of parser) {
            batch.push({
                ...data,
                ...metadata,
            });

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

async function processCnaeStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processCnaeStream');
    let batch: (typeof FavorecidosPjCnae.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => resolveHeaderKey(header, CNAE_HEADER_RULES),
            }));

        for await (const data of parser) {
            batch.push({
                ...data,
                ...metadata,
            });

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

async function processNaturezaJuridicaStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processNaturezaJuridicaStream');
    let batch: (typeof FavorecidosPjNaturezaJuridica.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => resolveHeaderKey(header, NATUREZA_JURIDICA_HEADER_RULES),
            }));

        for await (const data of parser) {
            batch.push({
                ...data,
                ...metadata,
            });

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
                            const upperName = entry.fileName.toUpperCase();
                            let rowsCount = 0;

                            if (upperName.includes('CNAE')) {
                                rowsCount = await processCnaeStream(readStream, metadata);
                            } else if (upperName.includes('CNPJ')) {
                                rowsCount = await processCnpjStream(readStream, metadata);
                            } else if (upperName.includes('NATUREZAJURIDICA')) {
                                rowsCount = await processNaturezaJuridicaStream(readStream, metadata);
                            } else {
                                log.info({ data: { fileName: entry.fileName } }, 'Arquivo CSV ignorado pois não corresponde aos padrões');
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