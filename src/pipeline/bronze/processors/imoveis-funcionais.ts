import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import iconv from 'iconv-lite';
import csvParser from 'csv-parser';
import { sql } from 'drizzle-orm';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import { 
    ImoveisFuncionais, 
    ImoveisFuncionaisPermissionarios 
} from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY, extractMetadataFromFileName, ReferenceMetadata, normalize } from '../shared.js';

const logger = new_logger(import.meta.url);

const BATCH_SIZE = 2000;

async function persistImoveisFuncionais(data: (typeof ImoveisFuncionais.$inferInsert)[]) {
    const log = logger.forMethod('persistImoveisFuncionais');
    try {
        await bronze_db.insert(ImoveisFuncionais).values(data).onDuplicateKeyUpdate({
            set: {
                endereco: sql`values(${ImoveisFuncionais.endereco})`,
                orgaoResponsavel: sql`values(${ImoveisFuncionais.orgaoResponsavel})`,
                situacao: sql`values(${ImoveisFuncionais.situacao})`,
                exercicioReferencia: sql`values(${ImoveisFuncionais.exercicioReferencia})`,
                mesReferencia: sql`values(${ImoveisFuncionais.mesReferencia})`,
                diaReferencia: sql`values(${ImoveisFuncionais.diaReferencia})`,
                dadosAdicionais: sql`values(${ImoveisFuncionais.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Imóveis Funcionais`);
        throw error;
    }
}

async function persistPermissionarios(data: (typeof ImoveisFuncionaisPermissionarios.$inferInsert)[]) {
    const log = logger.forMethod('persistPermissionarios');
    try {
        await bronze_db.insert(ImoveisFuncionaisPermissionarios).values(data).onDuplicateKeyUpdate({
            set: {
                nomePermissionario: sql`values(${ImoveisFuncionaisPermissionarios.nomePermissionario})`,
                cpf: sql`values(${ImoveisFuncionaisPermissionarios.cpf})`,
                cargoFuncao: sql`values(${ImoveisFuncionaisPermissionarios.cargoFuncao})`,
                orgaoExercicio: sql`values(${ImoveisFuncionaisPermissionarios.orgaoExercicio})`,
                dataOcupacao: sql`values(${ImoveisFuncionaisPermissionarios.dataOcupacao})`,
                exercicioReferencia: sql`values(${ImoveisFuncionaisPermissionarios.exercicioReferencia})`,
                mesReferencia: sql`values(${ImoveisFuncionaisPermissionarios.mesReferencia})`,
                diaReferencia: sql`values(${ImoveisFuncionaisPermissionarios.diaReferencia})`,
                dadosAdicionais: sql`values(${ImoveisFuncionaisPermissionarios.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Permissionários de Imóveis Funcionais`);
        throw error;
    }
}

interface HeaderRule {
    key: string;
    keywords?: string[];
    matchExact?: string[];
    require?: string[];
}

const IMOVEIS_FUNCIONAIS_HEADER_RULES: HeaderRule[] = [
    { key: 'endereco', keywords: ['ENDERECO'] },
    { key: 'orgaoResponsavel', keywords: ['ORGAO RESPONSAVEL', 'RESPONSAVEL'] },
    { key: 'situacao', keywords: ['SITUACAO'] },
];

const PERMISSIONARIOS_HEADER_RULES: HeaderRule[] = [
    { key: 'orgaoExercicio', keywords: ['ORGAOEXERCICIO', 'ORGAO EXERCICIO', 'EXERCICIO'] },
    { key: 'nomePermissionario', keywords: ['PERMISSIONARIO', 'NOME PERMISSIONARIO'] },
    { key: 'cpf', matchExact: ['CPF'] },
    { key: 'cargoFuncao', keywords: ['CARGO', 'CARGOFUNCAO', 'CARGO FUNCAO'] },
    { key: 'dataOcupacao', keywords: ['DATAOCUPACAO', 'DATA OCUPACAO', 'OCUPACAO'] },
];

// Helper genérico para buscar no array de regras
function resolveHeaderKey(header: string, rules: HeaderRule[]): string {
    if (!header) return '';
    const cleanHeader = normalize(header);

    for (const rule of rules) {
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
}

async function processImoveisFuncionaisStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processImoveisFuncionaisStream');
    let batch: (typeof ImoveisFuncionais.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => resolveHeaderKey(header, IMOVEIS_FUNCIONAIS_HEADER_RULES),
            }));

        for await (const data of parser) {
            batch.push({
                ...data,
                ...metadata,
            });
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

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento de Imóveis Funcionais concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream de Imóveis Funcionais');
        throw error;
    }
}

async function processPermissionariosStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processPermissionariosStream');
    let batch: (typeof ImoveisFuncionaisPermissionarios.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => resolveHeaderKey(header, PERMISSIONARIOS_HEADER_RULES),
            }));

        for await (const data of parser) {
            batch.push({
                ...data,
                ...metadata,
            });
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

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento de Permissionários concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream de Permissionários');
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
                            
                            if (upperName.includes('PERMISSIONARIOS') || upperName.includes('PERMISSIONÁRIOS')) {
                                rowsCount = await processPermissionariosStream(readStream, metadata);
                            } else if (upperName.includes('IMOVEIS') || upperName.includes('IMÓVEIS')) {
                                rowsCount = await processImoveisFuncionaisStream(readStream, metadata);
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