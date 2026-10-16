import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import csvParser from 'csv-parser';
import iconv from 'iconv-lite';
import { sql } from 'drizzle-orm';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import { AcordosLeniencia, AcordosLenienciaEfeitos } from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY, extractMetadataFromFileName, ReferenceMetadata, normalize } from '../shared.js';

const logger = new_logger(import.meta.url);
const BATCH_SIZE = 2000;

type AcordosLenienciaRow = typeof AcordosLeniencia.$inferInsert;
type AcordosLenienciaEfeitosRow = typeof AcordosLenienciaEfeitos.$inferInsert;

async function persistData(data: AcordosLenienciaRow[]) {
    const log = logger.forMethod('persistData');
    try {
        await bronze_db.insert(AcordosLeniencia).values(data).onDuplicateKeyUpdate({
            set: {
                idAcordo: sql`values(${AcordosLeniencia.idAcordo})`,
                cnpjSancionado: sql`values(${AcordosLeniencia.cnpjSancionado})`,
                nomeInformadoOrgaoSancionador: sql`values(${AcordosLeniencia.nomeInformadoOrgaoSancionador})`,
                razaoSocialCadastroReceita: sql`values(${AcordosLeniencia.razaoSocialCadastroReceita})`,
                nomeFantasiaCadastroReceita: sql`values(${AcordosLeniencia.nomeFantasiaCadastroReceita})`,
                numeroProcesso: sql`values(${AcordosLeniencia.numeroProcesso})`,
                dataInicioAcordo: sql`values(${AcordosLeniencia.dataInicioAcordo})`,
                dataConclusaoAcordo: sql`values(${AcordosLeniencia.dataConclusaoAcordo})`,
                orgaoSancionador: sql`values(${AcordosLeniencia.orgaoSancionador})`,
                ufSancionado: sql`values(${AcordosLeniencia.ufSancionado})`,
                origemInformacoes: sql`values(${AcordosLeniencia.origemInformacoes})`,
                situacaoAcordo: sql`values(${AcordosLeniencia.situacaoAcordo})`,
                contatoOrigemInformacao: sql`values(${AcordosLeniencia.contatoOrigemInformacao})`,
                dataInformacao: sql`values(${AcordosLeniencia.dataInformacao})`,
                termosAcordo: sql`values(${AcordosLeniencia.termosAcordo})`,
                exercicioReferencia: sql`values(${AcordosLeniencia.exercicioReferencia})`,
                mesReferencia: sql`values(${AcordosLeniencia.mesReferencia})`,
                diaReferencia: sql`values(${AcordosLeniencia.diaReferencia})`,
                dadosAdicionais: sql`values(${AcordosLeniencia.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Acordos de Leniência`);
        throw error;
    }
}

async function persistEfeitosData(data: AcordosLenienciaEfeitosRow[]) {
    const log = logger.forMethod('persistEfeitosData');
    try {
        await bronze_db.insert(AcordosLenienciaEfeitos).values(data).onDuplicateKeyUpdate({
            set: {
                idAcordo: sql`values(${AcordosLenienciaEfeitos.idAcordo})`,
                efeitoAcordo: sql`values(${AcordosLenienciaEfeitos.efeitoAcordo})`,
                complemento: sql`values(${AcordosLenienciaEfeitos.complemento})`,
                exercicioReferencia: sql`values(${AcordosLenienciaEfeitos.exercicioReferencia})`,
                mesReferencia: sql`values(${AcordosLenienciaEfeitos.mesReferencia})`,
                diaReferencia: sql`values(${AcordosLenienciaEfeitos.diaReferencia})`,
                dadosAdicionais: sql`values(${AcordosLenienciaEfeitos.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Efeitos do Acordo de Leniência`);
        throw error;
    }
}

interface HeaderRule {
    key: string;
    keywords?: string[];
    matchExact?: string[];
    require?: string[];
}

const ACORDOS_LENIENCIA_HEADER_RULES: HeaderRule[] = [
    { key: 'idAcordo', matchExact: ['ID DO ACORDO', 'ID ACORDO'] },
    { key: 'cnpjSancionado', keywords: ['SANCIONADO'], require: ['CNPJ'] },
    { key: 'nomeInformadoOrgaoSancionador', keywords: ['INFORMADO PELO ORGAO'] },
    { key: 'razaoSocialCadastroReceita', keywords: ['RAZAO SOCIAL'] },
    { key: 'nomeFantasiaCadastroReceita', keywords: ['NOME FANTASIA'] },
    { key: 'dataInicioAcordo', keywords: ['INICIO DO ACORDO'], require: ['DATA', 'INICIO'] },
    { key: 'dataConclusaoAcordo', keywords: ['FIM DO ACORDO'], require: ['DATA', 'FIM'] },
    { key: 'situacaoAcordo', keywords: ['SITUACAO DO ACORDO', 'SITUACAO ACORDO'] },
    { key: 'dataInformacao', keywords: ['DATA DA INFORMACAO', 'DATA INFORMACAO'] },
    { key: 'numeroProcesso', keywords: ['PROCESSO', 'NUMERO DO PROCESSO'] },
    { key: 'termosAcordo', keywords: ['TERMOS DO ACORDO', 'TERMOS ACORDO'] },
    { key: 'orgaoSancionador', matchExact: ['ORGAO SANCIONADOR'] },
    { key: 'ufSancionado', keywords: ['SANCIONADO'], require: ['UF'] },
    { key: 'contatoOrigemInformacao', keywords: ['CONTATO'] },
    { key: 'origemInformacoes', keywords: ['ORIGEM INFORMACAO', 'ORIGEM INFORMACOES'] },
];

const EFEITOS_HEADER_RULES: HeaderRule[] = [
    { key: 'idAcordo', matchExact: ['ID DO ACORDO', 'ID ACORDO'] },
    { key: 'efeitoAcordo', keywords: ['EFEITO DO ACORDO', 'EFEITO'] },
    { key: 'complemento', matchExact: ['COMPLEMENTO'] },
];

// Helper genérico para resolução das chaves
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

export async function processCsvStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processCsvStream');
    let batch: AcordosLenienciaRow[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => resolveHeaderKey(header, ACORDOS_LENIENCIA_HEADER_RULES),
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

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento dos Acordos de Leniência concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream dos Acordos de Leniência');
        throw error;
    }
}

export async function processEfeitosStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processEfeitosStream');
    let batch: AcordosLenienciaEfeitosRow[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => resolveHeaderKey(header, EFEITOS_HEADER_RULES),
            }));

        for await (const data of parser) {
            batch.push({
                ...data,
                ...metadata,
            });

            if (batch.length >= BATCH_SIZE) {
                await persistEfeitosData(batch);
                totalProcessed += batch.length;
                batch = [];
            }
        }

        if (batch.length > 0) {
            await persistEfeitosData(batch);
            totalProcessed += batch.length;
        }

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento dos Efeitos de Leniência concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream dos Efeitos de Leniência');
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

                            if (upperName.includes('EFEITO')) {
                                rowsCount = await processEfeitosStream(readStream, metadata);
                            } else if (upperName.includes('ACORDO') || upperName.includes('LENIENCIA') || upperName.includes('LENIÊNCIA')) {
                                rowsCount = await processCsvStream(readStream, metadata);
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

export async function runAcordosLenienciaIngestion() {
    const log = logger.forMethod('runAcordosLenienciaIngestion');
    const moduleName = 'acordos-leniencia';

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