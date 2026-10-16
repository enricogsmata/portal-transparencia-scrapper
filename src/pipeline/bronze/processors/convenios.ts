import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import iconv from 'iconv-lite';
import csvParser from 'csv-parser';
import { normalize } from '../shared.js';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import {
    Convenios,
    ConveniosOrdensBancarias
} from '../../../database/bronze_schema.js';
import { extractMetadataFromFileName, ReferenceMetadata, ZIP_FILES_DIRECTORY } from '../shared.js';
import { sql } from 'drizzle-orm';

const logger = new_logger(import.meta.url);
const BATCH_SIZE = 2000;

async function persistConvenios(data: (typeof Convenios.$inferInsert)[]) {
    const log = logger.forMethod('persistConvenios');
    try {
        await bronze_db.insert(Convenios).values(data).onDuplicateKeyUpdate({
            set: {
                numeroConvenio: sql`values(${Convenios.numeroConvenio})`,
                uf: sql`values(${Convenios.uf})`,
                codigoSiafiMunicipio: sql`values(${Convenios.codigoSiafiMunicipio})`,
                nomeMunicipio: sql`values(${Convenios.nomeMunicipio})`,
                situacaoConvenio: sql`values(${Convenios.situacaoConvenio})`,
                numeroOriginal: sql`values(${Convenios.numeroOriginal})`,
                numeroProcessoConvenio: sql`values(${Convenios.numeroProcessoConvenio})`,
                objetoConvenio: sql`values(${Convenios.objetoConvenio})`,
                codigoOrgaoSuperior: sql`values(${Convenios.codigoOrgaoSuperior})`,
                nomeOrgaoSuperior: sql`values(${Convenios.nomeOrgaoSuperior})`,
                codigoOrgaoConcedente: sql`values(${Convenios.codigoOrgaoConcedente})`,
                nomeOrgaoConcedente: sql`values(${Convenios.nomeOrgaoConcedente})`,
                codigoUgConcedente: sql`values(${Convenios.codigoUgConcedente})`,
                nomeUgConcedente: sql`values(${Convenios.nomeUgConcedente})`,
                codigoConvenente: sql`values(${Convenios.codigoConvenente})`,
                tipoConvenente: sql`values(${Convenios.tipoConvenente})`,
                nomeConvenente: sql`values(${Convenios.nomeConvenente})`,
                tipoEnteConvenente: sql`values(${Convenios.tipoEnteConvenente})`,
                tipoInstrumento: sql`values(${Convenios.tipoInstrumento})`,
                valorConvenio: sql`values(${Convenios.valorConvenio})`,
                valorLiberado: sql`values(${Convenios.valorLiberado})`,
                dataPublicacao: sql`values(${Convenios.dataPublicacao})`,
                dataInicioVigencia: sql`values(${Convenios.dataInicioVigencia})`,
                dataFimVigencia: sql`values(${Convenios.dataFimVigencia})`,
                valorContrapartida: sql`values(${Convenios.valorContrapartida})`,
                dataUltimaLiberacao: sql`values(${Convenios.dataUltimaLiberacao})`,
                valorUltimaLiberacao: sql`values(${Convenios.valorUltimaLiberacao})`,
                exercicioReferencia: sql`values(${Convenios.exercicioReferencia})`,
                mesReferencia: sql`values(${Convenios.mesReferencia})`,
                diaReferencia: sql`values(${Convenios.diaReferencia})`,
                dadosAdicionais: sql`values(${Convenios.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Convênios`);
        throw error;
    }
}

async function persistOrdensBancarias(data: (typeof ConveniosOrdensBancarias.$inferInsert)[]) {
    const log = logger.forMethod('persistOrdensBancarias');
    try {
        await bronze_db.insert(ConveniosOrdensBancarias).values(data).onDuplicateKeyUpdate({
            set: {
                numeroConvenio: sql`values(${ConveniosOrdensBancarias.numeroConvenio})`,
                numeroOriginal: sql`values(${ConveniosOrdensBancarias.numeroOriginal})`,
                dataEmissaoOb: sql`values(${ConveniosOrdensBancarias.dataEmissaoOb})`,
                numeroOrdemBancaria: sql`values(${ConveniosOrdensBancarias.numeroOrdemBancaria})`,
                valorLiberado: sql`values(${ConveniosOrdensBancarias.valorLiberado})`,
                exercicioReferencia: sql`values(${ConveniosOrdensBancarias.exercicioReferencia})`,
                mesReferencia: sql`values(${ConveniosOrdensBancarias.mesReferencia})`,
                diaReferencia: sql`values(${ConveniosOrdensBancarias.diaReferencia})`,
                dadosAdicionais: sql`values(${ConveniosOrdensBancarias.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Ordens Bancárias de Convênios`);
        throw error;
    }
}

const CONVENIOS_HEADER_RULES = [
    { key: 'numeroConvenio', keywords: ['NUMERO CONVENIO', 'NUMERO DO CONVENIO'] },
    { key: 'uf', matchExact: ['UF'] },
    { key: 'codigoSiafiMunicipio', keywords: ['SIAFI MUNICIPIO', 'SIAFI DO MUNICIPIO'] },
    { key: 'nomeMunicipio', keywords: ['NOME MUNICIPIO', 'NOME DO MUNICIPIO'] },
    { key: 'situacaoConvenio', keywords: ['SITUACAO CONVENIO', 'SITUACAO DO CONVENIO', 'SITUACAO'] },
    { key: 'numeroOriginal', keywords: ['NUMERO ORIGINAL'] },
    { key: 'numeroProcessoConvenio', keywords: ['NUMERO PROCESSO', 'NUMERO DO PROCESSO'] },
    { key: 'objetoConvenio', keywords: ['OBJETO'] },

    // Órgãos e UG Concedente
    { key: 'codigoOrgaoSuperior', keywords: ['ORGAO SUPERIOR'], require: ['CODIGO'] },
    { key: 'nomeOrgaoSuperior', keywords: ['ORGAO SUPERIOR'], require: ['NOME'] },
    { key: 'codigoOrgaoConcedente', keywords: ['ORGAO CONCEDENTE'], require: ['CODIGO'] },
    { key: 'nomeOrgaoConcedente', keywords: ['ORGAO CONCEDENTE'], require: ['NOME'] },
    { key: 'codigoUgConcedente', keywords: ['UG CONCEDENTE'], require: ['CODIGO'] },
    { key: 'nomeUgConcedente', keywords: ['UG CONCEDENTE'], require: ['NOME'] },

    // Convenente
    { key: 'codigoConvenente', keywords: ['CONVENENTE'], require: ['CODIGO'] },
    { key: 'tipoConvenente', keywords: ['CONVENENTE'], require: ['TIPO'] },
    { key: 'tipoEnteConvenente', keywords: ['TIPO CONVENENTE'], require: ['TIPO'] },
    { key: 'nomeConvenente', keywords: ['CONVENENTE'], require: ['NOME'] },

    // Instrumento e Valores
    { key: 'tipoInstrumento', keywords: ['TIPO INSTRUMENTO', 'TIPO DE INSTRUMENTO'] },
    { key: 'valorConvenio', keywords: ['VALOR CONVENIO', 'VALOR DO CONVENIO'] },
    { key: 'dataUltimaLiberacao', keywords: ['ULTIMA LIBERACAO'], require: ['DATA'] },
    { key: 'valorUltimaLiberacao', keywords: ['ULTIMA LIBERACAO'], require: ['VALOR'] },
    { key: 'valorLiberado', keywords: ['VALOR LIBERADO'] },
    { key: 'valorContrapartida', keywords: ['CONTRAPARTIDA'] },

    // Datas
    { key: 'dataPublicacao', keywords: ['DATA PUBLICACAO', 'DATA DE PUBLICACAO'] },
    { key: 'dataInicioVigencia', keywords: ['INICIO VIGENCIA', 'DATA INICIO VIGENCIA'] },
    { key: 'dataFimVigencia', keywords: ['FIM VIGENCIA', 'FINAL VIGENCIA'] },
];

const ORDENS_BANCARIAS_HEADER_RULES = [
    { key: 'numeroConvenio', keywords: ['NUMERO CONVENIO', 'NUMERO DO CONVENIO'] },
    { key: 'numeroOriginal', keywords: ['NUMERO ORIGINAL'] },
    { key: 'dataEmissaoOb', keywords: ['DATA EMISSAO OB', 'DATA DE EMISSAO OB'] },
    { key: 'numeroOrdemBancaria', keywords: ['ORDEM BANCARIA', 'NUMERO DA ORDEM BANCARIA'] },
    { key: 'valorLiberado', keywords: ['VALOR LIBERADO'] },
];

// Helper reutilizável para aplicar as regras
function resolveHeaderKey(header: string, rules: typeof CONVENIOS_HEADER_RULES): string {
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

async function processConveniosStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processConveniosStream');
    let batch: (typeof Convenios.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => resolveHeaderKey(header, CONVENIOS_HEADER_RULES),
            }));

        for await (const data of parser) {
            batch.push({
                ...data,
                ...metadata,
            });
            if (batch.length >= BATCH_SIZE) {
                await persistConvenios(batch);
                totalProcessed += batch.length;
                batch = [];
            }
        }
        if (batch.length > 0) {
            await persistConvenios(batch);
            totalProcessed += batch.length;
        }

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento do arquivo Convênios concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream do arquivo Convênios');
        throw error;
    }
}

async function processOrdensBancariasStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processOrdensBancariasStream');
    let batch: (typeof ConveniosOrdensBancarias.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => resolveHeaderKey(header, ORDENS_BANCARIAS_HEADER_RULES),
            }));

        for await (const data of parser) {
            batch.push({
                ...data,
                ...metadata,
            });
            if (batch.length >= BATCH_SIZE) {
                await persistOrdensBancarias(batch);
                totalProcessed += batch.length;
                batch = [];
            }
        }
        if (batch.length > 0) {
            await persistOrdensBancarias(batch);
            totalProcessed += batch.length;
        }

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento do arquivo Ordens Bancárias concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream do arquivo Ordens Bancárias');
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

                            const streamEndPromise = new Promise<void>((resolveStream, rejectStream) => {
                                readStream.on('end', resolveStream);
                                readStream.on('error', rejectStream);
                            });

                            if (upperName.includes('ORDEM_BANCARIA') || upperName.includes('ORDENS_BANCARIAS') || upperName.includes('ORDEMBANCARIA') || upperName.includes('ORDENSBANCARIAS') || upperName.includes('OB.CSV')) {
                                rowsCount = await processOrdensBancariasStream(readStream, metadata);
                            } else if (upperName.includes('CONVENIO')) {
                                rowsCount = await processConveniosStream(readStream, metadata);
                            } else {
                                log.info({ data: { fileName: entry.fileName } }, 'Arquivo CSV não processado (padrão de nome desconhecido)');
                                readStream.resume();
                            }

                            await streamEndPromise;

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

export async function runConveniosIngestion() {
    const log = logger.forMethod('runConveniosIngestion');
    const moduleName = 'convenios';

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