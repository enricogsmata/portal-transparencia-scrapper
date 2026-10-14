// src/modules/convenios/processor.ts
import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import csvParser from 'csv-parser';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import {
    Convenios,
    ConveniosOrdensBancarias
} from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY } from '../shared.js';

const logger = new_logger(import.meta.url);
const BATCH_SIZE = 2000;

async function persistConvenios(data: (typeof Convenios.$inferInsert)[]) {
    const log = logger.forMethod('persistConvenios');
    try {
        await bronze_db.insert(Convenios).values(data);
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Convênios`);
        throw error;
    }
}

async function persistOrdensBancarias(data: (typeof ConveniosOrdensBancarias.$inferInsert)[]) {
    const log = logger.forMethod('persistOrdensBancarias');
    try {
        await bronze_db.insert(ConveniosOrdensBancarias).values(data);
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Ordens Bancárias de Convênios`);
        throw error;
    }
}

async function processConveniosStream(stream: NodeJS.ReadableStream): Promise<number> {
    const log = logger.forMethod('processConveniosStream');
    let batch: (typeof Convenios.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase().trim();
                if (h.includes('NÚMERO CONVÊNIO') || h.includes('NUMERO CONVENIO')) return 'numeroConvenio';
                if (h === 'UF' || h.includes('UF ')) return 'uf';
                if (h.includes('SIAFI MUNICÍPIO') || h.includes('SIAFI MUNICIPIO')) return 'codigoSiafiMunicipio';
                if (h.includes('NOME MUNICÍPIO') || h.includes('NOME MUNICIPIO')) return 'nomeMunicipio';
                if (h.includes('SITUAÇÃO') || h.includes('SITUACAO')) return 'situacaoConvenio';
                if (h.includes('NÚMERO ORIGINAL') || h.includes('NUMERO ORIGINAL')) return 'numeroOriginal';
                if (h.includes('NÚMERO PROCESSO') || h.includes('NUMERO PROCESSO')) return 'numeroProcessoConvenio';
                if (h.includes('OBJETO')) return 'objetoConvenio';
                if (h.includes('CÓDIGO ÓRGÃO SUPERIOR') || h.includes('CODIGO ORGAO SUPERIOR')) return 'codigoOrgaoSuperior';
                if (h.includes('NOME ÓRGÃO SUPERIOR') || h.includes('NOME ORGAO SUPERIOR')) return 'nomeOrgaoSuperior';
                if (h.includes('CÓDIGO ÓRGÃO CONCEDENTE') || h.includes('CODIGO ORGAO CONCEDENTE')) return 'codigoOrgaoConcedente';
                if (h.includes('NOME ÓRGÃO CONCEDENTE') || h.includes('NOME ORGAO CONCEDENTE')) return 'nomeOrgaoConcedente';
                if (h.includes('CÓDIGO UG CONCEDENTE') || h.includes('CODIGO UG CONCEDENTE')) return 'codigoUgConcedente';
                if (h.includes('NOME UG CONCEDENTE')) return 'nomeUgConcedente';
                if (h.includes('CÓDIGO CONVENENTE') || h.includes('CODIGO CONVENENTE')) return 'codigoConvenente';
                if (h.includes('TIPO CONVENENTE')) return 'tipoConvenente';
                if (h.includes('NOME CONVENENTE')) return 'nomeConvenente';
                if (h.includes('TIPO ENTE CONVENENTE')) return 'tipoEnteConvenente';
                if (h.includes('TIPO INSTRUMENTO')) return 'tipoInstrumento';
                if (h.includes('VALOR CONVÊNIO') || h.includes('VALOR CONVENIO')) return 'valorConvenio';
                if (h.includes('VALOR LIBERADO')) return 'valorLiberado';
                if (h.includes('DATA PUBLICAÇÃO') || h.includes('DATA PUBLICACAO')) return 'dataPublicacao';
                if (h.includes('INÍCIO VIGÊNCIA') || h.includes('INICIO VIGENCIA')) return 'dataInicioVigencia';
                if (h.includes('FIM VIGÊNCIA') || h.includes('FIM VIGENCIA')) return 'dataFimVigencia';
                if (h.includes('CONTRAPARTIDA')) return 'valorContrapartida';
                if (h.includes('DATA ÚLTIMA LIBERAÇÃO') || h.includes('DATA ULTIMA LIBERACAO')) return 'dataUltimaLiberacao';
                if (h.includes('VALOR ÚLTIMA LIBERAÇÃO') || h.includes('VALOR ULTIMA LIBERACAO')) return 'valorUltimaLiberacao';
                return h;
            },
        }));

        for await (const data of parser) {
            batch.push(data);
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

async function processOrdensBancariasStream(stream: NodeJS.ReadableStream): Promise<number> {
    const log = logger.forMethod('processOrdensBancariasStream');
    let batch: (typeof ConveniosOrdensBancarias.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase().trim();
                if (h.includes('NÚMERO CONVÊNIO') || h.includes('NUMERO CONVENIO')) return 'numeroConvenio';
                if (h.includes('NÚMERO ORIGINAL') || h.includes('NUMERO ORIGINAL')) return 'numeroOriginal';
                if (h.includes('DATA EMISSÃO OB') || h.includes('DATA EMISSAO OB')) return 'dataEmissaoOb';
                if (h.includes('NÚMERO DA ORDEM BANCÁRIA') || h.includes('NUMERO DA ORDEM BANCARIA') || h.includes('NÚMERO ORDEM BANCÁRIA')) return 'numeroOrdemBancaria';
                if (h.includes('VALOR LIBERADO')) return 'valorLiberado';
                return h;
            },
        }));

        for await (const data of parser) {
            batch.push(data);
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

                            if (upperName.includes('ORDEM_BANCARIA') || upperName.includes('ORDENS_BANCARIAS') || upperName.includes('ORDEMBANCARIA') || upperName.includes('OB')) {
                                rowsCount = await processOrdensBancariasStream(readStream);
                            } else if (upperName.includes('CONVENIO')) {
                                rowsCount = await processConveniosStream(readStream);
                            } else {
                                log.info({ data: { fileName: entry.fileName } }, 'Arquivo CSV não processado (padrão de nome desconhecido)');
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