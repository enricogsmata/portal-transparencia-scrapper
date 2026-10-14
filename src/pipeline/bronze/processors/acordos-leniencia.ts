import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import csvParser from 'csv-parser';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import { AcordosLeniencia } from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY } from '../shared.js';

const logger = new_logger(import.meta.url);
const BATCH_SIZE = 2000;

type AcordosLenienciaRow = typeof AcordosLeniencia.$inferInsert;

async function persistData(data: AcordosLenienciaRow[]) {
    const log = logger.forMethod('persistData');
    try {
        await bronze_db.insert(AcordosLeniencia).values(data);
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Acordos de Leniência`);
        throw error;
    }
}

export async function processCsvStream(stream: NodeJS.ReadableStream): Promise<number> {
    const log = logger.forMethod('processCsvStream');
    let batch: AcordosLenienciaRow[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase().trim();
                
                if (h.includes('CNPJ DO SANCIONADO') || h.includes('CNPJ SANCIONADO')) return 'cnpjSancionado';
                if (h.includes('NOME INFORMADO PELO ÓRGÃO SANCIONADOR') || h.includes('NOME INFORMADO PELO ORGAO SANCIONADOR')) return 'nomeInformadoOrgaoSancionador';
                if (h.includes('RAZÃO SOCIAL') || h.includes('RAZAO SOCIAL')) return 'razaoSocialCadastroReceita';
                if (h.includes('NOME FANTASIA')) return 'nomeFantasiaCadastroReceita';
                if (h.includes('NÚMERO DO PROCESSO') || h.includes('NUMERO DO PROCESSO')) return 'numeroProcesso';
                if (h.includes('DATA INÍCIO ACORDO') || h.includes('DATA INICIO ACORDO')) return 'dataInicioAcordo';
                if (h.includes('DATA CONCLUSÃO ACORDO') || h.includes('DATA CONCLUSAO ACORDO')) return 'dataConclusaoAcordo';
                if (h === 'ÓRGÃO SANCIONADOR' || h === 'ORGAO SANCIONADOR') return 'orgaoSancionador';
                if (h.includes('UF DO SANCIONADO') || h.includes('UF SANCIONADO')) return 'ufSancionado';
                if (h.includes('ORIGEM INFORMAÇÕES') || h.includes('ORIGEM INFORMACOES') || h.includes('ORIGEM INFORMAÇÃO')) return 'origemInformacoes';
                if (h.includes('SITUAÇÃO DO ACORDO') || h.includes('SITUACAO DO ACORDO')) return 'situacaoAcordo';
                if (h.includes('CONTATO DA ORIGEM DA INFORMAÇÃO') || h.includes('CONTATO DA ORIGEM DA INFORMACAO') || h.includes('CONTATO')) return 'contatoOrigemInformacao';
                
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
                            
                            if (upperName.includes('ACORDO') || upperName.includes('LENIENCIA') || upperName.includes('LENIÊNCIA')) {
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