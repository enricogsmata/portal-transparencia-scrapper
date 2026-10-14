import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import csvParser from 'csv-parser';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import { Ceaf } from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY } from '../shared.js';

const logger = new_logger(import.meta.url);
const BATCH_SIZE = 2000;

type CeafRow = typeof Ceaf.$inferInsert;

async function persistData(data: CeafRow[]) {
    const log = logger.forMethod('persistData');
    try {
        await bronze_db.insert(Ceaf).values(data);
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de CEAF`);
        throw error;
    }
}

export async function processCsvStream(stream: NodeJS.ReadableStream): Promise<number> {
    const log = logger.forMethod('processCsvStream');
    let batch: CeafRow[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase().trim();
                
                if (h === 'CADASTRO') return 'cadastro';
                if (h.includes('CÓDIGO DA SANÇÃO') || h.includes('CODIGO DA SANCAO')) return 'codigoSancao';
                if (h.includes('TIPO DE PESSOA')) return 'tipoPessoa';
                if (h.includes('CPF OU CNPJ DO SANCIONADO') || h.includes('CPF/CNPJ DO SANCIONADO')) return 'cpfCnpjSancionado';
                if (h.includes('NOME DO SANCIONADO')) return 'nomeSancionado';
                if (h.includes('CATEGORIA DA SANÇÃO') || h.includes('CATEGORIA DA SANCAO')) return 'categoriaSancao';
                if (h.includes('NÚMERO DO DOCUMENTO') || h.includes('NUMERO DO DOCUMENTO')) return 'numeroDocumento';
                if (h.includes('NÚMERO DO PROCESSO') || h.includes('NUMERO DO PROCESSO') || h.includes('NÚMERO PROCESSO')) return 'numeroProcesso';
                if (h.includes('DATA INÍCIO SANÇÃO') || h.includes('DATA INICIO SANCAO')) return 'dataInicioSancao';
                if (h.includes('DATA FINAL SANÇÃO') || h.includes('DATA FINAL SANCAO')) return 'dataFinalSancao';
                if (h.includes('DATA PUBLICAÇÃO') || h.includes('DATA PUBLICACAO')) return 'dataPublicacao';
                if (h === 'PUBLICAÇÃO' || h === 'PUBLICACAO') return 'publicacao';
                if (h.includes('DETALHAMENTO DO MEIO DE PUBLICAÇÃO') || h.includes('DETALHAMENTO DO MEIO DE PUBLICACAO') || h.includes('DETALHAMENTO')) return 'detalhamentoMeioPublicacao';
                if (h.includes('DATA DO TRÂNSITO EM JULGADO') || h.includes('DATA DO TRANSITO EM JULGADO')) return 'dataTransitoJulgado';
                if (h.includes('ABRAGÊNCIA DA SANÇÃO') || h.includes('ABRANGENCIA DA SANCAO') || h.includes('ABRANGÊNCIA DA SANÇÃO')) return 'abrangenciaSancao';
                if (h.includes('CARGO EFETIVO')) return 'cargoEfetivo';
                if (h.includes('FUNÇÃO OU CARGO DE CONFIANÇA') || h.includes('FUNCAO OU CARGO DE CONFIANCA')) return 'funcaoCargoConfianca';
                if (h.includes('ÓRGÃO DE LOTAÇÃO') || h.includes('ORGAO DE LOTACAO')) return 'orgaoLotacao';
                if (h === 'ÓRGÃO SANCIONADOR' || h === 'ORGAO SANCIONADOR') return 'orgaoSancionador';
                if (h.includes('UF ÓRGÃO SANCIONADOR') || h.includes('UF ORGAO SANCIONADOR')) return 'ufOrgaoSancionador';
                if (h.includes('ESFERA ÓRGÃO SANCIONADOR') || h.includes('ESFERA ORGAO SANCIONADOR')) return 'esferaOrgaoSancionador';
                if (h.includes('FUNDAMENTAÇÃO LEGAL') || h.includes('FUNDAMENTACAO LEGAL')) return 'fundamentacaoLegal';
                if (h.includes('DATA ORIGEM INFORMAÇÃO') || h.includes('DATA ORIGEM INFORMACAO')) return 'dataOrigemInformacao';
                if (h.includes('ORIGEM INFORMAÇÕES') || h.includes('ORIGEM INFORMACOES') || h.includes('ORIGEM INFORMAÇÃO')) return 'origemInformacoes';
                if (h.includes('OBSERVAÇÕES') || h.includes('OBSERVACOES')) return 'observacoes';
                
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
                            
                            if (upperName.includes('CEAF')) {
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

export async function runCeafIngestion() {
    const log = logger.forMethod('runCeafIngestion');
    const moduleName = 'ceaf';

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