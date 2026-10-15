import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import csvParser from 'csv-parser';
import { sql } from 'drizzle-orm';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import {
    ViagensPagamento,
    ViagensPassagem,
    ViagensTrecho,
    ViagensViagem
} from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY, extractMetadataFromFileName, ReferenceMetadata } from '../shared.js';

const logger = new_logger(import.meta.url);
const BATCH_SIZE = 2000;

async function persistData(schema: any, data: any[], logPrefix: string) {
    const log = logger.forMethod(`persistData:${logPrefix}`);
    try {
        const columns = Object.keys(data[0] || {});
        const updateSet: Record<string, any> = {};
        for (const col of columns) {
            updateSet[col] = sql`values(${schema[col]})`;
        }

        await bronze_db.insert(schema).values(data).onDuplicateKeyUpdate({
            set: updateSet,
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de ${logPrefix}`);
        throw error;
    }
}

function mapHeaders(header: string): string {
    const h = header.toUpperCase().trim();

    if (h.includes('IDENTIFICADOR DO PROCESSO DE VIAGEM')) return 'identificadorProcessoViagem';

    // Pagamento
    if (h.includes('CÓDIGO DO ÓRGÃO SUPERIOR') || h.includes('CÓDIGO ÓRGÃO SUPERIOR') || h.includes('CODIGO ORGAO SUPERIOR')) return 'codigoOrgaoSuperior';
    if (h.includes('NOME ÓRGÃO SUPERIOR') || h.includes('NOME ORGAO SUPERIOR') || h.includes('NOME DO ÓRGÃO SUPERIOR')) return 'nomeOrgaoSuperior';
    if (h.includes('CÓDIGO ÓRGÃO PAGADOR') || h.includes('CODIGO ORGAO PAGADOR')) return 'codigoOrgaoPagador';
    if (h.includes('NOME ÓRGÃO PAGADOR') || h.includes('NOME ORGAO PAGADOR')) return 'nomeOrgaoPagador';
    if (h.includes('CÓDIGO UG PAGADORA') || h.includes('CODIGO UG PAGADORA')) return 'codigoUgPagadora';
    if (h.includes('NOME UG PAGADORA')) return 'nomeUgPagadora';
    if (h.includes('TIPO DE PAGAMENTO') || h.includes('TIPO PAGAMENTO')) return 'tipoPagamento';
    if (h.includes('VALOR DA DESPESA') || h.includes('VALOR DESPESA')) return 'valorDespesa';

    // Passagem
    if (h === 'MEIO DE TRANSPORTE') return 'meioTransporte';
    if (h.includes('PAÍS - ORIGEM IDA') || h.includes('PAÍS – ORIGEM IDA') || h.includes('PAIS - ORIGEM IDA')) return 'paisOrigemIda';
    if (h.includes('UF - ORIGEM IDA') || h.includes('UF – ORIGEM IDA')) return 'ufOrigemIda';
    if (h.includes('CIDADE - ORIGEM IDA') || h.includes('CIDADE – ORIGEM IDA')) return 'cidadeOrigemIda';
    if (h.includes('PAÍS - DESTINO IDA') || h.includes('PAÍS – DESTINO IDA') || h.includes('PAIS - DESTINO IDA')) return 'paisDestinoIda';
    if (h.includes('UF - DESTINO IDA') || h.includes('UF – DESTINO IDA')) return 'ufDestinoIda';
    if (h.includes('CIDADE - DESTINO IDA') || h.includes('CIDADE – DESTINO IDA')) return 'cidadeDestinoIda';
    if (h.includes('PAÍS - ORIGEM VOLTA') || h.includes('PAÍS – ORIGEM VOLTA') || h.includes('PAIS - ORIGEM VOLTA')) return 'paisOrigemVolta';
    if (h.includes('UF - ORIGEM VOLTA') || h.includes('UF – ORIGEM VOLTA')) return 'ufOrigemVolta';
    if (h.includes('CIDADE - ORIGEM VOLTA') || h.includes('CIDADE – ORIGEM VOLTA')) return 'cidadeOrigemVolta';
    if (h.includes('PAÍS - DESTINO VOLTA') || h.includes('PAÍS – DESTINO VOLTA') || h.includes('PAIS - DESTINO VOLTA')) return 'paisDestinoVolta';
    if (h.includes('UF - DESTINO VOLTA') || h.includes('UF – DESTINO VOLTA') || h.includes('UF - DESTINO VOLTA')) return 'ufDestinoVolta';
    if (h.includes('CIDADE - DESTINO VOLTA') || h.includes('CIDADE – DESTINO VOLTA')) return 'cidadeDestinoVolta';
    if (h.includes('VALOR DA PASSAGEM') || h.includes('VALOR PASSAGEM')) return 'valorPassagem';
    if (h.includes('TAXA DE SERVIÇO') || h.includes('TAXA SERVIÇO') || h.includes('TAXA SERVICO')) return 'taxaServico';

    // Trecho
    if (h.includes('SEQUÊNCIA TRECHO') || h.includes('SEQUENCIA TRECHO')) return 'sequenciaTrecho';
    if (h.includes('ORIGEM - DATA') || h.includes('ORIGEM – DATA')) return 'origemData';
    if (h.includes('ORIGEM - PAÍS') || h.includes('ORIGEM – PAÍS') || h.includes('ORIGEM - PAIS')) return 'origemPais';
    if (h.includes('ORIGEM - UF') || h.includes('ORIGEM – UF')) return 'origemUf';
    if (h.includes('ORIGEM - CIDADE') || h.includes('ORIGEM – CIDADE')) return 'origemCidade';
    if (h.includes('DESTINO - DATA') || h.includes('DESTINO – DATA')) return 'destinoData';
    if (h.includes('DESTINO - PAÍS') || h.includes('DESTINO – PAÍS') || h.includes('DESTINO - PAIS')) return 'destinoPais';
    if (h.includes('DESTINO - UF') || h.includes('DESTINO – UF')) return 'destinoUf';
    if (h.includes('DESTINO - CIDADE') || h.includes('DESTINO – CIDADE')) return 'destinoCidade';
    if (h.includes('NÚMERO DIÁRIAS') || h.includes('NUMERO DIARIAS')) return 'numeroDiarias';
    if (h.includes('MISSÃO?') || h.includes('MISSAO?')) return 'missao';

    // Viagem
    if (h === 'SITUAÇÃO' || h === 'SITUACAO') return 'situacao';
    if (h.includes('CÓDIGO SOLICITANTE') || h.includes('CODIGO SOLICITANTE')) return 'codigoOrgaoSolicitante';
    if (h.includes('NOME ÓRGÃO SOLICITANTE') || h.includes('NOME ORGAO SOLICITANTE')) return 'nomeOrgaoSolicitante';
    if (h === 'CPF VIAJANTE') return 'cpfViajante';
    if (h === 'NOME') return 'nome';
    if (h === 'CARGO') return 'cargo';
    if (h.includes('PERÍODO - DATA DE INÍCIO') || h.includes('PERIODO - DATA DE INICIO')) return 'periodoDataInicio';
    if (h.includes('PERÍODO - DATA DE FIM') || h.includes('PERIODO - DATA DE FIM') || h.includes('PERÍODO - DATA DE FIM')) return 'periodoDataFim';
    if (h === 'DESTINOS') return 'destinos';
    if (h === 'MOTIVO') return 'motivo';
    if (h.includes('VALOR DIÁRIAS') || h.includes('VALOR DIARIAS')) return 'valorDiarias';
    if (h.includes('VALOR PASSAGENS')) return 'valorPassagens';
    if (h.includes('VALOR OUTROS GASTOS')) return 'valorOutrosGastos';

    return header.trim();
}

async function processGenericStream(stream: NodeJS.ReadableStream, schema: any, logPrefix: string, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod(`processGenericStream:${logPrefix}`);
    let batch: any[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => mapHeaders(header),
        }));

        for await (const data of parser) {
            batch.push({
                ...data,
                ...metadata,
            });
            if (batch.length >= BATCH_SIZE) {
                await persistData(schema, batch, logPrefix);
                totalProcessed += batch.length;
                batch = [];
            }
        }
        if (batch.length > 0) {
            await persistData(schema, batch, logPrefix);
            totalProcessed += batch.length;
        }

        log.info({ data: { rowsProcessed: totalProcessed } }, `Processamento de ${logPrefix} concluído`);
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, `Erro ao processar stream de ${logPrefix}`);
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
                            const upperFileName = entry.fileName.toUpperCase();
                            let rowsCount = 0;

                            if (upperFileName.includes('PAGAMENTO')) {
                                rowsCount = await processGenericStream(readStream, ViagensPagamento, 'Pagamento', metadata);
                            } else if (upperFileName.includes('PASSAGEM')) {
                                rowsCount = await processGenericStream(readStream, ViagensPassagem, 'Passagem', metadata);
                            } else if (upperFileName.includes('TRECHO')) {
                                rowsCount = await processGenericStream(readStream, ViagensTrecho, 'Trecho', metadata);
                            } else if (upperFileName.includes('VIAGEM')) {
                                rowsCount = await processGenericStream(readStream, ViagensViagem, 'Viagem', metadata);
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

export async function runViagensIngestion() {
    const log = logger.forMethod('runViagensIngestion');
    const moduleName = 'viagens';

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