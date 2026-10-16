import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import iconv from 'iconv-lite';
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
import { ZIP_FILES_DIRECTORY, extractMetadataFromFileName, ReferenceMetadata, normalize } from '../shared.js';

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

interface HeaderRule {
    key: string;
    keywords?: string[];
    matchExact?: string[];
    require?: string[];
}

const VIAGENS_HEADER_RULES: HeaderRule[] = [
    { key: 'identificadorProcessoViagem', keywords: ['IDENTIFICADOR DO PROCESSO DE VIAGEM', 'PROCESSO DE VIAGEM'] },

    // Órgãos Pagadores e Solicitantes
    { key: 'codigoOrgaoSuperior', keywords: ['ORGAO SUPERIOR'], require: ['CODIGO'] },
    { key: 'nomeOrgaoSuperior', keywords: ['ORGAO SUPERIOR'], require: ['NOME'] },
    { key: 'codigoOrgaoPagador', keywords: ['ORGAO PAGADOR'], require: ['CODIGO'] },
    { key: 'nomeOrgaoPagador', keywords: ['ORGAO PAGADOR'], require: ['NOME'] },
    { key: 'codigoUgPagadora', keywords: ['UG PAGADORA', 'UNIDADE GESTORA PAGADORA'], require: ['CODIGO'] },
    { key: 'nomeUgPagadora', keywords: ['UG PAGADORA', 'UNIDADE GESTORA PAGADORA'], require: ['NOME'] },
    { key: 'codigoOrgaoSolicitante', keywords: ['SOLICITANTE'], require: ['CODIGO'] },
    { key: 'nomeOrgaoSolicitante', keywords: ['SOLICITANTE'], require: ['NOME'] },

    // Tipos e Valores
    { key: 'tipoPagamento', keywords: ['TIPO DE PAGAMENTO', 'TIPO PAGAMENTO'] },
    { key: 'valorDespesa', matchExact: ['VALOR'], keywords: ['VALOR DA DESPESA', 'VALOR DESPESA'] },
    { key: 'meioTransporte', keywords: ['MEIO DE TRANSPORTE', 'MEIO TRANSPORTE'] },
    { key: 'valorPassagem', keywords: ['VALOR DA PASSAGEM', 'VALOR PASSAGEM'] },
    { key: 'taxaServico', keywords: ['TAXA DE SERVICO', 'TAXA SERVICO'] },
    { key: 'valorDiarias', keywords: ['VALOR DIARIAS', 'VALOR DIARIA'] },
    { key: 'valorPassagens', keywords: ['VALOR PASSAGENS'] },
    { key: 'valorOutrosGastos', keywords: ['VALOR OUTROS GASTOS', 'OUTROS GASTOS'] },

    // Passagens - Ida
    { key: 'paisOrigemIda', keywords: ['ORIGEM IDA'], require: ['PAIS'] },
    { key: 'ufOrigemIda', keywords: ['ORIGEM IDA'], require: ['UF'] },
    { key: 'cidadeOrigemIda', keywords: ['ORIGEM IDA'], require: ['CIDADE'] },
    { key: 'paisDestinoIda', keywords: ['DESTINO IDA'], require: ['PAIS'] },
    { key: 'ufDestinoIda', keywords: ['DESTINO IDA'], require: ['UF'] },
    { key: 'cidadeDestinoIda', keywords: ['DESTINO IDA'], require: ['CIDADE'] },

    // Passagens - Volta
    { key: 'paisOrigemVolta', keywords: ['ORIGEM VOLTA'], require: ['PAIS'] },
    { key: 'ufOrigemVolta', keywords: ['ORIGEM VOLTA'], require: ['UF'] },
    { key: 'cidadeOrigemVolta', keywords: ['ORIGEM VOLTA'], require: ['CIDADE'] },
    { key: 'paisDestinoVolta', keywords: ['DESTINO VOLTA'], require: ['PAIS'] },
    { key: 'ufDestinoVolta', keywords: ['DESTINO VOLTA'], require: ['UF'] },
    { key: 'cidadeDestinoVolta', keywords: ['DESTINO VOLTA'], require: ['CIDADE'] },

    // Trechos
    { key: 'sequenciaTrecho', keywords: ['SEQUENCIA TRECHO', 'TRECHO'], require: ['SEQUENCIA'] },
    { key: 'origemData', keywords: ['ORIGEM'], require: ['DATA'] },
    { key: 'origemPais', keywords: ['ORIGEM'], require: ['PAIS'] },
    { key: 'origemUf', keywords: ['ORIGEM'], require: ['UF'] },
    { key: 'origemCidade', keywords: ['ORIGEM'], require: ['CIDADE'] },
    { key: 'destinoData', keywords: ['DESTINO'], require: ['DATA'] },
    { key: 'destinoPais', keywords: ['DESTINO'], require: ['PAIS'] },
    { key: 'destinoUf', keywords: ['DESTINO'], require: ['UF'] },
    { key: 'destinoCidade', keywords: ['DESTINO'], require: ['CIDADE'] },
    { key: 'numeroDiarias', keywords: ['NUMERO DIARIAS', 'DIARIAS'], require: ['NUMERO'] },
    { key: 'missao', keywords: ['MISSAO'] },

    // Viagem Geral e Viajante
    { key: 'situacao', matchExact: ['SITUACAO'] },
    { key: 'cpfViajante', keywords: ['VIAJANTE'], require: ['CPF'] },
    { key: 'nome', matchExact: ['NOME', 'NOME VIAJANTE'] },
    { key: 'cargo', matchExact: ['CARGO'] },
    { key: 'periodoDataInicio', keywords: ['PERIODO', 'DATA DE INICIO', 'DATA INICIO'], require: ['INICIO'] },
    { key: 'periodoDataFim', keywords: ['PERIODO', 'DATA DE FIM', 'DATA FIM'], require: ['FIM'] },
    { key: 'destinos', matchExact: ['DESTINOS'] },
    { key: 'motivo', matchExact: ['MOTIVO'] },
];

// Helper genérico para resolução de chaves
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

async function processGenericStream(stream: NodeJS.ReadableStream, schema: any, logPrefix: string, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod(`processGenericStream:${logPrefix}`);
    let batch: any[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => resolveHeaderKey(header, VIAGENS_HEADER_RULES),
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