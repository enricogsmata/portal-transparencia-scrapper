import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import csvParser from 'csv-parser';
import iconv from 'iconv-lite';
import { sql } from 'drizzle-orm';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import {
    NotaFiscal,
    NotaFiscalEvento,
    NotaFiscalItem
} from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY, extractMetadataFromFileName, ReferenceMetadata, normalize } from '../shared.js';

const logger = new_logger(import.meta.url);

const BATCH_SIZE = 2000;

async function persistNotaFiscal(data: (typeof NotaFiscal.$inferInsert)[]) {
    const log = logger.forMethod('persistNotaFiscal');
    try {
        await bronze_db.insert(NotaFiscal).values(data).onDuplicateKeyUpdate({
            set: {
                chaveAcesso: sql`values(${NotaFiscal.chaveAcesso})`,
                modelo: sql`values(${NotaFiscal.modelo})`,
                serie: sql`values(${NotaFiscal.serie})`,
                numero: sql`values(${NotaFiscal.numero})`,
                naturezaOperacao: sql`values(${NotaFiscal.naturezaOperacao})`,
                dataEmissao: sql`values(${NotaFiscal.dataEmissao})`,
                eventoMaisRecente: sql`values(${NotaFiscal.eventoMaisRecente})`,
                dataHoraEventoMaisRecente: sql`values(${NotaFiscal.dataHoraEventoMaisRecente})`,
                cpfCnpjEmitente: sql`values(${NotaFiscal.cpfCnpjEmitente})`,
                razaoSocialEmitente: sql`values(${NotaFiscal.razaoSocialEmitente})`,
                inscricaoEstadualEmitente: sql`values(${NotaFiscal.inscricaoEstadualEmitente})`,
                ufEmitente: sql`values(${NotaFiscal.ufEmitente})`,
                municipioEmitente: sql`values(${NotaFiscal.municipioEmitente})`,
                cnpjDestinatario: sql`values(${NotaFiscal.cnpjDestinatario})`,
                nomeDestinatario: sql`values(${NotaFiscal.nomeDestinatario})`,
                ufDestinatario: sql`values(${NotaFiscal.ufDestinatario})`,
                indicadorIeDestinatario: sql`values(${NotaFiscal.indicadorIeDestinatario})`,
                destinoOperacao: sql`values(${NotaFiscal.destinoOperacao})`,
                consumidorFinal: sql`values(${NotaFiscal.consumidorFinal})`,
                presencaComprador: sql`values(${NotaFiscal.presencaComprador})`,
                valorNotaFiscal: sql`values(${NotaFiscal.valorNotaFiscal})`,
                exercicioReferencia: sql`values(${NotaFiscal.exercicioReferencia})`,
                mesReferencia: sql`values(${NotaFiscal.mesReferencia})`,
                diaReferencia: sql`values(${NotaFiscal.diaReferencia})`,
                dadosAdicionais: sql`values(${NotaFiscal.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Nota Fiscal`);
        throw error;
    }
}

async function persistNotaFiscalEvento(data: (typeof NotaFiscalEvento.$inferInsert)[]) {
    const log = logger.forMethod('persistNotaFiscalEvento');
    try {
        await bronze_db.insert(NotaFiscalEvento).values(data).onDuplicateKeyUpdate({
            set: {
                chaveAcesso: sql`values(${NotaFiscalEvento.chaveAcesso})`,
                modelo: sql`values(${NotaFiscalEvento.modelo})`,
                serie: sql`values(${NotaFiscalEvento.serie})`,
                numero: sql`values(${NotaFiscalEvento.numero})`,
                naturezaOperacao: sql`values(${NotaFiscalEvento.naturezaOperacao})`,
                dataEmissao: sql`values(${NotaFiscalEvento.dataEmissao})`,
                evento: sql`values(${NotaFiscalEvento.evento})`,
                dataHoraEvento: sql`values(${NotaFiscalEvento.dataHoraEvento})`,
                descricaoEvento: sql`values(${NotaFiscalEvento.descricaoEvento})`,
                motivoEvento: sql`values(${NotaFiscalEvento.motivoEvento})`,
                exercicioReferencia: sql`values(${NotaFiscalEvento.exercicioReferencia})`,
                mesReferencia: sql`values(${NotaFiscalEvento.mesReferencia})`,
                diaReferencia: sql`values(${NotaFiscalEvento.diaReferencia})`,
                dadosAdicionais: sql`values(${NotaFiscalEvento.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Evento de Nota Fiscal`);
        throw error;
    }
}

async function persistNotaFiscalItem(data: (typeof NotaFiscalItem.$inferInsert)[]) {
    const log = logger.forMethod('persistNotaFiscalItem');
    try {
        await bronze_db.insert(NotaFiscalItem).values(data).onDuplicateKeyUpdate({
            set: {
                chaveAcesso: sql`values(${NotaFiscalItem.chaveAcesso})`,
                modelo: sql`values(${NotaFiscalItem.modelo})`,
                serie: sql`values(${NotaFiscalItem.serie})`,
                numero: sql`values(${NotaFiscalItem.numero})`,
                naturezaOperacao: sql`values(${NotaFiscalItem.naturezaOperacao})`,
                dataEmissao: sql`values(${NotaFiscalItem.dataEmissao})`,
                cpfCnpjEmitente: sql`values(${NotaFiscalItem.cpfCnpjEmitente})`,
                razaoSocialEmitente: sql`values(${NotaFiscalItem.razaoSocialEmitente})`,
                inscricaoEstadualEmitente: sql`values(${NotaFiscalItem.inscricaoEstadualEmitente})`,
                ufEmitente: sql`values(${NotaFiscalItem.ufEmitente})`,
                municipioEmitente: sql`values(${NotaFiscalItem.municipioEmitente})`,
                cnpjDestinatario: sql`values(${NotaFiscalItem.cnpjDestinatario})`,
                nomeDestinatario: sql`values(${NotaFiscalItem.nomeDestinatario})`,
                ufDestinatario: sql`values(${NotaFiscalItem.ufDestinatario})`,
                indicadorIeDestinatario: sql`values(${NotaFiscalItem.indicadorIeDestinatario})`,
                destinoOperacao: sql`values(${NotaFiscalItem.destinoOperacao})`,
                consumidorFinal: sql`values(${NotaFiscalItem.consumidorFinal})`,
                presencaComprador: sql`values(${NotaFiscalItem.presencaComprador})`,
                numeroProduto: sql`values(${NotaFiscalItem.numeroProduto})`,
                descricaoProdutoServico: sql`values(${NotaFiscalItem.descricaoProdutoServico})`,
                codigoNcmSh: sql`values(${NotaFiscalItem.codigoNcmSh})`,
                ncmShTipoProduto: sql`values(${NotaFiscalItem.ncmShTipoProduto})`,
                cfop: sql`values(${NotaFiscalItem.cfop})`,
                quantidade: sql`values(${NotaFiscalItem.quantidade})`,
                unidade: sql`values(${NotaFiscalItem.unidade})`,
                valorUnitario: sql`values(${NotaFiscalItem.valorUnitario})`,
                valorTotal: sql`values(${NotaFiscalItem.valorTotal})`,
                exercicioReferencia: sql`values(${NotaFiscalItem.exercicioReferencia})`,
                mesReferencia: sql`values(${NotaFiscalItem.mesReferencia})`,
                diaReferencia: sql`values(${NotaFiscalItem.diaReferencia})`,
                dadosAdicionais: sql`values(${NotaFiscalItem.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Item de Nota Fiscal`);
        throw error;
    }
}

interface HeaderRule {
    key: string;
    keywords?: string[];
    matchExact?: string[];
    require?: string[];
}

// Regras comuns a todos os arquivos de Nota Fiscal
const COMMON_NOTA_FISCAL_RULES: HeaderRule[] = [
    { key: 'chaveAcesso', keywords: ['CHAVE DE ACESSO', 'CHAVE ACESSO'] },
    { key: 'modelo', matchExact: ['MODELO'] },
    { key: 'serie', matchExact: ['SERIE'] },
    { key: 'numero', matchExact: ['NUMERO', 'NUMERO NOTA FISCAL'] },
    { key: 'naturezaOperacao', keywords: ['NATUREZA DA OPERACAO', 'NATUREZA OPERACAO'] },
    { key: 'dataEmissao', keywords: ['DATA EMISSAO', 'DATA DE EMISSAO'] },

    // Emitente
    { key: 'cpfCnpjEmitente', keywords: ['EMITENTE'], require: ['CPF', 'CNPJ'] },
    { key: 'razaoSocialEmitente', keywords: ['EMITENTE'], require: ['RAZAO SOCIAL', 'NOME'] },
    { key: 'inscricaoEstadualEmitente', keywords: ['EMITENTE'], require: ['INSCRICAO ESTADUAL', 'IE'] },
    { key: 'ufEmitente', keywords: ['EMITENTE'], require: ['UF'] },
    { key: 'municipioEmitente', keywords: ['EMITENTE'], require: ['MUNICIPIO'] },

    // Destinatário
    { key: 'cnpjDestinatario', keywords: ['DESTINATARIO'], require: ['CNPJ', 'CPF'] },
    { key: 'nomeDestinatario', keywords: ['DESTINATARIO'], require: ['NOME', 'RAZAO SOCIAL'] },
    { key: 'ufDestinatario', keywords: ['DESTINATARIO'], require: ['UF'] },
    { key: 'indicadorIeDestinatario', keywords: ['DESTINATARIO'], require: ['INDICADOR', 'IE'] },

    // Operação e Consumidor
    { key: 'destinoOperacao', keywords: ['DESTINO DA OPERACAO', 'DESTINO OPERACAO'] },
    { key: 'consumidorFinal', matchExact: ['CONSUMIDOR FINAL'] },
    { key: 'presencaComprador', keywords: ['PRESENCA DO COMPRADOR', 'PRESENCA COMPRADOR'] },
];

const NOTA_FISCAL_MAIN_RULES: HeaderRule[] = [
    { key: 'dataHoraEventoMaisRecente', keywords: ['EVENTO MAIS RECENTE'], require: ['DATA', 'HORA'] },
    { key: 'eventoMaisRecente', keywords: ['EVENTO MAIS RECENTE'] },
    { key: 'valorNotaFiscal', keywords: ['VALOR NOTA FISCAL', 'VALOR TOTAL NOTA'] },
];

const NOTA_FISCAL_EVENTO_RULES: HeaderRule[] = [
    { key: 'dataHoraEvento', keywords: ['EVENTO'], require: ['DATA'] },
    { key: 'descricaoEvento', keywords: ['EVENTO'], require: ['DESCRICAO'] },
    { key: 'motivoEvento', keywords: ['EVENTO'], require: ['MOTIVO'] },
    { key: 'evento', matchExact: ['EVENTO'] },
];

const NOTA_FISCAL_ITEM_RULES: HeaderRule[] = [
    { key: 'numeroProduto', keywords: ['PRODUTO'], require: ['NUMERO'] },
    { key: 'descricaoProdutoServico', keywords: ['PRODUTO', 'SERVICO'], require: ['DESCRICAO'] },
    { key: 'codigoNcmSh', keywords: ['NCM', 'SH'], require: ['CODIGO'] },
    { key: 'ncmShTipoProduto', keywords: ['NCM', 'SH', 'TIPO DE PRODUTO'] },
    { key: 'cfop', matchExact: ['CFOP'] },
    { key: 'quantidade', matchExact: ['QUANTIDADE'] },
    { key: 'unidade', matchExact: ['UNIDADE'] },
    { key: 'valorUnitario', keywords: ['VALOR UNITARIO'] },
    { key: 'valorTotal', keywords: ['VALOR TOTAL'] },
];

// Helper genérico para combinar regras específicas + comuns de Notas Fiscais
function resolveNotaFiscalHeaderKey(header: string, specificRules: HeaderRule[]): string {
    if (!header) return '';
    const cleanHeader = normalize(header);

    // 1. Tenta casar nas regras específicas da tabela atual
    for (const rule of specificRules) {
        if (rule.matchExact && rule.matchExact.includes(cleanHeader)) return rule.key;
        const hasKeyword = rule.keywords && rule.keywords.some(kw => cleanHeader.includes(kw));
        if (hasKeyword) {
            if (rule.require) {
                if (rule.require.some(req => cleanHeader.includes(req))) return rule.key;
            } else {
                return rule.key;
            }
        }
    }

    // 2. Se não casou, tenta as regras comuns de Nota Fiscal
    for (const rule of COMMON_NOTA_FISCAL_RULES) {
        if (rule.matchExact && rule.matchExact.includes(cleanHeader)) return rule.key;
        const hasKeyword = rule.keywords && rule.keywords.some(kw => cleanHeader.includes(kw));
        if (hasKeyword) {
            if (rule.require) {
                if (rule.require.some(req => cleanHeader.includes(req))) return rule.key;
            } else {
                return rule.key;
            }
        }
    }

    return cleanHeader;
}

async function processNotaFiscalStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processNotaFiscalStream');
    let batch: (typeof NotaFiscal.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => resolveNotaFiscalHeaderKey(header, NOTA_FISCAL_MAIN_RULES),
            }));

        for await (const data of parser) {
            batch.push({
                ...data,
                ...metadata,
            });
            if (batch.length >= BATCH_SIZE) {
                await persistNotaFiscal(batch);
                totalProcessed += batch.length;
                batch = [];
            }
        }
        if (batch.length > 0) {
            await persistNotaFiscal(batch);
            totalProcessed += batch.length;
        }

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento de Nota Fiscal concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream de Nota Fiscal');
        throw error;
    }
}

async function processNotaFiscalEventoStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processNotaFiscalEventoStream');
    let batch: (typeof NotaFiscalEvento.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => resolveNotaFiscalHeaderKey(header, NOTA_FISCAL_EVENTO_RULES),
            }));

        for await (const data of parser) {
            batch.push({
                ...data,
                ...metadata,
            });
            if (batch.length >= BATCH_SIZE) {
                await persistNotaFiscalEvento(batch);
                totalProcessed += batch.length;
                batch = [];
            }
        }
        if (batch.length > 0) {
            await persistNotaFiscalEvento(batch);
            totalProcessed += batch.length;
        }

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento de Eventos da Nota Fiscal concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream de Eventos da Nota Fiscal');
        throw error;
    }
}

async function processNotaFiscalItemStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processNotaFiscalItemStream');
    let batch: (typeof NotaFiscalItem.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => resolveNotaFiscalHeaderKey(header, NOTA_FISCAL_ITEM_RULES),
            }));

        for await (const data of parser) {
            batch.push({
                ...data,
                ...metadata,
            });
            if (batch.length >= BATCH_SIZE) {
                await persistNotaFiscalItem(batch);
                totalProcessed += batch.length;
                batch = [];
            }
        }
        if (batch.length > 0) {
            await persistNotaFiscalItem(batch);
            totalProcessed += batch.length;
        }

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento de Itens da Nota Fiscal concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream de Itens da Nota Fiscal');
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

                            if (upperName.includes('EVENTO')) {
                                rowsCount = await processNotaFiscalEventoStream(readStream, metadata);
                            } else if (upperName.includes('ITEM')) {
                                rowsCount = await processNotaFiscalItemStream(readStream, metadata);
                            } else if (upperName.includes('NOTAFISCAL') || upperName.includes('NOTA_FISCAL') || upperName.includes('NOTAS_FISCAIS')) {
                                rowsCount = await processNotaFiscalStream(readStream, metadata);
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

export async function runNotasFiscaisIngestion() {
    const log = logger.forMethod('runNotasFiscaisIngestion');
    const moduleName = 'notas-fiscais';

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