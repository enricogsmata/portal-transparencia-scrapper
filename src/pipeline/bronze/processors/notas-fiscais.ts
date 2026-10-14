import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import csvParser from 'csv-parser';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import { 
    NotaFiscal, 
    NotaFiscalEvento, 
    NotaFiscalItem 
} from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY } from '../shared.js';

const logger = new_logger(import.meta.url);

const BATCH_SIZE = 2000;

async function persistNotaFiscal(data: (typeof NotaFiscal.$inferInsert)[]) {
    const log = logger.forMethod('persistNotaFiscal');
    try {
        await bronze_db.insert(NotaFiscal).values(data);
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Nota Fiscal`);
        throw error;
    }
}

async function persistNotaFiscalEvento(data: (typeof NotaFiscalEvento.$inferInsert)[]) {
    const log = logger.forMethod('persistNotaFiscalEvento');
    try {
        await bronze_db.insert(NotaFiscalEvento).values(data);
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Evento de Nota Fiscal`);
        throw error;
    }
}

async function persistNotaFiscalItem(data: (typeof NotaFiscalItem.$inferInsert)[]) {
    const log = logger.forMethod('persistNotaFiscalItem');
    try {
        await bronze_db.insert(NotaFiscalItem).values(data);
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Item de Nota Fiscal`);
        throw error;
    }
}

function mapCommonHeaders(h: string): string | null {
    if (h.includes('CHAVE DE ACESSO')) return 'chaveAcesso';
    if (h === 'MODELO') return 'modelo';
    if (h === 'SÉRIE' || h === 'SERIE') return 'serie';
    if (h === 'NÚMERO' || h === 'NUMERO') return 'numero';
    if (h.includes('NATUREZA DA OPERAÇÃO') || h.includes('NATUREZA DA OPERACAO')) return 'naturezaOperacao';
    if (h.includes('DATA EMISSÃO') || h.includes('DATA EMISSAO')) return 'dataEmissao';
    
    if (h.includes('CPF/CNPJ EMITENTE')) return 'cpfCnpjEmitente';
    if (h.includes('RAZÃO SOCIAL EMITENTE') || h.includes('RAZAO SOCIAL EMITENTE')) return 'razaoSocialEmitente';
    if (h.includes('INSCRIÇÃO ESTADUAL EMITENTE') || h.includes('INSCRICAO ESTADUAL EMITENTE')) return 'inscricaoEstadualEmitente';
    if (h.includes('UF EMITENTE')) return 'ufEmitente';
    if (h.includes('MUNICÍPIO EMITENTE') || h.includes('MUNICIPIO EMITENTE')) return 'municipioEmitente';
    
    if (h.includes('CNPJ DESTINATÁRIO') || h.includes('CNPJ DESTINATARIO')) return 'cnpjDestinatario';
    if (h.includes('NOME DESTINATÁRIO') || h.includes('NOME DESTINATARIO')) return 'nomeDestinatario';
    if (h.includes('UF DESTINATÁRIO') || h.includes('UF DESTINATARIO')) return 'ufDestinatario';
    if (h.includes('INDICADOR IE DESTINATÁRIO') || h.includes('INDICADOR IE DESTINATARIO')) return 'indicadorIeDestinatario';
    
    if (h.includes('DESTINO DA OPERAÇÃO') || h.includes('DESTINO DA OPERACAO')) return 'destinoOperacao';
    if (h === 'CONSUMIDOR FINAL') return 'consumidorFinal';
    if (h.includes('PRESENÇA DO COMPRADOR') || h.includes('PRESENCA DO COMPRADOR')) return 'presencaComprador';
    
    return null;
}

async function processNotaFiscalStream(stream: NodeJS.ReadableStream): Promise<number> {
    const log = logger.forMethod('processNotaFiscalStream');
    let batch: (typeof NotaFiscal.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase().trim();
                const common = mapCommonHeaders(h);
                if (common) return common;

                if (h.includes('DATA/HORA EVENTO MAIS RECENTE')) return 'dataHoraEventoMaisRecente';
                if (h.includes('EVENTO MAIS RECENTE')) return 'eventoMaisRecente';
                if (h.includes('VALOR NOTA FISCAL')) return 'valorNotaFiscal';
                
                return h;
            },
        }));

        for await (const data of parser) {
            batch.push(data);
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

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream');
        throw error;
    }
}

async function processNotaFiscalEventoStream(stream: NodeJS.ReadableStream): Promise<number> {
    const log = logger.forMethod('processNotaFiscalEventoStream');
    let batch: (typeof NotaFiscalEvento.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase().trim();
                const common = mapCommonHeaders(h);
                if (common) return common;

                if (h === 'EVENTO') return 'evento';
                if (h.includes('DATA/HORA EVENTO')) return 'dataHoraEvento';
                if (h.includes('DESCRIÇÃO EVENTO') || h.includes('DESCRICAO EVENTO')) return 'descricaoEvento';
                if (h.includes('MOTIVO EVENTO')) return 'motivoEvento';
                
                return h;
            },
        }));

        for await (const data of parser) {
            batch.push(data);
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

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream');
        throw error;
    }
}

async function processNotaFiscalItemStream(stream: NodeJS.ReadableStream): Promise<number> {
    const log = logger.forMethod('processNotaFiscalItemStream');
    let batch: (typeof NotaFiscalItem.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase().trim();
                const common = mapCommonHeaders(h);
                if (common) return common;

                if (h.includes('NÚMERO PRODUTO') || h.includes('NUMERO PRODUTO')) return 'numeroProduto';
                if (h.includes('DESCRIÇÃO DO PRODUTO/SERVIÇO') || h.includes('DESCRICAO DO PRODUTO/SERVICO') || h.includes('DESCRIÇÃO DO PRODUTO')) return 'descricaoProdutoServico';
                if (h.includes('CÓDIGO NCM/SH') || h.includes('CODIGO NCM/SH')) return 'codigoNcmSh';
                if (h.includes('NCM/SH (TIPO DE PRODUTO)') || h === 'NCM/SH') return 'ncmShTipoProduto';
                if (h === 'CFOP') return 'cfop';
                if (h === 'QUANTIDADE') return 'quantidade';
                if (h === 'UNIDADE') return 'unidade';
                if (h.includes('VALOR UNITÁRIO') || h.includes('VALOR UNITARIO')) return 'valorUnitario';
                if (h.includes('VALOR TOTAL')) return 'valorTotal';

                return h;
            },
        }));

        for await (const data of parser) {
            batch.push(data);
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

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream');
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
                            
                            if (upperName.includes('EVENTO')) {
                                rowsCount = await processNotaFiscalEventoStream(readStream);
                            } else if (upperName.includes('ITEM')) {
                                rowsCount = await processNotaFiscalItemStream(readStream);
                            } else if (upperName.includes('NOTAFISCAL') || upperName.includes('NOTA_FISCAL') || upperName.includes('NOTAS_FISCAIS')) {
                                rowsCount = await processNotaFiscalStream(readStream);
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