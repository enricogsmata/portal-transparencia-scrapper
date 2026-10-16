import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import csvParser from 'csv-parser';
import iconv from 'iconv-lite';
import { sql } from 'drizzle-orm';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import { Ceis } from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY, extractMetadataFromFileName, ReferenceMetadata, normalize } from '../shared.js';

const logger = new_logger(import.meta.url);

const BATCH_SIZE = 2000;

async function persistCeis(data: (typeof Ceis.$inferInsert)[]) {
    const log = logger.forMethod('persistCeis');
    try {
        await bronze_db.insert(Ceis).values(data).onDuplicateKeyUpdate({
            set: {
                cadastro: sql`values(${Ceis.cadastro})`,
                codigoSancao: sql`values(${Ceis.codigoSancao})`,
                tipoPessoa: sql`values(${Ceis.tipoPessoa})`,
                cpfCnpjSancionado: sql`values(${Ceis.cpfCnpjSancionado})`,
                nomeSancionado: sql`values(${Ceis.nomeSancionado})`,
                nomeInformadoOrgaoSancionador: sql`values(${Ceis.nomeInformadoOrgaoSancionador})`,
                razaoSocialCadastroReceita: sql`values(${Ceis.razaoSocialCadastroReceita})`,
                nomeFantasiaCadastroReceita: sql`values(${Ceis.nomeFantasiaCadastroReceita})`,
                numeroProcesso: sql`values(${Ceis.numeroProcesso})`,
                categoriaSancao: sql`values(${Ceis.categoriaSancao})`,
                dataInicioSancao: sql`values(${Ceis.dataInicioSancao})`,
                dataFinalSancao: sql`values(${Ceis.dataFinalSancao})`,
                dataPublicacao: sql`values(${Ceis.dataPublicacao})`,
                publicacao: sql`values(${Ceis.publicacao})`,
                detalhamentoMeioPublicacao: sql`values(${Ceis.detalhamentoMeioPublicacao})`,
                dataTransitoJulgado: sql`values(${Ceis.dataTransitoJulgado})`,
                abrangenciaSancao: sql`values(${Ceis.abrangenciaSancao})`,
                orgaoSancionador: sql`values(${Ceis.orgaoSancionador})`,
                ufOrgaoSancionador: sql`values(${Ceis.ufOrgaoSancionador})`,
                esferaOrgaoSancionador: sql`values(${Ceis.esferaOrgaoSancionador})`,
                fundamentacaoLegal: sql`values(${Ceis.fundamentacaoLegal})`,
                dataOrigemInformacao: sql`values(${Ceis.dataOrigemInformacao})`,
                origemInformacoes: sql`values(${Ceis.origemInformacoes})`,
                observacoes: sql`values(${Ceis.observacoes})`,
                exercicioReferencia: sql`values(${Ceis.exercicioReferencia})`,
                mesReferencia: sql`values(${Ceis.mesReferencia})`,
                diaReferencia: sql`values(${Ceis.diaReferencia})`,
                dadosAdicionais: sql`values(${Ceis.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de CEIS`);
        throw error;
    }
}

interface HeaderRule {
    key: string;
    keywords?: string[];
    matchExact?: string[];
    require?: string[];
}

const CEIS_HEADER_RULES: HeaderRule[] = [
    { key: 'abrangenciaSancao', keywords: ['ABRAGENCIA DA SANCAO'] },
    { key: 'cadastro', matchExact: ['CADASTRO'] },
    { key: 'codigoSancao', keywords: ['SANCAO'], require: ['CODIGO'] },
    { key: 'tipoPessoa', keywords: ['TIPO DE PESSOA', 'TIPO PESSOA'] },
    { key: 'cpfCnpjSancionado', keywords: ['SANCIONADO'], require: ['CPF', 'CNPJ'] },
    { key: 'nomeSancionado', keywords: ['NOME DO SANCIONADO', 'NOME SANCIONADO'] },
    { key: 'nomeInformadoOrgaoSancionador', keywords: ['INFORMADO PELO ORGAO'] },
    { key: 'razaoSocialCadastroReceita', keywords: ['RAZAO SOCIAL'] },
    { key: 'nomeFantasiaCadastroReceita', keywords: ['NOME FANTASIA'] },
    { key: 'numeroProcesso', keywords: ['PROCESSO', 'NUMERO DO PROCESSO'] },
    { key: 'categoriaSancao', keywords: ['CATEGORIA DA SANCAO', 'CATEGORIA SANCAO'] },

    // Datas
    { key: 'dataInicioSancao', keywords: ['INICIO SANCAO'], require: ['DATA', 'INICIO'] },
    { key: 'dataFinalSancao', keywords: ['FINAL SANCAO'], require: ['DATA', 'FINAL'] },
    { key: 'dataPublicacao', keywords: ['PUBLICACAO'], require: ['DATA'] },
    { key: 'publicacao', matchExact: ['PUBLICACAO'] },
    { key: 'detalhamentoMeioPublicacao', keywords: ['DETALHAMENTO'] },
    { key: 'dataTransitoJulgado', keywords: ['TRANSITO EM JULGADO', 'TRANSITO JULGADO'] },

    // Órgão Sancionador
    { key: 'ufOrgaoSancionador', keywords: ['ORGAO SANCIONADOR'], require: ['UF'] },
    { key: 'esferaOrgaoSancionador', keywords: ['ORGAO SANCIONADOR'], require: ['ESFERA'] },
    { key: 'orgaoSancionador', keywords: ['ORGAO SANCIONADOR'] },

    { key: 'fundamentacaoLegal', keywords: ['FUNDAMENTACAO LEGAL'] },
    { key: 'dataOrigemInformacao', keywords: ['ORIGEM INFORMACAO', 'ORIGEM DA INFORMACAO'], require: ['DATA'] },
    { key: 'origemInformacoes', keywords: ['ORIGEM INFORMACAO', 'ORIGEM INFORMACOES'] },
    { key: 'observacoes', keywords: ['OBSERVACOES', 'OBSERVACAO'] },
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

async function processCeisStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processCeisStream');
    let batch: (typeof Ceis.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => resolveHeaderKey(header, CEIS_HEADER_RULES),
            }));

        for await (const data of parser) {
            batch.push({
                ...data,
                ...metadata,
            });
            if (batch.length >= BATCH_SIZE) {
                await persistCeis(batch);
                totalProcessed += batch.length;
                batch = [];
            }
        }
        if (batch.length > 0) {
            await persistCeis(batch);
            totalProcessed += batch.length;
        }

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento do CEIS concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream do CEIS');
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
                            const rowsCount = await processCeisStream(readStream, metadata);
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

export async function runCeisIngestion() {
    const log = logger.forMethod('runCeisIngestion');
    const moduleName = 'ceis';

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