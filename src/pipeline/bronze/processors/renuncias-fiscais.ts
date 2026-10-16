import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import csvParser from 'csv-parser';

import iconv from 'iconv-lite';
import { sql } from 'drizzle-orm';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import {
    RenunciasFiscais,
    RenunciasFiscaisPorBeneficiario,
    EmpresasImunesOuIsentas,
    EmpresasHabilitadas
} from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY, extractMetadataFromFileName, ReferenceMetadata, normalize } from '../shared.js';

const logger = new_logger(import.meta.url);
const BATCH_SIZE = 2000;

async function persistRenunciasFiscais(data: (typeof RenunciasFiscais.$inferInsert)[]) {
    const log = logger.forMethod('persistRenunciasFiscais');
    try {
        await bronze_db.insert(RenunciasFiscais).values(data).onDuplicateKeyUpdate({
            set: {
                anoCalendario: sql`values(${RenunciasFiscais.anoCalendario})`,
                cnpj: sql`values(${RenunciasFiscais.cnpj})`,
                razaoSocial: sql`values(${RenunciasFiscais.razaoSocial})`,
                nomeFantasia: sql`values(${RenunciasFiscais.nomeFantasia})`,
                codigoCnae: sql`values(${RenunciasFiscais.codigoCnae})`,
                cnae: sql`values(${RenunciasFiscais.cnae})`,
                municipio: sql`values(${RenunciasFiscais.municipio})`,
                uf: sql`values(${RenunciasFiscais.uf})`,
                tipoRenuncia: sql`values(${RenunciasFiscais.tipoRenuncia})`,
                beneficioFiscal: sql`values(${RenunciasFiscais.beneficioFiscal})`,
                fundamentoLegal: sql`values(${RenunciasFiscais.fundamentoLegal})`,
                descricao: sql`values(${RenunciasFiscais.descricao})`,
                tributo: sql`values(${RenunciasFiscais.tributo})`,
                formaTributacao: sql`values(${RenunciasFiscais.formaTributacao})`,
                valorRenunciaFiscal: sql`values(${RenunciasFiscais.valorRenunciaFiscal})`,
                exercicioReferencia: sql`values(${RenunciasFiscais.exercicioReferencia})`,
                mesReferencia: sql`values(${RenunciasFiscais.mesReferencia})`,
                diaReferencia: sql`values(${RenunciasFiscais.diaReferencia})`,
                dadosAdicionais: sql`values(${RenunciasFiscais.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Renúncias Fiscais`);
        throw error;
    }
}

async function persistRenunciasFiscaisPorBeneficiario(data: (typeof RenunciasFiscaisPorBeneficiario.$inferInsert)[]) {
    const log = logger.forMethod('persistRenunciasFiscaisPorBeneficiario');
    try {
        await bronze_db.insert(RenunciasFiscaisPorBeneficiario).values(data).onDuplicateKeyUpdate({
            set: {
                anoCalendario: sql`values(${RenunciasFiscaisPorBeneficiario.anoCalendario})`,
                cnpj: sql`values(${RenunciasFiscaisPorBeneficiario.cnpj})`,
                razaoSocial: sql`values(${RenunciasFiscaisPorBeneficiario.razaoSocial})`,
                nomeFantasia: sql`values(${RenunciasFiscaisPorBeneficiario.nomeFantasia})`,
                codigoCnae: sql`values(${RenunciasFiscaisPorBeneficiario.codigoCnae})`,
                cnae: sql`values(${RenunciasFiscaisPorBeneficiario.cnae})`,
                municipio: sql`values(${RenunciasFiscaisPorBeneficiario.municipio})`,
                uf: sql`values(${RenunciasFiscaisPorBeneficiario.uf})`,
                valorRenunciaFiscal: sql`values(${RenunciasFiscaisPorBeneficiario.valorRenunciaFiscal})`,
                exercicioReferencia: sql`values(${RenunciasFiscaisPorBeneficiario.exercicioReferencia})`,
                mesReferencia: sql`values(${RenunciasFiscaisPorBeneficiario.mesReferencia})`,
                diaReferencia: sql`values(${RenunciasFiscaisPorBeneficiario.diaReferencia})`,
                dadosAdicionais: sql`values(${RenunciasFiscaisPorBeneficiario.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Renúncias Fiscais Por Beneficiário`);
        throw error;
    }
}

async function persistEmpresasImunesOuIsentas(data: (typeof EmpresasImunesOuIsentas.$inferInsert)[]) {
    const log = logger.forMethod('persistEmpresasImunesOuIsentas');
    try {
        await bronze_db.insert(EmpresasImunesOuIsentas).values(data).onDuplicateKeyUpdate({
            set: {
                anoCalendario: sql`values(${EmpresasImunesOuIsentas.anoCalendario})`,
                cnpj: sql`values(${EmpresasImunesOuIsentas.cnpj})`,
                razaoSocial: sql`values(${EmpresasImunesOuIsentas.razaoSocial})`,
                nomeFantasia: sql`values(${EmpresasImunesOuIsentas.nomeFantasia})`,
                codigoCnae: sql`values(${EmpresasImunesOuIsentas.codigoCnae})`,
                cnae: sql`values(${EmpresasImunesOuIsentas.cnae})`,
                municipio: sql`values(${EmpresasImunesOuIsentas.municipio})`,
                uf: sql`values(${EmpresasImunesOuIsentas.uf})`,
                tipoEntidade: sql`values(${EmpresasImunesOuIsentas.tipoEntidade})`,
                beneficioFiscal: sql`values(${EmpresasImunesOuIsentas.beneficioFiscal})`,
                exercicioReferencia: sql`values(${EmpresasImunesOuIsentas.exercicioReferencia})`,
                mesReferencia: sql`values(${EmpresasImunesOuIsentas.mesReferencia})`,
                diaReferencia: sql`values(${EmpresasImunesOuIsentas.diaReferencia})`,
                dadosAdicionais: sql`values(${EmpresasImunesOuIsentas.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Empresas Imunes Ou Isentas`);
        throw error;
    }
}

async function persistEmpresasHabilitadas(data: (typeof EmpresasHabilitadas.$inferInsert)[]) {
    const log = logger.forMethod('persistEmpresasHabilitadas');
    try {
        await bronze_db.insert(EmpresasHabilitadas).values(data).onDuplicateKeyUpdate({
            set: {
                cnpj: sql`values(${EmpresasHabilitadas.cnpj})`,
                razaoSocial: sql`values(${EmpresasHabilitadas.razaoSocial})`,
                nomeFantasia: sql`values(${EmpresasHabilitadas.nomeFantasia})`,
                codigoCnae: sql`values(${EmpresasHabilitadas.codigoCnae})`,
                cnae: sql`values(${EmpresasHabilitadas.cnae})`,
                municipio: sql`values(${EmpresasHabilitadas.municipio})`,
                uf: sql`values(${EmpresasHabilitadas.uf})`,
                beneficioFiscal: sql`values(${EmpresasHabilitadas.beneficioFiscal})`,
                baseLegal: sql`values(${EmpresasHabilitadas.baseLegal})`,
                descricao: sql`values(${EmpresasHabilitadas.descricao})`,
                inicioHabilitacao: sql`values(${EmpresasHabilitadas.inicioHabilitacao})`,
                fimHabilitacao: sql`values(${EmpresasHabilitadas.fimHabilitacao})`,
                exercicioReferencia: sql`values(${EmpresasHabilitadas.exercicioReferencia})`,
                mesReferencia: sql`values(${EmpresasHabilitadas.mesReferencia})`,
                diaReferencia: sql`values(${EmpresasHabilitadas.diaReferencia})`,
                dadosAdicionais: sql`values(${EmpresasHabilitadas.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Empresas Habilitadas`);
        throw error;
    }
}

interface HeaderRule {
    key: string;
    keywords?: string[];
    matchExact?: string[];
    require?: string[];
}

const COMMON_RENUNCIAS_RULES: HeaderRule[] = [
    { key: 'anoCalendario', keywords: ['ANO CALENDARIO', 'ANO-CALENDARIO', 'ANOCALENDARIO'] },
    { key: 'cnpj', matchExact: ['CNPJ'] },
    { key: 'razaoSocial', keywords: ['RAZAO SOCIAL'] },
    { key: 'nomeFantasia', keywords: ['NOME FANTASIA'] },
    { key: 'codigoCnae', keywords: ['CODIGO CNAE', 'CNAE'], require: ['CODIGO'] },
    { key: 'cnae', matchExact: ['CNAE', 'NOME CNAE'] },
    { key: 'municipio', matchExact: ['MUNICIPIO'] },
    { key: 'uf', matchExact: ['UF'] },
    { key: 'beneficioFiscal', keywords: ['BENEFICIO FISCAL'] },
    { key: 'valorRenunciaFiscal', keywords: ['VALOR RENUNCIA FISCAL', 'VALOR RENUNCIA'] },
];

const RENUNCIAS_FISCAIS_RULES: HeaderRule[] = [
    { key: 'tipoRenuncia', keywords: ['TIPO RENUNCIA', 'TIPO DE RENUNCIA'] },
    { key: 'fundamentoLegal', keywords: ['FUNDAMENTO LEGAL'] },
    { key: 'descricao', matchExact: ['DESCRICAO'] },
    { key: 'tributo', matchExact: ['TRIBUTO'] },
    { key: 'formaTributacao', keywords: ['FORMA DE TRIBUTACAO', 'FORMA TRIBUTACAO'] },
];

const RENUNCIAS_BENEFICIARIO_RULES: HeaderRule[] = [];

const EMPRESAS_IMUNES_ISENTAS_RULES: HeaderRule[] = [
    { key: 'tipoEntidade', keywords: ['TIPO ENTIDADE', 'TIPO DE ENTIDADE'] },
];

const EMPRESAS_HABILITADAS_RULES: HeaderRule[] = [
    { key: 'baseLegal', keywords: ['BASE LEGAL'] },
    { key: 'descricao', matchExact: ['DESCRICAO'] },
    { key: 'inicioHabilitacao', keywords: ['INICIO DA HABILITACAO', 'INICIO HABILITACAO'] },
    { key: 'fimHabilitacao', keywords: ['FIM DA HABILITACAO', 'FIM HABILITACAO'] },
];

// Helper genérico para buscar regras específicas + comuns do módulo
function resolveRenunciasHeaderKey(header: string, specificRules: HeaderRule[]): string {
    if (!header) return '';
    const cleanHeader = normalize(header);

    // 1. Tenta casar nas regras específicas da sub-tabela
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

    // 2. Se não casou, tenta as regras comuns
    for (const rule of COMMON_RENUNCIAS_RULES) {
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

// ---------------------------------------------------------------------------
// 2. Processadores de Stream
// ---------------------------------------------------------------------------

async function processRenunciasFiscaisStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processRenunciasFiscaisStream');
    let batch: (typeof RenunciasFiscais.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => resolveRenunciasHeaderKey(header, RENUNCIAS_FISCAIS_RULES),
            }));

        for await (const data of parser) {
            batch.push({
                ...data,
                ...metadata,
            });
            if (batch.length >= BATCH_SIZE) {
                await persistRenunciasFiscais(batch);
                totalProcessed += batch.length;
                batch = [];
            }
        }
        if (batch.length > 0) {
            await persistRenunciasFiscais(batch);
            totalProcessed += batch.length;
        }

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento de Renúncias Fiscais concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream de Renúncias Fiscais');
        throw error;
    }
}

async function processRenunciasFiscaisPorBeneficiarioStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processRenunciasFiscaisPorBeneficiarioStream');
    let batch: (typeof RenunciasFiscaisPorBeneficiario.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => resolveRenunciasHeaderKey(header, RENUNCIAS_BENEFICIARIO_RULES),
            }));

        for await (const data of parser) {
            batch.push({
                ...data,
                ...metadata,
            });
            if (batch.length >= BATCH_SIZE) {
                await persistRenunciasFiscaisPorBeneficiario(batch);
                totalProcessed += batch.length;
                batch = [];
            }
        }
        if (batch.length > 0) {
            await persistRenunciasFiscaisPorBeneficiario(batch);
            totalProcessed += batch.length;
        }

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento de Renúncias Fiscais por Beneficiário concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream de Renúncias Fiscais por Beneficiário');
        throw error;
    }
}

async function processEmpresasImunesOuIsentasStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processEmpresasImunesOuIsentasStream');
    let batch: (typeof EmpresasImunesOuIsentas.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => resolveRenunciasHeaderKey(header, EMPRESAS_IMUNES_ISENTAS_RULES),
            }));

        for await (const data of parser) {
            batch.push({
                ...data,
                ...metadata,
            });
            if (batch.length >= BATCH_SIZE) {
                await persistEmpresasImunesOuIsentas(batch);
                totalProcessed += batch.length;
                batch = [];
            }
        }
        if (batch.length > 0) {
            await persistEmpresasImunesOuIsentas(batch);
            totalProcessed += batch.length;
        }

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento de Empresas Imunes ou Isentas concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream de Empresas Imunes ou Isentas');
        throw error;
    }
}

async function processEmpresasHabilitadasStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processEmpresasHabilitadasStream');
    let batch: (typeof EmpresasHabilitadas.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => resolveRenunciasHeaderKey(header, EMPRESAS_HABILITADAS_RULES),
            }));

        for await (const data of parser) {
            batch.push({
                ...data,
                ...metadata,
            });
            if (batch.length >= BATCH_SIZE) {
                await persistEmpresasHabilitadas(batch);
                totalProcessed += batch.length;
                batch = [];
            }
        }
        if (batch.length > 0) {
            await persistEmpresasHabilitadas(batch);
            totalProcessed += batch.length;
        }

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento de Empresas Habilitadas concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream de Empresas Habilitadas');
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

                            if (upperName.includes('EMPRESASIMUNESOUISENTAS') || upperName.includes('EMPRESASIMUNESOUSULENTAS') || upperName.includes('EMPRESAS_IMUNES_OU_ISENTAS')) {
                                rowsCount = await processEmpresasImunesOuIsentasStream(readStream, metadata);
                            } else if (upperName.includes('EMPRESASHABILITADAS') || upperName.includes('EMPRESAS_HABILITADAS')) {
                                rowsCount = await processEmpresasHabilitadasStream(readStream, metadata);
                            } else if (upperName.includes('RENUNCIASFISCAISPORBENEFICIARIO') || upperName.includes('RENÚNCIASFISCAISPORBENEFICIÁRIO') || upperName.includes('RENUNCIAS_FISCAIS_POR_BENEFICIARIO')) {
                                rowsCount = await processRenunciasFiscaisPorBeneficiarioStream(readStream, metadata);
                            } else if (upperName.includes('RENUNCIASFISCAIS') || upperName.includes('RENÚNCIASFISCAIS') || upperName.includes('RENUNCIAS_FISCAIS')) {
                                rowsCount = await processRenunciasFiscaisStream(readStream, metadata);
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

export async function runRenunciasFiscaisIngestion() {
    const log = logger.forMethod('runRenunciasFiscaisIngestion');
    const moduleName = 'renuncias';

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