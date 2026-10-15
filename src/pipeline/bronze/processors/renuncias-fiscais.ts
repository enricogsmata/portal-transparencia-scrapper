import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import csvParser from 'csv-parser';
import { sql } from 'drizzle-orm';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import {
    RenunciasFiscais,
    RenunciasFiscaisPorBeneficiario,
    EmpresasImunesOuIsentas,
    EmpresasHabilitadas
} from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY, extractMetadataFromFileName, ReferenceMetadata } from '../shared.js';

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
                tipoEntidade: sql`values(${RenunciasFiscaisPorBeneficiario.tipoEntidade})`,
                beneficioFiscal: sql`values(${RenunciasFiscaisPorBeneficiario.beneficioFiscal})`,
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
                beneficioFiscal: sql`values(${EmpresasImunesOuIsentas.beneficioFiscal})`,
                baseLegal: sql`values(${EmpresasImunesOuIsentas.baseLegal})`,
                descricao: sql`values(${EmpresasImunesOuIsentas.descricao})`,
                inicioHabilitacao: sql`values(${EmpresasImunesOuIsentas.inicioHabilitacao})`,
                fimHabilitacao: sql`values(${EmpresasImunesOuIsentas.fimHabilitacao})`,
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
                anoCalendario: sql`values(${EmpresasHabilitadas.anoCalendario})`,
                cnpj: sql`values(${EmpresasHabilitadas.cnpj})`,
                razaoSocial: sql`values(${EmpresasHabilitadas.razaoSocial})`,
                nomeFantasia: sql`values(${EmpresasHabilitadas.nomeFantasia})`,
                codigoCnae: sql`values(${EmpresasHabilitadas.codigoCnae})`,
                cnae: sql`values(${EmpresasHabilitadas.cnae})`,
                municipio: sql`values(${EmpresasHabilitadas.municipio})`,
                uf: sql`values(${EmpresasHabilitadas.uf})`,
                tipoRenuncia: sql`values(${EmpresasHabilitadas.tipoRenuncia})`,
                valorRenunciaFiscal: sql`values(${EmpresasHabilitadas.valorRenunciaFiscal})`,
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

function mapCommonHeaders(h: string): string | null {
    if (h.includes('ANO-CALENDÁRIO') || h.includes('ANO-CALENDARIO') || h.includes('ANO CALENDÁRIO') || h.includes('ANO CALENDARIO')) return 'anoCalendario';
    if (h === 'CNPJ') return 'cnpj';
    if (h.includes('RAZÃO SOCIAL') || h.includes('RAZAO SOCIAL')) return 'razaoSocial';
    if (h.includes('NOME FANTASIA')) return 'nomeFantasia';
    if (h.includes('CÓDIGO CNAE') || h.includes('CODIGO CNAE')) return 'codigoCnae';
    if (h === 'CNAE') return 'cnae';
    if (h === 'MUNICÍPIO' || h === 'MUNICIPIO') return 'municipio';
    if (h === 'UF' || h.includes('UF ')) return 'uf';
    if (h.includes('BENEFÍCIO FISCAL') || h.includes('BENEFICIO FISCAL')) return 'beneficioFiscal';
    if (h.includes('TIPO RENÚNCIA') || h.includes('TIPO RENUNCIA')) return 'tipoRenuncia';
    if (h === 'DESCRIÇÃO' || h === 'DESCRICAO') return 'descricao';
    if (h.includes('VALOR RENÚNCIA FISCAL') || h.includes('VALOR RENUNCIA FISCAL')) return 'valorRenunciaFiscal';
    return null;
}

async function processRenunciasFiscaisStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processRenunciasFiscaisStream');
    let batch: (typeof RenunciasFiscais.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase().trim();
                const common = mapCommonHeaders(h);
                if (common) return common;

                if (h.includes('FUNDAMENTO LEGAL')) return 'fundamentoLegal';
                if (h === 'TRIBUTO') return 'tributo';
                if (h.includes('FORMA DE TRIBUTAÇÃO') || h.includes('FORMA DE TRIBUTACAO')) return 'formaTributacao';
                return h;
            },
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
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream');
        throw error;
    }
}

async function processRenunciasFiscaisPorBeneficiarioStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processRenunciasFiscaisPorBeneficiarioStream');
    let batch: (typeof RenunciasFiscaisPorBeneficiario.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase().trim();
                const common = mapCommonHeaders(h);
                if (common) return common;

                if (h.includes('TIPO ENTIDADE')) return 'tipoEntidade';
                return h;
            },
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
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream');
        throw error;
    }
}

async function processEmpresasImunesOuIsentasStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processEmpresasImunesOuIsentasStream');
    let batch: (typeof EmpresasImunesOuIsentas.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase().trim();
                const common = mapCommonHeaders(h);
                if (common) return common;

                if (h.includes('BASE LEGAL')) return 'baseLegal';
                if (h.includes('INÍCIO DA HABILITAÇÃO') || h.includes('INICIO DA HABILITACAO') || h.includes('INÍCIO HABILITAÇÃO') || h.includes('INICIO HABILITACAO')) return 'inicioHabilitacao';
                if (h.includes('FIM DA HABILITAÇÃO') || h.includes('FIM DA HABILITACAO') || h.includes('FIM HABILITAÇÃO') || h.includes('FIM HABILITACAO')) return 'fimHabilitacao';
                return h;
            },
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
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream');
        throw error;
    }
}

async function processEmpresasHabilitadasStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processEmpresasHabilitadasStream');
    let batch: (typeof EmpresasHabilitadas.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase().trim();
                const common = mapCommonHeaders(h);
                if (common) return common;
                return h;
            },
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