import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import csvParser from 'csv-parser';
import { sql } from 'drizzle-orm';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import { GasDoPovo } from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY, extractMetadataFromFileName, ReferenceMetadata } from '../shared.js';
import iconv from 'iconv-lite';

const logger = new_logger(import.meta.url);

type GasDoPovoRow = typeof GasDoPovo.$inferInsert;

async function persistData(data: GasDoPovoRow[]) {
    const log = logger.forMethod('persistData');
    try {
        await bronze_db.insert(GasDoPovo).values(data).onDuplicateKeyUpdate({
            set: {
                mesReferencia: sql`values(${GasDoPovo.mesReferencia})`,
                uf: sql`values(${GasDoPovo.uf})`,
                codigoMunicipio: sql`values(${GasDoPovo.codigoMunicipio})`,
                nomeMunicipio: sql`values(${GasDoPovo.nomeMunicipio})`,
                cpfBeneficiario: sql`values(${GasDoPovo.cpfBeneficiario})`,
                nomeBeneficiario: sql`values(${GasDoPovo.nomeBeneficiario})`,
                cnpjEstabelecimento: sql`values(${GasDoPovo.cnpjEstabelecimento})`,
                nomeEstabelecimento: sql`values(${GasDoPovo.nomeEstabelecimento})`,
                quantidadePessoasFamilia: sql`values(${GasDoPovo.quantidadePessoasFamilia})`,
                validadeVale: sql`values(${GasDoPovo.validadeVale})`,
                dataInicioVigenciaVale: sql`values(${GasDoPovo.dataInicioVigenciaVale})`,
                dataFimVigenciaVale: sql`values(${GasDoPovo.dataFimVigenciaVale})`,
                dataRetiradaVale: sql`values(${GasDoPovo.dataRetiradaVale})`,
                valorBeneficio: sql`values(${GasDoPovo.valorBeneficio})`,
                exercicioReferencia: sql`values(${GasDoPovo.exercicioReferencia})`,
                diaReferencia: sql`values(${GasDoPovo.diaReferencia})`,
                dadosAdicionais: sql`values(${GasDoPovo.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência`);
        throw error;
    }
}

export async function processCsvStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processCsvStream');
    const BATCH_SIZE = 2000;
    let batch: GasDoPovoRow[] = [];
    let totalProcessed = 0;

    try {
        // 1. Função de normalização robusta (remove acentos, pontuações e múltiplos espaços)
        const normalize = (str: string) =>
            str ? str
                .normalize("NFD")
                .replace(/[\u0300-\u036f]/g, "") // Remove acentos
                .replace(/[^A-Z0-9\s]/gi, "")   // Remove parênteses, hífens, pontuações
                .replace(/\s+/g, " ")           // Transforma múltiplos espaços em 1 único espaço
                .trim()
                .toUpperCase()
                : "";

        // 2. Tabela de Mapeamento (Keywords em caixa alta e sem acento)
        const HEADER_RULES = [
            { key: 'mesReferencia', keywords: ['MES REFER'] },
            { key: 'uf', matchExact: ['UF'] },
            { key: 'codigoMunicipio', keywords: ['SIAFI', 'CODIGO MUNIC'] },
            { key: 'nomeMunicipio', keywords: ['NOME DO MUNIC', 'NOME MUNIC'] },
            { key: 'cpfBeneficiario', keywords: ['CPF DO BENEFICI', 'CPF BENEFICI'] },
            { key: 'nomeBeneficiario', keywords: ['NOME FAVORECIDO', 'NOME BENEFICI'] },
            { key: 'cnpjEstabelecimento', keywords: ['CNPJ ESTABELECIMENTO'] },
            { key: 'nomeEstabelecimento', keywords: ['NOME ESTABELECIMENTO', 'ESTABELECIMENTO'] },
            { key: 'quantidadePessoasFamilia', keywords: ['QUANTIDADE DE PESSOAS', 'PESSOAS NA FAMILIA'] },
            { key: 'validadeVale', keywords: ['VALIDADE VALE', 'PERIODO VALIDADE VALE'] },
            { key: 'dataInicioVigenciaVale', keywords: ['INICIO VIGENCIA', 'DATA INICIO'] },
            { key: 'dataFimVigenciaVale', keywords: ['FIM VIGENCIA', 'DATA FIM'] },
            { key: 'dataRetiradaVale', keywords: ['RETIRADA VALE', 'DATA RETIRADA'] },
            { key: 'valorBeneficio', keywords: ['VALOR BENEFICIO', 'VALOR BENEF'] }
        ];

        // 3. Parser no Stream
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1')) // ou 'utf-8', dependendo da origem
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => {
                    if (!header) return null;

                    const cleanHeader = normalize(header);

                    for (const rule of HEADER_RULES) {
                        if (rule.matchExact && rule.matchExact.includes(cleanHeader)) {
                            return rule.key;
                        }
                        if (rule.keywords && rule.keywords.some(kw => cleanHeader.includes(kw))) {
                            return rule.key;
                        }
                    }

                    log.info(`Não achou: ${cleanHeader}`);
                    return cleanHeader;
                }
            }));

        for await (const data of parser) {
            batch.push({
                ...data,
                ...metadata,
            });

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
                            const rowsCount = await processCsvStream(readStream, metadata);
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

export async function runGasDoPovoIngestion() {
    const log = logger.forMethod('runGasDoPovoIngestion');
    const moduleName = 'gas-do-povo';

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