import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import csvParser from 'csv-parser';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import {
    ServidoresCadastro,
    AposentadosCadastro,
    PensionistasCadastro,
    ServidoresAfastamentos,
    ServidoresObservacoes,
    ServidoresHonorariosAdvocaticios,
    ServidoresJetons,
    ServidoresRemuneracao
} from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY } from '../shared.js';

const logger = new_logger(import.meta.url);
const BATCH_SIZE = 2000;

async function persistData(schema: any, data: any[], logPrefix: string) {
    const log = logger.forMethod(`persistData:${logPrefix}`);
    try {
        await bronze_db.insert(schema).values(data);
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de ${logPrefix}`);
        throw error;
    }
}

function mapHeaders(header: string): string {
    const h = header.toUpperCase().trim();
    
    if (h === 'ANO') return 'ano';
    if (h === 'MÊS' || h === 'MES') return 'mes';
    if (h === 'ID_SERVIDOR_PORTAL') return 'idServidorPortal';
    if (h === 'NOME') return 'nome';
    if (h === 'CPF') return 'cpf';
    if (h === 'MATRICULA' || h === 'MATRÍCULA') return 'matricula';
    if (h === 'DESCRICAO_CARGO') return 'descricaoCargo';
    if (h === 'CLASSE_CARGO') return 'classeCargo';
    if (h === 'REFERENCIA_CARGO') return 'referenciaCargo';
    if (h === 'PADRAO_CARGO') return 'padraoCargo';
    if (h === 'NIVEL_CARGO') return 'nivelCargo';
    if (h === 'SIGLA_FUNCAO') return 'siglaFuncao';
    if (h === 'NIVEL_FUNCAO') return 'nivelFuncao';
    if (h === 'FUNCAO') return 'funcao';
    if (h === 'CODIGO_ATIVIDADE') return 'codigoAtividade';
    if (h === 'ATIVIDADE') return 'atividade';
    if (h === 'OPCAO_PARCIAL') return 'opcaoParcial';
    if (h === 'COD_UORG_LOTACAO') return 'codUorgLotacao';
    if (h === 'UORG_LOTACAO') return 'uorgLotacao';
    if (h === 'COD_ORGAO_LOTACAO') return 'codOrgaoLotacao';
    if (h === 'ORGAO_LOTACAO') return 'orgaoLotacao';
    if (h === 'COD_ORGSUP_LOTACAO') return 'codOrgsupLotacao';
    if (h === 'ORGSUP_LOTACAO') return 'orgsupLotacao';
    if (h === 'COD_UORG_EXERCICIO') return 'codUorgExercicio';
    if (h === 'UORG_EXERCICIO') return 'uorgExercicio';
    if (h === 'COD_ORGAO_EXERCICIO') return 'codOrgaoExercicio';
    if (h === 'ORGAO_EXERCICIO') return 'orgaoExercicio';
    if (h === 'COD_ORGSUP_EXERCICIO') return 'codOrgsupExercicio';
    if (h === 'ORGSUP_EXERCICIO') return 'orgsupExercicio';
    if (h === 'COD_TIPO_VINCULO') return 'codTipoVinculo';
    if (h === 'TIPO_VINCULO') return 'tipoVinculo';
    if (h === 'SITUACAO_VINCULO') return 'situacaoVinculo';
    if (h === 'DATA_INICIO_AFASTAMENTO') return 'dataInicioAfastamento';
    if (h === 'DATA_TERMINO_AFASTAMENTO' || h === 'DATA_FIM_AFASTAMENTO') return 'dataTerminoAfastamento';
    if (h === 'REGIME_JURIDICO' || h === 'REGIME_JURÍDICO') return 'regimeJuridico';
    if (h === 'JORNADA_DE_TRABALHO' || h === 'JORNADA_TRABALHO') return 'jornadaDeTrabalho';
    if (h === 'DATA_INGRESSO_CARGOFUNCAO') return 'dataIngressoCargofuncao';
    if (h === 'DATA_NOMEACAO_CARGOFUNCAO') return 'dataNomeacaoCargofuncao';
    if (h === 'DATA_INGRESSO_ORGAO') return 'dataIngressoOrgao';
    if (h === 'DOCUMENTO_INGRESSO_SERVICOPUBLICO') return 'documentoIngressoServicopublico';
    if (h === 'DATA_DIPLOMA_INGRESSO_SERVPUBLICO') return 'dataDiplomaIngressoServpublico';
    if (h === 'DIPLOMA_INGRESSO_CARGOFUNCAO') return 'diplomaIngressoCargofuncao';
    if (h === 'DIPLOMA_INGRESSO_ORGAO') return 'diplomaIngressoOrgao';
    if (h === 'DIPLOMA_INGRESSO_SERVPUBLICO') return 'diplomaIngressoServpublico';
    if (h === 'UF_EXERCICIO' || h === 'UF EXERCICIO') return 'ufExercicio';

    if (h === 'COD_TIPO_APOSENTADORIA') return 'codTipoAposentadoria';
    if (h === 'TIPO_APOSENTADORIA') return 'tipoAposentadoria';
    if (h === 'DATA_APOSENTADORIA') return 'dataAposentadoria';

    if (h === 'CPF_REPRESENTANTE_LEGAL') return 'cpfRepresentanteLegal';
    if (h === 'NOME_REPRESENTANTE_LEGAL') return 'nomeRepresentanteLegal';
    if (h === 'CPF_INSTITUIDOR_PENSAO') return 'cpfInstituidorPensao';
    if (h === 'NOME_INSTITUIDOR_PENSAO') return 'nomeInstituidorPensao';
    if (h === 'COD_TIPO_PENSAO') return 'codTipoPensao';
    if (h === 'TIPO_PENSAO') return 'tipoPensao';
    if (h === 'DATA_INICIO_PENSAO') return 'dataInicioPensao';
    if (h === 'DESCRICAO_CARGO_INSTITUIDOR_PENSAO') return 'descricaoCargoInstituidorPensao';
    if (h === 'COD_UORG_LOTACAO_INSTITUIDOR_PENSAO') return 'codUorgLotacaoInstituidorPensao';
    if (h === 'UORG_LOTACAO_INSTITUIDOR_PENSAO') return 'uorgLotacaoInstituidorPensao';
    if (h === 'COD_ORG_LOTACAO_INSTITUIDOR_PENSAO' || h === 'COD_ORGAO_LOTACAO_INSTITUIDOR_PENSAO') return 'codOrgLotacaoInstituidorPensao';
    if (h === 'ORG_LOTACAO_INSTITUIDOR_PENSAO' || h === 'ORGAO_LOTACAO_INSTITUIDOR_PENSAO') return 'orgLotacaoInstituidorPensao';
    if (h === 'COD_ORGSUP_LOTACAO_INSTITUIDOR_PENSAO') return 'codOrgsupLotacaoInstituidorPensao';
    if (h === 'ORGSUP_LOTACAO_INSTITUIDOR_PENSAO') return 'orgsupLotacaoInstituidorPensao';
    if (h === 'REGIME_JURIDICO_INSTITUIDOR_PENSAO') return 'regimeJuridicoInstituidorPensao';
    if (h === 'JORNADA_DE_TRABALHO_INSTITUIDOR_PENSAO') return 'jornadaDeTrabalhoInstituidorPensao';
    if (h === 'DATA_INGRESSO_CARGOFUNCAO_INSTITUIDOR_PENSAO') return 'dataIngressoCargofuncaoInstituidorPensao';
    if (h === 'DATA_NOMEACAO_CARGOFUNCAO_INSTITUIDOR_PENSAO') return 'dataNomeacaoCargofuncaoInstituidorPensao';
    if (h === 'DATA_INGRESSO_ORGAO_INSTITUIDOR_PENSAO') return 'dataIngressoOrgaoInstituidorPensao';
    if (h === 'DOCUMENTO_INGRESSO_SERVICOPUBLICO_INSTITUIDOR_PENSAO') return 'documentoIngressoServicopublicoInstituidorPensao';
    if (h === 'DATA_DIPLOMA_INGRESSO_SERVICOPUBLICO_INSTITUIDOR_PENSAO' || h === 'DATA_DIPLOMA_INGRESSO_SERVPUBLICO_INSTITUIDOR_PENSAO') return 'dataDiplomaIngressoServicopublicoInstituidorPensao';
    if (h === 'DIPLOMA_INGRESSO_CARGOFUNCAO_INSTITUIDOR_PENSAO') return 'diplomaIngressoCargofuncaoInstituidorPensao';
    if (h === 'DIPLOMA_INGRESSO_ORGAO_INSTITUIDOR_PENSAO') return 'diplomaIngressoOrgaoInstituidorPensao';
    if (h === 'DIPLOMA_INGRESSO_SERVICOPUBLICO_INSTITUIDOR_PENSAO' || h === 'DIPLOMA_INGRESSO_SERVPUBLICO_INSTITUIDOR_PENSAO') return 'diplomaIngressoServicopublicoInstituidorPensao';

    if (h === 'OBSERVACOES' || h === 'OBSERVACAO' || h === 'OBSERVAÇÕES' || h === 'OBSERVAÇÃO') return 'observacao';
    if (h === 'VALOR') return 'valor';
    if (h === 'EMPRESA') return 'empresa';

    if (h.includes('REMUNERAÇÃO BÁSICA BRUTA (R$)') || h.includes('REMUNERACAO BASICA BRUTA (R$)')) return 'remuneracaoBasicaBrutaRs';
    if (h.includes('REMUNERAÇÃO BÁSICA BRUTA (US$)') || h.includes('REMUNERACAO BASICA BRUTA (US$)')) return 'remuneracaoBasicaBrutaUs';
    if (h.includes('ABATE TETO (R$)')) return 'abateTetoRs';
    if (h.includes('ABATE TETO (US$)')) return 'abateTetoUs';
    if (h.includes('GRATIFICAÇÃO NATALINA (R$)') || h.includes('GRATIFICACAO NATALINA (R$)')) return 'gratificacaoNatalinaRs';
    if (h.includes('GRATIFICAÇÃO NATALINA (US$)') || h.includes('GRATIFICACAO NATALINA (US$)')) return 'gratificacaoNatalinaUs';
    if (h.includes('ABATE TETO DA GRATIFICAÇÃO NATALINA (R$)') || h.includes('ABATE TETO DA GRATIFICACAO NATALINA (R$)')) return 'abateTetoGratificacaoNatalinaRs';
    if (h.includes('ABATE TETO DA GRATIFICAÇÃO NATALINA (US$)') || h.includes('ABATE TETO DA GRATIFICACAO NATALINA (US$)')) return 'abateTetoGratificacaoNatalinaUs';
    if (h.includes('FÉRIAS (R$)') || h.includes('FERIAS (R$)')) return 'feriasRs';
    if (h.includes('FÉRIAS (US$)') || h.includes('FERIAS (US$)')) return 'feriasUs';
    if (h.includes('OUTRAS REMUNERAÇÕES EVENTUAIS (R$)') || h.includes('OUTRAS REMUNERACOES EVENTUAIS (R$)')) return 'outrasRemuneracoesEventuaisRs';
    if (h.includes('OUTRAS REMUNERAÇÕES EVENTUAIS (US$)') || h.includes('OUTRAS REMUNERACOES EVENTUAIS (US$)')) return 'outrasRemuneracoesEventuaisUs';
    if (h.includes('IRRF (R$)')) return 'irrfRs';
    if (h.includes('IRRF (US$)')) return 'irrfUs';
    if (h.includes('PSS/RPGS (R$)')) return 'pssRpgsRs';
    if (h.includes('PSS/RPGS (US$)')) return 'pssRpgsUs';
    if (h.includes('DEMAIS DEDUÇÕES (R$)') || h.includes('DEMAIS DEDUCOES (R$)')) return 'demaisDeducoesRs';
    if (h.includes('DEMAIS DEDUÇÕES (US$)') || h.includes('DEMAIS DEDUCOES (US$)')) return 'demaisDeducoesUs';
    if (h.includes('PENSÃO MILITAR (R$)') || h.includes('PENSAO MILITAR (R$)')) return 'pensaoMilitarRs';
    if (h.includes('PENSÃO MILITAR (US$)') || h.includes('PENSAO MILITAR (US$)')) return 'pensaoMilitarUs';
    if (h.includes('FUNDO DE SAÚDE (R$)') || h.includes('FUNDO DE SAUDE (R$)')) return 'fundoSaudeRs';
    if (h.includes('FUNDO DE SAÚDE (US$)') || h.includes('FUNDO DE SAUDE (US$)')) return 'fundoSaudeUs';
    if (h.includes('TAXA DE OCUPAÇÃO IMÓVEL FUNCIONAL (R$)') || h.includes('TAXA DE OCUPACAO IMOVEL FUNCIONAL (R$)')) return 'taxaOcupacaoImovelFuncionalRs';
    if (h.includes('TAXA DE OCUPAÇÃO IMÓVEL FUNCIONAL (US$)') || h.includes('TAXA DE OCUPACAO IMOVEL FUNCIONAL (US$)')) return 'taxaOcupacaoImovelFuncionalUs';
    if (h.includes('REMUNERAÇÃO APÓS DEDUÇÕES OBRIGATÓRIAS (R$)') || h.includes('REMUNERACAO APOS DEDUCOES OBRIGATORIAS (R$)')) return 'remuneracaoAposDeducoesObrigatoriasRs';
    if (h.includes('REMUNERAÇÃO APÓS DEDUÇÕES OBRIGATÓRIAS (US$)') || h.includes('REMUNERACAO APOS DEDUCOES OBRIGATORIAS (US$)')) return 'remuneracaoAposDeducoesObrigatoriasUs';
    if (h.includes('VERBAS INDENIZATÓRIAS REGISTRADAS EM SISTEMAS DE PESSOAL - CIVIL (R$)') || h.includes('VERBAS INDENIZATORIAS REGISTRADAS EM SISTEMAS DE PESSOAL - CIVIL (R$)')) return 'verbasIndenizatoriasCivilRs';
    if (h.includes('VERBAS INDENIZATÓRIAS REGISTRADAS EM SISTEMAS DE PESSOAL - CIVIL (US$)') || h.includes('VERBAS INDENIZATORIAS REGISTRADAS EM SISTEMAS DE PESSOAL - CIVIL (US$)')) return 'verbasIndenizatoriasCivilUs';
    if (h.includes('VERBAS INDENIZATÓRIAS REGISTRADAS EM SISTEMAS DE PESSOAL - MILITAR (R$)') || h.includes('VERBAS INDENIZATORIAS REGISTRADAS EM SISTEMAS DE PESSOAL - MILITAR (R$)')) return 'verbasIndenizatoriasMilitarRs';
    if (h.includes('VERBAS INDENIZATÓRIAS REGISTRADAS EM SISTEMAS DE PESSOAL - MILITAR (US$)') || h.includes('VERBAS INDENIZATORIAS REGISTRADAS EM SISTEMAS DE PESSOAL - MILITAR (US$)')) return 'verbasIndenizatoriasMilitarUs';
    if (h.includes('VERBAS INDENIZATÓRIAS PROGRAMA DESLIGAMENTO VOLUNTÁRIO') || h.includes('VERBAS INDENIZATORIAS PROGRAMA DESLIGAMENTO VOLUNTARIO')) {
        if (h.includes('(R$)')) return 'verbasIndenizatoriasPdvRs';
        if (h.includes('(U$)') || h.includes('(US$)')) return 'verbasIndenizatoriasPdvUs';
    }
    if (h.includes('TOTAL DE VERBAS INDENIZATÓRIAS (R$)') || h.includes('TOTAL DE VERBAS INDENIZATORIAS (R$)')) return 'totalVerbasIndenizatoriasRs';
    if (h.includes('TOTAL DE VERBAS INDENIZATÓRIAS (US$)') || h.includes('TOTAL DE VERBAS INDENIZATORIAS (US$)')) return 'totalVerbasIndenizatoriasUs';
    if (h.includes('TOTAL DE HONORÁRIOS (JETONS)') || h.includes('TOTAL DE HONORARIOS (JETONS)')) return 'totalHonorariosJetons';

    // fallback mapping common fixes
    if (h === 'OBSERVAÇÕES') return 'observacoes';
    
    return header.trim();
}

async function processGenericStream(stream: NodeJS.ReadableStream, schema: any, logPrefix: string): Promise<number> {
    const log = logger.forMethod(`processGenericStream:${logPrefix}`);
    let batch: any[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => mapHeaders(header),
        }));

        for await (const data of parser) {
            batch.push(data);
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
                            const upperZipName = file.toUpperCase();
                            const upperFileName = entry.fileName.toUpperCase();
                            let rowsCount = 0;

                            if (upperFileName.includes('AFASTAMENTOS')) {
                                rowsCount = await processGenericStream(readStream, ServidoresAfastamentos, 'Afastamentos');
                            } else if (upperFileName.includes('OBSERVACOES') || upperFileName.includes('OBSERVAÇÕES')) {
                                rowsCount = await processGenericStream(readStream, ServidoresObservacoes, 'Observacoes');
                            } else if (upperFileName.includes('HONORARIOSADVOCATICIOS') || upperFileName.includes('HONORARIOS_ADVOCATICIOS')) {
                                rowsCount = await processGenericStream(readStream, ServidoresHonorariosAdvocaticios, 'HonorariosAdvocaticios');
                            } else if (upperFileName.includes('JETONS') || upperFileName.includes('HONORARIOS(JETONS)')) {
                                rowsCount = await processGenericStream(readStream, ServidoresJetons, 'Jetons');
                            } else if (upperFileName.includes('REMUNERACAO') || upperFileName.includes('REMUNERAÇÃO')) {
                                rowsCount = await processGenericStream(readStream, ServidoresRemuneracao, 'Remuneracao');
                            } else if (upperFileName.includes('CADASTRO')) {
                                if (upperZipName.includes('PENSIONISTAS')) {
                                    rowsCount = await processGenericStream(readStream, PensionistasCadastro, 'PensionistasCadastro');
                                } else if (upperZipName.includes('APOSENTADOS') || upperZipName.includes('RESERVA_REFORMA')) {
                                    rowsCount = await processGenericStream(readStream, AposentadosCadastro, 'AposentadosCadastro');
                                } else {
                                    rowsCount = await processGenericStream(readStream, ServidoresCadastro, 'ServidoresCadastro');
                                }
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

export async function runServidoresIngestion() {
    const log = logger.forMethod('runServidoresIngestion');
    const moduleName = 'servidores';

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