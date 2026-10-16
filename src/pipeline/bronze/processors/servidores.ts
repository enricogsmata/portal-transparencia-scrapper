import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import iconv from 'iconv-lite';
import csvParser from 'csv-parser';
import { sql } from 'drizzle-orm';
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

const SERVIDORES_HEADER_RULES: HeaderRule[] = [
    // Campos fixos e isolados
    { key: 'ano', matchExact: ['ANO'] },
    { key: 'mes', matchExact: ['MES'] },
    { key: 'idServidorPortal', matchExact: ['ID SERVIDOR PORTAL', 'ID_SERVIDOR_PORTAL'] },
    { key: 'nome', matchExact: ['NOME'] },
    { key: 'cpf', matchExact: ['CPF'] },
    { key: 'matricula', matchExact: ['MATRICULA'] },
    { key: 'observacao', matchExact: ['OBSERVACAO', 'OBSERVACOES'] },
    { key: 'valor', matchExact: ['VALOR'] },
    { key: 'empresa', matchExact: ['EMPRESA'] },

    // Instituidor de Pensão
    { key: 'descricaoCargoInstituidorPensao', matchExact: ['DESCRICAO CARGO INSTITUIDOR PENSAO', 'CARGO INSTITUIDOR PENSAO', 'DESCRICAOCARGOINSTITUIDORPENSAO', 'CARGOINSTITUIDORPENSAO'] },
    { key: 'codUorgLotacaoInstituidorPensao', matchExact: ['COD UORG LOTACAO INSTITUIDOR PENSAO', 'CODUORGLOTACAOINSTITUIDORPENSAO'] },
    { key: 'uorgLotacaoInstituidorPensao', matchExact: ['UORG LOTACAO INSTITUIDOR PENSAO', 'UORGLOTACAOINSTITUIDORPENSAO'] },
    { key: 'codOrgLotacaoInstituidorPensao', matchExact: ['COD ORG LOTACAO INSTITUIDOR PENSAO', 'COD ORGAO LOTACAO INSTITUIDOR PENSAO', 'CODORGLOTACAOINSTITUIDORPENSAO', 'CODORGAOLOTACAOINSTITUIDORPENSAO'] },
    { key: 'orgLotacaoInstituidorPensao', matchExact: ['ORG LOTACAO INSTITUIDOR PENSAO', 'ORGAO LOTACAO INSTITUIDOR PENSAO', 'ORGLOTACAOINSTITUIDORPENSAO', 'ORGAOLOTACAOINSTITUIDORPENSAO'] },
    { key: 'codOrgsupLotacaoInstituidorPensao', matchExact: ['COD ORGSUP LOTACAO INSTITUIDOR PENSAO', 'CODORGSUPLOTACAOINSTITUIDORPENSAO'] },
    { key: 'orgsupLotacaoInstituidorPensao', matchExact: ['ORGSUP LOTACAO INSTITUIDOR PENSAO', 'ORGSUPLOTACAOINSTITUIDORPENSAO'] },
    { key: 'regimeJuridicoInstituidorPensao', matchExact: ['REGIME JURIDICO INSTITUIDOR PENSAO', 'REGIMEJURIDICOINSTITUIDORPENSAO'] },
    { key: 'jornadaDeTrabalhoInstituidorPensao', matchExact: ['JORNADA DE TRABALHO INSTITUIDOR PENSAO', 'JORNADA INSTITUIDOR PENSAO', 'JORNADADETRABALHOINSTITUIDORPENSAO', 'JORNADAINSTITUIDORPENSAO'] },
    { key: 'dataIngressoCargofuncaoInstituidorPensao', matchExact: ['DATA INGRESSO CARGOFUNCAO INSTITUIDOR PENSAO', 'INGRESSO CARGOFUNCAO INSTITUIDOR PENSAO', 'DATAINGRESSOCARGOFUNCAOINSTITUIDORPENSAO', 'INGRESSOCARGOFUNCAOINSTITUIDORPENSAO'] },
    { key: 'dataNomeacaoCargofuncaoInstituidorPensao', matchExact: ['DATA NOMEACAO CARGOFUNCAO INSTITUIDOR PENSAO', 'NOMEACAO CARGOFUNCAO INSTITUIDOR PENSAO', 'DATANOMEACAOCARGOFUNCAOINSTITUIDORPENSAO', 'NOMEACAOCARGOFUNCAOINSTITUIDORPENSAO'] },
    { key: 'dataIngressoOrgaoInstituidorPensao', matchExact: ['DATA INGRESSO ORGAO INSTITUIDOR PENSAO', 'INGRESSO ORGAO INSTITUIDOR PENSAO', 'DATAINGRESSOORGAOINSTITUIDORPENSAO', 'INGRESSOORGAOINSTITUIDORPENSAO'] },
    { key: 'documentoIngressoServicopublicoInstituidorPensao', matchExact: ['DOCUMENTO INGRESSO SERVICOPUBLICO INSTITUIDOR PENSAO', 'DOCUMENTO INGRESSO SERVPUBLICO INSTITUIDOR PENSAO', 'DOCUMENTOINGRESSOSERVICOPUBLICOINSTITUIDORPENSAO', 'DOCUMENTOINGRESSOSERVPUBLICOINSTITUIDORPENSAO'] },
    { key: 'dataDiplomaIngressoServicopublicoInstituidorPensao', matchExact: ['DATA DIPLOMA INGRESSO SERVICOPUBLICO INSTITUIDOR PENSAO', 'DATA DIPLOMA INGRESSO SERVPUBLICO INSTITUIDOR PENSAO', 'DATADIPLOMAINGRESSOSERVICOPUBLICOINSTITUIDORPENSAO', 'DATADIPLOMAINGRESSOSERVPUBLICOINSTITUIDORPENSAO'] },
    { key: 'diplomaIngressoCargofuncaoInstituidorPensao', matchExact: ['DIPLOMA INGRESSO CARGOFUNCAO INSTITUIDOR PENSAO', 'DIPLOMAINGRESSOCARGOFUNCAOINSTITUIDORPENSAO'] },
    { key: 'diplomaIngressoOrgaoInstituidorPensao', matchExact: ['DIPLOMA INGRESSO ORGAO INSTITUIDOR PENSAO', 'DIPLOMAINGRESSOORGAOINSTITUIDORPENSAO'] },
    { key: 'diplomaIngressoServicopublicoInstituidorPensao', matchExact: ['DIPLOMA INGRESSO SERVICOPUBLICO INSTITUIDOR PENSAO', 'DIPLOMA INGRESSO SERVPUBLICO INSTITUIDOR PENSAO', 'DIPLOMAINGRESSOSERVICOPUBLICOINSTITUIDORPENSAO', 'DIPLOMAINGRESSOSERVPUBLICOINSTITUIDORPENSAO'] },

    // Cargo e Função Servidor
    { key: 'descricaoCargo', matchExact: ['DESCRICAO CARGO', 'CARGO', 'DESCRICAOCARGO'] },
    { key: 'classeCargo', matchExact: ['CLASSE CARGO', 'CLASSE', 'CLASSECARGO'] },
    { key: 'referenciaCargo', matchExact: ['REFERENCIA CARGO', 'REFERENCIA', 'REFERENCIACARGO'] },
    { key: 'padraoCargo', matchExact: ['PADRAO CARGO', 'PADRAO', 'PADRAOCARGO'] },
    { key: 'nivelCargo', matchExact: ['NIVEL CARGO', 'NIVEL', 'NIVELCARGO'] },
    { key: 'siglaFuncao', matchExact: ['SIGLA FUNCAO', 'SIGLAFUNCAO'] },
    { key: 'nivelFuncao', matchExact: ['NIVEL FUNCAO', 'NIVELFUNCAO'] },
    { key: 'funcao', matchExact: ['FUNCAO'] },
    { key: 'codigoAtividade', matchExact: ['CODIGO ATIVIDADE', 'COD ATIVIDADE', 'CODIGOATIVIDADE', 'CODATIVIDADE'] },
    { key: 'atividade', matchExact: ['ATIVIDADE'] },
    { key: 'opcaoParcial', matchExact: ['OPCAO PARCIAL', 'OPCAOPARCIAL'] },

    // Lotação e Exercício
    { key: 'codUorgLotacao', matchExact: ['COD UORG LOTACAO', 'CODIGO UORG LOTACAO', 'CODUORGLOTACAO', 'CODIGOUORGLOTACAO'] },
    { key: 'uorgLotacao', matchExact: ['UORG LOTACAO', 'UORGLOTACAO'] },
    { key: 'codOrgaoLotacao', matchExact: ['COD ORGAO LOTACAO', 'COD ORG LOTACAO', 'CODORGAOLOTACAO', 'CODORGLOTACAO'] },
    { key: 'orgaoLotacao', matchExact: ['ORGAO LOTACAO', 'ORG LOTACAO', 'ORGAOLOTACAO', 'ORGLOTACAO'] },
    { key: 'codOrgsupLotacao', matchExact: ['COD ORGSUP LOTACAO', 'CODIGO ORGSUP LOTACAO', 'CODORGSUPLOTACAO', 'CODIGOORGSUPLOTACAO'] },
    { key: 'orgsupLotacao', matchExact: ['ORGSUP LOTACAO', 'ORGSUPLOTACAO'] },

    { key: 'codUorgExercicio', matchExact: ['COD UORG EXERCICIO', 'CODIGO UORG EXERCICIO', 'CODUORGEXERCICIO', 'CODIGOUORGEXERCICIO'] },
    { key: 'uorgExercicio', matchExact: ['UORG EXERCICIO', 'UORGEXERCICIO'] },
    { key: 'codOrgaoExercicio', matchExact: ['COD ORGAO EXERCICIO', 'COD ORG EXERCICIO', 'CODORGAOEXERCICIO', 'CODORGEXERCICIO'] },
    { key: 'orgaoExercicio', matchExact: ['ORGAO EXERCICIO', 'ORG EXERCICIO', 'ORGAOEXERCICIO', 'ORGEXERCICIO'] },
    { key: 'codOrgsupExercicio', matchExact: ['COD ORGSUP EXERCICIO', 'CODIGO ORGSUP EXERCICIO', 'CODORGSUPEXERCICIO', 'CODIGOORGSUPEXERCICIO'] },
    { key: 'orgsupExercicio', matchExact: ['ORGSUP EXERCICIO', 'ORGSUPEXERCICIO'] },
    { key: 'ufExercicio', matchExact: ['UF EXERCICIO', 'UFEXERCICIO'] },

    // Vínculo e Afastamento
    { key: 'codTipoVinculo', matchExact: ['COD TIPO VINCULO', 'CODIGO TIPO VINCULO', 'CODTIPOVINCULO', 'CODIGOTIPOVINCULO'] },
    { key: 'tipoVinculo', matchExact: ['TIPO VINCULO', 'TIPOVINCULO'] },
    { key: 'situacaoVinculo', matchExact: ['SITUACAO VINCULO', 'SITUACAOVINCULO'] },
    { key: 'dataFimAfastamento', matchExact: ['DATA FIM AFASTAMENTO', 'FIM AFASTAMENTO', 'DATAFIMAFASTAMENTO', 'FIMAFASTAMENTO'] },
    { key: 'dataTerminoAfastamento', matchExact: ['DATA TERMINO AFASTAMENTO', 'TERMINO AFASTAMENTO', 'DATATERMINOAFASTAMENTO', 'TERMINOAFASTAMENTO'] },
    { key: 'dataInicioAfastamento', matchExact: ['DATA INICIO AFASTAMENTO', 'INICIO AFASTAMENTO', 'DATAINICIOAFASTAMENTO', 'INICIOAFASTAMENTO'] },
    { key: 'regimeJuridico', matchExact: ['REGIME JURIDICO', 'REGIMEJURIDICO'] },
    { key: 'jornadaDeTrabalho', matchExact: ['JORNADA DE TRABALHO', 'JORNADA', 'JORNADADETRABALHO'] },
    { key: 'dataIngressoCargofuncao', matchExact: ['DATA INGRESSO CARGOFUNCAO', 'INGRESSO CARGOFUNCAO', 'DATAINGRESSOCARGOFUNCAO', 'INGRESSOCARGOFUNCAO'] },
    { key: 'dataNomeacaoCargofuncao', matchExact: ['DATA NOMEACAO CARGOFUNCAO', 'NOMEACAO CARGOFUNCAO', 'DATANOMEACAOCARGOFUNCAO', 'NOMEACAOCARGOFUNCAO'] },
    { key: 'dataIngressoOrgao', matchExact: ['DATA INGRESSO ORGAO', 'INGRESSO ORGAO', 'DATAINGRESSOORGAO', 'INGRESSOORGAO'] },
    { key: 'documentoIngressoServicopublico', matchExact: ['DOCUMENTO INGRESSO SERVICOPUBLICO', 'DOCUMENTO INGRESSO SERVPUBLICO', 'DOCUMENTO INGRESSO', 'DOCUMENTOINGRESSOSERVICOPUBLICO', 'DOCUMENTOINGRESSOSERVPUBLICO', 'DOCUMENTOINGRESSO'] },
    { key: 'dataDiplomaIngressoServpublico', matchExact: ['DATA DIPLOMA INGRESSO SERVICOPUBLICO', 'DATA DIPLOMA INGRESSO SERVPUBLICO', 'DATADIPLOMAINGRESSOSERVICOPUBLICO', 'DATADIPLOMAINGRESSOSERVPUBLICO'] },
    { key: 'diplomaIngressoCargofuncao', matchExact: ['DIPLOMA INGRESSO CARGOFUNCAO', 'DIPLOMAINGRESSOCARGOFUNCAO'] },
    { key: 'diplomaIngressoOrgao', matchExact: ['DIPLOMA INGRESSO ORGAO', 'DIPLOMAINGRESSOORGAO'] },
    { key: 'diplomaIngressoServpublico', matchExact: ['DIPLOMA INGRESSO SERVICOPUBLICO', 'DIPLOMA INGRESSO SERVPUBLICO', 'DIPLOMAINGRESSOSERVICOPUBLICO', 'DIPLOMAINGRESSOSERVPUBLICO'] },

    // Pensão e Representante
    { key: 'cpfRepresentanteLegal', matchExact: ['CPF REPRESENTANTE LEGAL', 'CPFREPRESENTANTELEGAL'] },
    { key: 'nomeRepresentanteLegal', matchExact: ['NOME REPRESENTANTE LEGAL', 'NOMEREPRESENTANTELEGAL'] },
    { key: 'cpfInstituidorPensao', matchExact: ['CPF INSTITUIDOR PENSAO', 'CPFINSTITUIDORPENSAO'] },
    { key: 'nomeInstituidorPensao', matchExact: ['NOME INSTITUIDOR PENSAO', 'NOMEINSTITUIDORPENSAO'] },
    { key: 'codTipoPensao', matchExact: ['COD TIPO PENSAO', 'CODIGO TIPO PENSAO', 'CODTIPOPENSAO', 'CODIGOTIPOPENSAO'] },
    { key: 'tipoPensao', matchExact: ['TIPO PENSAO', 'TIPOPENSAO'] },
    { key: 'dataInicioPensao', matchExact: ['DATA INICIO PENSAO', 'INICIO PENSAO', 'DATAINICIOPENSAO', 'INICIOPENSAO'] },

    // Aposentadoria
    { key: 'codTipoAposentadoria', matchExact: ['COD TIPO APOSENTADORIA', 'CODIGO TIPO APOSENTADORIA', 'CODTIPOAPOSENTADORIA', 'CODIGOTIPOAPOSENTADORIA'] },
    { key: 'tipoAposentadoria', matchExact: ['TIPO APOSENTADORIA', 'TIPOAPOSENTADORIA'] },
    { key: 'dataAposentadoria', matchExact: ['DATA APOSENTADORIA', 'DATAAPOSENTADORIA'] },

    // Remunerações
    { key: 'remuneracaoBasicaBrutaRs', matchExact: ['REMUNERACAO BASICA BRUTA RS', 'REMUNERACAOBASICABRUTARS'] },
    { key: 'remuneracaoBasicaBrutaUs', matchExact: ['REMUNERACAO BASICA BRUTA US', 'REMUNERACAOBASICABRUTAUS'] },
    { key: 'abateTetoGratificacaoNatalinaRs', matchExact: ['ABATETETO DA GRATIFICACAO NATALINA RS', 'ABATE TETO DA GRATIFICACAO NATALINA RS', 'ABATETETODAGRATIFICACAONATALINARS'] },
    { key: 'abateTetoGratificacaoNatalinaUs', matchExact: ['ABATETETO DA GRATIFICACAO NATALINA US', 'ABATE TETO DA GRATIFICACAO NATALINA US', 'ABATETETODAGRATIFICACAONATALINAUS'] },
    { key: 'abateTetoRs', matchExact: ['ABATETETO RS', 'ABATE TETO RS', 'ABATETETORS'] },
    { key: 'abateTetoUs', matchExact: ['ABATETETO US', 'ABATE TETO US', 'ABATETETOUS'] },
    { key: 'gratificacaoNatalinaRs', matchExact: ['GRATIFICACAO NATALINA RS', 'GRATIFICACAONATALINARS'] },
    { key: 'gratificacaoNatalinaUs', matchExact: ['GRATIFICACAO NATALINA US', 'GRATIFICACAONATALINAUS'] },
    { key: 'feriasRs', matchExact: ['FERIAS RS', 'FERIASRS'] },
    { key: 'feriasUs', matchExact: ['FERIAS US', 'FERIASUS'] },
    { key: 'outrasRemuneracoesEventuaisRs', matchExact: ['OUTRAS REMUNERACOES EVENTUAIS RS', 'OUTRASREMUNERACOESEVENTUAISRS'] },
    { key: 'outrasRemuneracoesEventuaisUs', matchExact: ['OUTRAS REMUNERACOES EVENTUAIS US', 'OUTRASREMUNERACOESEVENTUAISUS'] },
    { key: 'irrfRs', matchExact: ['IRRF RS', 'IRRFRS'] },
    { key: 'irrfUs', matchExact: ['IRRF US', 'IRRFUS'] },
    { key: 'pssRpgsRs', matchExact: ['PSSRPGS RS', 'PSS/RPGS RS', 'PSS RPGS RS', 'PSSRPGSRS'] },
    { key: 'pssRpgsUs', matchExact: ['PSSRPGS US', 'PSS/RPGS US', 'PSS RPGS US', 'PSSRPGSUS'] },
    { key: 'demaisDeducoesRs', matchExact: ['DEMAIS DEDUCOES RS', 'DEMAISDEDUCOESRS'] },
    { key: 'demaisDeducoesUs', matchExact: ['DEMAIS DEDUCOES US', 'DEMAISDEDUCOESUS'] },
    { key: 'pensaoMilitarRs', matchExact: ['PENSAO MILITAR RS', 'PENSAOMILITARRS'] },
    { key: 'pensaoMilitarUs', matchExact: ['PENSAO MILITAR US', 'PENSAOMILITARUS'] },
    { key: 'fundoSaudeRs', matchExact: ['FUNDO DE SAUDE RS', 'FUNDODESAUDERS'] },
    { key: 'fundoSaudeUs', matchExact: ['FUNDO DE SAUDE US', 'FUNDODESAUDEUS'] },
    { key: 'taxaOcupacaoImovelFuncionalRs', matchExact: ['TAXA DE OCUPACAO IMOVEL FUNCIONAL RS', 'TAXADEOCUPACAOIMOVELFUNCIONALRS'] },
    { key: 'taxaOcupacaoImovelFuncionalUs', matchExact: ['TAXA DE OCUPACAO IMOVEL FUNCIONAL US', 'TAXADEOCUPACAOIMOVELFUNCIONALUS'] },
    { key: 'remuneracaoAposDeducoesObrigatoriasRs', matchExact: ['REMUNERACAO APOS DEDUCOES OBRIGATORIAS RS', 'REMUNERACAOAPOSDEDUCOESOBRIGATORIASRS'] },
    { key: 'remuneracaoAposDeducoesObrigatoriasUs', matchExact: ['REMUNERACAO APOS DEDUCOES OBRIGATORIAS US', 'REMUNERACAOAPOSDEDUCOESOBRIGATORIASUS'] },
    { key: 'verbasIndenizatoriasCivilRs', matchExact: ['VERBAS INDENIZATORIAS REGISTRADAS EM SISTEMAS DE PESSOAL CIVIL RS', 'VERBASINDENIZATORIASREGISTRADASEMSISTEMASDEPESSOALCIVILRS'] },
    { key: 'verbasIndenizatoriasCivilUs', matchExact: ['VERBAS INDENIZATORIAS REGISTRADAS EM SISTEMAS DE PESSOAL CIVIL US', 'VERBASINDENIZATORIASREGISTRADASEMSISTEMASDEPESSOALCIVILUS'] },
    { key: 'verbasIndenizatoriasMilitarRs', matchExact: ['VERBAS INDENIZATORIAS REGISTRADAS EM SISTEMAS DE PESSOAL MILITAR RS', 'VERBASINDENIZATORIASREGISTRADASEMSISTEMASDEPESSOALMILITARRS'] },
    { key: 'verbasIndenizatoriasMilitarUs', matchExact: ['VERBAS INDENIZATORIAS REGISTRADAS EM SISTEMAS DE PESSOAL MILITAR US', 'VERBASINDENIZATORIASREGISTRADASEMSISTEMASDEPESSOALMILITARUS'] },
    { key: 'verbasIndenizatoriasPdvRs', matchExact: ['VERBAS INDENIZATORIAS PROGRAMA DESLIGAMENTO VOLUNTARIO MP 7922017 RS', 'VERBAS INDENIZATORIAS PROGRAMA DESLIGAMENTO VOLUNTARIO RS', 'VERBASINDENIZATORIASPROGRAMADESLIGAMENTOVOLUNTARIOMP7922017RS', 'VERBASINDENIZATORIASPROGRAMADESLIGAMENTOVOLUNTARIORS'] },
    { key: 'verbasIndenizatoriasPdvUs', matchExact: ['VERBAS INDENIZATORIAS PROGRAMA DESLIGAMENTO VOLUNTARIO MP 7922017 US', 'VERBAS INDENIZATORIAS PROGRAMA DESLIGAMENTO VOLUNTARIO US', 'VERBASINDENIZATORIASPROGRAMADESLIGAMENTOVOLUNTARIOMP7922017US', 'VERBASINDENIZATORIASPROGRAMADESLIGAMENTOVOLUNTARIOUS'] },
    { key: 'totalVerbasIndenizatoriasRs', matchExact: ['TOTAL DE VERBAS INDENIZATORIAS RS', 'TOTALDEVERBASINDENIZATORIASRS'] },
    { key: 'totalVerbasIndenizatoriasUs', matchExact: ['TOTAL DE VERBAS INDENIZATORIAS US', 'TOTALDEVERBASINDENIZATORIASUS'] },
    { key: 'totalHonorariosJetons', matchExact: ['HONORARIOS JETONS', 'HONORARIOSJETONS'] }
];

// Helper genérico para resolução das chaves
// Helper genérico para resolução das chaves
function resolveHeaderKey(header: string, rules: HeaderRule[]): string {
    if (!header) return '';
    const cleanHeader = normalize(header);

    // Remove os espaços do cabeçalho limpo para ignorar o agrupamento feito pela remoção do _
    const groupedHeader = cleanHeader.replace(/\s+/g, '');

    // Busca apenas por correspondência exata para evitar qualquer colisão
    for (const rule of rules) {
        if (rule.matchExact) {
            const match = rule.matchExact.some(exact => exact.replace(/\s+/g, '') === groupedHeader);
            if (match) return rule.key;
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
                mapHeaders: ({ header }) => resolveHeaderKey(header, SERVIDORES_HEADER_RULES),
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
                            const upperZipName = file.toUpperCase();
                            const upperFileName = entry.fileName.toUpperCase();
                            let rowsCount = 0;

                            if (upperFileName.includes('AFASTAMENTOS')) {
                                rowsCount = await processGenericStream(readStream, ServidoresAfastamentos, 'Afastamentos', metadata);
                            } else if (upperFileName.includes('OBSERVACOES') || upperFileName.includes('OBSERVAÇÕES')) {
                                rowsCount = await processGenericStream(readStream, ServidoresObservacoes, 'Observacoes', metadata);
                            } else if (upperFileName.includes('HONORARIOSADVOCATICIOS') || upperFileName.includes('HONORARIOS_ADVOCATICIOS')) {
                                rowsCount = await processGenericStream(readStream, ServidoresHonorariosAdvocaticios, 'HonorariosAdvocaticios', metadata);
                            } else if (upperFileName.includes('JETONS') || upperFileName.includes('HONORARIOS(JETONS)')) {
                                rowsCount = await processGenericStream(readStream, ServidoresJetons, 'Jetons', metadata);
                            } else if (upperFileName.includes('REMUNERACAO') || upperFileName.includes('REMUNERAÇÃO')) {
                                rowsCount = await processGenericStream(readStream, ServidoresRemuneracao, 'Remuneracao', metadata);
                            } else if (upperFileName.includes('CADASTRO')) {
                                if (upperZipName.includes('PENSIONISTAS')) {
                                    rowsCount = await processGenericStream(readStream, PensionistasCadastro, 'PensionistasCadastro', metadata);
                                } else if (upperZipName.includes('APOSENTADOS') || upperZipName.includes('RESERVA_REFORMA')) {
                                    rowsCount = await processGenericStream(readStream, AposentadosCadastro, 'AposentadosCadastro', metadata);
                                } else {
                                    rowsCount = await processGenericStream(readStream, ServidoresCadastro, 'ServidoresCadastro', metadata);
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