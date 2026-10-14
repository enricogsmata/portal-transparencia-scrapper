import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import csvParser from 'csv-parser';
import { sql } from 'drizzle-orm';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import { 
    DespesasEmpenho, 
    DespesasLiquidacao, 
    DespesasPagamento 
} from '../../../database/bronze_schema.js';
import { ZIP_FILES_DIRECTORY, extractMetadataFromFileName, ReferenceMetadata } from '../shared.js';

const logger = new_logger(import.meta.url);
const BATCH_SIZE = 2000;

async function persistEmpenho(data: (typeof DespesasEmpenho.$inferInsert)[]) {
    const log = logger.forMethod('persistEmpenho');
    try {
        await bronze_db.insert(DespesasEmpenho).values(data).onDuplicateKeyUpdate({
            set: {
                idEmpenho: sql`values(${DespesasEmpenho.idEmpenho})`,
                codigoEmpenho: sql`values(${DespesasEmpenho.codigoEmpenho})`,
                codigoEmpenhoResumido: sql`values(${DespesasEmpenho.codigoEmpenhoResumido})`,
                dataEmissao: sql`values(${DespesasEmpenho.dataEmissao})`,
                codigoTipoDocumento: sql`values(${DespesasEmpenho.codigoTipoDocumento})`,
                tipoDocumento: sql`values(${DespesasEmpenho.tipoDocumento})`,
                tipoEmpenho: sql`values(${DespesasEmpenho.tipoEmpenho})`,
                especieEmpenho: sql`values(${DespesasEmpenho.especieEmpenho})`,
                codigoOrgaoSuperior: sql`values(${DespesasEmpenho.codigoOrgaoSuperior})`,
                orgaoSuperior: sql`values(${DespesasEmpenho.orgaoSuperior})`,
                codigoOrgao: sql`values(${DespesasEmpenho.codigoOrgao})`,
                orgao: sql`values(${DespesasEmpenho.orgao})`,
                codigoUnidadeGestora: sql`values(${DespesasEmpenho.codigoUnidadeGestora})`,
                unidadeGestora: sql`values(${DespesasEmpenho.unidadeGestora})`,
                codigoGestao: sql`values(${DespesasEmpenho.codigoGestao})`,
                gestao: sql`values(${DespesasEmpenho.gestao})`,
                codigoFavorecido: sql`values(${DespesasEmpenho.codigoFavorecido})`,
                favorecido: sql`values(${DespesasEmpenho.favorecido})`,
                observacao: sql`values(${DespesasEmpenho.observacao})`,
                codigoEsferaOrcamentaria: sql`values(${DespesasEmpenho.codigoEsferaOrcamentaria})`,
                esferaOrcamentaria: sql`values(${DespesasEmpenho.esferaOrcamentaria})`,
                codigoTipoCredito: sql`values(${DespesasEmpenho.codigoTipoCredito})`,
                tipoCredito: sql`values(${DespesasEmpenho.tipoCredito})`,
                codigoGrupoFonteRecurso: sql`values(${DespesasEmpenho.codigoGrupoFonteRecurso})`,
                grupoFonteRecurso: sql`values(${DespesasEmpenho.grupoFonteRecurso})`,
                codigoFonteRecurso: sql`values(${DespesasEmpenho.codigoFonteRecurso})`,
                fonteRecurso: sql`values(${DespesasEmpenho.fonteRecurso})`,
                codigoUnidadeOrcamentaria: sql`values(${DespesasEmpenho.codigoUnidadeOrcamentaria})`,
                unidadeOrcamentaria: sql`values(${DespesasEmpenho.unidadeOrcamentaria})`,
                codigoFuncao: sql`values(${DespesasEmpenho.codigoFuncao})`,
                funcao: sql`values(${DespesasEmpenho.funcao})`,
                codigoSubfuncao: sql`values(${DespesasEmpenho.codigoSubfuncao})`,
                subfuncao: sql`values(${DespesasEmpenho.subfuncao})`,
                codigoPrograma: sql`values(${DespesasEmpenho.codigoPrograma})`,
                programa: sql`values(${DespesasEmpenho.programa})`,
                codigoAcao: sql`values(${DespesasEmpenho.codigoAcao})`,
                acao: sql`values(${DespesasEmpenho.acao})`,
                linguagemCidada: sql`values(${DespesasEmpenho.linguagemCidada})`,
                codigoSubtituloLocalizador: sql`values(${DespesasEmpenho.codigoSubtituloLocalizador})`,
                subtituloLocalizador: sql`values(${DespesasEmpenho.subtituloLocalizador})`,
                codigoPlanoOrcamentario: sql`values(${DespesasEmpenho.codigoPlanoOrcamentario})`,
                planoOrcamentario: sql`values(${DespesasEmpenho.planoOrcamentario})`,
                codigoProgramaGoverno: sql`values(${DespesasEmpenho.codigoProgramaGoverno})`,
                nomeProgramaGoverno: sql`values(${DespesasEmpenho.nomeProgramaGoverno})`,
                autorEmenda: sql`values(${DespesasEmpenho.autorEmenda})`,
                codigoCategoriaDespesa: sql`values(${DespesasEmpenho.codigoCategoriaDespesa})`,
                categoriaDespesa: sql`values(${DespesasEmpenho.categoriaDespesa})`,
                codigoGrupoDespesa: sql`values(${DespesasEmpenho.codigoGrupoDespesa})`,
                grupoDespesa: sql`values(${DespesasEmpenho.grupoDespesa})`,
                codigoModalidadeAplicacao: sql`values(${DespesasEmpenho.codigoModalidadeAplicacao})`,
                modalidadeAplicacao: sql`values(${DespesasEmpenho.modalidadeAplicacao})`,
                codigoElementoDespesa: sql`values(${DespesasEmpenho.codigoElementoDespesa})`,
                elementoDespesa: sql`values(${DespesasEmpenho.elementoDespesa})`,
                processo: sql`values(${DespesasEmpenho.processo})`,
                modalidadeLicitacao: sql`values(${DespesasEmpenho.modalidadeLicitacao})`,
                inciso: sql`values(${DespesasEmpenho.inciso})`,
                amparo: sql`values(${DespesasEmpenho.amparo})`,
                referenciaDispensaInexigibilidade: sql`values(${DespesasEmpenho.referenciaDispensaInexigibilidade})`,
                codigoConvenio: sql`values(${DespesasEmpenho.codigoConvenio})`,
                contratoRepasseTermoParceriaOutros: sql`values(${DespesasEmpenho.contratoRepasseTermoParceriaOutros})`,
                valorOriginalEmpenho: sql`values(${DespesasEmpenho.valorOriginalEmpenho})`,
                valorEmpenhoConvertido: sql`values(${DespesasEmpenho.valorEmpenhoConvertido})`,
                valorUtilizadoConversao: sql`values(${DespesasEmpenho.valorUtilizadoConversao})`,
                exercicioReferencia: sql`values(${DespesasEmpenho.exercicioReferencia})`,
                mesReferencia: sql`values(${DespesasEmpenho.mesReferencia})`,
                diaReferencia: sql`values(${DespesasEmpenho.diaReferencia})`,
                dadosAdicionais: sql`values(${DespesasEmpenho.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Despesas Empenho`);
        throw error;
    }
}

async function persistLiquidacao(data: (typeof DespesasLiquidacao.$inferInsert)[]) {
    const log = logger.forMethod('persistLiquidacao');
    try {
        await bronze_db.insert(DespesasLiquidacao).values(data).onDuplicateKeyUpdate({
            set: {
                codigoLiquidacao: sql`values(${DespesasLiquidacao.codigoLiquidacao})`,
                codigoLiquidacaoResumido: sql`values(${DespesasLiquidacao.codigoLiquidacaoResumido})`,
                dataEmissao: sql`values(${DespesasLiquidacao.dataEmissao})`,
                codigoTipoDocumento: sql`values(${DespesasLiquidacao.codigoTipoDocumento})`,
                tipoDocumento: sql`values(${DespesasLiquidacao.tipoDocumento})`,
                codigoOrgaoSuperior: sql`values(${DespesasLiquidacao.codigoOrgaoSuperior})`,
                orgaoSuperior: sql`values(${DespesasLiquidacao.orgaoSuperior})`,
                codigoOrgao: sql`values(${DespesasLiquidacao.codigoOrgao})`,
                orgao: sql`values(${DespesasLiquidacao.orgao})`,
                codigoUnidadeGestora: sql`values(${DespesasLiquidacao.codigoUnidadeGestora})`,
                unidadeGestora: sql`values(${DespesasLiquidacao.unidadeGestora})`,
                codigoGestao: sql`values(${DespesasLiquidacao.codigoGestao})`,
                gestao: sql`values(${DespesasLiquidacao.gestao})`,
                codigoFavorecido: sql`values(${DespesasLiquidacao.codigoFavorecido})`,
                favorecido: sql`values(${DespesasLiquidacao.favorecido})`,
                observacao: sql`values(${DespesasLiquidacao.observacao})`,
                codigoCategoriaDespesa: sql`values(${DespesasLiquidacao.codigoCategoriaDespesa})`,
                categoriaDespesa: sql`values(${DespesasLiquidacao.categoriaDespesa})`,
                codigoGrupoDespesa: sql`values(${DespesasLiquidacao.codigoGrupoDespesa})`,
                grupoDespesa: sql`values(${DespesasLiquidacao.grupoDespesa})`,
                codigoModalidadeAplicacao: sql`values(${DespesasLiquidacao.codigoModalidadeAplicacao})`,
                modalidadeAplicacao: sql`values(${DespesasLiquidacao.modalidadeAplicacao})`,
                codigoElementoDespesa: sql`values(${DespesasLiquidacao.codigoElementoDespesa})`,
                elementoDespesa: sql`values(${DespesasLiquidacao.elementoDespesa})`,
                codigoPlanoOrcamentario: sql`values(${DespesasLiquidacao.codigoPlanoOrcamentario})`,
                planoOrcamentario: sql`values(${DespesasLiquidacao.planoOrcamentario})`,
                codigoProgramaGoverno: sql`values(${DespesasLiquidacao.codigoProgramaGoverno})`,
                nomeProgramaGoverno: sql`values(${DespesasLiquidacao.nomeProgramaGoverno})`,
                exercicioReferencia: sql`values(${DespesasLiquidacao.exercicioReferencia})`,
                mesReferencia: sql`values(${DespesasLiquidacao.mesReferencia})`,
                diaReferencia: sql`values(${DespesasLiquidacao.diaReferencia})`,
                dadosAdicionais: sql`values(${DespesasLiquidacao.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Despesas Liquidação`);
        throw error;
    }
}

async function persistPagamento(data: (typeof DespesasPagamento.$inferInsert)[]) {
    const log = logger.forMethod('persistPagamento');
    try {
        await bronze_db.insert(DespesasPagamento).values(data).onDuplicateKeyUpdate({
            set: {
                codigoPagamento: sql`values(${DespesasPagamento.codigoPagamento})`,
                codigoPagamentoResumido: sql`values(${DespesasPagamento.codigoPagamentoResumido})`,
                dataEmissao: sql`values(${DespesasPagamento.dataEmissao})`,
                codigoTipoDocumento: sql`values(${DespesasPagamento.codigoTipoDocumento})`,
                tipoDocumento: sql`values(${DespesasPagamento.tipoDocumento})`,
                tipoOb: sql`values(${DespesasPagamento.tipoOb})`,
                extraorcamentario: sql`values(${DespesasPagamento.extraorcamentario})`,
                codigoOrgaoSuperior: sql`values(${DespesasPagamento.codigoOrgaoSuperior})`,
                orgaoSuperior: sql`values(${DespesasPagamento.orgaoSuperior})`,
                codigoOrgao: sql`values(${DespesasPagamento.codigoOrgao})`,
                orgao: sql`values(${DespesasPagamento.orgao})`,
                codigoUnidadeGestora: sql`values(${DespesasPagamento.codigoUnidadeGestora})`,
                unidadeGestora: sql`values(${DespesasPagamento.unidadeGestora})`,
                codigoGestao: sql`values(${DespesasPagamento.codigoGestao})`,
                gestao: sql`values(${DespesasPagamento.gestao})`,
                codigoFavorecido: sql`values(${DespesasPagamento.codigoFavorecido})`,
                favorecido: sql`values(${DespesasPagamento.favorecido})`,
                observacao: sql`values(${DespesasPagamento.observacao})`,
                processo: sql`values(${DespesasPagamento.processo})`,
                codigoCategoriaDespesa: sql`values(${DespesasPagamento.codigoCategoriaDespesa})`,
                categoriaDespesa: sql`values(${DespesasPagamento.categoriaDespesa})`,
                codigoGrupoDespesa: sql`values(${DespesasPagamento.codigoGrupoDespesa})`,
                grupoDespesa: sql`values(${DespesasPagamento.grupoDespesa})`,
                codigoModalidadeAplicacao: sql`values(${DespesasPagamento.codigoModalidadeAplicacao})`,
                modalidadeAplicacao: sql`values(${DespesasPagamento.modalidadeAplicacao})`,
                codigoElementoDespesa: sql`values(${DespesasPagamento.codigoElementoDespesa})`,
                elementoDespesa: sql`values(${DespesasPagamento.elementoDespesa})`,
                codigoPlanoOrcamentario: sql`values(${DespesasPagamento.codigoPlanoOrcamentario})`,
                planoOrcamentario: sql`values(${DespesasPagamento.planoOrcamentario})`,
                codigoProgramaGoverno: sql`values(${DespesasPagamento.codigoProgramaGoverno})`,
                nomeProgramaGoverno: sql`values(${DespesasPagamento.nomeProgramaGoverno})`,
                valorOriginalPagamento: sql`values(${DespesasPagamento.valorOriginalPagamento})`,
                valorPagamentoConvertido: sql`values(${DespesasPagamento.valorPagamentoConvertido})`,
                valorUtilizadoConversao: sql`values(${DespesasPagamento.valorUtilizadoConversao})`,
                exercicioReferencia: sql`values(${DespesasPagamento.exercicioReferencia})`,
                mesReferencia: sql`values(${DespesasPagamento.mesReferencia})`,
                diaReferencia: sql`values(${DespesasPagamento.diaReferencia})`,
                dadosAdicionais: sql`values(${DespesasPagamento.dadosAdicionais})`,
            },
        });
    } catch (error: any) {
        log.fatal({ data: error }, `Falha na persistência de Despesas Pagamento`);
        throw error;
    }
}

function mapCommonHeaders(h: string): string | null {
    if (h === 'DATA EMISSÃO' || h === 'DATA EMISSAO') return 'dataEmissao';
    if (h === 'CÓDIGO TIPO DOCUMENTO' || h === 'CODIGO TIPO DOCUMENTO') return 'codigoTipoDocumento';
    if (h === 'TIPO DOCUMENTO') return 'tipoDocumento';
    if (h === 'CÓDIGO ÓRGÃO SUPERIOR' || h === 'CODIGO ORGAO SUPERIOR') return 'codigoOrgaoSuperior';
    if (h === 'ÓRGÃO SUPERIOR' || h === 'ORGAO SUPERIOR') return 'orgaoSuperior';
    if (h === 'CÓDIGO ÓRGÃO' || h === 'CODIGO ORGAO') return 'codigoOrgao';
    if (h === 'ÓRGÃO' || h === 'ORGAO') return 'orgao';
    if (h === 'CÓDIGO UNIDADE GESTORA' || h === 'CODIGO UNIDADE GESTORA') return 'codigoUnidadeGestora';
    if (h === 'UNIDADE GESTORA') return 'unidadeGestora';
    if (h === 'CÓDIGO GESTÃO' || h === 'CODIGO GESTAO') return 'codigoGestao';
    if (h === 'GESTÃO' || h === 'GESTAO') return 'gestao';
    if (h === 'CÓDIGO FAVORECIDO' || h === 'CODIGO FAVORECIDO') return 'codigoFavorecido';
    if (h === 'FAVORECIDO') return 'favorecido';
    if (h === 'OBSERVAÇÃO' || h === 'OBSERVACAO') return 'observacao';
    if (h === 'CÓDIGO CATEGORIA DE DESPESA' || h === 'CODIGO CATEGORIA DE DESPESA') return 'codigoCategoriaDespesa';
    if (h === 'CATEGORIA DE DESPESA') return 'categoriaDespesa';
    if (h === 'CÓDIGO GRUPO DE DESPESA' || h === 'CODIGO GRUPO DE DESPESA') return 'codigoGrupoDespesa';
    if (h === 'GRUPO DE DESPESA') return 'grupoDespesa';
    if (h === 'CÓDIGO MODALIDADE DE APLICAÇÃO' || h === 'CODIGO MODALIDADE DE APLICACAO') return 'codigoModalidadeAplicacao';
    if (h === 'MODALIDADE DE APLICAÇÃO' || h === 'MODALIDADE DE APLICACAO') return 'modalidadeAplicacao';
    if (h === 'CÓDIGO ELEMENTO DE DESPESA' || h === 'CODIGO ELEMENTO DE DESPESA') return 'codigoElementoDespesa';
    if (h === 'ELEMENTO DE DESPESA') return 'elementoDespesa';
    if (h === 'CÓDIGO PLANO ORÇAMENTÁRIO' || h === 'CODIGO PLANO ORCAMENTARIO') return 'codigoPlanoOrcamentario';
    if (h === 'PLANO ORÇAMENTÁRIO' || h === 'PLANO ORCAMENTARIO') return 'planoOrcamentario';
    if (h === 'CÓDIGO PROGRAMA GOVERNO' || h === 'CODIGO PROGRAMA GOVERNO') return 'codigoProgramaGoverno';
    if (h === 'NOME PROGRAMA GOVERNO') return 'nomeProgramaGoverno';
    if (h === 'PROCESSO') return 'processo';
    if (h === 'VALOR UTILIZADO NA CONVERSÃO' || h === 'VALOR UTILIZADO NA CONVERSAO') return 'valorUtilizadoConversao';
    return null;
}

async function processEmpenhoStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processEmpenhoStream');
    let batch: (typeof DespesasEmpenho.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase().trim();
                const common = mapCommonHeaders(h);
                if (common) return common;
                
                if (h === 'ID EMPENHO') return 'idEmpenho';
                if (h === 'CÓDIGO EMPENHO' || h === 'CODIGO EMPENHO') return 'codigoEmpenho';
                if (h === 'CÓDIGO EMPENHO RESUMIDO' || h === 'CODIGO EMPENHO RESUMIDO') return 'codigoEmpenhoResumido';
                if (h === 'TIPO EMPENHO') return 'tipoEmpenho';
                if (h === 'ESPÉCIE EMPENHO' || h === 'ESPECIE EMPENHO') return 'especieEmpenho';
                if (h === 'CÓDIGO ESFERA ORÇAMENTÁRIA' || h === 'CODIGO ESFERA ORCAMENTARIA') return 'codigoEsferaOrcamentaria';
                if (h === 'ESFERA ORÇAMENTÁRIA' || h === 'ESFERA ORCAMENTARIA') return 'esferaOrcamentaria';
                if (h === 'CÓDIGO TIPO CRÉDITO' || h === 'CODIGO TIPO CREDITO') return 'codigoTipoCredito';
                if (h === 'TIPO CRÉDITO' || h === 'TIPO CREDITO') return 'tipoCredito';
                if (h === 'CÓDIGO GRUPO FONTE RECURSO' || h === 'CODIGO GRUPO FONTE RECURSO') return 'codigoGrupoFonteRecurso';
                if (h === 'GRUPO FONTE RECURSO') return 'grupoFonteRecurso';
                if (h === 'CÓDIGO FONTE RECURSO' || h === 'CODIGO FONTE RECURSO') return 'codigoFonteRecurso';
                if (h === 'FONTE RECURSO') return 'fonteRecurso';
                if (h === 'CÓDIGO UNIDADE ORÇAMENTÁRIA' || h === 'CODIGO UNIDADE ORCAMENTARIA') return 'codigoUnidadeOrcamentaria';
                if (h === 'UNIDADE ORÇAMENTÁRIA' || h === 'UNIDADE ORCAMENTARIA') return 'unidadeOrcamentaria';
                if (h === 'CÓDIGO FUNÇÃO' || h === 'CODIGO FUNCAO') return 'codigoFuncao';
                if (h === 'FUNÇÃO' || h === 'FUNCAO') return 'funcao';
                if (h === 'CÓDIGO SUBFUNÇÃO' || h === 'CODIGO SUBFUNCAO') return 'codigoSubfuncao';
                if (h === 'SUBFUNÇÃO' || h === 'SUBFUNCAO') return 'subfuncao';
                if (h === 'CÓDIGO PROGRAMA' || h === 'CODIGO PROGRAMA') return 'codigoPrograma';
                if (h === 'PROGRAMA') return 'programa';
                if (h === 'CÓDIGO AÇÃO' || h === 'CODIGO ACAO') return 'codigoAcao';
                if (h === 'AÇÃO' || h === 'ACAO') return 'acao';
                if (h === 'LINGUAGEM CIDADÃ' || h === 'LINGUAGEM CIDADA') return 'linguagemCidada';
                if (h === 'CÓDIGO SUBTÍTULO (LOCALIZADOR)' || h === 'CODIGO SUBTITULO (LOCALIZADOR)') return 'codigoSubtituloLocalizador';
                if (h === 'SUBTÍTULO (LOCALIZADOR)' || h === 'SUBTITULO (LOCALIZADOR)') return 'subtituloLocalizador';
                if (h === 'AUTOR EMENDA') return 'autorEmenda';
                if (h === 'MODALIDADE DE LICITAÇÃO' || h === 'MODALIDADE DE LICITACAO') return 'modalidadeLicitacao';
                if (h === 'INCISO') return 'inciso';
                if (h === 'AMPARO') return 'amparo';
                if (h === 'REFERÊNCIA DE DISPENSA OU INEXIGIBILIDADE' || h === 'REFERENCIA DE DISPENSA OU INEXIGIBILIDADE') return 'referenciaDispensaInexigibilidade';
                if (h === 'CÓDIGO CONVÊNIO' || h === 'CODIGO CONVENIO') return 'codigoConvenio';
                if (h === 'CONTRATO DE REPASSE / TERMO DE PARCERIA / OUTROS') return 'contratoRepasseTermoParceriaOutros';
                if (h === 'VALOR ORIGINAL DO EMPENHO') return 'valorOriginalEmpenho';
                if (h === 'VALOR DO EMPENHO CONVERTIDO PRA R$' || h === 'VALOR DO EMPENHO CONVERTIDO PARA R$') return 'valorEmpenhoConvertido';
                
                return h;
            },
        }));

        for await (const data of parser) {
            batch.push({
                ...data,
                ...metadata,
            });
            if (batch.length >= BATCH_SIZE) {
                await persistEmpenho(batch);
                totalProcessed += batch.length;
                batch = [];
            }
        }
        if (batch.length > 0) {
            await persistEmpenho(batch);
            totalProcessed += batch.length;
        }

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento do arquivo Despesas Empenho concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream do arquivo Despesas Empenho');
        throw error;
    }
}

async function processLiquidacaoStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processLiquidacaoStream');
    let batch: (typeof DespesasLiquidacao.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase().trim();
                const common = mapCommonHeaders(h);
                if (common) return common;
                
                if (h === 'CÓDIGO LIQUIDAÇÃO' || h === 'CODIGO LIQUIDACAO') return 'codigoLiquidacao';
                if (h === 'CÓDIGO LIQUIDAÇÃO RESUMIDO' || h === 'CODIGO LIQUIDACAO RESUMIDO') return 'codigoLiquidacaoResumido';
                
                return h;
            },
        }));

        for await (const data of parser) {
            batch.push({
                ...data,
                ...metadata,
            });
            if (batch.length >= BATCH_SIZE) {
                await persistLiquidacao(batch);
                totalProcessed += batch.length;
                batch = [];
            }
        }
        if (batch.length > 0) {
            await persistLiquidacao(batch);
            totalProcessed += batch.length;
        }

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento do arquivo Despesas Liquidação concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream do arquivo Despesas Liquidação');
        throw error;
    }
}

async function processPagamentoStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processPagamentoStream');
    let batch: (typeof DespesasPagamento.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream.pipe(csvParser({
            separator: ';',
            mapHeaders: ({ header }) => {
                const h = header.toUpperCase().trim();
                const common = mapCommonHeaders(h);
                if (common) return common;
                
                if (h === 'CÓDIGO PAGAMENTO' || h === 'CODIGO PAGAMENTO') return 'codigoPagamento';
                if (h === 'CÓDIGO PAGAMENTO RESUMIDO' || h === 'CODIGO PAGAMENTO RESUMIDO') return 'codigoPagamentoResumido';
                if (h === 'TIPO OB') return 'tipoOb';
                if (h === 'EXTRAORÇAMENTÁRIO' || h === 'EXTRAORCAMENTARIO') return 'extraorcamentario';
                if (h === 'VALOR ORIGINAL DO PAGAMENTO') return 'valorOriginalPagamento';
                if (h === 'VALOR DO PAGAMENTO CONVERTIDO PRA R$' || h === 'VALOR DO PAGAMENTO CONVERTIDO PARA R$') return 'valorPagamentoConvertido';

                return h;
            },
        }));

        for await (const data of parser) {
            batch.push({
                ...data,
                ...metadata,
            });
            if (batch.length >= BATCH_SIZE) {
                await persistPagamento(batch);
                totalProcessed += batch.length;
                batch = [];
            }
        }
        if (batch.length > 0) {
            await persistPagamento(batch);
            totalProcessed += batch.length;
        }

        log.info({ data: { rowsProcessed: totalProcessed } }, 'Processamento do arquivo Despesas Pagamento concluído');
        return totalProcessed;
    } catch (error) {
        log.error({ data: error }, 'Erro ao processar stream do arquivo Despesas Pagamento');
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
                            
                            if (upperName.includes('_EMPENHO.CSV') && !upperName.includes('ITEM') && !upperName.includes('IMPACTADOS')) {
                                rowsCount = await processEmpenhoStream(readStream, metadata);
                            } else if (upperName.includes('_LIQUIDACAO.CSV') && !upperName.includes('IMPACTADOS')) {
                                rowsCount = await processLiquidacaoStream(readStream, metadata);
                            } else if (upperName.includes('_PAGAMENTO.CSV') && !upperName.includes('IMPACTADOS') && !upperName.includes('FAVORECIDOS') && !upperName.includes('LISTA')) {
                                rowsCount = await processPagamentoStream(readStream, metadata);
                            } else {
                                log.info({ data: { fileName: entry.fileName } }, 'Arquivo CSV ignorado por não ser o arquivo primário da etapa processada nesta sprint');
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

export async function runDespesasIngestion() {
    const log = logger.forMethod('runDespesasIngestion');
    const moduleName = 'despesas';

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