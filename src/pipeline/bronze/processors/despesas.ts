import fs from 'node:fs';
import path from 'node:path';
import yauzl from 'yauzl';
import iconv from 'iconv-lite';
import csvParser from 'csv-parser';
import { normalize } from '../shared.js';
import { sql } from 'drizzle-orm';
import { new_logger } from '../../../services/logs.js';
import { bronze_db } from '../../../database/bronze_connection.js';
import {
    DespesasEmpenho,
    DespesasLiquidacao,
    DespesasPagamento,
    DespesasItemEmpenho,
    DespesasItemEmpenhoHistorico,
    DespesasLiquidacaoEmpenhosImpactados,
    DespesasPagamentoEmpenhosImpactados,
    DespesasPagamentoFavorecidosFinais,
    DespesasPagamentoListaBancos,
    DespesasPagamentoListaFaturas,
    DespesasPagamentoListaPrecatorios
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

async function persistItemEmpenho(data: (typeof DespesasItemEmpenho.$inferInsert)[]) {
    try {
        await bronze_db.insert(DespesasItemEmpenho).values(data);
    } catch (error) {
        logger.forMethod('persistItemEmpenho').fatal({ data: error }, 'Falha na persistência de Item Empenho');
        throw error;
    }
}

async function persistItemEmpenhoHistorico(data: (typeof DespesasItemEmpenhoHistorico.$inferInsert)[]) {
    try {
        await bronze_db.insert(DespesasItemEmpenhoHistorico).values(data);
    } catch (error) {
        logger.forMethod('persistItemEmpenhoHistorico').fatal({ data: error }, 'Falha na persistência de Item Empenho Historico');
        throw error;
    }
}

async function persistLiquidacaoEmpenhosImpactados(data: (typeof DespesasLiquidacaoEmpenhosImpactados.$inferInsert)[]) {
    try {
        await bronze_db.insert(DespesasLiquidacaoEmpenhosImpactados).values(data);
    } catch (error) {
        logger.forMethod('persistLiquidacaoEmpenhosImpactados').fatal({ data: error }, 'Falha na persistência de Liquidacao Empenhos Impactados');
        throw error;
    }
}

async function persistPagamentoEmpenhosImpactados(data: (typeof DespesasPagamentoEmpenhosImpactados.$inferInsert)[]) {
    try {
        await bronze_db.insert(DespesasPagamentoEmpenhosImpactados).values(data);
    } catch (error) {
        logger.forMethod('persistPagamentoEmpenhosImpactados').fatal({ data: error }, 'Falha na persistência de Pagamento Empenhos Impactados');
        throw error;
    }
}

async function persistPagamentoFavorecidosFinais(data: (typeof DespesasPagamentoFavorecidosFinais.$inferInsert)[]) {
    try {
        await bronze_db.insert(DespesasPagamentoFavorecidosFinais).values(data);
    } catch (error) {
        logger.forMethod('persistPagamentoFavorecidosFinais').fatal({ data: error }, 'Falha na persistência de Pagamento Favorecidos Finais');
        throw error;
    }
}

async function persistPagamentoListaBancos(data: (typeof DespesasPagamentoListaBancos.$inferInsert)[]) {
    try {
        await bronze_db.insert(DespesasPagamentoListaBancos).values(data);
    } catch (error) {
        logger.forMethod('persistPagamentoListaBancos').fatal({ data: error }, 'Falha na persistência de Pagamento Lista Bancos');
        throw error;
    }
}

async function persistPagamentoListaFaturas(data: (typeof DespesasPagamentoListaFaturas.$inferInsert)[]) {
    try {
        await bronze_db.insert(DespesasPagamentoListaFaturas).values(data);
    } catch (error) {
        logger.forMethod('persistPagamentoListaFaturas').fatal({ data: error }, 'Falha na persistência de Pagamento Lista Faturas');
        throw error;
    }
}

async function persistPagamentoListaPrecatorios(data: (typeof DespesasPagamentoListaPrecatorios.$inferInsert)[]) {
    try {
        await bronze_db.insert(DespesasPagamentoListaPrecatorios).values(data);
    } catch (error) {
        logger.forMethod('persistPagamentoListaPrecatorios').fatal({ data: error }, 'Falha na persistência de Pagamento Lista Precatorios');
        throw error;
    }
}

const ITEM_EMPENHO_HEADER_RULES = [
    { key: 'idEmpenho', matchExact: ['ID EMPENHO'] },
    { key: 'codigoEmpenho', keywords: ['EMPENHO'], require: ['CODIGO'] },
    { key: 'codigoCategoriaDespesa', keywords: ['CATEGORIA DE DESPESA'], require: ['CODIGO'] },
    { key: 'categoriaDespesa', keywords: ['CATEGORIA DE DESPESA'] },
    { key: 'codigoGrupoDespesa', keywords: ['GRUPO DE DESPESA'], require: ['CODIGO'] },
    { key: 'grupoDespesa', keywords: ['GRUPO DE DESPESA'] },
    { key: 'codigoModalidadeAplicacao', keywords: ['MODALIDADE DE APLICACAO'], require: ['CODIGO'] },
    { key: 'modalidadeAplicacao', keywords: ['MODALIDADE DE APLICACAO'] },
    { key: 'codigoElementoDespesa', keywords: ['ELEMENTO DE DESPESA'], require: ['CODIGO'] },
    { key: 'elementoDespesa', keywords: ['ELEMENTO DE DESPESA'] },
    { key: 'codigoSubElementoDespesa', keywords: ['SUBELEMENTO DE DESPESA', 'SUB ELEMENTO'], require: ['CODIGO'] },
    { key: 'subElementoDespesa', keywords: ['SUBELEMENTO DE DESPESA', 'SUB ELEMENTO'] },
    { key: 'descricao', matchExact: ['DESCRICAO'] },
    { key: 'quantidade', matchExact: ['QUANTIDADE'] },
    { key: 'valorUnitario', keywords: ['VALOR UNITARIO'] },
    { key: 'valorTotal', keywords: ['VALOR TOTAL'] },
    { key: 'sequencial', matchExact: ['SEQUENCIAL'] },
    { key: 'valorAtual', keywords: ['VALOR ATUAL'] },
];

const ITEM_EMPENHO_HISTORICO_HEADER_RULES = [
    { key: 'idEmpenho', matchExact: ['ID EMPENHO'] },
    { key: 'codigoEmpenho', keywords: ['EMPENHO'], require: ['CODIGO'] },
    { key: 'sequencial', matchExact: ['SEQUENCIAL'] },
    { key: 'tipoOperacao', keywords: ['TIPO OPERACAO'] },
    { key: 'dataOperacao', keywords: ['DATA OPERACAO'] },
    { key: 'quantidadeItem', keywords: ['QUANTIDADE ITEM'] },
    { key: 'valorUnitarioItem', keywords: ['VALOR UNITARIO ITEM'] },
    { key: 'valorTotalItem', keywords: ['VALOR TOTAL ITEM'] },
];

const IMPACTADOS_HEADER_RULES = [
    { key: 'codigoLiquidacao', keywords: ['LIQUIDACAO'], require: ['CODIGO'] },
    { key: 'codigoPagamento', keywords: ['PAGAMENTO'], require: ['CODIGO'] },
    { key: 'codigoEmpenho', keywords: ['EMPENHO'], require: ['CODIGO'] },
    { key: 'codigoNaturezaDespesaCompleta', keywords: ['NATUREZA DESPESA COMPLETA'] },
    { key: 'subitem', matchExact: ['SUBITEM'] },
    { key: 'valorLiquidado', keywords: ['VALOR LIQUIDADO'] },
    { key: 'valorPago', keywords: ['VALOR PAGO'] },
    { key: 'valorRestosAPagarInscritos', keywords: ['RESTOS A PAGAR INSCRITOS'] },
    { key: 'valorRestosAPagarCancelado', keywords: ['RESTOS A PAGAR CANCELADO'] },
    { key: 'valorRestosAPagarPagos', keywords: ['RESTOS A PAGAR PAGOS'] },
];

const FAVORECIDOS_FINAIS_HEADER_RULES = [
    { key: 'codigoPagamento', keywords: ['PAGAMENTO'], require: ['CODIGO'] },
    { key: 'codigoLista', keywords: ['LISTA'], require: ['CODIGO'] },
    { key: 'dataEmissao', keywords: ['DATA EMISSAO'] },
    { key: 'codigoFavorecido', keywords: ['FAVORECIDO'], require: ['CODIGO'] },
    { key: 'favorecido', matchExact: ['FAVORECIDO'] },
    { key: 'valorPagamento', keywords: ['VALOR DO PAGAMENTO'] },
];

const LISTA_BANCOS_HEADER_RULES = [
    { key: 'codigoPagamento', keywords: ['PAGAMENTO'], require: ['CODIGO'] },
    { key: 'codigoLista', keywords: ['LISTA'], require: ['CODIGO'] },
    { key: 'dataEmissao', keywords: ['DATA EMISSAO'] },
    { key: 'codigoBanco', keywords: ['BANCO'], require: ['CODIGO'] },
    { key: 'nomeBanco', keywords: ['NOME BANCO'] },
    { key: 'agencia', matchExact: ['AGENCIA'] },
    { key: 'valor', matchExact: ['VALOR EM R$'] },
];

const LISTA_FATURAS_HEADER_RULES = [
    { key: 'codigoPagamento', keywords: ['PAGAMENTO'], require: ['CODIGO'] },
    { key: 'codigoLista', keywords: ['LISTA'], require: ['CODIGO'] },
    { key: 'dataEmissao', keywords: ['DATA EMISSAO'] },
    { key: 'codigoSequencia', keywords: ['SEQUENCIA'], require: ['CODIGO'] },
    { key: 'valorLancamento', keywords: ['VALOR LANCAMENTO'] },
    { key: 'valorDesconto', keywords: ['VALOR DESCONTO'] },
    { key: 'valorJuros', keywords: ['VALOR JUROS'] },
    { key: 'valorDeducao', keywords: ['VALOR DEDUCAO'] },
    { key: 'valorAcrescimo', keywords: ['VALOR ACRESCIMO'] },
];

const LISTA_PRECATORIOS_HEADER_RULES = [
    { key: 'codigoPagamento', keywords: ['PAGAMENTO'], require: ['CODIGO'] },
    { key: 'codigoLista', keywords: ['LISTA'], require: ['CODIGO'] },
    { key: 'dataEmissao', keywords: ['DATA EMISSAO'] },
    { key: 'numeroParcela', keywords: ['NUMERO PARCELA'] },
    { key: 'valorPrecatorio', keywords: ['VALOR PRECATORIO'] },
];

async function createProcessStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata, rules: any, persistFn: any, logName: string): Promise<number> {
    let batch: any[] = [];
    let totalProcessed = 0;
    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => resolveDespesasHeaderKey(header, rules),
            }));

        for await (const data of parser) {
            batch.push({ ...data, ...metadata });
            if (batch.length >= BATCH_SIZE) {
                await persistFn(batch);
                totalProcessed += batch.length;
                batch = [];
            }
        }
        if (batch.length > 0) {
            await persistFn(batch);
            totalProcessed += batch.length;
        }
        logger.forMethod(logName).info({ data: { rowsProcessed: totalProcessed } }, `Processamento de ${logName} concluído`);
        return totalProcessed;
    } catch (error) {
        logger.forMethod(logName).error({ data: error }, `Erro ao processar stream de ${logName}`);
        throw error;
    }
}

const COMMON_HEADER_RULES = [
    { key: 'dataEmissao', keywords: ['DATA EMISSAO', 'DATA DE EMISSAO'] },
    { key: 'codigoTipoDocumento', keywords: ['TIPO DOCUMENTO'], require: ['CODIGO'] },
    { key: 'tipoDocumento', matchExact: ['TIPO DOCUMENTO'] },

    // Hierarquia Órgão e UG (Ordem importa)
    { key: 'codigoOrgaoSuperior', keywords: ['ORGAO SUPERIOR'], require: ['CODIGO'] },
    { key: 'orgaoSuperior', keywords: ['ORGAO SUPERIOR'] },
    { key: 'codigoOrgao', keywords: ['CODIGO ORGAO', 'CODIGO DO ORGAO'] },
    { key: 'orgao', matchExact: ['ORGAO'] },

    { key: 'codigoUnidadeGestora', keywords: ['UNIDADE GESTORA'], require: ['CODIGO'] },
    { key: 'unidadeGestora', keywords: ['UNIDADE GESTORA'] },

    { key: 'codigoGestao', keywords: ['GESTAO'], require: ['CODIGO'] },
    { key: 'gestao', matchExact: ['GESTAO'] },

    { key: 'codigoFavorecido', keywords: ['FAVORECIDO'], require: ['CODIGO'] },
    { key: 'favorecido', matchExact: ['FAVORECIDO', 'NOME FAVORECIDO'] },

    { key: 'observacao', keywords: ['OBSERVACAO'] },

    // Classificação Orçamentária
    { key: 'codigoCategoriaDespesa', keywords: ['CATEGORIA DE DESPESA', 'CATEGORIA DESPESA'], require: ['CODIGO'] },
    { key: 'categoriaDespesa', keywords: ['CATEGORIA DE DESPESA', 'CATEGORIA DESPESA'] },
    { key: 'codigoGrupoDespesa', keywords: ['GRUPO DE DESPESA', 'GRUPO DESPESA'], require: ['CODIGO'] },
    { key: 'grupoDespesa', keywords: ['GRUPO DE DESPESA', 'GRUPO DESPESA'] },
    { key: 'codigoModalidadeAplicacao', keywords: ['MODALIDADE DE APLICACAO', 'MODALIDADE APLICACAO'], require: ['CODIGO'] },
    { key: 'modalidadeAplicacao', keywords: ['MODALIDADE DE APLICACAO', 'MODALIDADE APLICACAO'] },
    { key: 'codigoElementoDespesa', keywords: ['ELEMENTO DE DESPESA', 'ELEMENTO DESPESA'], require: ['CODIGO'] },
    { key: 'elementoDespesa', keywords: ['ELEMENTO DE DESPESA', 'ELEMENTO DESPESA'] },
    { key: 'codigoPlanoOrcamentario', keywords: ['PLANO ORCAMENTARIO'], require: ['CODIGO'] },
    { key: 'planoOrcamentario', keywords: ['PLANO ORCAMENTARIO'] },

    { key: 'codigoProgramaGoverno', keywords: ['PROGRAMA GOVERNO'], require: ['CODIGO'] },
    { key: 'nomeProgramaGoverno', keywords: ['PROGRAMA GOVERNO'], require: ['NOME'] },
    { key: 'processo', keywords: ['PROCESSO', 'NUMERO PROCESSO'] },
    { key: 'valorUtilizadoConversao', keywords: ['VALOR UTILIZADO NA CONVERSAO', 'VALOR UTILIZADO CONVERSAO'] },
];

const EMPENHO_HEADER_RULES = [
    { key: 'idEmpenho', matchExact: ['ID EMPENHO'] },
    { key: 'codigoEmpenhoResumido', keywords: ['EMPENHO RESUMIDO'] },
    { key: 'codigoEmpenho', keywords: ['EMPENHO'], require: ['CODIGO'] },
    { key: 'tipoEmpenho', keywords: ['TIPO EMPENHO', 'TIPO DE EMPENHO'] },
    { key: 'especieEmpenho', keywords: ['ESPECIE EMPENHO', 'ESPECIE DE EMPENHO'] },

    { key: 'codigoEsferaOrcamentaria', keywords: ['ESFERA ORCAMENTARIA'], require: ['CODIGO'] },
    { key: 'esferaOrcamentaria', keywords: ['ESFERA ORCAMENTARIA'] },

    { key: 'codigoTipoCredito', keywords: ['TIPO CREDITO', 'TIPO DE CREDITO'], require: ['CODIGO'] },
    { key: 'tipoCredito', keywords: ['TIPO CREDITO', 'TIPO DE CREDITO'] },

    { key: 'codigoGrupoFonteRecurso', keywords: ['GRUPO FONTE RECURSO'], require: ['CODIGO'] },
    { key: 'grupoFonteRecurso', keywords: ['GRUPO FONTE RECURSO'] },
    { key: 'codigoFonteRecurso', keywords: ['FONTE RECURSO'], require: ['CODIGO'] },
    { key: 'fonteRecurso', keywords: ['FONTE RECURSO'] },

    { key: 'codigoUnidadeOrcamentaria', keywords: ['UNIDADE ORCAMENTARIA'], require: ['CODIGO'] },
    { key: 'unidadeOrcamentaria', keywords: ['UNIDADE ORCAMENTARIA'] },

    { key: 'codigoSubfuncao', keywords: ['SUBFUNCAO'], require: ['CODIGO'] },
    { key: 'subfuncao', keywords: ['SUBFUNCAO'] },
    { key: 'codigoFuncao', keywords: ['FUNCAO'], require: ['CODIGO'] },
    { key: 'funcao', matchExact: ['FUNCAO'] },

    { key: 'codigoPrograma', keywords: ['PROGRAMA'], require: ['CODIGO'] },
    { key: 'programa', matchExact: ['PROGRAMA'] },

    { key: 'codigoAcao', keywords: ['ACAO'], require: ['CODIGO'] },
    { key: 'acao', matchExact: ['ACAO'] },

    { key: 'linguagemCidada', keywords: ['LINGUAGEM CIDADA'] },
    { key: 'codigoSubtituloLocalizador', keywords: ['SUBTITULO LOCALIZADOR', 'SUBTITULO (LOCALIZADOR)'], require: ['CODIGO'] },
    { key: 'subtituloLocalizador', keywords: ['SUBTITULO LOCALIZADOR', 'SUBTITULO (LOCALIZADOR)'] },

    { key: 'autorEmenda', keywords: ['AUTOR EMENDA'] },
    { key: 'modalidadeLicitacao', keywords: ['MODALIDADE DE LICITACAO', 'MODALIDADE LICITACAO'] },
    { key: 'inciso', keywords: ['INCISO'] },
    { key: 'amparo', keywords: ['AMPARO'] },
    { key: 'referenciaDispensaInexigibilidade', keywords: ['REFERENCIA DE DISPENSA', 'DISPENSA OU INEXIGIBILIDADE'] },
    { key: 'codigoConvenio', keywords: ['CONVENIO'], require: ['CODIGO'] },
    { key: 'contratoRepasseTermoParceriaOutros', keywords: ['CONTRATO DE REPASSE', 'TERMO DE PARCERIA'] },
    { key: 'valorOriginalEmpenho', keywords: ['VALOR ORIGINAL DO EMPENHO', 'VALOR ORIGINAL EMPENHO'] },
    { key: 'valorEmpenhoConvertido', keywords: ['EMPENHO CONVERTIDO'] },
];

const LIQUIDACAO_HEADER_RULES = [
    { key: 'codigoLiquidacaoResumido', keywords: ['LIQUIDACAO RESUMIDO'] },
    { key: 'codigoLiquidacao', keywords: ['LIQUIDACAO'], require: ['CODIGO'] },
];

const PAGAMENTO_HEADER_RULES = [
    { key: 'codigoPagamentoResumido', keywords: ['PAGAMENTO RESUMIDO'] },
    { key: 'codigoPagamento', keywords: ['PAGAMENTO'], require: ['CODIGO'] },
    { key: 'tipoOb', keywords: ['TIPO OB'] },
    { key: 'extraorcamentario', keywords: ['EXTRAORCAMENTARIO'] },
    { key: 'valorOriginalPagamento', keywords: ['VALOR ORIGINAL DO PAGAMENTO', 'VALOR ORIGINAL PAGAMENTO'] },
    { key: 'valorPagamentoConvertido', keywords: ['PAGAMENTO CONVERTIDO'] },
];

// Helper para combinar as regras comuns + específicas com priorização
function resolveDespesasHeaderKey(header: string, specificRules: Array<any>): string {
    if (!header) return '';
    const cleanHeader = normalize(header);

    // 1. Tenta casar primeiro nas regras específicas da tabela (Empenho/Liquidação/Pagamento)
    for (const rule of specificRules) {
        if (rule.matchExact && rule.matchExact.includes(cleanHeader)) return rule.key;
        const hasKeyword = rule.keywords && rule.keywords.some((kw: string) => cleanHeader.includes(kw));
        if (hasKeyword) {
            if (rule.require) {
                if (rule.require.some((req: string) => cleanHeader.includes(req))) return rule.key;
            } else {
                return rule.key;
            }
        }
    }

    // 2. Se não casou, tenta as regras comuns (mapCommonHeaders)
    for (const rule of COMMON_HEADER_RULES) {
        if (rule.matchExact && rule.matchExact.includes(cleanHeader)) return rule.key;
        const hasKeyword = rule.keywords && rule.keywords.some((kw: string) => cleanHeader.includes(kw));
        if (hasKeyword) {
            if (rule.require) {
                if (rule.require.some((req: string) => cleanHeader.includes(req))) return rule.key;
            } else {
                return rule.key;
            }
        }
    }

    return cleanHeader;
}

async function processEmpenhoStream(stream: NodeJS.ReadableStream, metadata: ReferenceMetadata): Promise<number> {
    const log = logger.forMethod('processEmpenhoStream');
    let batch: (typeof DespesasEmpenho.$inferInsert)[] = [];
    let totalProcessed = 0;

    try {
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => resolveDespesasHeaderKey(header, EMPENHO_HEADER_RULES),
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
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => resolveDespesasHeaderKey(header, LIQUIDACAO_HEADER_RULES),
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
        const parser = stream
            .pipe(iconv.decodeStream('iso-8859-1'))
            .pipe(csvParser({
                separator: ';',
                mapHeaders: ({ header }) => resolveDespesasHeaderKey(header, PAGAMENTO_HEADER_RULES),
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

                            const streamEndPromise = new Promise<void>((resolveStream, rejectStream) => {
                                readStream.on('end', resolveStream);
                                readStream.on('error', rejectStream);
                            });

                            if (upperName.includes('_ITEMEMPENHOHISTORICO.CSV')) {
                                rowsCount = await createProcessStream(readStream, metadata, ITEM_EMPENHO_HISTORICO_HEADER_RULES, persistItemEmpenhoHistorico, 'processItemEmpenhoHistorico');
                            } else if (upperName.includes('_ITEMEMPENHO.CSV')) {
                                rowsCount = await createProcessStream(readStream, metadata, ITEM_EMPENHO_HEADER_RULES, persistItemEmpenho, 'processItemEmpenho');
                            } else if (upperName.includes('_LIQUIDACAO_EMPENHOSIMPACTADOS.CSV')) {
                                rowsCount = await createProcessStream(readStream, metadata, IMPACTADOS_HEADER_RULES, persistLiquidacaoEmpenhosImpactados, 'processLiquidacaoEmpenhosImpactados');
                            } else if (upperName.includes('_LIQUIDACAO.CSV')) {
                                rowsCount = await processLiquidacaoStream(readStream, metadata);
                            } else if (upperName.includes('_PAGAMENTO_EMPENHOSIMPACTADOS.CSV')) {
                                rowsCount = await createProcessStream(readStream, metadata, IMPACTADOS_HEADER_RULES, persistPagamentoEmpenhosImpactados, 'processPagamentoEmpenhosImpactados');
                            } else if (upperName.includes('_PAGAMENTO_FAVORECIDOSFINAIS.CSV')) {
                                rowsCount = await createProcessStream(readStream, metadata, FAVORECIDOS_FINAIS_HEADER_RULES, persistPagamentoFavorecidosFinais, 'processPagamentoFavorecidosFinais');
                            } else if (upperName.includes('_PAGAMENTO_LISTABANCOS.CSV')) {
                                rowsCount = await createProcessStream(readStream, metadata, LISTA_BANCOS_HEADER_RULES, persistPagamentoListaBancos, 'processPagamentoListaBancos');
                            } else if (upperName.includes('_PAGAMENTO_LISTAFATURAS.CSV') || upperName.includes('_PAGAMENTO_LISTAFATURA.CSV')) {
                                rowsCount = await createProcessStream(readStream, metadata, LISTA_FATURAS_HEADER_RULES, persistPagamentoListaFaturas, 'processPagamentoListaFaturas');
                            } else if (upperName.includes('_PAGAMENTO_LISTAPRECATÓRIOS.CSV') || upperName.includes('_PAGAMENTO_LISTAPRECATORIOS.CSV')) {
                                rowsCount = await createProcessStream(readStream, metadata, LISTA_PRECATORIOS_HEADER_RULES, persistPagamentoListaPrecatorios, 'processPagamentoListaPrecatorios');
                            } else if (upperName.includes('_PAGAMENTO.CSV')) {
                                rowsCount = await processPagamentoStream(readStream, metadata);
                            } else if (upperName.includes('_EMPENHO.CSV')) {
                                rowsCount = await processEmpenhoStream(readStream, metadata);
                            } else {
                                log.info({ data: { fileName: entry.fileName } }, 'Arquivo CSV ignorado');
                                readStream.resume();
                            }

                            await streamEndPromise;
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