/**
 * @file processor_orchestrator.ts
 * @description Orquestrador da etapa de processamento (Camada Bronze).
 *
 * Responsabilidades:
 *  - Coordenar a execução sequencial dos 44 processadores de ingestão.
 *  - Cada processador varre os arquivos ZIP do seu módulo em `src/files/<módulo>/`,
 *    descompacta em memória, converte encoding (Latin-1 → UTF-8 via iconv-lite),
 *    parseia o CSV em streaming (csv-parser) e persiste os dados no banco Bronze
 *    usando upsert idempotente (`INSERT ... ON DUPLICATE KEY UPDATE`).
 *
 * Padrões arquiteturais:
 *  - Execução estritamente sequencial para controlar memória e conexões abertas.
 *  - Batching de 2.000 registros por inserção (constante BATCH_SIZE nos processadores).
 *  - Metadados de referência temporal (exercício, mês, dia) extraídos do nome do arquivo ZIP
 *    e persistidos em todas as tabelas Bronze para rastreabilidade de período.
 *
 * Organização por domínio temático (comentada inline na função `runProcessors`).
 */
import { runAcordosLenienciaIngestion } from "./processors/acordos-leniencia.js";
import { runApoiamentosEmendasParlamentaresIngestion } from "./processors/apoiamentos-emendas-parlamentares.js";
import { runAuxilioBrasilIngestion } from "./processors/auxilio-brasil.js";
import { runAuxilioEmergencialIngestion } from "./processors/auxilio-emergencial.js";
import { runAuxilioReconstrucaoIngestion } from "./processors/auxilio-reconstrucao.js";
import { runBolsaPagamentosFamiliaIngestion } from "./processors/bolsa-familia-pagamentos.js";
import { runBolsaFamiliaSaqueIngestion } from "./processors/bolsa-familia-saque.js";
import { runBpcIngestion } from "./processors/bpc.js";
import { runCartaoPagamentoCpccIngestion } from "./processors/cartao-pagamento-cpcc.js";
import { runCartaoPagamentoDefesaCivilIngestion } from "./processors/cartao-pagamento-defesa-civil.js";
import { runCartaoPagamentoCpgfIngestion } from "./processors/cartao_pagamento_cpgf.js";
import { runCeafIngestion } from "./processors/ceaf.js";
import { runCeisIngestion } from "./processors/ceis.js";
import { runCepimIngestion } from "./processors/cepim.js";
import { runCnepIngestion } from "./processors/cnep.js";
import { runContratosHistoricoIngestion } from "./processors/contratos-historico.js";
import { runContratosIngestion } from "./processors/contratos.js";
import { runConveniosIngestion } from "./processors/convenios.js";
import { runDespesasIngestion } from "./processors/despesas.js";
import { runDiariasEPassagensIngestion } from "./processors/diarias-e-passagens.js";
import { runEmendasParlamentaresDocumentosDespesaIngestion } from "./processors/emendas-parlamentares-documentos-despesa.js";
import { runEmendasParlamentaresIngestion } from "./processors/emendas-parlamentares.js";
import { runExecucaoDespesaIngestion } from "./processors/execucao-despesa.js";
import { runExecucaoHistoricoIngestion } from "./processors/execucao-historico.js";
import { runFavorecidosPjIngestion } from "./processors/favorecidos-pj.js";
import { runGarantiaSafraIngestion } from "./processors/garantia-safra.js";
import { runGasDoPovoIngestion } from "./processors/gas-do-povo.js";
import { runImoveisFuncionaisIngestion } from "./processors/imoveis-funcionais.js";
import { runLicitacoesHistoricoIngestion } from "./processors/licitacoes-historico.js";
import { runLicitacoesIngestion } from "./processors/licitacoes.js";
import { runNotasFiscaisIngestion } from "./processors/notas-fiscais.js";
import { runNovoBolsaFamiliaIngestion } from "./processors/novo-bolsa-familia.js";
import { runOrcamentoDespesaIngestion } from "./processors/orcamento-despesa.js";
import { runPeDeMeiaIngestion } from "./processors/pe-de-meia.js";
import { runPepIngestion } from "./processors/pep.js";
import { runPetiIngestion } from "./processors/peti.js";
import { runRecebimentoRecursosFavorecidoIngestion } from "./processors/recebimento-recursos-favorecido.js";
import { runReceitasIngestion } from "./processors/receitas.js";
import { runRecursosTransferidosIngestion } from "./processors/recursos-transferidos.js";
import { runRenunciasFiscaisIngestion } from "./processors/renuncias-fiscais.js";
import { runSeguroDefesoIngestion } from "./processors/seguro-defeso.js";
import { runServidoresIngestion } from "./processors/servidores.js";
import { runViagensIngestion } from "./processors/viagens.js";

/**
 * Orquestra a execução sequencial de todos os processadores de ingestão da Camada Bronze.
 *
 * Cada grupo de processadores é delimitado por comentários de domínio temático.
 * A ordem de execução não é restritiva a nível de Bronze (sem dependência entre tabelas),
 * mas segue uma sequência lógica para facilitar o monitoramento e o diagnóstico de erros.
 *
 * Pré-requisito: os arquivos ZIP já devem estar em `src/files/<módulo>/`
 * (etapa anterior: `runScrapper`).
 */
export async function runProcessors() {
    // BENEFÍCIOS AO CIDADÃO
    await runAuxilioBrasilIngestion();
    await runAuxilioEmergencialIngestion();
    await runAuxilioReconstrucaoIngestion();
    await runBolsaPagamentosFamiliaIngestion();
    await runBolsaFamiliaSaqueIngestion();
    await runNovoBolsaFamiliaIngestion();
    await runGarantiaSafraIngestion();
    await runGasDoPovoIngestion();
    await runSeguroDefesoIngestion();
    await runPetiIngestion();
    await runPeDeMeiaIngestion();
    await runBpcIngestion();

    // CARTÃO DE PAGAMENTO
    await runCartaoPagamentoCpgfIngestion();
    await runCartaoPagamentoCpccIngestion();
    await runCartaoPagamentoDefesaCivilIngestion();

    // CNPJ
    await runFavorecidosPjIngestion();

    // CONVÊNIOS E INSTRUMENTOS CONGÊNERES
    await runConveniosIngestion();

    // DESPESAS PÚBLICAS
    await runDespesasIngestion();
    await runExecucaoDespesaIngestion();
    await runRecursosTransferidosIngestion();
    await runRecebimentoRecursosFavorecidoIngestion();

    // EMENDAS PARLAMENTARES
    await runEmendasParlamentaresIngestion();
    await runEmendasParlamentaresDocumentosDespesaIngestion();
    await runApoiamentosEmendasParlamentaresIngestion();

    // IMÓVEIS FUNCIONAIS
    await runImoveisFuncionaisIngestion();

    // LICITAÇÕES E CONTRATOS
    await runLicitacoesIngestion();
    await runContratosIngestion();

    // NOTAS FISCAIS
    await runNotasFiscaisIngestion();

    // ORÇAMENTO PÚBLICO
    await runOrcamentoDespesaIngestion();
    
    // RECEITAS PÚBLICAS
    await runReceitasIngestion();
    
    // RENÚNCIAS FISCAIS
    await runRenunciasFiscaisIngestion();
    
    // SANÇÕES
    await runCeisIngestion();
    await runCepimIngestion();
    await runCnepIngestion();
    await runAcordosLenienciaIngestion();

    // SERVIDORES
    await runServidoresIngestion();
    await runCeafIngestion();
    await runPepIngestion();

    // VIAGENS A SERVIÇO
    await runViagensIngestion();

    // HISTORICO
    await runDiariasEPassagensIngestion();
    await runLicitacoesHistoricoIngestion();
    await runContratosHistoricoIngestion();
    await runExecucaoHistoricoIngestion();
    return;
}