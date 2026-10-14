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
import { runContratosIngestion } from "./processors/contratos.js";
import { runConveniosIngestion } from "./processors/convenios.js";
import { runDespesasIngestion } from "./processors/despesas.js";
import { runEmendasParlamentaresDocumentosDespesaIngestion } from "./processors/emendas-parlamentares-documentos-despesa.js";
import { runEmendasParlamentaresIngestion } from "./processors/emendas-parlamentares.js";
import { runExecucaoDespesaIngestion } from "./processors/execucao-despesa.js";
import { runFavorecidosPjIngestion } from "./processors/favorecidos-pj.js";
import { runGarantiaSafraIngestion } from "./processors/garantia-safra.js";
import { runGasDoPovoIngestion } from "./processors/gas-do-povo.js";
import { runImoveisFuncionaisIngestion } from "./processors/imoveis-funcionais.js";
import { runLicitacoesIngestion } from "./processors/licitacoes.js";
import { runNotasFiscaisIngestion } from "./processors/notas-fiscais.js";
import { runNovoBolsaFamiliaIngestion } from "./processors/novo-bolsa-familia.js";
import { runOrcamentoDespesaIngestion } from "./processors/orcamento-despesa.js";
import { runPeDeMeiaIngestion } from "./processors/pe-de-meia.js";
import { runPepIngestion } from "./processors/pep.js";
import { runPetiIngestion } from "./processors/peti.js";
import { runRecebimentoRecursosFavorecidoIngestion } from "./processors/recebimento-recursos-favorecido.js";
import { runRecursosTransferidosIngestion } from "./processors/recursos-transferidos.js";
import { runRenunciasFiscaisIngestion } from "./processors/renuncias-fiscais.js";
import { runSeguroDefesoIngestion } from "./processors/seguro-defeso.js";
import { runServidoresIngestion } from "./processors/servidores.js";
import { runViagensIngestion } from "./processors/viagens.js";

export async function runProcessors() {
    // BENEFÍCIOS AO CIDADÃO
    //await runAuxilioBrasilIngestion();
    //await runAuxilioEmergencialIngestion();
    await runAuxilioReconstrucaoIngestion();
    //await runBolsaPagamentosFamiliaIngestion();
    //await runBolsaFamiliaSaqueIngestion();
    //await runNovoBolsaFamiliaIngestion();
    //await runGarantiaSafraIngestion();
    //await runGasDoPovoIngestion();
    //await runSeguroDefesoIngestion();
    //await runPetiIngestion();
    //await runPeDeMeiaIngestion();
    //await runBpcIngestion();

    // CARTÃO DE PAGAMENTO
    //await runCartaoPagamentoCpgfIngestion();
    //await runCartaoPagamentoCpccIngestion();
    //await runCartaoPagamentoDefesaCivilIngestion();

    // CNPJ
    //await runFavorecidosPjIngestion();

    // CONVÊNIOS E INSTRUMENTOS CONGÊNERES
    //await runConveniosIngestion();

    // DESPESAS PÚBLICAS
    //await runDespesasIngestion();
    //await runExecucaoDespesaIngestion();
    //await runRecursosTransferidosIngestion();
    //await runRecebimentoRecursosFavorecidoIngestion();

    // EMENDAS PARLAMENTARES
    //await runEmendasParlamentaresIngestion();
    //await runEmendasParlamentaresDocumentosDespesaIngestion();
    //await runApoiamentosEmendasParlamentaresIngestion();

    // IMÓVEIS FUNCIONAIS
    //await runImoveisFuncionaisIngestion();

    // LICITAÇÕES E CONTRATOS
    //await runLicitacoesIngestion();
    //await runContratosIngestion();

    // NOTAS FISCAIS
    //await runNotasFiscaisIngestion();

    // ORÇAMENTO PÚBLICO
    //await runOrcamentoDespesaIngestion();

    // RECEITAS PÚBLICAS
    //await runExecucaoDespesaIngestion();

    // RENÚNCIAS FISCAIS
    //await runRenunciasFiscaisIngestion();

    // SANÇÕES
    //await runCeisIngestion();
    //await runCepimIngestion();
    //await runCnepIngestion();
    //await runAcordosLenienciaIngestion();

    // SERVIDORES
    //await runServidoresIngestion();
    //await runCeafIngestion();
    //await runPepIngestion();

    // VIAGENS A SERVIÇO
    //await runViagensIngestion();
    return;
}