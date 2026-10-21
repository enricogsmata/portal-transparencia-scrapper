import { new_logger } from '../../services/logs.js';
import { generateMunicipiosSeed } from '../bronze/processors/municipios-seed-generator.js';
import { scrapeSiafiMunicipios } from '../bronze/processors/siafi-scrapper.js';
import { generateEstadosSeed } from './seed/estados.seed.js';
import { generateProgramasSociaisSeed } from './seed/seed_programas_sociais.js';
import { MunicipioResolver } from './shared/municipio-resolver.js';
import { processExecucaoDespesa } from './transformers/despesas-execucao.js';
import { processDespesasEmpenho, processDespesasItemEmpenho, processDespesasItemEmpenhoHistorico, processDespesasLiquidacao, processDespesasLiquidacaoEmpenhosImpactados, processDespesasPagamento, processDespesasPagamentoEmpenhosImpactados, processDespesasPagamentoFavorecidosFinais, processDespesasPagamentoListaBancos, processDespesasPagamentoListaFaturas, processDespesasPagamentoListaPrecatorios } from './transformers/despesas.js';
import { processFavorecidosPjCnae, processFavorecidosPjCnpj, processFavorecidosPjNaturezaJuridica } from './transformers/favorecidos-pj.js';
import { initMunicipios } from './transformers/init-municipios.js';
import { processOrcamentoDespesa } from './transformers/orcamento-despesa.js';
import { processReceitas } from './transformers/receitas.js';
import { processRecebimentoRecursosFavorecido } from './transformers/despesas-favorecidos.js';
import { processConvenios, processConveniosOrdensBancarias } from './transformers/convenios.js';
import { processRecursosTransferidos } from './transformers/transferencias.js';
import { processLicitacao, processLicitacaoItem, processLicitacaoParticipantes, processLicitacaoEmpenhosRelacionados } from './transformers/licitacoes.js';
import { processContratosCompras, processContratosItemCompra, processContratosTermoAditivo, processContratosApostilamento } from './transformers/compras.js';
import { processNotaFiscal, processNotaFiscalItem, processNotaFiscalEvento } from './transformers/notas-fiscais.js';
import { processCartaoPagamentoCpgf } from './transformers/cpgf.js';
import { processCartaoPagamentoCpcc } from './transformers/cpcc.js';
import { processCartaoPagamentoDefesaCivil } from './transformers/cpdc.js';
import { processViagensViagem, processViagensTrecho, processViagensPassagem, processViagensPagamento } from './transformers/viagens.js';
import { processServidoresCadastro, processAposentadosCadastro, processPensionistasCadastro, processServidoresRemuneracao, processServidoresAfastamentos, processServidoresJetons, processServidoresHonorariosAdvocaticios, processServidoresObservacoes } from './transformers/servidores.js';
import { processImoveisFuncionais, processImoveisFuncionaisPermissionarios } from './transformers/imoveis-funcionais.js';
import { processEmendasParlamentares, processEmendasParlamentaresConvenios, processEmendasParlamentaresFavorecido } from './transformers/emendas-parlamentares.js';
import { processApoiamentosEmendasParlamentares } from './transformers/apoiamento-emendas-parlamentares-documentos.js';
import { processEmendasParlamentaresDocumentosDespesa } from './transformers/emendas-parlamentares-documentos.js';
import { processAuxilioBrasil } from './transformers/auxilio-brasil.js';
import { processAuxilioEmergencial } from './transformers/auxilio-emergencial.js';
import { processAuxilioReconstrucao } from './transformers/auxilio-reconstrucao.js';
import { processBolsaFamiliaPagamentos } from './transformers/bolsa-familia-pagamentos.js';
import { processBolsaFamiliaSaque } from './transformers/bolsa-familia-saques.js';
import { processNovoBolsaFamilia } from './transformers/novo-bolsa-familia.js';
import { processGarantiaSafra } from './transformers/garantia-safra.js';
import { processGasDoPovo } from './transformers/gas-do-povo.js';
import { processPeDeMeia } from './transformers/pe-de-meia.js';
import { processPeti } from './transformers/peti.js';
import { processBpc } from './transformers/bpc.js';
import { processSeguroDefeso } from './transformers/seguro-defeso.js';
import { processPep } from './transformers/pep.js';
import { processAcordosLeniencia, processAcordosLenienciaEfeitos } from './transformers/acordos-leniencia.js';
import { processCeis } from './transformers/ceis.js';
import { processCnep } from './transformers/cnep.js';
import { processCeaf } from './transformers/ceaf.js';
import { processCepim } from './transformers/cepim.js';
import { processRenunciasFiscais, processRenunciasFiscaisPorBeneficiario, processEmpresasImunesOuIsentas, processEmpresasHabilitadas } from './transformers/renuncias.js';

const logger = new_logger(import.meta.url);

interface TransformerTask {
  name: string;
  execute: () => Promise<void>;
}

export async function runSilverOrchestrator() {
  const log = logger.forMethod('runSilverOrchestrator');

  const tasks: TransformerTask[] = [
    // FASE 0: Seeds Estruturais de Infraestrutura (Pré-Orquestrador)
    { name: 'siafi-municipios', execute: scrapeSiafiMunicipios },
    { name: 'seed-estados', execute: generateEstadosSeed },
    { name: 'seed-programas-sociais', execute: generateProgramasSociaisSeed },
    { name: 'municipios-seed', execute: generateMunicipiosSeed },
    { name: 'init-municipios', execute: initMunicipios },
    { name: 'municipio-resolver', execute: () => MunicipioResolver.load() },

    // FASE 1: Cadastros Base e Dimensões Mestras Independentes
    //{ name: 'favorecidos-pj-cnae', execute: processFavorecidosPjCnae },
    //{ name: 'favorecidos-pj-natureza-juridica', execute: processFavorecidosPjNaturezaJuridica },
    //{ name: 'favorecidos-pj-cnpj', execute: processFavorecidosPjCnpj },

    // FASE 2: Estrutura Institucional, Gestão e Orçamento
    //{ name: 'orcamento-despesa', execute: processOrcamentoDespesa },
    //{ name: 'receitas', execute: processReceitas },
    { name: 'despesas-execucao', execute: processExecucaoDespesa },

    // FASE 3: Ciclo Central da Despesa Federal (SIAFI)
    { name: 'despesas-empenho', execute: processDespesasEmpenho },
    { name: 'despesas-item-empenho', execute: processDespesasItemEmpenho },
    { name: 'despesas-item-empenho-historico', execute: processDespesasItemEmpenhoHistorico },
    { name: 'despesas-liquidacao', execute: processDespesasLiquidacao },
    { name: 'despesas-liquidacao-empenhos-impactados', execute: processDespesasLiquidacaoEmpenhosImpactados },
    { name: 'despesas-pagamento', execute: processDespesasPagamento },
    { name: 'despesas-pagamento-empenhos-impactados', execute: processDespesasPagamentoEmpenhosImpactados },
    { name: 'despesas-pagamento-favorecidos-finais', execute: processDespesasPagamentoFavorecidosFinais },
    { name: 'despesas-pagamento-lista-bancos', execute: processDespesasPagamentoListaBancos },
    { name: 'despesas-pagamento-lista-faturas', execute: processDespesasPagamentoListaFaturas },
    { name: 'despesas-pagamento-lista-precatorios', execute: processDespesasPagamentoListaPrecatorios },
    { name: 'recebimento-recursos-favorecido', execute: processRecebimentoRecursosFavorecido },

    // FASE 4: Convênios e Transferências da União
    { name: 'convenios', execute: processConvenios },
    { name: 'convenios-ordens-bancarias', execute: processConveniosOrdensBancarias },
    { name: 'recursos-transferidos', execute: processRecursosTransferidos },

    // FASE 5: Compras Públicas, Licitações e Notas Fiscais
    { name: 'licitacao', execute: processLicitacao },
    { name: 'licitacao-item', execute: processLicitacaoItem },
    { name: 'licitacao-participantes', execute: processLicitacaoParticipantes },
    { name: 'licitacao-empenhos-relacionados', execute: processLicitacaoEmpenhosRelacionados },
    { name: 'contratos-compras', execute: processContratosCompras },
    { name: 'contratos-item-compra', execute: processContratosItemCompra },
    { name: 'contratos-termo-aditivo', execute: processContratosTermoAditivo },
    { name: 'contratos-apostilamento', execute: processContratosApostilamento },
    { name: 'nota-fiscal', execute: processNotaFiscal },
    { name: 'nota-fiscal-item', execute: processNotaFiscalItem },
    { name: 'nota-fiscal-evento', execute: processNotaFiscalEvento },

    // FASE 6: Cartões de Pagamento e Viagens a Serviço
    { name: 'cartao-pagamento-cpgf', execute: processCartaoPagamentoCpgf },
    { name: 'cartao-pagamento-cpcc', execute: processCartaoPagamentoCpcc },
    { name: 'cartao-pagamento-defesa-civil', execute: processCartaoPagamentoDefesaCivil },
    { name: 'viagens-viagem', execute: processViagensViagem },
    { name: 'viagens-trecho', execute: processViagensTrecho },
    { name: 'viagens-passagem', execute: processViagensPassagem },
    { name: 'viagens-pagamento', execute: processViagensPagamento },

    // FASE 7: Recursos Humanos, Servidores Públicos e Imóveis Funcionais
    { name: 'servidores-cadastro', execute: processServidoresCadastro },
    { name: 'aposentados-cadastro', execute: processAposentadosCadastro },
    { name: 'pensionistas-cadastro', execute: processPensionistasCadastro },
    { name: 'servidores-remuneracao', execute: processServidoresRemuneracao },
    { name: 'servidores-afastamentos', execute: processServidoresAfastamentos },
    { name: 'servidores-jetons', execute: processServidoresJetons },
    { name: 'servidores-honorarios-advocaticios', execute: processServidoresHonorariosAdvocaticios },
    { name: 'servidores-observacoes', execute: processServidoresObservacoes },
    { name: 'imoveis-funcionais', execute: processImoveisFuncionais },
    { name: 'imoveis-funcionais-permissionarios', execute: processImoveisFuncionaisPermissionarios },

    // FASE 8: Emendas Parlamentares e Vínculos
    { name: 'emendas-parlamentares', execute: processEmendasParlamentares },
    { name: 'emendas-parlamentares-convenios', execute: processEmendasParlamentaresConvenios },
    { name: 'emendas-parlamentares-favorecido', execute: processEmendasParlamentaresFavorecido },
    { name: 'apoiamentos-emendas-parlamentares', execute: processApoiamentosEmendasParlamentares },
    { name: 'emendas-parlamentares-documentos-despesa', execute: processEmendasParlamentaresDocumentosDespesa },

    // FASE 9: Programas Sociais e Benefícios ao Cidadão (Paralelizáveis)
    { name: 'auxilio-brasil', execute: processAuxilioBrasil },
    { name: 'auxilio-emergencial', execute: processAuxilioEmergencial },
    { name: 'auxilio-reconstrucao', execute: processAuxilioReconstrucao },
    { name: 'bolsa-familia-pagamentos', execute: processBolsaFamiliaPagamentos },
    { name: 'bolsa-familia-saques', execute: processBolsaFamiliaSaque },
    { name: 'novo-bolsa-familia', execute: processNovoBolsaFamilia },
    { name: 'garantia-safra', execute: processGarantiaSafra },
    { name: 'gas-do-povo', execute: processGasDoPovo },
    { name: 'pe-de-meia', execute: processPeDeMeia },
    { name: 'peti', execute: processPeti },
    { name: 'bpc', execute: processBpc },
    { name: 'seguro-defeso', execute: processSeguroDefeso },

    // FASE 10: Integridade, Sanções, PEP e Acordos de Leniência
    { name: 'pep', execute: processPep },
    { name: 'acordos-leniencia', execute: processAcordosLeniencia },
    { name: 'acordos-leniencia-efeitos', execute: processAcordosLenienciaEfeitos },
    { name: 'ceis', execute: processCeis },
    { name: 'cnep', execute: processCnep },
    { name: 'ceaf', execute: processCeaf },
    { name: 'cepim', execute: processCepim },

    // FASE 11: Renúncias Fiscais e Desonerações Tributárias
    { name: 'renuncias-fiscais', execute: processRenunciasFiscais },
    { name: 'renuncias-fiscais-por-beneficiario', execute: processRenunciasFiscaisPorBeneficiario },
    { name: 'empresas-imunes-ou-isentas', execute: processEmpresasImunesOuIsentas },
    { name: 'empresas-habilitadas', execute: processEmpresasHabilitadas },
  ];

  log.info({ data: { totalTasks: tasks.length } }, 'Iniciando orquestração da camada Silver');

  for (const task of tasks) {
    log.info(`Iniciando processamento: ${task.name}`);
    const start = Date.now();

    try {
      await task.execute();
      const durationMs = Date.now() - start;
      log.info({ data: { durationMs } }, `Processamento concluído com sucesso: ${task.name}`);
    } catch (error) {
      log.error({ data: error }, `Erro crítico ao processar: ${task.name}`);
      throw error;
    }
  }

  log.info('Orquestração da camada Silver finalizada com sucesso');
}

await runSilverOrchestrator();