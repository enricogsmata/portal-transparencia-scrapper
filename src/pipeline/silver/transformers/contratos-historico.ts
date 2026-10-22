// src/pipeline/silver/transformers/contratos-historico.ts
//
// Bronze: contratos_historico (1 linha = 1 contrato, valor único)
// Silver: contratos (uq_contratos_nk = UG + numeroContrato)
//
// Decisões de encaixe:
//  - Histórico NÃO sobrescreve contratos do módulo principal: insert-if-absent, completando só campos nulos.
//  - contratos.contratado_id referencia pessoas_juridicas (NOT NULL). Contratado com CPF (ou documento
//    inválido) não cabe no modelo: a linha é contada e descartada no log.
//  - valorContrato é único no bronze; o silver tem valor inicial e final (NOT NULL). Grava o mesmo valor
//    nos dois (sem aditivos o inicial = final). Se o contrato já existir, os valores atuais são preservados.
//  - Vínculo com licitação: somente LOOKUP por (UG, modalidade, número). Não cria licitação "stub"
//    (o processContratosCompras atual cria, com objeto genérico). Rode ANTES o histórico de licitações.
//  - Sem destino no silver (perdido): numeroProcesso. Ver sugestão de coluna no relatório.

import { sql, inArray, and } from 'drizzle-orm';
import { bronze_db } from '../../../database/bronze_connection.js';
import { silver_db } from '../../../database/silver_connection.js';
import { ContratosHistorico } from '../../../database/bronze_schema.js';
import { contratos, licitacoes } from '../../../database/silver_schema.js';
import { parseDate, parseDecimal } from '../shared/formatters.js';
import {
  chunk,
  cnpjOuNull,
  ensurePessoasJuridicas,
  ensureUnidadesGestoras,
  k,
  nz,
} from '../shared/historicos.js';
import { new_logger } from '../../../services/logs.js';

const logger = new_logger(import.meta.url);
const BATCH_SIZE = 1000;

const licKey = (ugId: number, modalidade: string, numero: string) => `${ugId}|${k(modalidade)}|${k(numero)}`;

export async function processContratosHistorico() {
  const log = logger.forMethod('processContratosHistorico');
  let offset = 0;
  const descartes = { semChave: 0, contratadoNaoPj: 0 };

  log.info('Iniciando processamento de Contratos - Histórico');

  try {
    while (true) {
      const bronzeData = await bronze_db
        .select()
        .from(ContratosHistorico)
        .orderBy(ContratosHistorico.id)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        log.info({ data: { offset, descartes } }, 'Processamento concluído.');
        break;
      }

      // 1. Unidades Gestoras
      const ugIdMap = await ensureUnidadesGestoras(
        bronzeData
          .filter((b) => b.codigoUnidadeGestora)
          .map((b) => ({ codigo: b.codigoUnidadeGestora!, nome: b.nomeUnidadeGestora || 'NÃO INFORMADO' })),
      );

      // 2. Contratados (somente PJ)
      const pjIdMap = await ensurePessoasJuridicas(
        bronzeData
          .map((b) => ({ cnpj: cnpjOuNull(b.cpfCnpj), razaoSocial: b.nomeEmpresa || 'NÃO INFORMADO' }))
          .filter((p): p is { cnpj: string; razaoSocial: string } => !!p.cnpj),
      );

      // 3. Licitações já existentes (lookup puro)
      const ugIds = [...new Set(Array.from(ugIdMap.values()))];
      const numerosLic = [...new Set(bronzeData.map((b) => b.numeroLicitacao).filter(Boolean) as string[])];
      const licDb =
        ugIds.length > 0 && numerosLic.length > 0
          ? await silver_db
              .select({
                id: licitacoes.id,
                ugId: licitacoes.unidadeGestoraId,
                modalidade: licitacoes.modalidadeCompra,
                numero: licitacoes.numeroLicitacao,
              })
              .from(licitacoes)
              .where(and(inArray(licitacoes.unidadeGestoraId, ugIds), inArray(licitacoes.numeroLicitacao, numerosLic)))
          : [];
      const licIdMap = new Map(licDb.map((l) => [licKey(l.ugId, l.modalidade, l.numero), l.id]));

      // 4. Contratos — dedupe por (UG, número); a última linha do lote vence
      const contratosMap = new Map<string, typeof contratos.$inferInsert>();
      for (const b of bronzeData) {
        const ugId = b.codigoUnidadeGestora ? ugIdMap.get(b.codigoUnidadeGestora) : null;
        if (!ugId || !b.numeroContrato || !b.objeto) {
          descartes.semChave++;
          continue;
        }
        const cnpj = cnpjOuNull(b.cpfCnpj);
        const contratadoId = cnpj ? pjIdMap.get(cnpj) : null;
        if (!contratadoId) {
          descartes.contratadoNaoPj++;
          continue;
        }

        const licitacaoId =
          b.numeroLicitacao && b.modalidadeLicitacao
            ? licIdMap.get(licKey(ugId, b.modalidadeLicitacao, b.numeroLicitacao)) ?? null
            : null;
        const valor = nz(parseDecimal(b.valorContrato));

        contratosMap.set(`${ugId}|${k(b.numeroContrato)}`, {
          numeroContrato: b.numeroContrato,
          unidadeGestoraId: ugId,
          licitacaoId,
          contratadoId,
          objeto: b.objeto,
          fundamentoLegal: b.fundamentoLegal || null,
          situacaoContrato: b.situacaoContrato || null,
          modalidadeCompra: b.modalidadeLicitacao || null,
          dataPublicacaoDou: parseDate(b.dataPublicacaoDou) || null,
          dataInicioVigencia: parseDate(b.inicioVigencia) || null,
          dataFimVigencia: parseDate(b.fimVigencia) || null,
          valorInicialCompra: valor,
          valorFinalCompra: valor,
          exercicioReferencia: b.exercicioReferencia || null,
        });
      }

      const rows = Array.from(contratosMap.values());
      for (const part of chunk(rows)) {
        await silver_db
          .insert(contratos)
          .values(part)
          .onDuplicateKeyUpdate({
            // contrato do módulo principal manda; histórico só completa lacunas
            set: {
              licitacaoId: sql`COALESCE(licitacao_id, VALUES(licitacao_id))`,
              fundamentoLegal: sql`COALESCE(fundamento_legal, VALUES(fundamento_legal))`,
              modalidadeCompra: sql`COALESCE(modalidade_compra, VALUES(modalidade_compra))`,
              situacaoContrato: sql`COALESCE(situacao_contrato, VALUES(situacao_contrato))`,
              dataPublicacaoDou: sql`COALESCE(data_publicacao_dou, VALUES(data_publicacao_dou))`,
              dataInicioVigencia: sql`COALESCE(data_inicio_vigencia, VALUES(data_inicio_vigencia))`,
              dataFimVigencia: sql`COALESCE(data_fim_vigencia, VALUES(data_fim_vigencia))`,
              exercicioReferencia: sql`COALESCE(exercicio_referencia, VALUES(exercicio_referencia))`,
            },
          });
      }

      log.info({ data: { offset, gravados: rows.length } }, 'Lote de histórico de contratos processado');
      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento de Contratos - Histórico');
    throw error;
  }
}