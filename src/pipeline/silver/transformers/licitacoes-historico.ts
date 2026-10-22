// src/pipeline/silver/transformers/licitacoes-historico.ts
//
// Bronze: licitacoes_historico  (1 linha = 1 item da licitação + o vencedor daquele item)
// Silver: licitacoes, licitacao_itens, licitacao_participantes (vencedores)
//
// Decisões de encaixe:
//  - Chave da licitação = (UG, modalidade, número), a mesma do uq_licitacao_nk.
//    (O processLicitacao atual resolve por numeroLicitacao isolado; aqui NÃO, para não colidir entre UGs.)
//  - Histórico NÃO sobrescreve o cadastro principal: insert-if-absent, preenchendo só campos nulos.
//  - numeroItem é sequência da licitação, não código de catálogo (codigoItemCompra do módulo principal).
//    Por isso é gravado com prefixo 'HIST-' para não misturar semânticas no mesmo uq (licitacao_id, codigo).
//  - Participantes: o histórico só traz o vencedor => flagVencedor = true. Nunca faz DELETE por licitação
//    (o processLicitacaoParticipantes atual faz, e apagaria participantes não vencedores).
//  - Sem destino no silver (perdidos): localAbertura, responsavelLicitacao, quantidadeItem, valorItem.
//    Ver sugestão de colunas no relatório.

import { sql, inArray, and } from 'drizzle-orm';
import { bronze_db } from '../../../database/bronze_connection.js';
import { silver_db } from '../../../database/silver_connection.js';
import { LicitacoesHistorico } from '../../../database/bronze_schema.js';
import { licitacoes, licitacaoItens, licitacaoParticipantes } from '../../../database/silver_schema.js';
import { parseDate, parseDecimal } from '../shared/formatters.js';
import {
  chunk,
  cnpjOuNull,
  ensurePessoasJuridicas,
  ensureUnidadesGestoras,
  k,
} from '../shared/historicos.js';
import { new_logger } from '../../../services/logs.js';

const logger = new_logger(import.meta.url);

const PREFIXO_ITEM = 'HIST-';
const BATCH_SIZE = 1000;

const licKey = (ugId: number, modalidade: string, numero: string) => `${ugId}|${k(modalidade)}|${k(numero)}`;

/** 'dd/mm/yyyy hh:mm', 'yyyy-mm-dd hh:mm:ss' ou 'yyyy-mm-ddThh:mm' -> só a parte da data. */
function dataDeDataHora(v?: string | null): string | null {
  if (!v) return null;
  return parseDate(v.trim().split(/[ T]/)[0]) || null;
}

export async function processLicitacoesHistorico() {
  const log = logger.forMethod('processLicitacoesHistorico');
  let offset = 0;
  let descartadasSemChave = 0;

  log.info('Iniciando processamento de Licitações - Histórico');

  try {
    while (true) {
      const bronzeData = await bronze_db
        .select()
        .from(LicitacoesHistorico)
        .orderBy(LicitacoesHistorico.id) // paginação estável
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        log.info({ data: { offset, descartadasSemChave } }, 'Processamento concluído.');
        break;
      }

      // 1. Unidades Gestoras
      const ugIdMap = await ensureUnidadesGestoras(
        bronzeData
          .filter((b) => b.codigoUnidadeGestora && b.codigoUnidadeGestora.trim().length <= 30)
          .map((b) => ({
            codigo: b.codigoUnidadeGestora!.trim(),
            nome: (b.nomeUnidadeGestora || 'NÃO INFORMADO').trim().substring(0, 255),
          })),
      );

      // 2. Licitações (cabeçalho) — deduplicadas por chave; a última linha do lote vence
      const headerMap = new Map<string, typeof licitacoes.$inferInsert>();
      for (const b of bronzeData) {
        const ugId = b.codigoUnidadeGestora ? ugIdMap.get(b.codigoUnidadeGestora) : null;
        if (!ugId || !b.numeroLicitacao || !b.modalidadeLicitacao || !b.objeto) {
          descartadasSemChave++;
          continue;
        }
        headerMap.set(licKey(ugId, b.modalidadeLicitacao, b.numeroLicitacao), {
          numeroLicitacao: b.numeroLicitacao,
          unidadeGestoraId: ugId,
          modalidadeCompra: b.modalidadeLicitacao,
          numeroProcesso: b.numeroProcesso || null,
          objeto: b.objeto,
          situacaoLicitacao: b.situacaoLicitacao || null,
          dataAbertura: dataDeDataHora(b.dataHoraAbertura),
          exercicioReferencia: b.exercicioReferencia || null,
        });
      }

      const headers = Array.from(headerMap.values());
      for (const part of chunk(headers)) {
        await silver_db
          .insert(licitacoes)
          .values(part)
          .onDuplicateKeyUpdate({
            // não sobrescreve o principal: só completa o que está nulo
            set: {
              numeroProcesso: sql`COALESCE(numero_processo, VALUES(numero_processo))`,
              situacaoLicitacao: sql`COALESCE(situacao_licitacao, VALUES(situacao_licitacao))`,
              dataAbertura: sql`COALESCE(data_abertura, VALUES(data_abertura))`,
              exercicioReferencia: sql`COALESCE(exercicio_referencia, VALUES(exercicio_referencia))`,
            },
          });
      }

      // 3. IDs resolvidos pela chave composta
      const ugIdsLote = [...new Set(headers.map((h) => h.unidadeGestoraId))];
      const numerosLote = [...new Set(headers.map((h) => h.numeroLicitacao))];
      const licDb =
        ugIdsLote.length > 0
          ? await silver_db
              .select({
                id: licitacoes.id,
                ugId: licitacoes.unidadeGestoraId,
                modalidade: licitacoes.modalidadeCompra,
                numero: licitacoes.numeroLicitacao,
              })
              .from(licitacoes)
              .where(
                and(
                  inArray(licitacoes.unidadeGestoraId, ugIdsLote),
                  inArray(licitacoes.numeroLicitacao, numerosLote),
                ),
              )
          : [];
      const licIdMap = new Map(licDb.map((l) => [licKey(l.ugId, l.modalidade, l.numero), l.id]));

      // 4. Itens
      const itensMap = new Map<string, typeof licitacaoItens.$inferInsert>();
      for (const b of bronzeData) {
        const ugId = b.codigoUnidadeGestora ? ugIdMap.get(b.codigoUnidadeGestora) : null;
        if (!ugId || !b.numeroLicitacao || !b.modalidadeLicitacao) continue;
        const licitacaoId = licIdMap.get(licKey(ugId, b.modalidadeLicitacao, b.numeroLicitacao));
        const numeroItem = b.numeroItem?.trim();
        if (!licitacaoId || !numeroItem || !b.descricaoItem) continue;

        const codigoItemCompra = `${PREFIXO_ITEM}${numeroItem}`.substring(0, 30);
        itensMap.set(`${licitacaoId}|${codigoItemCompra}`, {
          licitacaoId,
          codigoItemCompra,
          descricaoItemCompra: b.descricaoItem,
        });
      }
      const itens = Array.from(itensMap.values());
      for (const part of chunk(itens)) {
        await silver_db.insert(licitacaoItens).values(part).onDuplicateKeyUpdate({ set: { id: sql`id` } });
      }

      const licIdsComItem = [...new Set(itens.map((i) => i.licitacaoId))];
      const itensDb =
        licIdsComItem.length > 0
          ? await silver_db
              .select({
                id: licitacaoItens.id,
                licitacaoId: licitacaoItens.licitacaoId,
                codigo: licitacaoItens.codigoItemCompra,
              })
              .from(licitacaoItens)
              .where(inArray(licitacaoItens.licitacaoId, licIdsComItem))
          : [];
      const itemIdMap = new Map(itensDb.map((i) => [`${i.licitacaoId}|${i.codigo}`, i.id]));

      // 5. Vencedores -> pessoas jurídicas + participantes
      const pjIdMap = await ensurePessoasJuridicas(
        bronzeData
          .map((b) => ({ cnpj: cnpjOuNull(b.cnpjVencedor), razaoSocial: b.nomeVencedor || 'NÃO INFORMADO' }))
          .filter((p): p is { cnpj: string; razaoSocial: string } => !!p.cnpj),
      );

      const partMap = new Map<string, typeof licitacaoParticipantes.$inferInsert>();
      for (const b of bronzeData) {
        const ugId = b.codigoUnidadeGestora ? ugIdMap.get(b.codigoUnidadeGestora) : null;
        const cnpj = cnpjOuNull(b.cnpjVencedor);
        if (!ugId || !cnpj || !b.numeroLicitacao || !b.modalidadeLicitacao) continue;

        const licitacaoId = licIdMap.get(licKey(ugId, b.modalidadeLicitacao, b.numeroLicitacao));
        const participanteId = pjIdMap.get(cnpj);
        if (!licitacaoId || !participanteId) continue;

        const numeroItem = b.numeroItem?.trim();
        const licitacaoItemId = numeroItem
          ? itemIdMap.get(`${licitacaoId}|${`${PREFIXO_ITEM}${numeroItem}`.substring(0, 30)}`) ?? null
          : null;

        partMap.set(`${licitacaoId}|${licitacaoItemId ?? 0}|${participanteId}`, {
          licitacaoId,
          licitacaoItemId,
          participanteId,
          flagVencedor: true,
        });
      }

      for (const part of chunk(Array.from(partMap.values()))) {
        await silver_db
          .insert(licitacaoParticipantes)
          .values(part)
          .onDuplicateKeyUpdate({
            // GREATEST: se já era vencedor no módulo principal, continua; se era perdedor, passa a vencedor
            set: { flagVencedor: sql`GREATEST(flag_vencedor, VALUES(flag_vencedor))` },
          });
      }

      log.info(
        { data: { offset, headers: headers.length, itens: itens.length, vencedores: partMap.size } },
        'Lote de histórico de licitações processado',
      );
      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento de Licitações - Histórico');
    throw error;
  }
}