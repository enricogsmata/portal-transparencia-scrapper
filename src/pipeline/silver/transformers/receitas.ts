import { sql, inArray, and, eq } from 'drizzle-orm';
import { bronze_db } from '../../../database/bronze_connection.js';
import { silver_db } from '../../../database/silver_connection.js';
import { Receitas } from '../../../database/bronze_schema.js';
import {
  receitas,
  unidadesGestoras,
  orgaosSubordinados,
  orgaosSuperiores
} from '../../../database/silver_schema.js';
import { parseDate, parseDecimal } from '../shared/formatters.js';
import { new_logger } from '../../../services/logs.js';

const logger = new_logger(import.meta.url);

export async function processReceitas() {
  const log = logger.forMethod('processReceitas');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento do módulo Receitas');

  try {
    while (hasMore) {
      const bronzeData = await bronze_db.select()
        .from(Receitas)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        log.info({ data: { offset } }, 'Processamento concluído. Nenhum registro adicional encontrado.');
        hasMore = false;
        break;
      }

      log.info({ data: { offset, batchSize: bronzeData.length } }, 'Lote extraído da tabela receitas');

      const osToUpsertMap = new Map<string, typeof orgaosSuperiores.$inferInsert>();
      bronzeData.forEach((b) => {
        if (b.codigoOrgaoSuperior) {
          osToUpsertMap.set(b.codigoOrgaoSuperior, {
            codigo: b.codigoOrgaoSuperior,
            nome: b.nomeOrgaoSuperior || 'NÃO INFORMADO',
            sistemaOrigem: 'SIAFI',
          });
        }
      });
      const osToUpsert = Array.from(osToUpsertMap.values());

      if (osToUpsert.length > 0) {
        await silver_db.insert(orgaosSuperiores)
          .values(osToUpsert)
          .onDuplicateKeyUpdate({ set: { nome: sql`VALUES(nome)` } });
      }

      const osDb = osToUpsert.length > 0
        ? await silver_db.select({ id: orgaosSuperiores.id, codigo: orgaosSuperiores.codigo })
          .from(orgaosSuperiores)
          .where(and(eq(orgaosSuperiores.sistemaOrigem, 'SIAFI'), inArray(orgaosSuperiores.codigo, osToUpsert.map((o) => o.codigo))))
        : [];
      const osIdMap = new Map(osDb.map((o) => [o.codigo, o.id]));

      // 2. Dimensão Órgãos Subordinados
      const osubToUpsertMap = new Map<string, typeof orgaosSubordinados.$inferInsert>();
      bronzeData.forEach((b) => {
        if (b.codigoOrgao) {
          const superiorId = b.codigoOrgaoSuperior ? osIdMap.get(b.codigoOrgaoSuperior) : null;
          if (superiorId) {
            osubToUpsertMap.set(b.codigoOrgao, {
              codigo: b.codigoOrgao,
              nome: b.nomeOrgao || 'NÃO INFORMADO',
              orgaoSuperiorId: superiorId,
              sistemaOrigem: 'SIAFI',
            });
          }
        }
      });
      const osubToUpsert = Array.from(osubToUpsertMap.values());

      if (osubToUpsert.length > 0) {
        await silver_db.insert(orgaosSubordinados)
          .values(osubToUpsert)
          .onDuplicateKeyUpdate({
            set: {
              nome: sql`VALUES(nome)`,
              orgaoSuperiorId: sql`VALUES(orgao_superior_id)`,
            },
          });
      }

      const osubDb = osubToUpsert.length > 0
        ? await silver_db.select({ id: orgaosSubordinados.id, codigo: orgaosSubordinados.codigo })
          .from(orgaosSubordinados)
          .where(and(eq(orgaosSubordinados.sistemaOrigem, 'SIAFI'), inArray(orgaosSubordinados.codigo, osubToUpsert.map((o) => o.codigo))))
        : [];
      const osubIdMap = new Map(osubDb.map((o) => [o.codigo, o.id]));

      // 3. Dimensão Unidades Gestoras
      const ugsToUpsertMap = new Map<string, typeof unidadesGestoras.$inferInsert>();
      bronzeData.forEach((b) => {
        if (b.codigoUnidadeGestora) {
          const orgaoSubordinadoId = b.codigoOrgao ? osubIdMap.get(b.codigoOrgao) || null : null;
          ugsToUpsertMap.set(b.codigoUnidadeGestora, {
            codigo: b.codigoUnidadeGestora,
            nome: b.nomeUnidadeGestora || 'NÃO INFORMADO',
            orgaoSubordinadoId,
          });
        }
      });
      const ugsToUpsert = Array.from(ugsToUpsertMap.values());

      if (ugsToUpsert.length > 0) {
        await silver_db.insert(unidadesGestoras)
          .values(ugsToUpsert)
          .onDuplicateKeyUpdate({ set: { id: sql`id` } });
      }

      const ugsDb = ugsToUpsert.length > 0
        ? await silver_db.select({ id: unidadesGestoras.id, codigo: unidadesGestoras.codigo })
          .from(unidadesGestoras)
          .where(inArray(unidadesGestoras.codigo, ugsToUpsert.map((u) => u.codigo)))
        : [];
      const ugIdMap = new Map(ugsDb.map((ug) => [ug.codigo, ug.id]));

      // 4. Entidade Receitas
      const receitasToInsert = bronzeData
        .map((b) => {
          const exercicio = b.exercicio || b.exercicioReferencia || (b.dataLancamento ? parseDate(b.dataLancamento)?.substring(0, 4) : null);
          if (!exercicio) return null;

          const unidadeGestoraId = b.codigoUnidadeGestora ? ugIdMap.get(b.codigoUnidadeGestora) || null : null;

          return {
            exercicio: exercicio.substring(0, 4),
            unidadeGestoraId,
            categoriaEconomica: b.categoriaEconomica || null,
            origem: b.origem || null,
            detalhamento: b.detalhamento || null,
            valorPrevisto: parseDecimal(b.valorPrevisto) || '0',
            valorLancado: parseDecimal(b.valorLancado) || '0',
            valorRealizado: parseDecimal(b.valorRealizado) || '0',
            percentualRealizado: parseDecimal(b.percentualRealizado) || '0',
            dataLancamento: parseDate(b.dataLancamento) || null,
          };
        })
        .filter(Boolean) as typeof receitas.$inferInsert[];

      if (receitasToInsert.length > 0) {
        const exerciciosLote = [...new Set(receitasToInsert.map((r) => r.exercicio))];
        const ugIdsLote = [...new Set(receitasToInsert.map((r) => r.unidadeGestoraId).filter(Boolean) as number[])];

        if (exerciciosLote.length > 0 && ugIdsLote.length > 0) {
          await silver_db.delete(receitas)
            .where(
              and(
                inArray(receitas.exercicio, exerciciosLote),
                inArray(receitas.unidadeGestoraId, ugIdsLote)
              )
            );
        }

        log.info({ data: { count: receitasToInsert.length } }, 'Inserindo receitas');
        for (let i = 0; i < receitasToInsert.length; i += 1000) {
          await silver_db.insert(receitas).values(receitasToInsert.slice(i, i + 1000));
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento de Receitas');
    throw error;
  }
}