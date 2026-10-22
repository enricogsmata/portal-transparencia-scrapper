// src/pipeline/silver/transformers/execucao-historico.ts
//
// Bronze: execucao_por_natureza, execucao_por_programa
// Silver: execucao_por_natureza, execucao_por_programa (NOVAS — ver silver_schema.historicos.patch.ts)
//
// Por que não execucao_despesas: o grão aqui é Unidade Orçamentária x mês x (natureza | programa),
// sem UG/ação/município e sem valor empenhado. Forçar nessa tabela geraria empenhado = 0 e
// dupla contagem da execução já carregada de execucao_despesa.
//
// Idempotência: a chave natural (hash gerado pelo MySQL) deduplica; o mesmo período reaparece em
// snapshots diferentes do bronze e o ÚLTIMO (maior id) vence, por isso a leitura é ordenada por id.

import { sql, inArray } from 'drizzle-orm';
import { bronze_db } from '../../../database/bronze_connection.js';
import { silver_db } from '../../../database/silver_connection.js';
import { ExecucaoPorNatureza, ExecucaoPorPrograma } from '../../../database/bronze_schema.js';
import {
  execucaoPorNatureza,
  execucaoPorPrograma,
  programasOrcamentarios,
  categoriasDespesa,
  gruposDespesa,
  modalidadesAplicacao,
  elementosDespesa,
} from '../../../database/silver_schema.js';
import { parseDecimal } from '../shared/formatters.js';
import { chunk, ensureUnidadesOrcamentarias, nz, soDigitos, toAnoMes } from '../shared/historicos.js';
import { new_logger } from '../../../services/logs.js';

const logger = new_logger(import.meta.url);
const BATCH_SIZE = 1000;

/**
 * Natureza da despesa = C G MM EE [SS]  (ex.: 33903000 => cat 3, grupo 3, modalidade 90, elemento 30).
 * Aceita máscara (3.3.90.30) e códigos truncados (só categoria, categoria+grupo...).
 */
function parseNatureza(codigo?: string | null) {
  const d = soDigitos(codigo);
  return {
    categoria: d.length >= 1 ? d.slice(0, 1) : null,
    grupo: d.length >= 2 ? d.slice(1, 2) : null,
    modalidade: d.length >= 4 ? d.slice(2, 4) : null,
    elemento: d.length >= 6 ? d.slice(4, 6) : null,
  };
}

/** Dimensões de natureza são pequenas: carrega uma vez por execução. Só lookup, nunca cria. */
async function carregarDimensoesNatureza() {
  const [cats, grupos, mods, elems] = await Promise.all([
    silver_db.select({ id: categoriasDespesa.id, c: categoriasDespesa.codigo }).from(categoriasDespesa),
    silver_db.select({ id: gruposDespesa.id, c: gruposDespesa.codigo }).from(gruposDespesa),
    silver_db.select({ id: modalidadesAplicacao.id, c: modalidadesAplicacao.codigo }).from(modalidadesAplicacao),
    silver_db.select({ id: elementosDespesa.id, c: elementosDespesa.codigo }).from(elementosDespesa),
  ]);
  return {
    cat: new Map(cats.map((r) => [r.c, r.id])),
    grupo: new Map(grupos.map((r) => [r.c, r.id])),
    mod: new Map(mods.map((r) => [r.c, r.id])),
    elem: new Map(elems.map((r) => [r.c, r.id])),
  };
}

export async function processExecucaoPorNatureza() {
  const log = logger.forMethod('processExecucaoPorNatureza');
  let offset = 0;
  let descartadas = 0;

  log.info('Iniciando processamento de Execução por Natureza');

  try {
    const dim = await carregarDimensoesNatureza();

    while (true) {
      const bronzeData = await bronze_db
        .select()
        .from(ExecucaoPorNatureza)
        .orderBy(ExecucaoPorNatureza.id)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        log.info({ data: { offset, descartadas } }, 'Processamento concluído.');
        break;
      }

      const uoIdMap = await ensureUnidadesOrcamentarias(bronzeData);

      const rowsMap = new Map<string, typeof execucaoPorNatureza.$inferInsert>();
      for (const b of bronzeData) {
        const anoMes = toAnoMes(b.anoExecucao, b.mesExecucao);
        const natureza = b.codigoNaturezaDespesa?.trim();
        if (!anoMes || !natureza) {
          descartadas++;
          continue;
        }
        const uoId = b.codigoUnidadeOrcamentaria ? uoIdMap.get(b.codigoUnidadeOrcamentaria) ?? null : null;
        const p = parseNatureza(natureza);

        // mesma composição da chave natural do banco; o último registro do lote vence
        rowsMap.set(`${anoMes}|${uoId ?? 0}|${natureza.toUpperCase()}`, {
          anoMes,
          unidadeOrcamentariaId: uoId,
          codigoNaturezaDespesa: natureza,
          descricaoNaturezaDespesa: b.descricaoNaturezaDespesa || null,
          categoriaDespesaId: p.categoria ? dim.cat.get(p.categoria) ?? null : null,
          grupoDespesaId: p.grupo ? dim.grupo.get(p.grupo) ?? null : null,
          modalidadeAplicacaoId: p.modalidade ? dim.mod.get(p.modalidade) ?? null : null,
          elementoDespesaId: p.elemento ? dim.elem.get(p.elemento) ?? null : null,
          valorLiquidado: nz(parseDecimal(b.valorLiquidado)),
          valorPago: nz(parseDecimal(b.valorPago)),
          exercicioReferencia: b.anoExecucao!.trim().substring(0, 4),
        });
      }

      for (const part of chunk(Array.from(rowsMap.values()))) {
        await silver_db
          .insert(execucaoPorNatureza)
          .values(part)
          .onDuplicateKeyUpdate({
            set: {
              descricaoNaturezaDespesa: sql`VALUES(descricao_natureza_despesa)`,
              categoriaDespesaId: sql`VALUES(categoria_despesa_id)`,
              grupoDespesaId: sql`VALUES(grupo_despesa_id)`,
              modalidadeAplicacaoId: sql`VALUES(modalidade_aplicacao_id)`,
              elementoDespesaId: sql`VALUES(elemento_despesa_id)`,
              valorLiquidado: sql`VALUES(valor_liquidado)`,
              valorPago: sql`VALUES(valor_pago)`,
            },
          });
      }

      log.info({ data: { offset, gravadas: rowsMap.size } }, 'Lote de execução por natureza processado');
      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento de Execução por Natureza');
    throw error;
  }
}

export async function processExecucaoPorPrograma() {
  const log = logger.forMethod('processExecucaoPorPrograma');
  let offset = 0;
  let descartadas = 0;

  log.info('Iniciando processamento de Execução por Programa');

  try {
    while (true) {
      const bronzeData = await bronze_db
        .select()
        .from(ExecucaoPorPrograma)
        .orderBy(ExecucaoPorPrograma.id)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        log.info({ data: { offset, descartadas } }, 'Processamento concluído.');
        break;
      }

      const uoIdMap = await ensureUnidadesOrcamentarias(bronzeData);

      // Programas
      const progMap = new Map<string, typeof programasOrcamentarios.$inferInsert>();
      for (const b of bronzeData) {
        if (b.codigoPrograma) {
          progMap.set(b.codigoPrograma, { codigo: b.codigoPrograma, nome: b.nomePrograma || 'NÃO INFORMADO' });
        }
      }
      const progs = Array.from(progMap.values());
      for (const part of chunk(progs)) {
        await silver_db
          .insert(programasOrcamentarios)
          .values(part)
          .onDuplicateKeyUpdate({ set: { nome: sql`VALUES(nome)` } });
      }
      const progDb = progs.length
        ? await silver_db
            .select({ id: programasOrcamentarios.id, codigo: programasOrcamentarios.codigo })
            .from(programasOrcamentarios)
            .where(inArray(programasOrcamentarios.codigo, progs.map((p) => p.codigo)))
        : [];
      const progIdMap = new Map(progDb.map((p) => [p.codigo, p.id]));

      const rowsMap = new Map<string, typeof execucaoPorPrograma.$inferInsert>();
      for (const b of bronzeData) {
        const anoMes = toAnoMes(b.anoExecucao, b.mesExecucao);
        if (!anoMes || !b.codigoPrograma) {
          descartadas++;
          continue;
        }
        const uoId = b.codigoUnidadeOrcamentaria ? uoIdMap.get(b.codigoUnidadeOrcamentaria) ?? null : null;
        const programaId = progIdMap.get(b.codigoPrograma) ?? null;

        rowsMap.set(`${anoMes}|${uoId ?? 0}|${programaId ?? 0}`, {
          anoMes,
          unidadeOrcamentariaId: uoId,
          programaId,
          valorAtualizado: parseDecimal(b.valorAtualizado) || null,
          valorLiquidado: nz(parseDecimal(b.valorLiquidado)),
          valorPago: nz(parseDecimal(b.valorPago)),
          exercicioReferencia: b.anoExecucao!.trim().substring(0, 4),
        });
      }

      for (const part of chunk(Array.from(rowsMap.values()))) {
        await silver_db
          .insert(execucaoPorPrograma)
          .values(part)
          .onDuplicateKeyUpdate({
            set: {
              valorAtualizado: sql`VALUES(valor_atualizado)`,
              valorLiquidado: sql`VALUES(valor_liquidado)`,
              valorPago: sql`VALUES(valor_pago)`,
            },
          });
      }

      log.info({ data: { offset, gravadas: rowsMap.size } }, 'Lote de execução por programa processado');
      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento de Execução por Programa');
    throw error;
  }
}

export async function processExecucaoHistorico() {
  const log = logger.forMethod('processExecucaoHistorico');
  try {
    await processExecucaoPorNatureza();
    await processExecucaoPorPrograma();
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante a orquestração de Execução (histórico)');
    throw error;
  }
}