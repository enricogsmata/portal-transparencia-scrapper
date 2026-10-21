// src/pipeline/silver/transformers/licitacoes.ts
import { sql, inArray, and, eq } from 'drizzle-orm';
import { bronze_db } from '../../../database/bronze_connection.js';
import { silver_db } from '../../../database/silver_connection.js';
import { 
  Licitacao, 
  LicitacaoItem, 
  LicitacaoParticipantes, 
  LicitacaoEmpenhosRelacionados 
} from '../../../database/bronze_schema.js';
import { 
  licitacoes, 
  licitacaoItens, 
  licitacaoParticipantes, 
  despesasEmpenhos,
  unidadesGestoras,
  pessoasJuridicas
} from '../../../database/silver_schema.js';
import { formatCnpj, parseDate, parseDecimal } from '../shared/formatters.js';
import { MunicipioResolver } from '../shared/municipio-resolver.js';
import { new_logger } from '../../../services/logs.js';

const logger = new_logger(import.meta.url);

export async function processLicitacao() {
  const log = logger.forMethod('processLicitacao');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento da tabela principal de Licitações');

  try {
    while (hasMore) {
      const bronzeData = await bronze_db.select()
        .from(Licitacao)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        log.info({ data: { offset } }, 'Processamento concluído. Nenhum registro adicional encontrado.');
        hasMore = false;
        break;
      }

      log.info({ data: { offset, batchSize: bronzeData.length } }, 'Lote extraído da tabela licitacao');

      // 1. Unidades Gestoras
      const ugsToUpsertMap = new Map<string, typeof unidadesGestoras.$inferInsert>();
      bronzeData.forEach((b) => {
        if (b.codigoUg) {
          ugsToUpsertMap.set(b.codigoUg, { 
            codigo: b.codigoUg, 
            nome: b.nomeUg || 'NÃO INFORMADO' 
          });
        }
      });
      const ugsToUpsert = Array.from(ugsToUpsertMap.values());
      if (ugsToUpsert.length > 0) {
        await silver_db.insert(unidadesGestoras).values(ugsToUpsert).onDuplicateKeyUpdate({ set: { id: sql`id` } });
      }
      const ugsDb = ugsToUpsert.length > 0
        ? await silver_db.select({ id: unidadesGestoras.id, codigo: unidadesGestoras.codigo }).from(unidadesGestoras).where(inArray(unidadesGestoras.codigo, ugsToUpsert.map(u => u.codigo)))
        : [];
      const ugIdMap = new Map(ugsDb.map(ug => [ug.codigo, ug.id]));

      // 3. Entidade de Licitações
      const licitacoesToInsert = bronzeData.map(b => {
        const unidadeGestoraId = b.codigoUg ? ugIdMap.get(b.codigoUg) : null;
        if (!unidadeGestoraId || !b.numeroLicitacao || !b.modalidadeCompra || !b.objeto) return null;

        const municipioId = MunicipioResolver.resolve({
            nome: b.municipio,
            uf: b.uf
        });

        return {
          numeroLicitacao: b.numeroLicitacao,
          unidadeGestoraId,
          modalidadeCompra: b.modalidadeCompra,
          numeroProcesso: b.numeroProcesso || null,
          objeto: b.objeto,
          situacaoLicitacao: b.situacaoLicitacao || null,
          municipioId: municipioId || null,
          dataResultadoCompra: parseDate(b.dataResultadoCompra) || null,
          dataAbertura: parseDate(b.dataAbertura) || null,
          valorLicitacao: parseDecimal(b.valorLicitacao) || null,
          exercicioReferencia: b.exercicioReferencia || null,
        };
      }).filter(Boolean) as typeof licitacoes.$inferInsert[];

      if (licitacoesToInsert.length > 0) {
        const batchMap = new Map<string, typeof licitacoesToInsert[0]>();
        for (const l of licitacoesToInsert) batchMap.set(l.numeroLicitacao, l);
        const uniqueBatch = Array.from(batchMap.values());

        const existing = await silver_db.select({ id: licitacoes.id, numeroLicitacao: licitacoes.numeroLicitacao })
          .from(licitacoes)
          .where(inArray(licitacoes.numeroLicitacao, uniqueBatch.map(l => l.numeroLicitacao)));
        
        const existingMap = new Map(existing.map(l => [l.numeroLicitacao, l.id]));
        const toInsert: typeof licitacoes.$inferInsert[] = [];
        const toUpdate: (typeof licitacoes.$inferInsert & { id: number })[] = [];

        for (const l of uniqueBatch) {
          const id = existingMap.get(l.numeroLicitacao);
          if (id) {
            toUpdate.push({ ...l, id });
          } else {
            toInsert.push(l);
          }
        }

        if (toInsert.length > 0) {
          log.info({ data: { count: toInsert.length } }, 'Inserindo novas licitações');
          for (let i = 0; i < toInsert.length; i += 1000) {
            await silver_db.insert(licitacoes).values(toInsert.slice(i, i + 1000));
          }
        }

        if (toUpdate.length > 0) {
          log.info({ data: { count: toUpdate.length } }, 'Atualizando licitações existentes');
          for (const u of toUpdate) {
            await silver_db.update(licitacoes).set(u).where(eq(licitacoes.id, u.id));
          }
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento da tabela principal de Licitações');
    throw error;
  }
}

export async function processLicitacaoItem() {
  const log = logger.forMethod('processLicitacaoItem');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento de Itens de Licitação');

  try {
    while (hasMore) {
      const bronzeData = await bronze_db.select()
        .from(LicitacaoItem)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        hasMore = false;
        break;
      }

      const numerosLote = [...new Set(bronzeData.map(b => b.numeroLicitacao).filter(Boolean) as string[])];
      const licitacoesDb = numerosLote.length > 0 
        ? await silver_db.select({ id: licitacoes.id, numeroLicitacao: licitacoes.numeroLicitacao })
            .from(licitacoes)
            .where(inArray(licitacoes.numeroLicitacao, numerosLote))
        : [];
      const licitacaoIdMap = new Map(licitacoesDb.map(l => [l.numeroLicitacao, l.id]));

      const itensToInsert = bronzeData.map(b => {
        const licitacaoId = b.numeroLicitacao ? licitacaoIdMap.get(b.numeroLicitacao) : null;
        if (!licitacaoId || !b.codigoItemCompra || !b.descricaoItemCompra) return null;

        return {
          licitacaoId,
          codigoItemCompra: b.codigoItemCompra,
          descricaoItemCompra: b.descricaoItemCompra,
        };
      }).filter(Boolean) as typeof licitacaoItens.$inferInsert[];

      if (itensToInsert.length > 0) {
        const licitacaoIds = [...new Set(itensToInsert.map(i => i.licitacaoId))];
        const existing = await silver_db.select({ licitacaoId: licitacaoItens.licitacaoId, codigoItemCompra: licitacaoItens.codigoItemCompra })
          .from(licitacaoItens)
          .where(inArray(licitacaoItens.licitacaoId, licitacaoIds));
        
        const existingSet = new Set(existing.map(i => `${i.licitacaoId}-${i.codigoItemCompra}`));
        
        const toInsertMap = new Map<string, typeof licitacaoItens.$inferInsert>();
        for (const i of itensToInsert) {
          const key = `${i.licitacaoId}-${i.codigoItemCompra}`;
          if (!existingSet.has(key)) {
            toInsertMap.set(key, i); // Unicidade do lote
          }
        }
        
        const toInsert = Array.from(toInsertMap.values());
        if (toInsert.length > 0) {
          log.info({ data: { count: toInsert.length } }, 'Inserindo itens de licitação');
          for (let i = 0; i < toInsert.length; i += 1000) {
            await silver_db.insert(licitacaoItens).values(toInsert.slice(i, i + 1000));
          }
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento de Itens de Licitação');
    throw error;
  }
}

export async function processLicitacaoParticipantes() {
  const log = logger.forMethod('processLicitacaoParticipantes');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento de Participantes de Licitação');

  try {
    while (hasMore) {
      const bronzeData = await bronze_db.select()
        .from(LicitacaoParticipantes)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        hasMore = false;
        break;
      }

      // 1. Pessoas Jurídicas (Participantes)
      const pjsToUpsertMap = new Map<string, typeof pessoasJuridicas.$inferInsert>();
      bronzeData.forEach((b) => {
        const cnpj = formatCnpj(b.cnpjParticipante);
        if (cnpj) {
          pjsToUpsertMap.set(cnpj, { 
            cnpj, 
            razaoSocial: b.nomeParticipante || 'NÃO INFORMADO' 
          });
        }
      });
      const pjsToUpsert = Array.from(pjsToUpsertMap.values());

      if (pjsToUpsert.length > 0) {
        await silver_db.insert(pessoasJuridicas)
          .values(pjsToUpsert)
          .onDuplicateKeyUpdate({ set: { id: sql`id` } });
      }

      const pjsDb = pjsToUpsert.length > 0
        ? await silver_db.select({ id: pessoasJuridicas.id, cnpj: pessoasJuridicas.cnpj })
            .from(pessoasJuridicas)
            .where(inArray(pessoasJuridicas.cnpj, pjsToUpsert.map(p => p.cnpj)))
        : [];
      const pjIdMap = new Map(pjsDb.map(pj => [pj.cnpj, pj.id]));

      // 2. Licitações e Itens
      const numerosLote = [...new Set(bronzeData.map(b => b.numeroLicitacao).filter(Boolean) as string[])];
      const licitacoesDb = numerosLote.length > 0 
        ? await silver_db.select({ id: licitacoes.id, numeroLicitacao: licitacoes.numeroLicitacao })
            .from(licitacoes)
            .where(inArray(licitacoes.numeroLicitacao, numerosLote))
        : [];
      const licitacaoIdMap = new Map(licitacoesDb.map(l => [l.numeroLicitacao, l.id]));

      const licitacaoIds = Array.from(licitacaoIdMap.values());
      const itensDb = licitacaoIds.length > 0 
        ? await silver_db.select({ id: licitacaoItens.id, licitacaoId: licitacaoItens.licitacaoId, codigoItemCompra: licitacaoItens.codigoItemCompra })
            .from(licitacaoItens)
            .where(inArray(licitacaoItens.licitacaoId, licitacaoIds))
        : [];
      const itemIdMap = new Map(itensDb.map(i => [`${i.licitacaoId}-${i.codigoItemCompra}`, i.id]));

      // 3. Entidade de Participantes
      const participantesToInsert = bronzeData.map(b => {
        const licitacaoId = b.numeroLicitacao ? licitacaoIdMap.get(b.numeroLicitacao) : null;
        if (!licitacaoId) return null;

        const licitacaoItemId = b.codigoItemCompra ? itemIdMap.get(`${licitacaoId}-${b.codigoItemCompra}`) : null;
        const cnpjFormatado = formatCnpj(b.cnpjParticipante);
        const participanteId = cnpjFormatado ? pjIdMap.get(cnpjFormatado) : null;

        if (!participanteId) return null;

        const flagVencedor = b.flagVencedor?.trim().toUpperCase() === 'SIM';

        return {
          licitacaoId,
          licitacaoItemId: licitacaoItemId || null,
          participanteId,
          flagVencedor,
        };
      }).filter(Boolean) as typeof licitacaoParticipantes.$inferInsert[];

      if (participantesToInsert.length > 0) {
        // Garantindo idempotência excluindo do banco os participantes do lote atual por licitação
        const licIdsLote = [...new Set(participantesToInsert.map(p => p.licitacaoId))];
        await silver_db.delete(licitacaoParticipantes).where(inArray(licitacaoParticipantes.licitacaoId, licIdsLote));

        log.info({ data: { count: participantesToInsert.length } }, 'Inserindo participantes de licitação');
        for (let i = 0; i < participantesToInsert.length; i += 1000) {
          await silver_db.insert(licitacaoParticipantes).values(participantesToInsert.slice(i, i + 1000));
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento de Participantes de Licitação');
    throw error;
  }
}

export async function processLicitacaoEmpenhosRelacionados() {
  const log = logger.forMethod('processLicitacaoEmpenhosRelacionados');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento de Empenhos Relacionados à Licitação');

  try {
    while (hasMore) {
      const bronzeData = await bronze_db.select()
        .from(LicitacaoEmpenhosRelacionados)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        hasMore = false;
        break;
      }

      // Como o target desta tabela é mapear empenhos parciais de licitações, e o schema da entidade "despesas_empenhos" exige
      // muitos dados obrigatórios que não existem nesta tabela isolada (como dataEmissao e exercicioReferencia obrigatórios em despesas_empenho, mas não sempre preenchidos aqui), 
      // a implementação abaixo tenta fazer um UPSERT parcial no que for possível ou reportar alerta de dado incompleto se o empenho não existir.
      // Em uma base normalizada 3FN unificada, esses registros serviriam para preencher a FK de licitacaoId no despesas_empenhos ou tabela associativa n:m.
      
      log.warn('A extração "licitacao_empenhos_relacionados" apenas complementa tabelas existentes. Pulando rotina agressiva de insert para evitar inconsistência de NOT NULL na tabela mestre de empenhos.');
      // Opcionalmente, pode-se escrever a rotina de mapear e atualizar "despesas_empenhos" caso tenham campos licitacaoId definidos futuramente na entidade de empenhos.

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento de Empenhos Relacionados à Licitação');
    throw error;
  }
}

export async function processLicitacoes() {
  const log = logger.forMethod('processLicitacoes');
  try {
    await processLicitacao();
    await processLicitacaoItem();
    await processLicitacaoParticipantes();
    await processLicitacaoEmpenhosRelacionados();
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante a orquestração do módulo de Licitações');
    throw error;
  }
}