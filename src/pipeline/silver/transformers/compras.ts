import { sql, inArray, eq, and } from 'drizzle-orm';
import { bronze_db } from '../../../database/bronze_connection.js';
import { silver_db } from '../../../database/silver_connection.js';
import { 
  ContratosCompras, 
  ContratosItemCompra, 
  ContratosTermoAditivo, 
  ContratosApostilamento 
} from '../../../database/bronze_schema.js';
import { 
  contratos, 
  contratoItens, 
  contratoTermosAditivos, 
  unidadesGestoras, 
  pessoasJuridicas, 
  licitacoes 
} from '../../../database/silver_schema.js';
import { formatCnpj, parseDate, parseDecimal } from '../shared/formatters.js';
import { new_logger } from '../../../services/logs.js';

const logger = new_logger(import.meta.url);

export async function processContratosCompras() {
  const log = logger.forMethod('processContratosCompras');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento de Contratos de Compras');

  try {
    while (hasMore) {
      const bronzeData = await bronze_db.select()
        .from(ContratosCompras)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        log.info({ data: { offset } }, 'Processamento concluído. Nenhum registro adicional encontrado.');
        hasMore = false;
        break;
      }

      log.info({ data: { offset, batchSize: bronzeData.length } }, 'Lote extraído da tabela contratos_compras');

      // 1. Unidades Gestoras
      const ugsToUpsertMap = new Map<string, typeof unidadesGestoras.$inferInsert>();
      bronzeData.forEach((b) => {
        if (b.codigoUg) {
          ugsToUpsertMap.set(b.codigoUg, { codigo: b.codigoUg, nome: b.nomeUg || 'NÃO INFORMADO' });
        }
        if (b.codigoUgLicitacao) {
          ugsToUpsertMap.set(b.codigoUgLicitacao, { codigo: b.codigoUgLicitacao, nome: b.nomeUgLicitacao || 'NÃO INFORMADO' });
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
            .where(inArray(unidadesGestoras.codigo, ugsToUpsert.map(u => u.codigo)))
        : [];
      const ugIdMap = new Map(ugsDb.map(ug => [ug.codigo, ug.id]));

      // 2. Pessoas Jurídicas (Contratados)
      const pjsToUpsertMap = new Map<string, typeof pessoasJuridicas.$inferInsert>();
      bronzeData.forEach((b) => {
        const cnpj = formatCnpj(b.codigoContratado);
        if (cnpj) {
          pjsToUpsertMap.set(cnpj, { cnpj, razaoSocial: b.nomeContratado || 'NÃO INFORMADO' });
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

      // 3. Licitações
      const licitacoesToUpsertMap = new Map<string, typeof licitacoes.$inferInsert>();
      bronzeData.forEach((b) => {
        if (b.numeroLicitacao && b.codigoUgLicitacao) {
          const ugId = ugIdMap.get(b.codigoUgLicitacao);
          if (ugId) {
            licitacoesToUpsertMap.set(b.numeroLicitacao, {
              numeroLicitacao: b.numeroLicitacao,
              unidadeGestoraId: ugId,
              modalidadeCompra: b.modalidadeCompraLicitacao || 'NÃO INFORMADO',
              objeto: 'LICITAÇÃO VINCULADA AO CONTRATO',
            });
          }
        }
      });
      const licitacoesToInsertList = Array.from(licitacoesToUpsertMap.values());

      if (licitacoesToInsertList.length > 0) {
        const existingLicitacoes = await silver_db.select({ id: licitacoes.id, numeroLicitacao: licitacoes.numeroLicitacao })
          .from(licitacoes)
          .where(inArray(licitacoes.numeroLicitacao, licitacoesToInsertList.map(l => l.numeroLicitacao)));
        
        const existingLicSet = new Set(existingLicitacoes.map(l => l.numeroLicitacao));
        const toInsert = licitacoesToInsertList.filter(l => !existingLicSet.has(l.numeroLicitacao));
        
        if (toInsert.length > 0) {
          for (let i = 0; i < toInsert.length; i += 1000) {
            await silver_db.insert(licitacoes).values(toInsert.slice(i, i + 1000));
          }
        }
      }

      const licDb = licitacoesToInsertList.length > 0
        ? await silver_db.select({ id: licitacoes.id, numeroLicitacao: licitacoes.numeroLicitacao })
            .from(licitacoes)
            .where(inArray(licitacoes.numeroLicitacao, licitacoesToInsertList.map(l => l.numeroLicitacao)))
        : [];
      const licIdMap = new Map(licDb.map(l => [l.numeroLicitacao, l.id]));

      // 4. Contratos (Entidade Principal)
      const contratosMapped = bronzeData.map((b) => {
        const unidadeGestoraId = b.codigoUg ? ugIdMap.get(b.codigoUg) : null;
        const contratadoCnpj = formatCnpj(b.codigoContratado);
        const contratadoId = contratadoCnpj ? pjIdMap.get(contratadoCnpj) : null;
        const licitacaoId = b.numeroLicitacao ? licIdMap.get(b.numeroLicitacao) : null;

        if (!unidadeGestoraId || !contratadoId || !b.numeroContrato || !b.objeto) return null;

        return {
          numeroContrato: b.numeroContrato,
          unidadeGestoraId,
          licitacaoId: licitacaoId || null,
          contratadoId,
          objeto: b.objeto,
          fundamentoLegal: b.fundamentoLegal || null,
          modalidadeCompra: b.modalidadeCompra || null,
          situacaoContrato: b.situacaoContrato || null,
          dataAssinatura: parseDate(b.dataAssinaturaContrato) || null,
          dataPublicacaoDou: parseDate(b.dataPublicacaoDou) || null,
          dataInicioVigencia: parseDate(b.dataInicioVigencia) || null,
          dataFimVigencia: parseDate(b.dataFimVigencia) || null,
          valorInicialCompra: parseDecimal(b.valorInicialCompra) || '0',
          valorFinalCompra: parseDecimal(b.valorFinalCompra) || '0',
          exercicioReferencia: b.exercicioReferencia || null,
        };
      }).filter(Boolean) as typeof contratos.$inferInsert[];

      if (contratosMapped.length > 0) {
        // Garantindo unicidade no lote atual
        const batchMap = new Map<string, typeof contratosMapped[0]>();
        for (const c of contratosMapped) batchMap.set(c.numeroContrato, c);
        const uniqueContratosBatch = Array.from(batchMap.values());

        const existingContratos = await silver_db.select({ id: contratos.id, numeroContrato: contratos.numeroContrato })
          .from(contratos)
          .where(inArray(contratos.numeroContrato, uniqueContratosBatch.map(c => c.numeroContrato)));
        
        const existingMap = new Map(existingContratos.map(c => [c.numeroContrato, c.id]));
        const toInsert: typeof contratos.$inferInsert[] = [];
        const toUpdate: (typeof contratos.$inferInsert & { id: number })[] = [];

        for (const c of uniqueContratosBatch) {
          const id = existingMap.get(c.numeroContrato);
          if (id) {
            toUpdate.push({ ...c, id });
          } else {
            toInsert.push(c);
          }
        }

        if (toInsert.length > 0) {
          log.info({ data: { count: toInsert.length } }, 'Inserindo novos contratos');
          for (let i = 0; i < toInsert.length; i += 1000) {
            await silver_db.insert(contratos).values(toInsert.slice(i, i + 1000));
          }
        }

        if (toUpdate.length > 0) {
          log.info({ data: { count: toUpdate.length } }, 'Atualizando contratos existentes');
          for (const u of toUpdate) {
            await silver_db.update(contratos).set(u).where(eq(contratos.id, u.id));
          }
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento de Contratos de Compras');
    throw error;
  }
}

export async function processContratosItemCompra() {
  const log = logger.forMethod('processContratosItemCompra');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento de Itens de Contrato');

  try {
    while (hasMore) {
      const bronzeData = await bronze_db.select()
        .from(ContratosItemCompra)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        log.info({ data: { offset } }, 'Processamento concluído. Nenhum registro adicional encontrado.');
        hasMore = false;
        break;
      }

      const numerosLote = [...new Set(bronzeData.map(b => b.numeroContrato).filter(Boolean) as string[])];
      const contratosDb = numerosLote.length > 0 
        ? await silver_db.select({ id: contratos.id, numeroContrato: contratos.numeroContrato })
            .from(contratos)
            .where(inArray(contratos.numeroContrato, numerosLote))
        : [];
      const contratoIdMap = new Map(contratosDb.map(c => [c.numeroContrato, c.id]));

      const itensToInsertAll = bronzeData.map(b => {
        const contratoId = b.numeroContrato ? contratoIdMap.get(b.numeroContrato) : null;
        if (!contratoId || !b.codigoItemCompra || !b.descricaoItemCompra) return null;
        
        return {
          contratoId,
          codigoItemCompra: b.codigoItemCompra,
          descricaoItemCompra: b.descricaoItemCompra,
          quantidade: parseDecimal(b.quantidadeItem) || '0',
          valorItem: parseDecimal(b.valorItem) || '0',
        };
      }).filter(Boolean) as typeof contratoItens.$inferInsert[];

      if (itensToInsertAll.length > 0) {
        const contratoIds = [...new Set(itensToInsertAll.map(i => i.contratoId))];
        const existing = await silver_db.select({ contratoId: contratoItens.contratoId, codigoItemCompra: contratoItens.codigoItemCompra })
          .from(contratoItens)
          .where(inArray(contratoItens.contratoId, contratoIds));
        
        const existingSet = new Set(existing.map(i => `${i.contratoId}-${i.codigoItemCompra}`));
        
        const toInsertMap = new Map<string, typeof contratoItens.$inferInsert>();
        for (const i of itensToInsertAll) {
          const key = `${i.contratoId}-${i.codigoItemCompra}`;
          if (!existingSet.has(key)) {
            toInsertMap.set(key, i); // Garante unicidade no lote
          }
        }
        
        const toInsert = Array.from(toInsertMap.values());
        if (toInsert.length > 0) {
          log.info({ data: { count: toInsert.length } }, 'Inserindo itens de contrato');
          for (let i = 0; i < toInsert.length; i += 1000) {
            await silver_db.insert(contratoItens).values(toInsert.slice(i, i + 1000));
          }
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento de Itens de Contrato');
    throw error;
  }
}

export async function processContratosTermoAditivo() {
  const log = logger.forMethod('processContratosTermoAditivo');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento de Termos Aditivos');

  try {
    while (hasMore) {
      const bronzeData = await bronze_db.select()
        .from(ContratosTermoAditivo)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        log.info({ data: { offset } }, 'Processamento concluído. Nenhum registro adicional encontrado.');
        hasMore = false;
        break;
      }

      const numerosLote = [...new Set(bronzeData.map(b => b.numeroContrato).filter(Boolean) as string[])];
      const contratosDb = numerosLote.length > 0 
        ? await silver_db.select({ id: contratos.id, numeroContrato: contratos.numeroContrato })
            .from(contratos)
            .where(inArray(contratos.numeroContrato, numerosLote))
        : [];
      const contratoIdMap = new Map(contratosDb.map(c => [c.numeroContrato, c.id]));

      const aditivosToInsertAll = bronzeData.map(b => {
        const contratoId = b.numeroContrato ? contratoIdMap.get(b.numeroContrato) : null;
        const dataPub = parseDate(b.dataPublicacao);
        if (!contratoId || !b.numeroTermoAditivo || !dataPub || !b.objeto) return null;
        
        return {
          contratoId,
          numeroTermoAditivo: b.numeroTermoAditivo,
          dataPublicacao: dataPub,
          objeto: b.objeto,
        };
      }).filter(Boolean) as typeof contratoTermosAditivos.$inferInsert[];

      if (aditivosToInsertAll.length > 0) {
        const contratoIds = [...new Set(aditivosToInsertAll.map(a => a.contratoId))];
        const existing = await silver_db.select({ contratoId: contratoTermosAditivos.contratoId, numeroTermoAditivo: contratoTermosAditivos.numeroTermoAditivo })
          .from(contratoTermosAditivos)
          .where(inArray(contratoTermosAditivos.contratoId, contratoIds));
        
        const existingSet = new Set(existing.map(a => `${a.contratoId}-${a.numeroTermoAditivo}`));
        
        const toInsertMap = new Map<string, typeof contratoTermosAditivos.$inferInsert>();
        for (const a of aditivosToInsertAll) {
          const key = `${a.contratoId}-${a.numeroTermoAditivo}`;
          if (!existingSet.has(key)) {
            toInsertMap.set(key, a);
          }
        }
        
        const toInsert = Array.from(toInsertMap.values());
        if (toInsert.length > 0) {
          log.info({ data: { count: toInsert.length } }, 'Inserindo termos aditivos');
          for (let i = 0; i < toInsert.length; i += 1000) {
            await silver_db.insert(contratoTermosAditivos).values(toInsert.slice(i, i + 1000));
          }
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento de Termos Aditivos');
    throw error;
  }
}

export async function processContratosApostilamento() {
  const log = logger.forMethod('processContratosApostilamento');
  log.warn('Tabela silver para apostilamentos não mapeada nas especificações. Pulando processamento.');
}

export async function processCompras() {
  const log = logger.forMethod('processCompras');
  try {
    await processContratosCompras();
    await processContratosItemCompra();
    await processContratosTermoAditivo();
    await processContratosApostilamento();
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante a orquestração do módulo de Compras');
    throw error;
  }
}