// src/pipeline/silver/transformers/emendas-parlamentares.ts
import { sql, inArray } from 'drizzle-orm';
import { bronze_db } from '../../../database/bronze_connection.js';
import { silver_db } from '../../../database/silver_connection.js';
import {
  EmendasParlamentares,
  EmendasParlamentaresConvenios,
  EmendasParlamentaresFavorecido
} from '../../../database/bronze_schema.js';
import {
  emendasParlamentares,
  emendasParlamentaresConvenios,
  emendasParlamentaresFavorecidos,
  funcoesOrcamentarias,
  subfuncoesOrcamentarias,
  programasOrcamentarios,
  acoesOrcamentarias,
  planosOrcamentarios,
  pessoasFisicas,
  pessoasJuridicas,
  convenios
} from '../../../database/silver_schema.js';
import { formatCnpj, getDocumentType, formatCpf, parseDecimal, parseDate, formatAnoMes } from '../shared/formatters.js';
import { MunicipioResolver } from '../shared/municipio-resolver.js';
import { new_logger } from '../../../services/logs.js';
import { BATCH_SIZE } from '../shared/constants.js';

const logger = new_logger(import.meta.url);

export async function processEmendasParlamentares() {
  const log = logger.forMethod('processEmendasParlamentares');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento de Emendas Parlamentares (Entidade Principal)');

  try {
    while (hasMore) {
      const bronzeData = await bronze_db.select()
        .from(EmendasParlamentares)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        log.info({ data: { offset } }, 'Processamento concluído. Nenhum registro adicional encontrado.');
        hasMore = false;
        break;
      }

      // 1. Funções
      const funToUpsertMap = new Map<string, typeof funcoesOrcamentarias.$inferInsert>();
      bronzeData.forEach(b => { if (b.codigoFuncao) funToUpsertMap.set(b.codigoFuncao, { codigo: b.codigoFuncao, nome: b.nomeFuncao || 'NÃO INFORMADO' }); });
      const funToUpsert = Array.from(funToUpsertMap.values());
      if (funToUpsert.length > 0) await silver_db.insert(funcoesOrcamentarias).values(funToUpsert).onDuplicateKeyUpdate({ set: { nome: sql`VALUES(nome)` } });
      const funDb = funToUpsert.length > 0 ? await silver_db.select({ id: funcoesOrcamentarias.id, codigo: funcoesOrcamentarias.codigo }).from(funcoesOrcamentarias).where(inArray(funcoesOrcamentarias.codigo, funToUpsert.map(f => f.codigo))) : [];
      const funIdMap = new Map(funDb.map(f => [f.codigo, f.id]));

      // 2. Subfunções
      const sfunToUpsertMap = new Map<string, typeof subfuncoesOrcamentarias.$inferInsert>();
      bronzeData.forEach(b => { if (b.codigoSubfuncao) sfunToUpsertMap.set(b.codigoSubfuncao, { codigo: b.codigoSubfuncao, nome: b.nomeSubfuncao || 'NÃO INFORMADO' }); });
      const sfunToUpsert = Array.from(sfunToUpsertMap.values());
      if (sfunToUpsert.length > 0) await silver_db.insert(subfuncoesOrcamentarias).values(sfunToUpsert).onDuplicateKeyUpdate({ set: { nome: sql`VALUES(nome)` } });
      const sfunDb = sfunToUpsert.length > 0 ? await silver_db.select({ id: subfuncoesOrcamentarias.id, codigo: subfuncoesOrcamentarias.codigo }).from(subfuncoesOrcamentarias).where(inArray(subfuncoesOrcamentarias.codigo, sfunToUpsert.map(f => f.codigo))) : [];
      const sfunIdMap = new Map(sfunDb.map(f => [f.codigo, f.id]));

      // 3. Programas
      const progToUpsertMap = new Map<string, typeof programasOrcamentarios.$inferInsert>();
      bronzeData.forEach(b => { if (b.codigoPrograma) progToUpsertMap.set(b.codigoPrograma, { codigo: b.codigoPrograma, nome: b.nomePrograma || 'NÃO INFORMADO' }); });
      const progToUpsert = Array.from(progToUpsertMap.values());
      if (progToUpsert.length > 0) await silver_db.insert(programasOrcamentarios).values(progToUpsert).onDuplicateKeyUpdate({ set: { nome: sql`VALUES(nome)` } });
      const progDb = progToUpsert.length > 0 ? await silver_db.select({ id: programasOrcamentarios.id, codigo: programasOrcamentarios.codigo }).from(programasOrcamentarios).where(inArray(programasOrcamentarios.codigo, progToUpsert.map(f => f.codigo))) : [];
      const progIdMap = new Map(progDb.map(f => [f.codigo, f.id]));

      // 4. Ações
      const acaoToUpsertMap = new Map<string, typeof acoesOrcamentarias.$inferInsert>();
      bronzeData.forEach(b => {
        if (b.codigoAcao) {
          const programaId = b.codigoPrograma ? progIdMap.get(b.codigoPrograma) : null;
          acaoToUpsertMap.set(b.codigoAcao, { codigo: b.codigoAcao, nome: b.nomeAcao || 'NÃO INFORMADO', programaId: programaId || null, linguagemCidada: null });
        }
      });
      const acaoToUpsert = Array.from(acaoToUpsertMap.values());
      if (acaoToUpsert.length > 0) {
        const existAcoes = await silver_db.select({ id: acoesOrcamentarias.id, codigo: acoesOrcamentarias.codigo }).from(acoesOrcamentarias).where(inArray(acoesOrcamentarias.codigo, acaoToUpsert.map(a => a.codigo)));
        const acoesMapSet = new Map(existAcoes.map(a => [a.codigo, a.id]));
        const acaoToInsert = acaoToUpsert.filter(a => !acoesMapSet.has(a.codigo));
        if (acaoToInsert.length > 0) await silver_db.insert(acoesOrcamentarias).values(acaoToInsert);
      }
      const acaoDb = acaoToUpsert.length > 0 ? await silver_db.select({ id: acoesOrcamentarias.id, codigo: acoesOrcamentarias.codigo }).from(acoesOrcamentarias).where(inArray(acoesOrcamentarias.codigo, acaoToUpsert.map(a => a.codigo))) : [];
      const acaoIdMap = new Map(acaoDb.map(a => [a.codigo, a.id]));

      // 5. Planos Orcamentarios
      const poToUpsertMap = new Map<string, typeof planosOrcamentarios.$inferInsert>();
      bronzeData.forEach(b => { if (b.codigoPlanoOrcamentario) poToUpsertMap.set(b.codigoPlanoOrcamentario, { codigo: b.codigoPlanoOrcamentario, nome: b.nomePlanoOrcamentario || 'NÃO INFORMADO' }); });
      const poToUpsert = Array.from(poToUpsertMap.values());
      if (poToUpsert.length > 0) await silver_db.insert(planosOrcamentarios).values(poToUpsert).onDuplicateKeyUpdate({ set: { nome: sql`VALUES(nome)` } });
      const poDb = poToUpsert.length > 0 ? await silver_db.select({ id: planosOrcamentarios.id, codigo: planosOrcamentarios.codigo }).from(planosOrcamentarios).where(inArray(planosOrcamentarios.codigo, poToUpsert.map(f => f.codigo))) : [];
      const poIdMap = new Map(poDb.map(f => [f.codigo, f.id]));

      // 7. Entidade Principal (emendas_parlamentares)
      const emendasToUpsert = bronzeData.map(b => {
        if (!b.codigoEmenda || !b.anoEmenda || !b.tipoEmenda || !b.nomeAutorEmenda) return null;

        const municipioId = MunicipioResolver.resolve({
          ibge: b.codigoMunicipioIbge,
          nome: b.municipio,
          uf: b.uf,
        });

        return {
          codigoEmenda: b.codigoEmenda,
          anoEmenda: b.anoEmenda,
          tipoEmenda: b.tipoEmenda,
          autorEmenda: b.nomeAutorEmenda,
          numeroEmenda: b.numeroEmenda || null,
          municipioId: municipioId || null,
          funcaoId: b.codigoFuncao ? funIdMap.get(b.codigoFuncao) || null : null,
          subfuncaoId: b.codigoSubfuncao ? sfunIdMap.get(b.codigoSubfuncao) || null : null,
          programaId: b.codigoPrograma ? progIdMap.get(b.codigoPrograma) || null : null,
          acaoId: b.codigoAcao ? acaoIdMap.get(b.codigoAcao) || null : null,
          planoOrcamentarioId: b.codigoPlanoOrcamentario ? poIdMap.get(b.codigoPlanoOrcamentario) || null : null,
          valorEmpenhado: parseDecimal(b.valorEmpenhado) || '0',
          valorLiquidado: parseDecimal(b.valorLiquidado) || '0',
          valorPago: parseDecimal(b.valorPago) || '0',
          valorRestosAPagarInscritos: parseDecimal(b.valorRestosAPagarInscritos) || null,
          valorRestosAPagarCancelados: parseDecimal(b.valorRestosAPagarCancelados) || null,
          valorRestosAPagarPagos: parseDecimal(b.valorRestosAPagarPagos) || null,
        };
      }).filter(Boolean) as typeof emendasParlamentares.$inferInsert[];

      if (emendasToUpsert.length > 0) {
        // Garantindo unicidade no lote atual
        const batchMap = new Map<string, typeof emendasToUpsert[0]>();
        for (const e of emendasToUpsert) batchMap.set(e.codigoEmenda, e);
        const uniqueBatch = Array.from(batchMap.values());

        log.info({ data: { count: uniqueBatch.length } }, 'Inserindo/Atualizando emendas_parlamentares');
        for (let i = 0; i < uniqueBatch.length; i += 1000) {
          await silver_db.insert(emendasParlamentares)
            .values(uniqueBatch.slice(i, i + 1000))
            .onDuplicateKeyUpdate({
              set: {
                valorEmpenhado: sql`VALUES(valor_empenhado)`,
                valorLiquidado: sql`VALUES(valor_liquidado)`,
                valorPago: sql`VALUES(valor_pago)`,
                valorRestosAPagarInscritos: sql`VALUES(valor_restos_a_pagar_inscritos)`,
                valorRestosAPagarCancelados: sql`VALUES(valor_restos_a_pagar_cancelados)`,
                valorRestosAPagarPagos: sql`VALUES(valor_restos_a_pagar_pagos)`
              }
            });
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento de Emendas Parlamentares');
    throw error;
  }
}

export async function processEmendasParlamentaresConvenios() {
  const log = logger.forMethod('processEmendasParlamentaresConvenios');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento de Emendas Parlamentares - Convênios');

  try {
    while (hasMore) {
      const bronzeData = await bronze_db.select()
        .from(EmendasParlamentaresConvenios)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        hasMore = false;
        break;
      }

      // Funções
      const funToUpsertMap = new Map<string, typeof funcoesOrcamentarias.$inferInsert>();
      bronzeData.forEach(b => { if (b.codigoFuncao) funToUpsertMap.set(b.codigoFuncao, { codigo: b.codigoFuncao, nome: b.nomeFuncao || 'NÃO INFORMADO' }); });
      const funToUpsert = Array.from(funToUpsertMap.values());
      if (funToUpsert.length > 0) await silver_db.insert(funcoesOrcamentarias).values(funToUpsert).onDuplicateKeyUpdate({ set: { nome: sql`VALUES(nome)` } });
      const funDb = funToUpsert.length > 0 ? await silver_db.select({ id: funcoesOrcamentarias.id, codigo: funcoesOrcamentarias.codigo }).from(funcoesOrcamentarias).where(inArray(funcoesOrcamentarias.codigo, funToUpsert.map(f => f.codigo))) : [];
      const funIdMap = new Map(funDb.map(f => [f.codigo, f.id]));

      // Subfunções
      const sfunToUpsertMap = new Map<string, typeof subfuncoesOrcamentarias.$inferInsert>();
      bronzeData.forEach(b => { if (b.codigoSubfuncao) sfunToUpsertMap.set(b.codigoSubfuncao, { codigo: b.codigoSubfuncao, nome: b.nomeSubfuncao || 'NÃO INFORMADO' }); });
      const sfunToUpsert = Array.from(sfunToUpsertMap.values());
      if (sfunToUpsert.length > 0) await silver_db.insert(subfuncoesOrcamentarias).values(sfunToUpsert).onDuplicateKeyUpdate({ set: { nome: sql`VALUES(nome)` } });
      const sfunDb = sfunToUpsert.length > 0 ? await silver_db.select({ id: subfuncoesOrcamentarias.id, codigo: subfuncoesOrcamentarias.codigo }).from(subfuncoesOrcamentarias).where(inArray(subfuncoesOrcamentarias.codigo, sfunToUpsert.map(f => f.codigo))) : [];
      const sfunIdMap = new Map(sfunDb.map(f => [f.codigo, f.id]));

      const emendasLote = [...new Set(bronzeData.map(b => b.codigoEmenda).filter(Boolean) as string[])];
      const emendasDb = emendasLote.length > 0 ? await silver_db.select({ id: emendasParlamentares.id, codigoEmenda: emendasParlamentares.codigoEmenda }).from(emendasParlamentares).where(inArray(emendasParlamentares.codigoEmenda, emendasLote)) : [];
      const emendaIdMap = new Map(emendasDb.map(e => [e.codigoEmenda, e.id]));

      const conveniosLote = [...new Set(bronzeData.map(b => b.numeroConvenio).filter(Boolean) as string[])];
      const conveniosDb = conveniosLote.length > 0 ? await silver_db.select({ id: convenios.id, numeroConvenio: convenios.numeroConvenio }).from(convenios).where(inArray(convenios.numeroConvenio, conveniosLote)) : [];
      const convenioIdMap = new Map(conveniosDb.map(c => [c.numeroConvenio, c.id]));

      const epcToInsert = bronzeData.map(b => {
        const emendaId = b.codigoEmenda ? emendaIdMap.get(b.codigoEmenda) : null;
        if (!emendaId) return null;

        const exRef = b.exercicioReferencia || '0000';

        return {
          emendaId,
          convenioId: b.numeroConvenio ? convenioIdMap.get(b.numeroConvenio) || null : null,
          funcaoId: b.codigoFuncao ? funIdMap.get(b.codigoFuncao) || null : null,
          subfuncaoId: b.codigoSubfuncao ? sfunIdMap.get(b.codigoSubfuncao) || null : null,
          localidadeGasto: b.localidadeGasto || null,
          dataPublicacaoConvenio: parseDate(b.dataPublicacaoConvenio) || null,
          valorConvenio: parseDecimal(b.valorConvenio) || '0',
          exercicioReferencia: exRef,
        };
      }).filter(Boolean) as typeof emendasParlamentaresConvenios.$inferInsert[];

      if (epcToInsert.length > 0) {
        // Idempotência por emenda no lote atual (substituição limpa)
        const emendaIds = [...new Set(epcToInsert.map(e => e.emendaId))];
        await silver_db.delete(emendasParlamentaresConvenios).where(inArray(emendasParlamentaresConvenios.emendaId, emendaIds));

        log.info({ data: { count: epcToInsert.length } }, 'Inserindo emendas_parlamentares_convenios');
        for (let i = 0; i < epcToInsert.length; i += 1000) {
          await silver_db.insert(emendasParlamentaresConvenios).values(epcToInsert.slice(i, i + 1000));
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento de Emendas Parlamentares - Convênios');
    throw error;
  }
}

export async function processEmendasParlamentaresFavorecido() {
  const log = logger.forMethod('processEmendasParlamentaresFavorecido');
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento de Emendas Parlamentares - Favorecidos');

  try {
    while (hasMore) {
      const bronzeData = await bronze_db.select()
        .from(EmendasParlamentaresFavorecido)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        hasMore = false;
        break;
      }

      const pfMap = new Map<string, typeof pessoasFisicas.$inferInsert>();
      const pjMap = new Map<string, typeof pessoasJuridicas.$inferInsert>();

      bronzeData.forEach(b => {
        const type = getDocumentType(b.codigoFavorecido);
        if (type === 'PF') {
          const cpf = formatCpf(b.codigoFavorecido)!;
          pfMap.set(cpf, { cpf, nome: b.favorecido || 'NÃO INFORMADO' });
        } else if (type === 'PJ') {
          const cnpj = formatCnpj(b.codigoFavorecido)!;
          pjMap.set(cnpj, { cnpj, razaoSocial: b.favorecido || 'NÃO INFORMADO' });
        }
      });

      if (pjMap.size > 0) {
        await silver_db.insert(pessoasJuridicas).values(Array.from(pjMap.values())).onDuplicateKeyUpdate({ set: { id: sql`id` } });
      }

      if (pfMap.size > 0) {
        const exists = await silver_db.select({ cpf: pessoasFisicas.cpf }).from(pessoasFisicas).where(inArray(pessoasFisicas.cpf, Array.from(pfMap.keys())));
        const eSet = new Set(exists.map(e => e.cpf));
        const toInsert = Array.from(pfMap.values()).filter(p => !eSet.has(p.cpf));
        if (toInsert.length > 0) await silver_db.insert(pessoasFisicas).values(toInsert);
      }

      const pfDb = pfMap.size > 0 ? await silver_db.select({ id: pessoasFisicas.id, cpf: pessoasFisicas.cpf }).from(pessoasFisicas).where(inArray(pessoasFisicas.cpf, Array.from(pfMap.keys()))) : [];
      const pjDb = pjMap.size > 0 ? await silver_db.select({ id: pessoasJuridicas.id, cnpj: pessoasJuridicas.cnpj }).from(pessoasJuridicas).where(inArray(pessoasJuridicas.cnpj, Array.from(pjMap.keys()))) : [];
      const pfIdMap = new Map(pfDb.map(p => [p.cpf, p.id]));
      const pjIdMap = new Map(pjDb.map(p => [p.cnpj, p.id]));

      const emendasLote = [...new Set(bronzeData.map(b => b.codigoEmenda).filter(Boolean) as string[])];
      const emendasDb = emendasLote.length > 0 ? await silver_db.select({ id: emendasParlamentares.id, codigoEmenda: emendasParlamentares.codigoEmenda }).from(emendasParlamentares).where(inArray(emendasParlamentares.codigoEmenda, emendasLote)) : [];
      const emendaIdMap = new Map(emendasDb.map(e => [e.codigoEmenda, e.id]));

      const favsToInsert = bronzeData.map(b => {
        const emendaId = b.codigoEmenda ? emendaIdMap.get(b.codigoEmenda) : null;
        if (!emendaId) return null;

        const docType = getDocumentType(b.codigoFavorecido);
        let favorecidoPfId = null;
        let favorecidoPjId = null;

        if (docType === 'PF') {
           favorecidoPfId = pfIdMap.get(formatCpf(b.codigoFavorecido)!) || null;
        } else if (docType === 'PJ') {
           favorecidoPjId = pjIdMap.get(formatCnpj(b.codigoFavorecido)!) || null;
        }

        const municipioId = MunicipioResolver.resolve({
            nome: b.municipioFavorecido,
            uf: b.ufFavorecido,
        });

        return {
          emendaId,
          favorecidoPfId,
          favorecidoPjId,
          municipioId: municipioId || null,
          anoMes: formatAnoMes(b.anoMes) || null,
          valorRecebido: parseDecimal(b.valorRecebido) || '0',
          exercicioReferencia: b.exercicioReferencia || '0000',
        };
      }).filter(Boolean) as typeof emendasParlamentaresFavorecidos.$inferInsert[];

      if (favsToInsert.length > 0) {
        // Idempotência por emenda no lote atual
        const emendaIds = [...new Set(favsToInsert.map(f => f.emendaId))];
        await silver_db.delete(emendasParlamentaresFavorecidos).where(inArray(emendasParlamentaresFavorecidos.emendaId, emendaIds));

        log.info({ data: { count: favsToInsert.length } }, 'Inserindo emendas_parlamentares_favorecidos');
        for (let i = 0; i < favsToInsert.length; i += 1000) {
          await silver_db.insert(emendasParlamentaresFavorecidos).values(favsToInsert.slice(i, i + 1000));
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento de Emendas Parlamentares - Favorecidos');
    throw error;
  }
}