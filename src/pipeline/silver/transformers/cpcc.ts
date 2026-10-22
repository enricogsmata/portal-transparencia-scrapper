import { sql, inArray, and, eq } from 'drizzle-orm';
import { bronze_db } from '../../../database/bronze_connection.js';
import { silver_db } from '../../../database/silver_connection.js';
import { CartaoPagamentoCpcc } from '../../../database/bronze_schema.js';
import {
  cartaoPagamentoCpcc,
  unidadesGestoras,
  pessoasFisicas,
  pessoasJuridicas
} from '../../../database/silver_schema.js';
import { formatCnpj, parseDate, parseDecimal } from '../shared/formatters.js';
import { new_logger } from '../../../services/logs.js';

const logger = new_logger(import.meta.url);

function formatCpf(cpf?: string | null): string | null {
  if (!cpf || cpf.trim() === '' || cpf.trim().toUpperCase() === 'SIGILOSO' || cpf.trim().toUpperCase() === 'NÃO APLICÁVEL') return null;
  const numbers = cpf.replace(/\D/g, '');
  return numbers.length > 0 ? numbers.padStart(11, '0') : null;
}

export async function processCartaoPagamentoCpcc() {
  const log = logger.forMethod('processCartaoPagamentoCpcc');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento do módulo CPCC (Cartão de Pagamento de Compras Centralizadas)');

  try {
    while (hasMore) {
      // 1. Extração da Camada Bronze
      const bronzeData = await bronze_db.select()
        .from(CartaoPagamentoCpcc)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        log.info({ data: { offset } }, 'Processamento concluído. Nenhum registro adicional encontrado.');
        hasMore = false;
        break;
      }

      log.info({ data: { offset, batchSize: bronzeData.length } }, 'Lote extraído da tabela cartao_pagamento_cpcc');

      // 2. Dimensão Unidades Gestoras
      const ugsToUpsertMap = new Map<string, typeof unidadesGestoras.$inferInsert>();
      bronzeData.forEach((b) => {
        if (b.codigoUnidadeGestora) {
          ugsToUpsertMap.set(b.codigoUnidadeGestora, {
            codigo: b.codigoUnidadeGestora,
            nome: b.nomeUnidadeGestora || 'NÃO INFORMADO'
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
          .where(inArray(unidadesGestoras.codigo, ugsToUpsert.map(u => u.codigo)))
        : [];
      const ugIdMap = new Map(ugsDb.map(ug => [ug.codigo, ug.id]));

      // 3. Dimensão Pessoas Físicas (Portadores)
      const cpfsLote = [...new Set(bronzeData.map(b => formatCpf(b.cpfPortador)).filter(Boolean) as string[])];
      const cpfsExistentesDb = cpfsLote.length > 0
        ? await silver_db.select({ id: pessoasFisicas.id, cpf: pessoasFisicas.cpf })
          .from(pessoasFisicas)
          .where(inArray(pessoasFisicas.cpf, cpfsLote))
        : [];

      const cpfIdMap = new Map(cpfsExistentesDb.map(pf => [pf.cpf, pf.id]));
      const cpfsExistentesSet = new Set(cpfsExistentesDb.map(pf => pf.cpf));

      const pfToInsert = bronzeData
        .filter(b => b.cpfPortador && !cpfsExistentesSet.has(formatCpf(b.cpfPortador)!))
        .map(b => ({
          cpf: formatCpf(b.cpfPortador)!,
          nome: b.nomePortador || 'NÃO INFORMADO',
        }));

      if (pfToInsert.length > 0) {
        const uniquePfMap = new Map<string, typeof pfToInsert[0]>();
        for (const pf of pfToInsert) uniquePfMap.set(pf.cpf, pf);
        const uniquePfs = Array.from(uniquePfMap.values());

        await silver_db.insert(pessoasFisicas).values(uniquePfs);

        const novosPfsDb = await silver_db.select({ id: pessoasFisicas.id, cpf: pessoasFisicas.cpf })
          .from(pessoasFisicas)
          .where(inArray(pessoasFisicas.cpf, uniquePfs.map(p => p.cpf)));

        novosPfsDb.forEach(pf => cpfIdMap.set(pf.cpf, pf.id));
      }

      // 4. Dimensão Pessoas Jurídicas (Favorecidos)
      // Favorecidos de compras centralizadas tendem a ser empresas (PJs)
      const pjsToUpsertMap = new Map<string, typeof pessoasJuridicas.$inferInsert>();
      bronzeData.forEach((b) => {
        // Filtrar apenas se parecer CNPJ
        const numbers = (b.documentoFavorecido || '').replace(/\D/g, '');
        if (numbers.length > 11) {
          const cnpj = formatCnpj(b.documentoFavorecido);
          if (cnpj) {
            pjsToUpsertMap.set(cnpj, {
              cnpj,
              razaoSocial: b.nomeFavorecido || 'NÃO INFORMADO'
            });
          }
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

      // 5. Entidade de Pagamentos CPCC
      const pagamentosToInsert = bronzeData
        .map(b => {
          const unidadeGestoraId = b.codigoUnidadeGestora ? ugIdMap.get(b.codigoUnidadeGestora) : null;
          const cpfPortador = formatCpf(b.cpfPortador);
          const portadorId = cpfPortador ? cpfIdMap.get(cpfPortador) : null;

          let favorecidoPjId = null;
          const numDoc = (b.documentoFavorecido || '').replace(/\D/g, '');
          if (numDoc.length > 11) {
            const cnpjFormatado = formatCnpj(b.documentoFavorecido);
            favorecidoPjId = cnpjFormatado ? pjIdMap.get(cnpjFormatado) : null;
          }

          const dataTransacao = parseDate(b.dataTransacao);
          const valorTransacao = parseDecimal(b.valorTransacao);

          if (!unidadeGestoraId || !b.anoExtrato || !b.mesExtrato || !b.transacao || !dataTransacao || valorTransacao === null) return null;

          return {
            unidadeGestoraId,
            portadorId: portadorId || null,
            favorecidoPjId: favorecidoPjId || null,
            anoExtrato: b.anoExtrato,
            mesExtrato: b.mesExtrato.padStart(2, '0'),
            transacao: b.transacao,
            dataTransacao,
            valorTransacao,
          };
        })
        .filter(Boolean) as typeof cartaoPagamentoCpcc.$inferInsert[];

      if (pagamentosToInsert.length > 0) {
        // Garantir UPSERT seguro deletando as transações do mesmo ano/mês para a mesma UG extraída
        // para manter idempotência (pois a tabela não possui UK forte unindo todos os campos transacionais).
        const anoMesUgIds = pagamentosToInsert.map(p => `${p.anoExtrato}-${p.mesExtrato}-${p.unidadeGestoraId}`);
        const uniqueAnoMesUgIds = [...new Set(anoMesUgIds)];

        for (const item of uniqueAnoMesUgIds) {
          const [ano, mes, ugId] = item.split('-');
          await silver_db.delete(cartaoPagamentoCpcc)
            .where(
              and(
                eq(cartaoPagamentoCpcc.anoExtrato, ano),
                eq(cartaoPagamentoCpcc.mesExtrato, mes),
                eq(cartaoPagamentoCpcc.unidadeGestoraId, Number(ugId))
              )
            );
        }

        log.info({ data: { count: pagamentosToInsert.length } }, 'Inserindo cartao_pagamento_cpcc');
        for (let i = 0; i < pagamentosToInsert.length; i += 1000) {
          await silver_db.insert(cartaoPagamentoCpcc).values(pagamentosToInsert.slice(i, i + 1000));
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento do CPCC');
    throw error;
  }
}