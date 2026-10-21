import { sql, inArray, and, eq } from 'drizzle-orm';
import { bronze_db } from '../../../database/bronze_connection.js';
import { silver_db } from '../../../database/silver_connection.js';
import { CartaoPagamentoCpgf } from '../../../database/bronze_schema.js';
import {
  cartaoPagamentoCpgf,
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

function getDocumentType(doc?: string | null): 'PF' | 'PJ' | null {
  if (!doc || doc.trim().toUpperCase() === 'SIGILOSO' || doc.trim().toUpperCase() === 'NÃO APLICÁVEL') return null;
  const numbers = doc.replace(/\D/g, '');
  if (numbers.length === 11) return 'PF';
  if (numbers.length > 11) return 'PJ';
  return null;
}

export async function processCartaoPagamentoCpgf() {
  const log = logger.forMethod('processCartaoPagamentoCpgf');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento do módulo CPGF (Cartão de Pagamento do Governo Federal)');

  try {
    while (hasMore) {
      // 1. Extração da Camada Bronze
      const bronzeData = await bronze_db.select()
        .from(CartaoPagamentoCpgf)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        log.info({ data: { offset } }, 'Processamento concluído. Nenhum registro adicional encontrado.');
        hasMore = false;
        break;
      }

      log.info({ data: { offset, batchSize: bronzeData.length } }, 'Lote extraído da tabela cartao_pagamento_cpgf');

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

      // 3. Dimensão Pessoas Físicas (Portadores e Favorecidos PF)
      const pfsToUpsertMap = new Map<string, typeof pessoasFisicas.$inferInsert>();

      bronzeData.forEach(b => {
        const cpfPortador = formatCpf(b.cpfPortador);
        if (cpfPortador) {
          pfsToUpsertMap.set(cpfPortador, {
            cpf: cpfPortador,
            nome: b.nomePortador || 'NÃO INFORMADO',
          });
        }

        if (getDocumentType(b.documentoFavorecido) === 'PF') {
          const cpfFavorecido = formatCpf(b.documentoFavorecido);
          if (cpfFavorecido) {
            // Preserva o nome do portador se houver conflito de CPF (mesma pessoa), ou adiciona o favorecido
            if (!pfsToUpsertMap.has(cpfFavorecido)) {
              pfsToUpsertMap.set(cpfFavorecido, {
                cpf: cpfFavorecido,
                nome: b.nomeFavorecido || 'NÃO INFORMADO',
              });
            }
          }
        }
      });

      const pfToInsertList = Array.from(pfsToUpsertMap.values());
      const cpfsLote = pfToInsertList.map(p => p.cpf);

      const cpfsExistentesDb = cpfsLote.length > 0
        ? await silver_db.select({ id: pessoasFisicas.id, cpf: pessoasFisicas.cpf })
          .from(pessoasFisicas)
          .where(inArray(pessoasFisicas.cpf, cpfsLote))
        : [];

      const cpfIdMap = new Map(cpfsExistentesDb.map(pf => [pf.cpf, pf.id]));
      const cpfsExistentesSet = new Set(cpfsExistentesDb.map(pf => pf.cpf));

      const pfToInsert = pfToInsertList.filter(p => !cpfsExistentesSet.has(p.cpf));

      if (pfToInsert.length > 0) {
        await silver_db.insert(pessoasFisicas).values(pfToInsert);

        const novosPfsDb = await silver_db.select({ id: pessoasFisicas.id, cpf: pessoasFisicas.cpf })
          .from(pessoasFisicas)
          .where(inArray(pessoasFisicas.cpf, pfToInsert.map(p => p.cpf)));

        novosPfsDb.forEach(pf => cpfIdMap.set(pf.cpf, pf.id));
      }

      // 4. Dimensão Pessoas Jurídicas (Favorecidos PJ)
      const pjsToUpsertMap = new Map<string, typeof pessoasJuridicas.$inferInsert>();
      bronzeData.forEach((b) => {
        if (getDocumentType(b.documentoFavorecido) === 'PJ') {
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

      // 5. Entidade de Pagamentos CPGF
      const pagamentosToInsert = bronzeData
        .map(b => {
          const unidadeGestoraId = b.codigoUnidadeGestora ? ugIdMap.get(b.codigoUnidadeGestora) : null;
          const cpfPortador = formatCpf(b.cpfPortador);
          const portadorId = cpfPortador ? cpfIdMap.get(cpfPortador) : null;

          let favorecidoPfId = null;
          let favorecidoPjId = null;

          const docType = getDocumentType(b.documentoFavorecido);
          if (docType === 'PF') {
            const cpfFavorecido = formatCpf(b.documentoFavorecido);
            favorecidoPfId = cpfFavorecido ? cpfIdMap.get(cpfFavorecido) : null;
          } else if (docType === 'PJ') {
            const cnpjFavorecido = formatCnpj(b.documentoFavorecido);
            favorecidoPjId = cnpjFavorecido ? pjIdMap.get(cnpjFavorecido) : null;
          }

          const dataTransacao = parseDate(b.dataTransacao);
          const valorTransacao = parseDecimal(b.valorTransacao);

          if (!unidadeGestoraId || !portadorId || !b.anoExtrato || !b.mesExtrato || !b.transacao || !dataTransacao || valorTransacao === null) return null;

          return {
            unidadeGestoraId,
            portadorId,
            favorecidoPfId: favorecidoPfId || null,
            favorecidoPjId: favorecidoPjId || null,
            anoExtrato: b.anoExtrato,
            mesExtrato: b.mesExtrato.padStart(2, '0'),
            transacao: b.transacao,
            dataTransacao,
            valorTransacao,
          };
        })
        .filter(Boolean) as typeof cartaoPagamentoCpgf.$inferInsert[];

      if (pagamentosToInsert.length > 0) {
        // Garantir UPSERT seguro deletando as transações do mesmo ano/mês para a mesma UG extraída
        // para manter idempotência (pois a tabela não possui UK forte unindo todos os campos transacionais).
        const anoMesUgIds = pagamentosToInsert.map(p => `${p.anoExtrato}-${p.mesExtrato}-${p.unidadeGestoraId}`);
        const uniqueAnoMesUgIds = [...new Set(anoMesUgIds)];

        for (const item of uniqueAnoMesUgIds) {
          const [ano, mes, ugId] = item.split('-');
          await silver_db.delete(cartaoPagamentoCpgf)
            .where(
              and(
                eq(cartaoPagamentoCpgf.anoExtrato, ano),
                eq(cartaoPagamentoCpgf.mesExtrato, mes),
                eq(cartaoPagamentoCpgf.unidadeGestoraId, Number(ugId))
              )
            );
        }

        log.info({ data: { count: pagamentosToInsert.length } }, 'Inserindo cartao_pagamento_cpgf');
        for (let i = 0; i < pagamentosToInsert.length; i += 1000) {
          await silver_db.insert(cartaoPagamentoCpgf).values(pagamentosToInsert.slice(i, i + 1000));
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento do CPGF');
    throw error;
  }
}