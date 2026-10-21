import { sql, inArray, and, eq } from 'drizzle-orm';
import { bronze_db } from '../../../database/bronze_connection.js';
import { silver_db } from '../../../database/silver_connection.js';
import { CartaoPagamentoDefesaCivil } from '../../../database/bronze_schema.js';
import { 
  cartaoPagamentoDefesaCivil, 
  convenios, 
  pessoasFisicas, 
  pessoasJuridicas, 
  estados 
} from '../../../database/silver_schema.js';
import { formatCnpj, parseDate, parseDecimal } from '../shared/formatters.js';
import { new_logger } from '../../../services/logs.js';

const logger = new_logger(import.meta.url);

function formatCpf(cpf?: string | null): string | null {
  if (!cpf || cpf.trim() === '' || cpf.trim().toUpperCase() === 'SIGILOSO' || cpf.trim().toUpperCase() === 'NÃO APLICÁVEL') return null;
  const numbers = cpf.replace(/\D/g, '');
  return numbers.length > 0 ? numbers.padStart(11, '0') : null;
}

export async function processCartaoPagamentoDefesaCivil() {
  const log = logger.forMethod('processCartaoPagamentoDefesaCivil');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento do módulo CPDC (Cartão de Pagamento da Defesa Civil)');

  try {
    while (hasMore) {
      // 1. Extração da Camada Bronze
      const bronzeData = await bronze_db.select()
        .from(CartaoPagamentoDefesaCivil)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        log.info({ data: { offset } }, 'Processamento concluído. Nenhum registro adicional encontrado.');
        hasMore = false;
        break;
      }

      log.info({ data: { offset, batchSize: bronzeData.length } }, 'Lote extraído da tabela cartao_pagamento_defesa_civil');

      // 2. Dimensão Estados (UFs de referência)
      // Presume-se que a dimensão estados já esteja preenchida como domínio imutável.
      // Apenas pegamos as siglas válidas do lote.
      const ufsLote = [...new Set(bronzeData.map(b => b.uf ? b.uf.substring(0, 2).toUpperCase() : null).filter(Boolean) as string[])];
      const estadosDb = ufsLote.length > 0 
        ? await silver_db.select({ sigla: estados.sigla }).from(estados).where(inArray(estados.sigla, ufsLote))
        : [];
      const ufsValidas = new Set(estadosDb.map(e => e.sigla));

      // 3. Dimensão Pessoas Jurídicas (Convenente e Favorecido)
      const pjsToUpsertMap = new Map<string, typeof pessoasJuridicas.$inferInsert>();
      bronzeData.forEach((b) => {
        const cnpjConvenente = formatCnpj(b.codigoConvenente);
        if (cnpjConvenente) {
          pjsToUpsertMap.set(cnpjConvenente, { 
            cnpj: cnpjConvenente, 
            razaoSocial: b.nomeConvenente || 'NÃO INFORMADO' 
          });
        }

        const numDocFav = (b.documentoFavorecido || '').replace(/\D/g, '');
        if (numDocFav.length > 11) {
          const cnpjFavorecido = formatCnpj(b.documentoFavorecido);
          if (cnpjFavorecido) {
            pjsToUpsertMap.set(cnpjFavorecido, { 
              cnpj: cnpjFavorecido, 
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

      // 4. Dimensão Convênios
      // Inserimos placeholders mínimos se o convênio for citado no CPDC mas não existir na tabela mestra.
      const conveniosToUpsertMap = new Map<string, typeof convenios.$inferInsert>();
      bronzeData.forEach((b) => {
        if (b.numeroConvenio) {
          const convenenteCnpj = formatCnpj(b.codigoConvenente);
          const convenenteId = convenenteCnpj ? pjIdMap.get(convenenteCnpj) : null;
          
          if (convenenteId) {
            conveniosToUpsertMap.set(b.numeroConvenio, {
              numeroConvenio: b.numeroConvenio,
              convenenteId,
              objeto: 'CONVÊNIO DA DEFESA CIVIL (CARGA CPDC)',
              valorConvenio: '0', 
            });
          }
        }
      });
      const conveniosToUpsert = Array.from(conveniosToUpsertMap.values());

      if (conveniosToUpsert.length > 0) {
        await silver_db.insert(convenios)
          .values(conveniosToUpsert)
          .onDuplicateKeyUpdate({ set: { id: sql`id` } });
      }

      const conveniosDb = conveniosToUpsert.length > 0
        ? await silver_db.select({ id: convenios.id, numeroConvenio: convenios.numeroConvenio })
            .from(convenios)
            .where(inArray(convenios.numeroConvenio, conveniosToUpsert.map(c => c.numeroConvenio)))
        : [];
      const convenioIdMap = new Map(conveniosDb.map(c => [c.numeroConvenio, c.id]));

      // 5. Dimensão Pessoas Físicas (Portadores)
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

      // 6. Entidade de Pagamentos CPDC
      const pagamentosToInsert = bronzeData
        .map(b => {
          const uf = b.uf ? b.uf.substring(0, 2).toUpperCase() : null;
          const ufSigla = uf && ufsValidas.has(uf) ? uf : null;
          
          const convenioId = b.numeroConvenio ? convenioIdMap.get(b.numeroConvenio) : null;
          
          const cnpjConvenente = formatCnpj(b.codigoConvenente);
          const convenenteId = cnpjConvenente ? pjIdMap.get(cnpjConvenente) : null;

          const cpfPortador = formatCpf(b.cpfPortador);
          const portadorId = cpfPortador ? cpfIdMap.get(cpfPortador) : null;
          
          let favorecidoPjId = null;
          const numDocFav = (b.documentoFavorecido || '').replace(/\D/g, '');
          if (numDocFav.length > 11) {
            const cnpjFormatado = formatCnpj(b.documentoFavorecido);
            favorecidoPjId = cnpjFormatado ? pjIdMap.get(cnpjFormatado) : null;
          }

          const dataTransacao = parseDate(b.dataTransacao);
          const valorTransacao = parseDecimal(b.valorTransacao);
          
          if (!portadorId || !b.anoExtrato || !b.mesExtrato || !b.transacao || !dataTransacao || valorTransacao === null) return null;

          return {
            ufSigla,
            convenioId: convenioId || null,
            convenenteId: convenenteId || null,
            portadorId,
            favorecidoPjId: favorecidoPjId || null,
            anoExtrato: b.anoExtrato,
            mesExtrato: b.mesExtrato.padStart(2, '0'),
            transacao: b.transacao,
            dataTransacao,
            valorTransacao,
          };
        })
        .filter(Boolean) as typeof cartaoPagamentoDefesaCivil.$inferInsert[];

      if (pagamentosToInsert.length > 0) {
        // Garantir UPSERT seguro deletando as transações do mesmo ano/mês para o mesmo convênio/portador extraído
        // para manter idempotência sem chaves compostas pesadas no schema.
        const anoMesConvIds = pagamentosToInsert
          .filter(p => p.convenioId)
          .map(p => `${p.anoExtrato}-${p.mesExtrato}-${p.convenioId}`);
        const uniqueAnoMesConvIds = [...new Set(anoMesConvIds)];
        
        for (const item of uniqueAnoMesConvIds) {
          const [ano, mes, convId] = item.split('-');
          await silver_db.delete(cartaoPagamentoDefesaCivil)
            .where(
              and(
                eq(cartaoPagamentoDefesaCivil.anoExtrato, ano),
                eq(cartaoPagamentoDefesaCivil.mesExtrato, mes),
                eq(cartaoPagamentoDefesaCivil.convenioId, Number(convId))
              )
            );
        }

        log.info({ data: { count: pagamentosToInsert.length } }, 'Inserindo cartao_pagamento_defesa_civil');
        for (let i = 0; i < pagamentosToInsert.length; i += 1000) {
          await silver_db.insert(cartaoPagamentoDefesaCivil).values(pagamentosToInsert.slice(i, i + 1000));
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento do CPDC');
    throw error;
  }
}