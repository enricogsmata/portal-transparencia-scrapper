import { sql, inArray, and, eq } from 'drizzle-orm';
import { bronze_db } from '../../../database/bronze_connection.js';
import { silver_db } from '../../../database/silver_connection.js';
import { RecebimentoRecursosFavorecido } from '../../../database/bronze_schema.js';
import { 
  despesasPagamentos,
  pessoasFisicas,
  pessoasJuridicas,
  unidadesGestoras,
  municipios,
  estados
} from '../../../database/silver_schema.js';
import { formatAnoMes, formatCnpj, formatCpf, getDocumentType, parseDecimal } from '../shared/formatters.js';
import { new_logger } from '../../../services/logs.js';

const logger = new_logger(import.meta.url);

export async function processRecebimentoRecursosFavorecido() {
  const log = logger.forMethod('processRecebimentoRecursosFavorecido');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento do módulo Despesas - Favorecidos (Recebimentos agregados)');

  try {
    while (hasMore) {
      // 1. Extração da Camada Bronze
      const bronzeData = await bronze_db.select()
        .from(RecebimentoRecursosFavorecido)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        log.info({ data: { offset } }, 'Processamento concluído. Nenhum registro adicional encontrado.');
        hasMore = false;
        break;
      }

      log.info({ data: { offset, batchSize: bronzeData.length } }, 'Lote extraído da tabela recebimento_recursos_favorecido');

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
          .onDuplicateKeyUpdate({ set: { nome: sql`VALUES(nome)` } });
      }

      const ugsDb = ugsToUpsert.length > 0
        ? await silver_db.select({ id: unidadesGestoras.id, codigo: unidadesGestoras.codigo })
            .from(unidadesGestoras)
            .where(inArray(unidadesGestoras.codigo, ugsToUpsert.map(u => u.codigo)))
        : [];
      const ugIdMap = new Map(ugsDb.map(ug => [ug.codigo, ug.id]));

      // 3. Dimensão Municípios (Baseado em Nome e UF, pois não há código SIAFI/IBGE direto aqui)
      const ufsLote = [...new Set(bronzeData.map(b => b.siglaUf ? b.siglaUf.substring(0, 2).toUpperCase() : null).filter(Boolean) as string[])];
      const estadosDb = ufsLote.length > 0 
        ? await silver_db.select({ sigla: estados.sigla }).from(estados).where(inArray(estados.sigla, ufsLote))
        : [];
      const ufsValidas = new Set(estadosDb.map(e => e.sigla));

      const munToUpsertMap = new Map<string, typeof municipios.$inferInsert>();
      bronzeData.forEach(b => {
        if (b.nomeMunicipio && b.siglaUf && ufsValidas.has(b.siglaUf.substring(0, 2).toUpperCase())) {
          const nomeMun = b.nomeMunicipio;
          const ufSigla = b.siglaUf.substring(0, 2).toUpperCase();
          const key = `${nomeMun.toUpperCase()}-${ufSigla}`;
          munToUpsertMap.set(key, { nome: nomeMun, ufSigla });
        }
      });
      
      const munToInsertList = Array.from(munToUpsertMap.values());
      if (munToInsertList.length > 0) {
        await silver_db.insert(municipios).values(munToInsertList).onDuplicateKeyUpdate({ set: { id: sql`id` } });
      }

      const municipiosDb = munToInsertList.length > 0
        ? await silver_db.select({ id: municipios.id, nome: municipios.nome, ufSigla: municipios.ufSigla })
            .from(municipios)
            .where(inArray(municipios.nome, munToInsertList.map(m => m.nome)))
        : [];
      const munIdMap = new Map(municipiosDb.map(m => [`${m.nome.toUpperCase()}-${m.ufSigla.toUpperCase()}`, m.id]));

      // 4. Dimensão Pessoas Físicas e Jurídicas (Favorecidos)
      const pfMap = new Map<string, typeof pessoasFisicas.$inferInsert>();
      const pjMap = new Map<string, typeof pessoasJuridicas.$inferInsert>();
      
      bronzeData.forEach(b => {
        const type = getDocumentType(b.codigoFavorecido);
        const municipioId = (b.nomeMunicipio && b.siglaUf) ? munIdMap.get(`${b.nomeMunicipio.toUpperCase()}-${b.siglaUf.substring(0, 2).toUpperCase()}`) : null;
        
        if (type === 'PF') {
          const cpf = formatCpf(b.codigoFavorecido)!;
          pfMap.set(cpf, { cpf, nome: b.nomeFavorecido || 'NÃO INFORMADO', municipioId: municipioId || null });
        } else if (type === 'PJ') {
          const cnpj = formatCnpj(b.codigoFavorecido)!;
          pjMap.set(cnpj, { cnpj, razaoSocial: b.nomeFavorecido || 'NÃO INFORMADO', municipioId: municipioId || null });
        }
      });

      // Upsert PJ
      const pjToUpsert = Array.from(pjMap.values());
      if (pjToUpsert.length > 0) {
        await silver_db.insert(pessoasJuridicas).values(pjToUpsert).onDuplicateKeyUpdate({set: {razaoSocial: sql`VALUES(razao_social)`}});
      }

      // Insert PF
      const pfToUpsert = Array.from(pfMap.values());
      if (pfToUpsert.length > 0) {
         const exists = await silver_db.select({cpf: pessoasFisicas.cpf}).from(pessoasFisicas).where(inArray(pessoasFisicas.cpf, pfToUpsert.map(p=>p.cpf)));
         const eSet = new Set(exists.map(e => e.cpf));
         const toInsert = pfToUpsert.filter(p => !eSet.has(p.cpf));
         if (toInsert.length > 0) await silver_db.insert(pessoasFisicas).values(toInsert);
      }

      const pfDb = pfToUpsert.length > 0 ? await silver_db.select({ id: pessoasFisicas.id, cpf: pessoasFisicas.cpf }).from(pessoasFisicas).where(inArray(pessoasFisicas.cpf, pfToUpsert.map(p => p.cpf))) : [];
      const pjDb = pjToUpsert.length > 0 ? await silver_db.select({ id: pessoasJuridicas.id, cnpj: pessoasJuridicas.cnpj }).from(pessoasJuridicas).where(inArray(pessoasJuridicas.cnpj, pjToUpsert.map(p => p.cnpj))) : [];
      const pfIdMap = new Map(pfDb.map(p => [p.cpf, p.id]));
      const pjIdMap = new Map(pjDb.map(p => [p.cnpj, p.id]));

      // 5. Entidade Despesas Pagamentos (Agregação sintética)
      // Como a tabela de pagamentos exige data_emissao e codigo_pagamento (UK), vamos gerar um código sintético baseado no anoMes e favorecido.
      const pagamentosToInsert = bronzeData.map(b => {
        const anoMesLancamento = formatAnoMes(b.anoMesLancamento);
        const unidadeGestoraId = b.codigoUnidadeGestora ? ugIdMap.get(b.codigoUnidadeGestora) : null;
        
        if (!anoMesLancamento || !unidadeGestoraId || !b.codigoFavorecido) return null;

        const docType = getDocumentType(b.codigoFavorecido);
        let favorecidoPfId = null;
        let favorecidoPjId = null;

        if (docType === 'PF') {
           favorecidoPfId = pfIdMap.get(formatCpf(b.codigoFavorecido)!) || null;
        } else if (docType === 'PJ') {
           favorecidoPjId = pjIdMap.get(formatCnpj(b.codigoFavorecido)!) || null;
        }

        if (!favorecidoPfId && !favorecidoPjId) return null;

        // Gerando código de pagamento sintético para agregar recebimentos mensais
        const codigoFavorecidoSintetico = docType === 'PF' ? formatCpf(b.codigoFavorecido) : formatCnpj(b.codigoFavorecido);
        const codigoPagamento = `REC-${anoMesLancamento}-${b.codigoUnidadeGestora}-${codigoFavorecidoSintetico}`;
        
        // Data de emissão assumida como 1º dia do mês de lançamento para cumprir NOT NULL
        const dataEmissao = `${anoMesLancamento}-01`;

        return {
          codigoPagamento,
          dataEmissao,
          unidadeGestoraId,
          favorecidoPfId,
          favorecidoPjId,
          valorOriginalPagamento: parseDecimal(b.valorRecebido) || '0',
          exercicioReferencia: b.exercicioReferencia || anoMesLancamento.substring(0, 4),
          observacao: 'RECEBIMENTO AGREGADO MENSAL',
        };
      }).filter(Boolean) as typeof despesasPagamentos.$inferInsert[];

      if (pagamentosToInsert.length > 0) {
        // Garantindo unicidade no lote
        const uniqueMap = new Map<string, typeof pagamentosToInsert[0]>();
        for (const p of pagamentosToInsert) uniqueMap.set(p.codigoPagamento, p);
        const uniquePagamentos = Array.from(uniqueMap.values());

        // Deleção segura por código sintético gerado (idempotência)
        const codigosPagamentos = uniquePagamentos.map(p => p.codigoPagamento);
        await silver_db.delete(despesasPagamentos)
          .where(inArray(despesasPagamentos.codigoPagamento, codigosPagamentos));

        log.info({ data: { count: uniquePagamentos.length } }, 'Inserindo despesas_pagamentos (Recebimentos por Favorecido)');
        for (let i = 0; i < uniquePagamentos.length; i += 1000) {
          await silver_db.insert(despesasPagamentos).values(uniquePagamentos.slice(i, i + 1000));
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento de Despesas - Favorecidos (Recebimentos)');
    throw error;
  }
}