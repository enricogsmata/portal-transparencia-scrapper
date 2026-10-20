import { sql, inArray } from 'drizzle-orm';
import { bronze_db } from '../../../database/bronze_connection.js';
import { silver_db } from '../../../database/silver_connection.js';
import { EmendasParlamentaresDocumentosDespesa } from '../../../database/bronze_schema.js';
import { 
  emendasParlamentares,
  unidadesGestoras,
  pessoasFisicas,
  pessoasJuridicas,
  municipios,
  estados,
  despesasEmpenhos,
  despesasPagamentos
} from '../../../database/silver_schema.js';
import { formatCnpj, formatCpf, getDocumentType, parseDate, parseDecimal } from '../shared/formatters.js';
import { new_logger } from '../../../services/logs.js';

const logger = new_logger(import.meta.url);

export async function processEmendasParlamentaresDocumentosDespesa() {
  const log = logger.forMethod('processEmendasParlamentaresDocumentosDespesa');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento de Documentos de Despesa de Emendas Parlamentares');

  try {
    while (hasMore) {
      const bronzeData = await bronze_db.select()
        .from(EmendasParlamentaresDocumentosDespesa)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        log.info({ data: { offset } }, 'Processamento concluído. Nenhum registro adicional encontrado.');
        hasMore = false;
        break;
      }

      // 1. Unidades Gestoras
      const ugsToUpsertMap = new Map<string, typeof unidadesGestoras.$inferInsert>();
      bronzeData.forEach((b) => {
        if (b.codigoUg) {
          ugsToUpsertMap.set(b.codigoUg, { 
            codigo: b.codigoUg, 
            nome: b.ug || 'NÃO INFORMADO' 
          });
        }
      });
      const ugsToUpsert = Array.from(ugsToUpsertMap.values());
      if (ugsToUpsert.length > 0) {
        await silver_db.insert(unidadesGestoras).values(ugsToUpsert).onDuplicateKeyUpdate({ set: { nome: sql`VALUES(nome)` } });
      }
      const ugsDb = ugsToUpsert.length > 0
        ? await silver_db.select({ id: unidadesGestoras.id, codigo: unidadesGestoras.codigo }).from(unidadesGestoras).where(inArray(unidadesGestoras.codigo, ugsToUpsert.map(u => u.codigo)))
        : [];
      const ugIdMap = new Map(ugsDb.map(ug => [ug.codigo, ug.id]));

      // 2. Municípios e Estados
      const ufsLote = [...new Set(bronzeData.map(b => b.ufFavorecido ? b.ufFavorecido.substring(0, 2).toUpperCase() : null).filter(Boolean) as string[])];
      const estadosDb = ufsLote.length > 0 
        ? await silver_db.select({ sigla: estados.sigla }).from(estados).where(inArray(estados.sigla, ufsLote))
        : [];
      const ufsValidas = new Set(estadosDb.map(e => e.sigla));

      const munToUpsertMap = new Map<string, typeof municipios.$inferInsert>();
      bronzeData.forEach(b => {
        if (b.municipioFavorecido && b.ufFavorecido && ufsValidas.has(b.ufFavorecido.substring(0, 2).toUpperCase())) {
          const nomeMun = b.municipioFavorecido;
          const ufSigla = b.ufFavorecido.substring(0, 2).toUpperCase();
          const key = `${nomeMun.toUpperCase()}-${ufSigla}`;
          munToUpsertMap.set(key, { nome: nomeMun, ufSigla });
        }
      });
      
      const munToInsertList = Array.from(munToUpsertMap.values());
      if (munToInsertList.length > 0) {
        await silver_db.insert(municipios).values(munToInsertList).onDuplicateKeyUpdate({ set: { id: sql`id` } });
      }
      const municipiosDb = munToInsertList.length > 0
        ? await silver_db.select({ id: municipios.id, nome: municipios.nome, ufSigla: municipios.ufSigla }).from(municipios).where(inArray(municipios.nome, munToInsertList.map(m => m.nome)))
        : [];
      const munIdMap = new Map(municipiosDb.map(m => [`${m.nome.toUpperCase()}-${m.ufSigla.toUpperCase()}`, m.id]));

      // 3. Favorecidos (PF/PJ)
      const pfMap = new Map<string, typeof pessoasFisicas.$inferInsert>();
      const pjMap = new Map<string, typeof pessoasJuridicas.$inferInsert>();
      
      bronzeData.forEach(b => {
        const type = getDocumentType(b.codigoFavorecido);
        const municipioId = (b.municipioFavorecido && b.ufFavorecido) ? munIdMap.get(`${b.municipioFavorecido.toUpperCase()}-${b.ufFavorecido.substring(0, 2).toUpperCase()}`) : null;
        
        if (type === 'PF') {
          const cpf = formatCpf(b.codigoFavorecido)!;
          pfMap.set(cpf, { cpf, nome: b.favorecido || 'NÃO INFORMADO', municipioId: municipioId || null });
        } else if (type === 'PJ') {
          const cnpj = formatCnpj(b.codigoFavorecido)!;
          pjMap.set(cnpj, { cnpj, razaoSocial: b.favorecido || 'NÃO INFORMADO', municipioId: municipioId || null });
        }
      });

      if (pjMap.size > 0) {
        await silver_db.insert(pessoasJuridicas).values(Array.from(pjMap.values())).onDuplicateKeyUpdate({set: {razaoSocial: sql`VALUES(razao_social)`}});
      }
      
      if (pfMap.size > 0) {
         const exists = await silver_db.select({cpf: pessoasFisicas.cpf}).from(pessoasFisicas).where(inArray(pessoasFisicas.cpf, Array.from(pfMap.keys())));
         const eSet = new Set(exists.map(e => e.cpf));
         const toInsert = Array.from(pfMap.values()).filter(p => !eSet.has(p.cpf));
         if (toInsert.length > 0) await silver_db.insert(pessoasFisicas).values(toInsert);
      }

      const pfDb = pfMap.size > 0 ? await silver_db.select({ id: pessoasFisicas.id, cpf: pessoasFisicas.cpf }).from(pessoasFisicas).where(inArray(pessoasFisicas.cpf, Array.from(pfMap.keys()))) : [];
      const pjDb = pjMap.size > 0 ? await silver_db.select({ id: pessoasJuridicas.id, cnpj: pessoasJuridicas.cnpj }).from(pessoasJuridicas).where(inArray(pessoasJuridicas.cnpj, Array.from(pjMap.keys()))) : [];
      const pfIdMap = new Map(pfDb.map(p => [p.cpf, p.id]));
      const pjIdMap = new Map(pjDb.map(p => [p.cnpj, p.id]));

      // 4. Emendas (Recuperação de IDs)
      const emendasLote = [...new Set(bronzeData.map(b => b.codigoEmenda).filter(Boolean) as string[])];
      const emendasDb = emendasLote.length > 0 ? await silver_db.select({ id: emendasParlamentares.id, codigoEmenda: emendasParlamentares.codigoEmenda }).from(emendasParlamentares).where(inArray(emendasParlamentares.codigoEmenda, emendasLote)) : [];
      // Neste modelo, o relacionamento com emenda não existe de forma direta nem em despesas_empenhos nem em despesas_pagamentos, 
      // pois os documentos estão sendo apenas "complementados" na base principal de despesas.

      // 5. Inserção de Empenhos
      const empenhosToUpsert = bronzeData
        .filter(b => b.faseDespesa === 'Empenho' && b.codigoDocumento)
        .map(b => {
          const unidadeGestoraId = b.codigoUg ? ugIdMap.get(b.codigoUg) : null;
          if (!unidadeGestoraId) return null;

          const docType = getDocumentType(b.codigoFavorecido);
          let favorecidoPfId = null, favorecidoPjId = null;
          if (docType === 'PF') favorecidoPfId = pfIdMap.get(formatCpf(b.codigoFavorecido)!) || null;
          else if (docType === 'PJ') favorecidoPjId = pjIdMap.get(formatCnpj(b.codigoFavorecido)!) || null;

          return {
            codigoEmpenho: b.codigoDocumento!,
            dataEmissao: parseDate(b.dataDocumento) || '1970-01-01',
            unidadeGestoraId,
            favorecidoPfId,
            favorecidoPjId,
            valorOriginalEmpenho: parseDecimal(b.valorEmpenhado) || '0',
            exercicioReferencia: b.exercicioReferencia || '0000'
          };
        }).filter(Boolean) as typeof despesasEmpenhos.$inferInsert[];

      if (empenhosToUpsert.length > 0) {
        const uniqueMap = new Map<string, typeof empenhosToUpsert[0]>();
        for (const e of empenhosToUpsert) uniqueMap.set(e.codigoEmpenho, e);
        const uniqueEmpenhos = Array.from(uniqueMap.values());

        // Inserir apenas os novos empenhos (caso existam pela extração direta do SIAFI, ignora)
        // Isso assume que as emendas geraram documentos não listados no bulk principal.
        await silver_db.insert(despesasEmpenhos)
            .values(uniqueEmpenhos)
            .onDuplicateKeyUpdate({ set: { valorOriginalEmpenho: sql`VALUES(valor_original_empenho)` }});
      }

      // 6. Inserção de Pagamentos
      const pagamentosToUpsert = bronzeData
        .filter(b => b.faseDespesa === 'Pagamento' && b.codigoDocumento)
        .map(b => {
          const unidadeGestoraId = b.codigoUg ? ugIdMap.get(b.codigoUg) : null;
          if (!unidadeGestoraId) return null;

          const docType = getDocumentType(b.codigoFavorecido);
          let favorecidoPfId = null, favorecidoPjId = null;
          if (docType === 'PF') favorecidoPfId = pfIdMap.get(formatCpf(b.codigoFavorecido)!) || null;
          else if (docType === 'PJ') favorecidoPjId = pjIdMap.get(formatCnpj(b.codigoFavorecido)!) || null;

          return {
            codigoPagamento: b.codigoDocumento!,
            dataEmissao: parseDate(b.dataDocumento) || '1970-01-01',
            unidadeGestoraId,
            favorecidoPfId,
            favorecidoPjId,
            valorOriginalPagamento: parseDecimal(b.valorPago) || '0',
            exercicioReferencia: b.exercicioReferencia || '0000'
          };
        }).filter(Boolean) as typeof despesasPagamentos.$inferInsert[];

      if (pagamentosToUpsert.length > 0) {
        const uniqueMap = new Map<string, typeof pagamentosToUpsert[0]>();
        for (const p of pagamentosToUpsert) uniqueMap.set(p.codigoPagamento, p);
        const uniquePagamentos = Array.from(uniqueMap.values());

        await silver_db.insert(despesasPagamentos)
            .values(uniquePagamentos)
            .onDuplicateKeyUpdate({ set: { valorOriginalPagamento: sql`VALUES(valor_original_pagamento)` }});
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento de Documentos de Despesa de Emendas');
    throw error;
  }
}