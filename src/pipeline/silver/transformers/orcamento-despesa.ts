import { sql, inArray, and, eq } from 'drizzle-orm';
import { bronze_db } from '../../../database/bronze_connection.js';
import { silver_db } from '../../../database/silver_connection.js';
import { OrcamentoDespesa } from '../../../database/bronze_schema.js';
import { 
  orcamentoDespesa, 
  unidadesGestoras, 
  acoesOrcamentarias, 
  categoriasDespesa, 
  gruposDespesa, 
  elementosDespesa,
  programasOrcamentarios
} from '../../../database/silver_schema.js';
import { parseDecimal } from '../shared/formatters.js';
import { new_logger } from '../../../services/logs.js';

const logger = new_logger(import.meta.url);

export async function processOrcamentoDespesa() {
  const log = logger.forMethod('processOrcamentoDespesa');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento do módulo Orçamento Despesa');

  try {
    while (hasMore) {
      // 1. Extração da Camada Bronze
      const bronzeData = await bronze_db.select()
        .from(OrcamentoDespesa)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        log.info({ data: { offset } }, 'Processamento concluído. Nenhum registro adicional encontrado.');
        hasMore = false;
        break;
      }

      log.info({ data: { offset, batchSize: bronzeData.length } }, 'Lote extraído da tabela orcamento_despesa');

      // 2. Dimensão Unidades Gestoras (Unidade Orçamentária mapeada como UG para simplificação do relacional)
      const uosToUpsertMap = new Map<string, typeof unidadesGestoras.$inferInsert>();
      bronzeData.forEach((b) => {
        if (b.codigoUnidadeOrcamentaria) {
          uosToUpsertMap.set(b.codigoUnidadeOrcamentaria, { 
            codigo: b.codigoUnidadeOrcamentaria, 
            nome: b.nomeUnidadeOrcamentaria || 'NÃO INFORMADO' 
          });
        }
      });
      const uosToUpsert = Array.from(uosToUpsertMap.values());

      if (uosToUpsert.length > 0) {
        await silver_db.insert(unidadesGestoras)
          .values(uosToUpsert)
          .onDuplicateKeyUpdate({ set: { nome: sql`VALUES(nome)` } });
      }

      const uosDb = uosToUpsert.length > 0
        ? await silver_db.select({ id: unidadesGestoras.id, codigo: unidadesGestoras.codigo })
            .from(unidadesGestoras)
            .where(inArray(unidadesGestoras.codigo, uosToUpsert.map(u => u.codigo)))
        : [];
      const uoIdMap = new Map(uosDb.map(uo => [uo.codigo, uo.id]));

      // 3. Dimensão Programas e Ações Orçamentárias
      const programasToUpsertMap = new Map<string, typeof programasOrcamentarios.$inferInsert>();
      bronzeData.forEach((b) => {
        if (b.codigoProgramaOrcamentario) {
          programasToUpsertMap.set(b.codigoProgramaOrcamentario, { 
            codigo: b.codigoProgramaOrcamentario, 
            nome: b.nomeProgramaOrcamentario || 'NÃO INFORMADO' 
          });
        }
      });
      const programasToUpsert = Array.from(programasToUpsertMap.values());
      
      if (programasToUpsert.length > 0) {
        await silver_db.insert(programasOrcamentarios)
          .values(programasToUpsert)
          .onDuplicateKeyUpdate({ set: { nome: sql`VALUES(nome)` } });
      }

      const programasDb = programasToUpsert.length > 0
        ? await silver_db.select({ id: programasOrcamentarios.id, codigo: programasOrcamentarios.codigo })
            .from(programasOrcamentarios)
            .where(inArray(programasOrcamentarios.codigo, programasToUpsert.map(p => p.codigo)))
        : [];
      const programaIdMap = new Map(programasDb.map(p => [p.codigo, p.id]));

      const acoesToUpsertMap = new Map<string, typeof acoesOrcamentarias.$inferInsert>();
      bronzeData.forEach(b => {
        if (b.codigoAcao) {
          const programaId = b.codigoProgramaOrcamentario ? programaIdMap.get(b.codigoProgramaOrcamentario) : null;
          acoesToUpsertMap.set(b.codigoAcao, { 
            codigo: b.codigoAcao, 
            nome: b.nomeAcao || 'NÃO INFORMADO',
            programaId: programaId || null,
            linguagemCidada: null
          });
        }
      });
      const acoesToUpsert = Array.from(acoesToUpsertMap.values());
      if (acoesToUpsert.length > 0) {
        const acoesEx = await silver_db.select({codigo: acoesOrcamentarias.codigo}).from(acoesOrcamentarias).where(inArray(acoesOrcamentarias.codigo, acoesToUpsert.map(a => a.codigo)));
        const acoesExSet = new Set(acoesEx.map(a => a.codigo));
        const toInsert = acoesToUpsert.filter(a => !acoesExSet.has(a.codigo));
        if (toInsert.length > 0) await silver_db.insert(acoesOrcamentarias).values(toInsert);
      }
      
      const acoesDb = acoesToUpsert.length > 0
        ? await silver_db.select({ id: acoesOrcamentarias.id, codigo: acoesOrcamentarias.codigo })
            .from(acoesOrcamentarias)
            .where(inArray(acoesOrcamentarias.codigo, acoesToUpsert.map(a => a.codigo)))
        : [];
      const acaoIdMap = new Map(acoesDb.map(a => [a.codigo, a.id]));

      // 4. Dimensão Categorias de Despesa
      const categoriasToUpsertMap = new Map<string, typeof categoriasDespesa.$inferInsert>();
      bronzeData.forEach((b) => {
        if (b.codigoCategoriaEconomica) {
          categoriasToUpsertMap.set(b.codigoCategoriaEconomica, { 
            codigo: b.codigoCategoriaEconomica, 
            nome: b.nomeCategoriaEconomica || 'NÃO INFORMADO' 
          });
        }
      });
      const categoriasToUpsert = Array.from(categoriasToUpsertMap.values());

      if (categoriasToUpsert.length > 0) {
        await silver_db.insert(categoriasDespesa)
          .values(categoriasToUpsert)
          .onDuplicateKeyUpdate({ set: { nome: sql`VALUES(nome)` } });
      }

      const categoriasDb = categoriasToUpsert.length > 0
        ? await silver_db.select({ id: categoriasDespesa.id, codigo: categoriasDespesa.codigo })
            .from(categoriasDespesa)
            .where(inArray(categoriasDespesa.codigo, categoriasToUpsert.map(c => c.codigo)))
        : [];
      const categoriaIdMap = new Map(categoriasDb.map(c => [c.codigo, c.id]));

      // 5. Dimensão Grupos de Despesa
      const gruposToUpsertMap = new Map<string, typeof gruposDespesa.$inferInsert>();
      bronzeData.forEach((b) => {
        if (b.codigoGrupoDespesa) {
          gruposToUpsertMap.set(b.codigoGrupoDespesa, { 
            codigo: b.codigoGrupoDespesa, 
            nome: b.nomeGrupoDespesa || 'NÃO INFORMADO' 
          });
        }
      });
      const gruposToUpsert = Array.from(gruposToUpsertMap.values());

      if (gruposToUpsert.length > 0) {
        await silver_db.insert(gruposDespesa)
          .values(gruposToUpsert)
          .onDuplicateKeyUpdate({ set: { nome: sql`VALUES(nome)` } });
      }

      const gruposDb = gruposToUpsert.length > 0
        ? await silver_db.select({ id: gruposDespesa.id, codigo: gruposDespesa.codigo })
            .from(gruposDespesa)
            .where(inArray(gruposDespesa.codigo, gruposToUpsert.map(g => g.codigo)))
        : [];
      const grupoIdMap = new Map(gruposDb.map(g => [g.codigo, g.id]));

      // 6. Dimensão Elementos de Despesa
      const elementosToUpsertMap = new Map<string, typeof elementosDespesa.$inferInsert>();
      bronzeData.forEach((b) => {
        if (b.codigoElementoDespesa) {
          elementosToUpsertMap.set(b.codigoElementoDespesa, { 
            codigo: b.codigoElementoDespesa, 
            nome: b.nomeElementoDespesa || 'NÃO INFORMADO' 
          });
        }
      });
      const elementosToUpsert = Array.from(elementosToUpsertMap.values());

      if (elementosToUpsert.length > 0) {
        await silver_db.insert(elementosDespesa)
          .values(elementosToUpsert)
          .onDuplicateKeyUpdate({ set: { nome: sql`VALUES(nome)` } });
      }

      const elementosDb = elementosToUpsert.length > 0
        ? await silver_db.select({ id: elementosDespesa.id, codigo: elementosDespesa.codigo })
            .from(elementosDespesa)
            .where(inArray(elementosDespesa.codigo, elementosToUpsert.map(e => e.codigo)))
        : [];
      const elementoIdMap = new Map(elementosDb.map(e => [e.codigo, e.id]));

      // 7. Entidade de Orçamento Despesa
      const orcamentoToInsert = bronzeData
        .map(b => {
          const exercicio = b.exercicio;
          if (!exercicio) return null;

          const unidadeOrcamentariaId = b.codigoUnidadeOrcamentaria ? uoIdMap.get(b.codigoUnidadeOrcamentaria) : null;
          const acaoOrcamentariaId = b.codigoAcao ? acaoIdMap.get(b.codigoAcao) : null;
          const categoriaDespesaId = b.codigoCategoriaEconomica ? categoriaIdMap.get(b.codigoCategoriaEconomica) : null;
          const grupoDespesaId = b.codigoGrupoDespesa ? grupoIdMap.get(b.codigoGrupoDespesa) : null;
          const elementoDespesaId = b.codigoElementoDespesa ? elementoIdMap.get(b.codigoElementoDespesa) : null;

          return {
            exercicio,
            unidadeOrcamentariaId: unidadeOrcamentariaId || null,
            acaoOrcamentariaId: acaoOrcamentariaId || null,
            categoriaDespesaId: categoriaDespesaId || null,
            grupoDespesaId: grupoDespesaId || null,
            elementoDespesaId: elementoDespesaId || null,
            orcamentoInicial: parseDecimal(b.orcamentoInicial) || '0',
            orcamentoAtualizado: parseDecimal(b.orcamentoAtualizado) || '0',
            orcamentoEmpenhado: parseDecimal(b.orcamentoEmpenhado) || '0',
            orcamentoRealizado: parseDecimal(b.orcamentoRealizado) || '0',
            percentualRealizado: parseDecimal(b.percentualRealizado) || '0',
          };
        })
        .filter(Boolean) as typeof orcamentoDespesa.$inferInsert[];

      if (orcamentoToInsert.length > 0) {
        // Garantindo unicidade no lote para evitar erros de duplicate caso a granularidade da bronze tenha linhas repetidas
        const batchMap = new Map<string, typeof orcamentoToInsert[0]>();
        for (const o of orcamentoToInsert) {
          const key = `${o.exercicio}-${o.unidadeOrcamentariaId}-${o.acaoOrcamentariaId}-${o.elementoDespesaId}`;
          batchMap.set(key, o);
        }
        const uniqueOrcamentos = Array.from(batchMap.values());

        // Deletar os orçamentos do ano do lote para inserção limpa (Idempotência por ano fiscal extraído)
        const exerciciosLote = [...new Set(uniqueOrcamentos.map(o => o.exercicio))];
        await silver_db.delete(orcamentoDespesa)
          .where(inArray(orcamentoDespesa.exercicio, exerciciosLote));

        log.info({ data: { count: uniqueOrcamentos.length } }, 'Inserindo orcamento_despesa');
        for (let i = 0; i < uniqueOrcamentos.length; i += 1000) {
          await silver_db.insert(orcamentoDespesa).values(uniqueOrcamentos.slice(i, i + 1000));
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento do Orçamento Despesa');
    throw error;
  }
}