import { sql, inArray, eq } from 'drizzle-orm';
import { bronze_db } from '../../../database/bronze_connection.js';
import { silver_db } from '../../../database/silver_connection.js';
import { 
  Convenios, 
  ConveniosOrdensBancarias 
} from '../../../database/bronze_schema.js';
import { 
  convenios, 
  convenioOrdensBancarias, 
  unidadesGestoras, 
  pessoasJuridicas, 
  municipios
} from '../../../database/silver_schema.js';
import { formatCnpj, parseDate, parseDecimal } from '../shared/formatters.js';
import { MunicipioResolver } from '../shared/municipio-resolver.js';
import { new_logger } from '../../../services/logs.js';

const logger = new_logger(import.meta.url);

export async function processConveniosTable() {
  const log = logger.forMethod('processConveniosTable');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento da tabela Convenios');

  try {
    while (hasMore) {
      const bronzeData = await bronze_db.select()
        .from(Convenios)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        log.info({ data: { offset } }, 'Processamento concluído. Nenhum registro adicional encontrado.');
        hasMore = false;
        break;
      }

      log.info({ data: { offset, batchSize: bronzeData.length } }, 'Lote extraído da tabela convenios');

      // 1. Unidades Gestoras
      const ugsToUpsertMap = new Map<string, typeof unidadesGestoras.$inferInsert>();
      bronzeData.forEach((b) => {
        if (b.codigoUgConcedente) {
          ugsToUpsertMap.set(b.codigoUgConcedente, { 
            codigo: b.codigoUgConcedente, 
            nome: b.nomeUgConcedente || 'NÃO INFORMADO' 
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

      // 2. Pessoas Jurídicas (Convenente)
      const pjsToUpsertMap = new Map<string, typeof pessoasJuridicas.$inferInsert>();
      bronzeData.forEach((b) => {
        const cnpj = formatCnpj(b.codigoConvenente);
        if (cnpj) {
          pjsToUpsertMap.set(cnpj, { 
            cnpj, 
            razaoSocial: b.nomeConvenente || 'NÃO INFORMADO' 
          });
        }
      });
      const pjsToUpsert = Array.from(pjsToUpsertMap.values());

      if (pjsToUpsert.length > 0) {
        await silver_db.insert(pessoasJuridicas)
          .values(pjsToUpsert)
          .onDuplicateKeyUpdate({ set: { razaoSocial: sql`VALUES(razao_social)` } });
      }

      const pjsDb = pjsToUpsert.length > 0
        ? await silver_db.select({ id: pessoasJuridicas.id, cnpj: pessoasJuridicas.cnpj })
            .from(pessoasJuridicas)
            .where(inArray(pessoasJuridicas.cnpj, pjsToUpsert.map(p => p.cnpj)))
        : [];
      const pjIdMap = new Map(pjsDb.map(pj => [pj.cnpj, pj.id]));

      // 3. Municípios (SIAFI)
      const municipiosToUpsert = bronzeData
        .filter((b) => b.codigoSiafiMunicipio && b.uf)
        .map((b) => ({
          codigoSiafi: b.codigoSiafiMunicipio!,
          nome: b.nomeMunicipio || 'NÃO INFORMADO',
          ufSigla: b.uf!.substring(0, 2).toUpperCase(),
        }));

      if (municipiosToUpsert.length > 0) {
        const uniqueMunMap = new Map<string, typeof municipiosToUpsert[0]>();
        for (const m of municipiosToUpsert) uniqueMunMap.set(m.codigoSiafi, m);
        const uniqueMunicipios = Array.from(uniqueMunMap.values());

        await silver_db.insert(municipios)
          .values(uniqueMunicipios)
          .onDuplicateKeyUpdate({
            set: {
              nome: sql`VALUES(nome)`,
              ufSigla: sql`VALUES(uf_sigla)`,
            },
          });
      }

      const codigosSiafi = [...new Set(municipiosToUpsert.map(m => m.codigoSiafi))];
      const municipiosDb = codigosSiafi.length > 0 
        ? await silver_db.select({ id: municipios.id, codigoSiafi: municipios.codigoSiafi })
            .from(municipios)
            .where(inArray(municipios.codigoSiafi, codigosSiafi))
        : [];
      const munIdMap = new Map(municipiosDb.map(m => [m.codigoSiafi!, m.id]));

      // 4. Convênios (Entidade Principal)
      const conveniosMapped = bronzeData.map((b) => {
        const unidadeGestoraConcedenteId = b.codigoUgConcedente ? ugIdMap.get(b.codigoUgConcedente) : null;
        const convenenteCnpj = formatCnpj(b.codigoConvenente);
        const convenenteId = convenenteCnpj ? pjIdMap.get(convenenteCnpj) : null;
        const municipioId = MunicipioResolver.resolve({
          siafi: b.codigoSiafiMunicipio,
          nome: b.nomeMunicipio,
          uf: b.uf,
        });
        
        const valorConvenio = parseDecimal(b.valorConvenio);

        if (!convenenteId || !b.numeroConvenio || !b.objetoConvenio || valorConvenio === null) return null;

        return {
          numeroConvenio: b.numeroConvenio,
          municipioId: municipioId || null,
          situacaoConvenio: b.situacaoConvenio || null,
          numeroOriginal: b.numeroOriginal || null,
          numeroProcesso: b.numeroProcessoConvenio || null,
          objeto: b.objetoConvenio,
          unidadeGestoraConcedenteId: unidadeGestoraConcedenteId || null,
          convenenteId,
          tipoConvenente: b.tipoConvenente || null,
          tipoInstrumento: b.tipoInstrumento || null,
          valorConvenio,
          valorLiberado: parseDecimal(b.valorLiberado) || null,
          valorContrapartida: parseDecimal(b.valorContrapartida) || null,
          dataPublicacao: parseDate(b.dataPublicacao) || null,
          dataInicioVigencia: parseDate(b.dataInicioVigencia) || null,
          dataFimVigencia: parseDate(b.dataFimVigencia) || null,
          dataUltimaLiberacao: parseDate(b.dataUltimaLiberacao) || null,
          valorUltimaLiberacao: parseDecimal(b.valorUltimaLiberacao) || null,
          exercicioReferencia: b.exercicioReferencia || null,
        };
      }).filter(Boolean) as typeof convenios.$inferInsert[];

      if (conveniosMapped.length > 0) {
        const batchMap = new Map<string, typeof conveniosMapped[0]>();
        for (const c of conveniosMapped) batchMap.set(c.numeroConvenio, c);
        const uniqueConveniosBatch = Array.from(batchMap.values());

        const existingConvenios = await silver_db.select({ id: convenios.id, numeroConvenio: convenios.numeroConvenio })
          .from(convenios)
          .where(inArray(convenios.numeroConvenio, uniqueConveniosBatch.map(c => c.numeroConvenio)));
        
        const existingMap = new Map(existingConvenios.map(c => [c.numeroConvenio, c.id]));
        const toInsert: typeof convenios.$inferInsert[] = [];
        const toUpdate: (typeof convenios.$inferInsert & { id: number })[] = [];

        for (const c of uniqueConveniosBatch) {
          const id = existingMap.get(c.numeroConvenio);
          if (id) {
            toUpdate.push({ ...c, id });
          } else {
            toInsert.push(c);
          }
        }

        if (toInsert.length > 0) {
          log.info({ data: { count: toInsert.length } }, 'Inserindo novos convenios');
          for (let i = 0; i < toInsert.length; i += 1000) {
            await silver_db.insert(convenios).values(toInsert.slice(i, i + 1000));
          }
        }

        if (toUpdate.length > 0) {
          log.info({ data: { count: toUpdate.length } }, 'Atualizando convenios existentes');
          for (const u of toUpdate) {
            await silver_db.update(convenios).set(u).where(eq(convenios.id, u.id));
          }
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento de Convenios');
    throw error;
  }
}

export async function processConveniosOrdensBancarias() {
  const log = logger.forMethod('processConveniosOrdensBancarias');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento de Ordens Bancarias de Convenios');

  try {
    while (hasMore) {
      const bronzeData = await bronze_db.select()
        .from(ConveniosOrdensBancarias)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        log.info({ data: { offset } }, 'Processamento concluído. Nenhum registro adicional encontrado.');
        hasMore = false;
        break;
      }

      const numerosLote = [...new Set(bronzeData.map(b => b.numeroConvenio).filter(Boolean) as string[])];
      const conveniosDb = numerosLote.length > 0 
        ? await silver_db.select({ id: convenios.id, numeroConvenio: convenios.numeroConvenio })
            .from(convenios)
            .where(inArray(convenios.numeroConvenio, numerosLote))
        : [];
      const convenioIdMap = new Map(conveniosDb.map(c => [c.numeroConvenio, c.id]));

      const obsToInsertAll = bronzeData.map(b => {
        const convenioId = b.numeroConvenio ? convenioIdMap.get(b.numeroConvenio) : null;
        const dataEmissaoOb = parseDate(b.dataEmissaoOb);
        const valorLiberado = parseDecimal(b.valorLiberado);
        
        if (!convenioId || !b.numeroOrdemBancaria || !dataEmissaoOb || valorLiberado === null) return null;
        
        return {
          convenioId,
          numeroOrdemBancaria: b.numeroOrdemBancaria,
          dataEmissaoOb,
          valorLiberado,
        };
      }).filter(Boolean) as typeof convenioOrdensBancarias.$inferInsert[];

      if (obsToInsertAll.length > 0) {
        const convenioIds = [...new Set(obsToInsertAll.map(o => o.convenioId))];
        const existing = await silver_db.select({ 
            convenioId: convenioOrdensBancarias.convenioId, 
            numeroOrdemBancaria: convenioOrdensBancarias.numeroOrdemBancaria 
          })
          .from(convenioOrdensBancarias)
          .where(inArray(convenioOrdensBancarias.convenioId, convenioIds));
        
        const existingSet = new Set(existing.map(o => `${o.convenioId}-${o.numeroOrdemBancaria}`));
        
        const toInsertMap = new Map<string, typeof convenioOrdensBancarias.$inferInsert>();
        for (const o of obsToInsertAll) {
          const key = `${o.convenioId}-${o.numeroOrdemBancaria}`;
          if (!existingSet.has(key)) {
            toInsertMap.set(key, o); 
          }
        }
        
        const toInsert = Array.from(toInsertMap.values());
        if (toInsert.length > 0) {
          log.info({ data: { count: toInsert.length } }, 'Inserindo ordens bancarias de convenios');
          for (let i = 0; i < toInsert.length; i += 1000) {
            await silver_db.insert(convenioOrdensBancarias).values(toInsert.slice(i, i + 1000));
          }
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento de Ordens Bancarias de Convenios');
    throw error;
  }
}

export async function processConvenios() {
  const log = logger.forMethod('processConvenios');
  try {
    await processConveniosTable();
    await processConveniosOrdensBancarias();
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante a orquestração do módulo de Convenios');
    throw error;
  }
}