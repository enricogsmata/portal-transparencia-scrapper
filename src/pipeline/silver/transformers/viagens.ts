import { sql, inArray, eq } from 'drizzle-orm';
import { bronze_db } from '../../../database/bronze_connection.js';
import { silver_db } from '../../../database/silver_connection.js';
import { 
  ViagensViagem, 
  ViagensPassagem, 
  ViagensTrecho, 
  ViagensPagamento 
} from '../../../database/bronze_schema.js';
import { 
  viagens, 
  viagemTrechos, 
  viagemPassagens, 
  pessoasFisicas, 
  unidadesGestoras, 
  orgaosSubordinados, 
  orgaosSuperiores 
} from '../../../database/silver_schema.js';
import { formatCpf, parseDate, parseDecimal } from '../shared/formatters.js';
import { new_logger } from '../../../services/logs.js';

const logger = new_logger(import.meta.url);

export async function processViagensViagem() {
  const log = logger.forMethod('processViagensViagem');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento da tabela principal de Viagens');

  try {
    while (hasMore) {
      const bronzeData = await bronze_db.select()
        .from(ViagensViagem)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        log.info({ data: { offset } }, 'Processamento concluído. Nenhum registro adicional encontrado.');
        hasMore = false;
        break;
      }

      log.info({ data: { offset, batchSize: bronzeData.length } }, 'Lote extraído da tabela viagens_viagem');

      // 1. Dimensão Órgãos Superiores e Subordinados (Solicitante)
      const osToUpsertMap = new Map<string, typeof orgaosSuperiores.$inferInsert>();
      bronzeData.forEach((b) => {
        if (b.codigoOrgaoSuperior) {
          osToUpsertMap.set(b.codigoOrgaoSuperior, {
            codigo: b.codigoOrgaoSuperior,
            nome: b.nomeOrgaoSuperior || 'NÃO INFORMADO',
          });
        }
      });
      const osToUpsert = Array.from(osToUpsertMap.values());
      if (osToUpsert.length > 0) {
        await silver_db.insert(orgaosSuperiores)
          .values(osToUpsert)
          .onDuplicateKeyUpdate({ set: { nome: sql`VALUES(nome)` } });
      }

      const osDb = osToUpsert.length > 0
        ? await silver_db.select({ id: orgaosSuperiores.id, codigo: orgaosSuperiores.codigo })
            .from(orgaosSuperiores)
            .where(inArray(orgaosSuperiores.codigo, osToUpsert.map(o => o.codigo)))
        : [];
      const osIdMap = new Map(osDb.map(o => [o.codigo, o.id]));

      const osubToUpsertMap = new Map<string, typeof orgaosSubordinados.$inferInsert>();
      bronzeData.forEach((b) => {
        if (b.codigoOrgaoSolicitante && b.codigoOrgaoSuperior && osIdMap.has(b.codigoOrgaoSuperior)) {
          osubToUpsertMap.set(b.codigoOrgaoSolicitante, {
            codigo: b.codigoOrgaoSolicitante,
            nome: b.nomeOrgaoSolicitante || 'NÃO INFORMADO',
            orgaoSuperiorId: osIdMap.get(b.codigoOrgaoSuperior)!,
          });
        }
      });
      const osubToUpsert = Array.from(osubToUpsertMap.values());
      if (osubToUpsert.length > 0) {
        await silver_db.insert(orgaosSubordinados)
          .values(osubToUpsert)
          .onDuplicateKeyUpdate({
            set: {
              nome: sql`VALUES(nome)`,
              orgaoSuperiorId: sql`VALUES(orgao_superior_id)`,
            },
          });
      }

      // 2. Dimensão Pessoas Físicas (Viajante)
      const pfsLote = bronzeData
        .filter(b => formatCpf(b.cpfViajante))
        .map(b => ({
          cpf: formatCpf(b.cpfViajante)!,
          nome: b.nome || 'NÃO INFORMADO',
        }));

      const cpfsSet = [...new Set(pfsLote.map(p => p.cpf))];
      const cpfsExistentesDb = cpfsSet.length > 0
        ? await silver_db.select({ id: pessoasFisicas.id, cpf: pessoasFisicas.cpf })
            .from(pessoasFisicas)
            .where(inArray(pessoasFisicas.cpf, cpfsSet))
        : [];
      
      const cpfIdMap = new Map(cpfsExistentesDb.map(pf => [pf.cpf, pf.id]));
      const cpfsExistentesSet = new Set(cpfsExistentesDb.map(pf => pf.cpf));

      const pfToInsert = pfsLote.filter(p => !cpfsExistentesSet.has(p.cpf));
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

      // 3. Entidade Viagens
      const viagensToInsert = bronzeData
        .map(b => {
          if (!b.identificadorProcessoViagem) return null;

          const cpfFormatado = formatCpf(b.cpfViajante);
          const viajanteId = cpfFormatado ? cpfIdMap.get(cpfFormatado) : null;
          if (!viajanteId) return null;

          const periodoDataInicio = parseDate(b.periodoDataInicio) || '1970-01-01';
          const periodoDataFim = parseDate(b.periodoDataFim) || '1970-01-01';

          return {
            identificadorProcessoViagem: b.identificadorProcessoViagem.trim().slice(0, 25),
            situacao: b.situacao || 'NÃO INFORMADO',
            viajanteId,
            cargo: b.cargo || null,
            periodoDataInicio,
            periodoDataFim,
            destinos: b.destinos || null,
            motivo: b.motivo || 'NÃO INFORMADO',
            valorDiarias: parseDecimal(b.valorDiarias) || '0',
            valorPassagens: parseDecimal(b.valorPassagens) || '0',
            valorOutrosGastos: parseDecimal(b.valorOutrosGastos) || '0',
            exercicioReferencia: b.exercicioReferencia ? b.exercicioReferencia.substring(0, 4) : null,
          };
        })
        .filter(Boolean) as typeof viagens.$inferInsert[];

      if (viagensToInsert.length > 0) {
        const uniqueMap = new Map<string, typeof viagensToInsert[0]>();
        for (const v of viagensToInsert) uniqueMap.set(v.identificadorProcessoViagem, v);
        const uniqueViagens = Array.from(uniqueMap.values());

        log.info({ data: { count: uniqueViagens.length } }, 'Inserindo/Atualizando viagens');
        for (let i = 0; i < uniqueViagens.length; i += 1000) {
          await silver_db.insert(viagens)
            .values(uniqueViagens.slice(i, i + 1000))
            .onDuplicateKeyUpdate({
              set: {
                situacao: sql`VALUES(situacao)`,
                viajanteId: sql`VALUES(viajante_id)`,
                cargo: sql`COALESCE(VALUES(cargo), viagens.cargo)`,
                periodoDataInicio: sql`VALUES(periodo_data_inicio)`,
                periodoDataFim: sql`VALUES(periodo_data_fim)`,
                destinos: sql`COALESCE(VALUES(destinos), viagens.destinos)`,
                motivo: sql`VALUES(motivo)`,
                valorDiarias: sql`VALUES(valor_diarias)`,
                valorPassagens: sql`VALUES(valor_passagens)`,
                valorOutrosGastos: sql`VALUES(valor_outros_gastos)`,
                exercicioReferencia: sql`COALESCE(VALUES(exercicio_referencia), viagens.exercicio_referencia)`,
              },
            });
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento de Viagens');
    throw error;
  }
}

export async function processViagensPagamento() {
  const log = logger.forMethod('processViagensPagamento');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento de Pagamentos de Viagens (UG Pagadora)');

  try {
    while (hasMore) {
      const bronzeData = await bronze_db.select()
        .from(ViagensPagamento)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        hasMore = false;
        break;
      }

      // 1. Órgãos Superiores e Subordinados
      const osToUpsertMap = new Map<string, typeof orgaosSuperiores.$inferInsert>();
      bronzeData.forEach((b) => {
        if (b.codigoOrgaoSuperior) {
          osToUpsertMap.set(b.codigoOrgaoSuperior, {
            codigo: b.codigoOrgaoSuperior,
            nome: b.nomeOrgaoSuperior || 'NÃO INFORMADO',
          });
        }
      });
      const osToUpsert = Array.from(osToUpsertMap.values());
      if (osToUpsert.length > 0) {
        await silver_db.insert(orgaosSuperiores)
          .values(osToUpsert)
          .onDuplicateKeyUpdate({ set: { nome: sql`VALUES(nome)` } });
      }

      const osDb = osToUpsert.length > 0
        ? await silver_db.select({ id: orgaosSuperiores.id, codigo: orgaosSuperiores.codigo })
            .from(orgaosSuperiores)
            .where(inArray(orgaosSuperiores.codigo, osToUpsert.map(o => o.codigo)))
        : [];
      const osIdMap = new Map(osDb.map(o => [o.codigo, o.id]));

      const osubToUpsertMap = new Map<string, typeof orgaosSubordinados.$inferInsert>();
      bronzeData.forEach((b) => {
        if (b.codigoOrgaoPagador && b.codigoOrgaoSuperior && osIdMap.has(b.codigoOrgaoSuperior)) {
          osubToUpsertMap.set(b.codigoOrgaoPagador, {
            codigo: b.codigoOrgaoPagador,
            nome: b.nomeOrgaoPagador || 'NÃO INFORMADO',
            orgaoSuperiorId: osIdMap.get(b.codigoOrgaoSuperior)!,
          });
        }
      });
      const osubToUpsert = Array.from(osubToUpsertMap.values());
      if (osubToUpsert.length > 0) {
        await silver_db.insert(orgaosSubordinados)
          .values(osubToUpsert)
          .onDuplicateKeyUpdate({
            set: {
              nome: sql`VALUES(nome)`,
              orgaoSuperiorId: sql`VALUES(orgao_superior_id)`,
            },
          });
      }

      const osubDb = osubToUpsert.length > 0
        ? await silver_db.select({ id: orgaosSubordinados.id, codigo: orgaosSubordinados.codigo })
            .from(orgaosSubordinados)
            .where(inArray(orgaosSubordinados.codigo, osubToUpsert.map(o => o.codigo)))
        : [];
      const osubIdMap = new Map(osubDb.map(o => [o.codigo, o.id]));

      // 2. Unidades Gestoras
      const ugsToUpsertMap = new Map<string, typeof unidadesGestoras.$inferInsert>();
      bronzeData.forEach((b) => {
        if (b.codigoUgPagadora) {
          const orgaoSubordinadoId = b.codigoOrgaoPagador ? osubIdMap.get(b.codigoOrgaoPagador) || null : null;
          ugsToUpsertMap.set(b.codigoUgPagadora, {
            codigo: b.codigoUgPagadora,
            nome: b.nomeUgPagadora || 'NÃO INFORMADO',
            orgaoSubordinadoId,
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
      const ugIdMap = new Map(ugsDb.map(u => [u.codigo, u.id]));

      // 3. Atualizar FK de Unidade Gestora Pagadora na tabela Viagens
      for (const b of bronzeData) {
        if (b.identificadorProcessoViagem && b.codigoUgPagadora && ugIdMap.has(b.codigoUgPagadora)) {
          const processo = b.identificadorProcessoViagem.trim().slice(0, 25);
          const ugId = ugIdMap.get(b.codigoUgPagadora)!;

          await silver_db.update(viagens)
            .set({ unidadeGestoraPagadoraId: ugId })
            .where(eq(viagens.identificadorProcessoViagem, processo));
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento de Pagamentos de Viagens');
    throw error;
  }
}

export async function processViagensTrecho() {
  const log = logger.forMethod('processViagensTrecho');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento de Trechos de Viagem');

  try {
    while (hasMore) {
      const bronzeData = await bronze_db.select()
        .from(ViagensTrecho)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        hasMore = false;
        break;
      }

      const processosLote = [...new Set(bronzeData.map(b => b.identificadorProcessoViagem?.trim().slice(0, 25)).filter(Boolean) as string[])];
      const viagensDb = processosLote.length > 0
        ? await silver_db.select({ id: viagens.id, identificadorProcessoViagem: viagens.identificadorProcessoViagem })
            .from(viagens)
            .where(inArray(viagens.identificadorProcessoViagem, processosLote))
        : [];
      const viagemIdMap = new Map(viagensDb.map(v => [v.identificadorProcessoViagem, v.id]));

      const trechosToInsert = bronzeData
        .map(b => {
          const processo = b.identificadorProcessoViagem?.trim().slice(0, 25);
          const viagemId = processo ? viagemIdMap.get(processo) : null;
          if (!viagemId) return null;

          const origemData = parseDate(b.origemData) || '1970-01-01';
          const destinoData = parseDate(b.destinoData) || '1970-01-01';
          const sequenciaTrecho = parseInt(b.sequenciaTrecho || '0', 10) || 0;
          const numeroDiarias = parseInt(b.numeroDiarias || '0', 10) || 0;
          const missao = b.missao?.trim().toUpperCase() === 'SIM';

          return {
            viagemId,
            sequenciaTrecho,
            origemData,
            origemPais: b.origemPais ? b.origemPais.slice(0, 100) : null,
            origemUf: b.origemUf ? b.origemUf.slice(0, 50) : null,
            origemCidade: b.origemCidade ? b.origemCidade.slice(0, 150) : null,
            destinoData,
            destinoPais: b.destinoPais ? b.destinoPais.slice(0, 100) : null,
            destinoUf: b.destinoUf ? b.destinoUf.slice(0, 50) : null,
            destinoCidade: b.destinoCidade ? b.destinoCidade.slice(0, 150) : null,
            meioTransporte: b.meioTransporte ? b.meioTransporte.slice(0, 50) : 'NÃO INFORMADO',
            numeroDiarias,
            missao,
          };
        })
        .filter(Boolean) as typeof viagemTrechos.$inferInsert[];

      if (trechosToInsert.length > 0) {
        const viagemIds = [...new Set(trechosToInsert.map(t => t.viagemId))];
        await silver_db.delete(viagemTrechos).where(inArray(viagemTrechos.viagemId, viagemIds));

        log.info({ data: { count: trechosToInsert.length } }, 'Inserindo viagem_trechos');
        for (let i = 0; i < trechosToInsert.length; i += 1000) {
          await silver_db.insert(viagemTrechos).values(trechosToInsert.slice(i, i + 1000));
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento de Trechos de Viagem');
    throw error;
  }
}

export async function processViagensPassagem() {
  const log = logger.forMethod('processViagensPassagem');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento de Passagens de Viagem');

  try {
    while (hasMore) {
      const bronzeData = await bronze_db.select()
        .from(ViagensPassagem)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        hasMore = false;
        break;
      }

      const processosLote = [...new Set(bronzeData.map(b => b.identificadorProcessoViagem?.trim().slice(0, 25)).filter(Boolean) as string[])];
      const viagensDb = processosLote.length > 0
        ? await silver_db.select({ id: viagens.id, identificadorProcessoViagem: viagens.identificadorProcessoViagem })
            .from(viagens)
            .where(inArray(viagens.identificadorProcessoViagem, processosLote))
        : [];
      const viagemIdMap = new Map(viagensDb.map(v => [v.identificadorProcessoViagem, v.id]));

      const passagensToInsert = bronzeData
        .map(b => {
          const processo = b.identificadorProcessoViagem?.trim().slice(0, 25);
          const viagemId = processo ? viagemIdMap.get(processo) : null;
          if (!viagemId) return null;

          const origemUf = (b.ufOrigemIda || b.ufOrigemVolta || '').slice(0, 50) || null;
          const origemCidade = (b.cidadeOrigemIda || b.cidadeOrigemVolta || '').slice(0, 150) || null;
          const destinoUf = (b.ufDestinoIda || b.ufDestinoVolta || '').slice(0, 50) || null;
          const destinoCidade = (b.cidadeDestinoIda || b.cidadeDestinoVolta || '').slice(0, 150) || null;

          return {
            viagemId,
            meioTransporte: b.meioTransporte ? b.meioTransporte.slice(0, 50) : 'NÃO INFORMADO',
            origemUf,
            origemCidade,
            destinoUf,
            destinoCidade,
            valorPassagem: parseDecimal(b.valorPassagem) || '0',
            taxaServico: parseDecimal(b.taxaServico) || null,
          };
        })
        .filter(Boolean) as typeof viagemPassagens.$inferInsert[];

      if (passagensToInsert.length > 0) {
        const viagemIds = [...new Set(passagensToInsert.map(p => p.viagemId))];
        await silver_db.delete(viagemPassagens).where(inArray(viagemPassagens.viagemId, viagemIds));

        log.info({ data: { count: passagensToInsert.length } }, 'Inserindo viagem_passagens');
        for (let i = 0; i < passagensToInsert.length; i += 1000) {
          await silver_db.insert(viagemPassagens).values(passagensToInsert.slice(i, i + 1000));
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento de Passagens de Viagem');
    throw error;
  }
}

export async function processViagens() {
  const log = logger.forMethod('processViagens');
  try {
    await processViagensViagem();
    await processViagensPagamento();
    await processViagensTrecho();
    await processViagensPassagem();
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante a orquestração do módulo de Viagens');
    throw error;
  }
}