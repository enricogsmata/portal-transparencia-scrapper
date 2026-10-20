import { sql, inArray, and } from 'drizzle-orm';
import { bronze_db } from '../../../database/bronze_connection.js';
import { silver_db } from '../../../database/silver_connection.js';
import { 
  ServidoresCadastro,
  AposentadosCadastro,
  PensionistasCadastro,
  ServidoresAfastamentos,
  ServidoresObservacoes,
  ServidoresHonorariosAdvocaticios,
  ServidoresJetons,
  ServidoresRemuneracao
} from '../../../database/bronze_schema.js';
import { 
  servidores,
  servidoresRemuneracoes,
  servidoresAfastamentos,
  servidoresJetons,
  servidoresHonorarios,
  pessoasFisicas,
  orgaosSubordinados,
  orgaosSuperiores,
  estados
} from '../../../database/silver_schema.js';
import { formatCpf, parseDate, parseDecimal } from '../shared/formatters.js';
import { new_logger } from '../../../services/logs.js';

const logger = new_logger(import.meta.url);

interface CadastroItem {
  idServidorPortal?: string | null;
  nome?: string | null;
  cpf?: string | null;
  matricula?: string | null;
  descricaoCargo?: string | null;
  classeCargo?: string | null;
  padraoCargo?: string | null;
  nivelCargo?: string | null;
  funcao?: string | null;
  codOrgsupLotacao?: string | null;
  orgsupLotacao?: string | null;
  codOrgaoLotacao?: string | null;
  orgaoLotacao?: string | null;
  codOrgsupExercicio?: string | null;
  orgsupExercicio?: string | null;
  codOrgaoExercicio?: string | null;
  orgaoExercicio?: string | null;
  codTipoVinculo?: string | null;
  tipoVinculo?: string | null;
  situacaoVinculo?: string | null;
  regimeJuridico?: string | null;
  jornadaDeTrabalho?: string | null;
  dataIngressoOrgao?: string | null;
  ufExercicio?: string | null;
}

async function syncServidoresDimensions(bronzeBatch: CadastroItem[]) {
  // 1. Órgãos Superiores
  const osToUpsertMap = new Map<string, typeof orgaosSuperiores.$inferInsert>();
  bronzeBatch.forEach(b => {
    if (b.codOrgsupLotacao) {
      osToUpsertMap.set(b.codOrgsupLotacao, {
        codigo: b.codOrgsupLotacao,
        nome: b.orgsupLotacao || 'NÃO INFORMADO',
      });
    }
    if (b.codOrgsupExercicio) {
      osToUpsertMap.set(b.codOrgsupExercicio, {
        codigo: b.codOrgsupExercicio,
        nome: b.orgsupExercicio || 'NÃO INFORMADO',
      });
    }
  });

  const osToUpsert = Array.from(osToUpsertMap.values());
  if (osToUpsert.length > 0) {
    await silver_db.insert(orgaosSuperiores)
      .values(osToUpsert)
      .onDuplicateKeyUpdate({ set: { nome: sql`VALUES(nome)` } });
  }

  const codigosOs = osToUpsert.map(o => o.codigo);
  const osDb = codigosOs.length > 0
    ? await silver_db.select({ id: orgaosSuperiores.id, codigo: orgaosSuperiores.codigo })
        .from(orgaosSuperiores)
        .where(inArray(orgaosSuperiores.codigo, codigosOs))
    : [];
  const osIdMap = new Map(osDb.map(o => [o.codigo, o.id]));

  // 2. Órgãos Subordinados
  const osubToUpsertMap = new Map<string, typeof orgaosSubordinados.$inferInsert>();
  bronzeBatch.forEach(b => {
    if (b.codOrgaoLotacao && b.codOrgsupLotacao && osIdMap.has(b.codOrgsupLotacao)) {
      osubToUpsertMap.set(b.codOrgaoLotacao, {
        codigo: b.codOrgaoLotacao,
        nome: b.orgaoLotacao || 'NÃO INFORMADO',
        orgaoSuperiorId: osIdMap.get(b.codOrgsupLotacao)!,
      });
    }
    if (b.codOrgaoExercicio && b.codOrgsupExercicio && osIdMap.has(b.codOrgsupExercicio)) {
      osubToUpsertMap.set(b.codOrgaoExercicio, {
        codigo: b.codOrgaoExercicio,
        nome: b.orgaoExercicio || 'NÃO INFORMADO',
        orgaoSuperiorId: osIdMap.get(b.codOrgsupExercicio)!,
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

  const codigosOsub = osubToUpsert.map(o => o.codigo);
  const osubDb = codigosOsub.length > 0
    ? await silver_db.select({ id: orgaosSubordinados.id, codigo: orgaosSubordinados.codigo })
        .from(orgaosSubordinados)
        .where(inArray(orgaosSubordinados.codigo, codigosOsub))
    : [];
  const osubIdMap = new Map(osubDb.map(o => [o.codigo, o.id]));

  // 3. Pessoas Físicas
  const pfsLote = bronzeBatch
    .filter(b => formatCpf(b.cpf))
    .map(b => ({
      cpf: formatCpf(b.cpf)!,
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

  // 4. Estados válidos
  const ufsLote = [...new Set(bronzeBatch.map(b => b.ufExercicio ? b.ufExercicio.substring(0, 2).toUpperCase() : null).filter(Boolean) as string[])];
  const estadosDb = ufsLote.length > 0
    ? await silver_db.select({ sigla: estados.sigla }).from(estados).where(inArray(estados.sigla, ufsLote))
    : [];
  const ufsValidas = new Set(estadosDb.map(e => e.sigla));

  return { osubIdMap, cpfIdMap, ufsValidas };
}

async function upsertServidoresBatch(bronzeBatch: CadastroItem[]) {
  const { osubIdMap, cpfIdMap, ufsValidas } = await syncServidoresDimensions(bronzeBatch);

  const servidoresToInsert = bronzeBatch
    .map(b => {
      if (!b.idServidorPortal) return null;

      const cpfFormatado = formatCpf(b.cpf);
      const pessoaFisicaId = cpfFormatado ? cpfIdMap.get(cpfFormatado) : null;
      if (!pessoaFisicaId) return null;

      const orgaoLotacaoId = b.codOrgaoLotacao ? osubIdMap.get(b.codOrgaoLotacao) || null : null;
      const orgaoExercicioId = b.codOrgaoExercicio ? osubIdMap.get(b.codOrgaoExercicio) || null : null;
      const ufSigla = b.ufExercicio && ufsValidas.has(b.ufExercicio.substring(0, 2).toUpperCase())
        ? b.ufExercicio.substring(0, 2).toUpperCase()
        : null;

      return {
        idServidorPortal: b.idServidorPortal,
        pessoaFisicaId,
        matricula: b.matricula || null,
        descricaoCargo: b.descricaoCargo || null,
        classeCargo: b.classeCargo || null,
        padraoCargo: b.padraoCargo || null,
        nivelCargo: b.nivelCargo || null,
        funcao: b.funcao || null,
        orgaoLotacaoId,
        orgaoExercicioId,
        ufExercicioSigla: ufSigla,
        tipoVinculo: b.tipoVinculo || null,
        situacaoVinculo: b.situacaoVinculo || null,
        regimeJuridico: b.regimeJuridico || null,
        jornadaDeTrabalho: b.jornadaDeTrabalho || null,
        dataIngressoOrgao: parseDate(b.dataIngressoOrgao) || null,
      };
    })
    .filter(Boolean) as typeof servidores.$inferInsert[];

  if (servidoresToInsert.length > 0) {
    const batchMap = new Map<string, typeof servidoresToInsert[0]>();
    for (const s of servidoresToInsert) batchMap.set(s.idServidorPortal, s);
    const uniqueServidores = Array.from(batchMap.values());

    for (let i = 0; i < uniqueServidores.length; i += 1000) {
      await silver_db.insert(servidores)
        .values(uniqueServidores.slice(i, i + 1000))
        .onDuplicateKeyUpdate({
          set: {
            pessoaFisicaId: sql`VALUES(pessoa_fisica_id)`,
            matricula: sql`COALESCE(VALUES(matricula), servidores.matricula)`,
            descricaoCargo: sql`COALESCE(VALUES(descricao_cargo), servidores.descricao_cargo)`,
            classeCargo: sql`COALESCE(VALUES(classe_cargo), servidores.classe_cargo)`,
            padraoCargo: sql`COALESCE(VALUES(padrao_cargo), servidores.padrao_cargo)`,
            nivelCargo: sql`COALESCE(VALUES(nivel_cargo), servidores.nivel_cargo)`,
            funcao: sql`COALESCE(VALUES(funcao), servidores.funcao)`,
            orgaoLotacaoId: sql`COALESCE(VALUES(orgao_lotacao_id), servidores.orgao_lotacao_id)`,
            orgaoExercicioId: sql`COALESCE(VALUES(orgao_exercicio_id), servidores.orgao_exercicio_id)`,
            ufExercicioSigla: sql`COALESCE(VALUES(uf_exercicio_sigla), servidores.uf_exercicio_sigla)`,
            tipoVinculo: sql`COALESCE(VALUES(tipo_vinculo), servidores.tipo_vinculo)`,
            situacaoVinculo: sql`COALESCE(VALUES(situacao_vinculo), servidores.situacao_vinculo)`,
            regimeJuridico: sql`COALESCE(VALUES(regime_juridico), servidores.regime_juridico)`,
            jornadaDeTrabalho: sql`COALESCE(VALUES(jornada_de_trabalho), servidores.jornada_de_trabalho)`,
            dataIngressoOrgao: sql`COALESCE(VALUES(data_ingresso_orgao), servidores.data_ingresso_orgao)`,
          },
        });
    }
  }
}

export async function processServidoresCadastro() {
  const log = logger.forMethod('processServidoresCadastro');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento de Servidores Ativos');

  try {
    while (hasMore) {
      const bronzeData = await bronze_db.select()
        .from(ServidoresCadastro)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        log.info({ data: { offset } }, 'Processamento concluído. Nenhum registro adicional encontrado.');
        hasMore = false;
        break;
      }

      await upsertServidoresBatch(bronzeData);
      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento de Servidores Ativos');
    throw error;
  }
}

export async function processAposentadosCadastro() {
  const log = logger.forMethod('processAposentadosCadastro');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento de Aposentados');

  try {
    while (hasMore) {
      const bronzeData = await bronze_db.select()
        .from(AposentadosCadastro)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        hasMore = false;
        break;
      }

      await upsertServidoresBatch(bronzeData);
      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento de Aposentados');
    throw error;
  }
}

export async function processPensionistasCadastro() {
  const log = logger.forMethod('processPensionistasCadastro');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento de Pensionistas');

  try {
    while (hasMore) {
      const bronzeData = await bronze_db.select()
        .from(PensionistasCadastro)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        hasMore = false;
        break;
      }

      const mappedData: CadastroItem[] = bronzeData.map(b => ({
        idServidorPortal: b.idServidorPortal,
        nome: b.nome,
        cpf: b.cpf,
        matricula: b.matricula,
        descricaoCargo: b.descricaoCargoInstituidorPensao,
        codOrgsupLotacao: b.codOrgsupLotacaoInstituidorPensao,
        orgsupLotacao: b.orgsupLotacaoInstituidorPensao,
        codOrgaoLotacao: b.codOrgLotacaoInstituidorPensao,
        orgaoLotacao: b.orgLotacaoInstituidorPensao,
        tipoVinculo: b.tipoVinculo,
        situacaoVinculo: b.situacaoVinculo,
        regimeJuridico: b.regimeJuridicoInstituidorPensao,
        jornadaDeTrabalho: b.jornadaDeTrabalhoInstituidorPensao,
        dataIngressoOrgao: b.dataIngressoOrgaoInstituidorPensao,
      }));

      await upsertServidoresBatch(mappedData);
      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento de Pensionistas');
    throw error;
  }
}

export async function processServidoresAfastamentos() {
  const log = logger.forMethod('processServidoresAfastamentos');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento de Afastamentos de Servidores');

  try {
    while (hasMore) {
      const bronzeData = await bronze_db.select()
        .from(ServidoresAfastamentos)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        hasMore = false;
        break;
      }

      const portaisLote = [...new Set(bronzeData.map(b => b.idServidorPortal).filter(Boolean) as string[])];
      const servidoresDb = portaisLote.length > 0
        ? await silver_db.select({ id: servidores.id, idServidorPortal: servidores.idServidorPortal })
            .from(servidores)
            .where(inArray(servidores.idServidorPortal, portaisLote))
        : [];
      const servidorIdMap = new Map(servidoresDb.map(s => [s.idServidorPortal, s.id]));

      const afastamentosToInsert = bronzeData
        .map(b => {
          const servidorId = b.idServidorPortal ? servidorIdMap.get(b.idServidorPortal) : null;
          const dataInicio = parseDate(b.dataInicioAfastamento);
          if (!servidorId || !dataInicio) return null;

          return {
            servidorId,
            dataInicioAfastamento: dataInicio,
            dataFimAfastamento: parseDate(b.dataFimAfastamento) || null,
          };
        })
        .filter(Boolean) as typeof servidoresAfastamentos.$inferInsert[];

      if (afastamentosToInsert.length > 0) {
        const servidorIds = [...new Set(afastamentosToInsert.map(a => a.servidorId))];
        await silver_db.delete(servidoresAfastamentos)
          .where(inArray(servidoresAfastamentos.servidorId, servidorIds));

        log.info({ data: { count: afastamentosToInsert.length } }, 'Inserindo servidores_afastamentos');
        for (let i = 0; i < afastamentosToInsert.length; i += 1000) {
          await silver_db.insert(servidoresAfastamentos).values(afastamentosToInsert.slice(i, i + 1000));
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento de Afastamentos');
    throw error;
  }
}

export async function processServidoresObservacoes() {
  const log = logger.forMethod('processServidoresObservacoes');
  log.info('Observações de servidores não possuem tabela silver correspondente mapeada. Pulando persistência.');
}

export async function processServidoresHonorariosAdvocaticios() {
  const log = logger.forMethod('processServidoresHonorariosAdvocaticios');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento de Honorários Advocatícios');

  try {
    while (hasMore) {
      const bronzeData = await bronze_db.select()
        .from(ServidoresHonorariosAdvocaticios)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        hasMore = false;
        break;
      }

      const portaisLote = [...new Set(bronzeData.map(b => b.idServidorPortal).filter(Boolean) as string[])];
      const servidoresDb = portaisLote.length > 0
        ? await silver_db.select({ id: servidores.id, idServidorPortal: servidores.idServidorPortal })
            .from(servidores)
            .where(inArray(servidores.idServidorPortal, portaisLote))
        : [];
      const servidorIdMap = new Map(servidoresDb.map(s => [s.idServidorPortal, s.id]));

      const honorariosToInsert = bronzeData
        .map(b => {
          const servidorId = b.idServidorPortal ? servidorIdMap.get(b.idServidorPortal) : null;
          const ano = b.ano?.trim();
          const mes = b.mes?.trim().padStart(2, '0');
          const valor = parseDecimal(b.valor);

          if (!servidorId || !ano || !mes || valor === null) return null;

          return {
            servidorId,
            ano: ano.substring(0, 4),
            mes: mes.substring(0, 2),
            observacoes: b.observacoes || null,
            valor,
          };
        })
        .filter(Boolean) as typeof servidoresHonorarios.$inferInsert[];

      if (honorariosToInsert.length > 0) {
        const servidorIds = [...new Set(honorariosToInsert.map(h => h.servidorId))];
        const anos = [...new Set(honorariosToInsert.map(h => h.ano))];
        const meses = [...new Set(honorariosToInsert.map(h => h.mes))];

        await silver_db.delete(servidoresHonorarios)
          .where(
            and(
              inArray(servidoresHonorarios.servidorId, servidorIds),
              inArray(servidoresHonorarios.ano, anos),
              inArray(servidoresHonorarios.mes, meses)
            )
          );

        log.info({ data: { count: honorariosToInsert.length } }, 'Inserindo servidores_honorarios');
        for (let i = 0; i < honorariosToInsert.length; i += 1000) {
          await silver_db.insert(servidoresHonorarios).values(honorariosToInsert.slice(i, i + 1000));
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento de Honorários Advocatícios');
    throw error;
  }
}

export async function processServidoresJetons() {
  const log = logger.forMethod('processServidoresJetons');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento de Jetons');

  try {
    while (hasMore) {
      const bronzeData = await bronze_db.select()
        .from(ServidoresJetons)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        hasMore = false;
        break;
      }

      const portaisLote = [...new Set(bronzeData.map(b => b.idServidorPortal).filter(Boolean) as string[])];
      const servidoresDb = portaisLote.length > 0
        ? await silver_db.select({ id: servidores.id, idServidorPortal: servidores.idServidorPortal })
            .from(servidores)
            .where(inArray(servidores.idServidorPortal, portaisLote))
        : [];
      const servidorIdMap = new Map(servidoresDb.map(s => [s.idServidorPortal, s.id]));

      const jetonsToInsert = bronzeData
        .map(b => {
          const servidorId = b.idServidorPortal ? servidorIdMap.get(b.idServidorPortal) : null;
          const ano = b.ano?.trim();
          const mes = b.mes?.trim().padStart(2, '0');
          const valor = parseDecimal(b.valor);

          if (!servidorId || !ano || !mes || !b.empresa || valor === null) return null;

          return {
            servidorId,
            ano: ano.substring(0, 4),
            mes: mes.substring(0, 2),
            empresa: b.empresa,
            valor,
          };
        })
        .filter(Boolean) as typeof servidoresJetons.$inferInsert[];

      if (jetonsToInsert.length > 0) {
        const servidorIds = [...new Set(jetonsToInsert.map(j => j.servidorId))];
        const anos = [...new Set(jetonsToInsert.map(j => j.ano))];
        const meses = [...new Set(jetonsToInsert.map(j => j.mes))];

        await silver_db.delete(servidoresJetons)
          .where(
            and(
              inArray(servidoresJetons.servidorId, servidorIds),
              inArray(servidoresJetons.ano, anos),
              inArray(servidoresJetons.mes, meses)
            )
          );

        log.info({ data: { count: jetonsToInsert.length } }, 'Inserindo servidores_jetons');
        for (let i = 0; i < jetonsToInsert.length; i += 1000) {
          await silver_db.insert(servidoresJetons).values(jetonsToInsert.slice(i, i + 1000));
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento de Jetons');
    throw error;
  }
}

export async function processServidoresRemuneracao() {
  const log = logger.forMethod('processServidoresRemuneracao');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento de Remunerações de Servidores');

  try {
    while (hasMore) {
      const bronzeData = await bronze_db.select()
        .from(ServidoresRemuneracao)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        hasMore = false;
        break;
      }

      const portaisLote = [...new Set(bronzeData.map(b => b.idServidorPortal).filter(Boolean) as string[])];
      const servidoresDb = portaisLote.length > 0
        ? await silver_db.select({ id: servidores.id, idServidorPortal: servidores.idServidorPortal })
            .from(servidores)
            .where(inArray(servidores.idServidorPortal, portaisLote))
        : [];
      const servidorIdMap = new Map(servidoresDb.map(s => [s.idServidorPortal, s.id]));

      const remuneracoesToInsert = bronzeData
        .map(b => {
          const servidorId = b.idServidorPortal ? servidorIdMap.get(b.idServidorPortal) : null;
          const ano = b.ano?.trim();
          const mes = b.mes?.trim().padStart(2, '0');
          const remuneracaoBasicaBrutaRs = parseDecimal(b.remuneracaoBasicaBrutaRs);
          const remuneracaoAposDeducoesRs = parseDecimal(b.remuneracaoAposDeducoesObrigatoriasRs);

          if (!servidorId || !ano || !mes || remuneracaoBasicaBrutaRs === null || remuneracaoAposDeducoesRs === null) {
            return null;
          }

          return {
            servidorId,
            ano: ano.substring(0, 4),
            mes: mes.substring(0, 2),
            remuneracaoBasicaBrutaRs,
            abateTetoRs: parseDecimal(b.abateTetoRs) || null,
            gratificacaoNatalinaRs: parseDecimal(b.gratificacaoNatalinaRs) || null,
            feriasRs: parseDecimal(b.feriasRs) || null,
            outrasRemuneracoesEventuaisRs: parseDecimal(b.outrasRemuneracoesEventuaisRs) || null,
            irrfRs: parseDecimal(b.irrfRs) || null,
            pssRpgsRs: parseDecimal(b.pssRpgsRs) || null,
            demaisDeducoesRs: parseDecimal(b.demaisDeducoesRs) || null,
            remuneracaoAposDeducoesRs,
            totalVerbasIndenizatoriasRs: parseDecimal(b.totalVerbasIndenizatoriasRs) || null,
            totalHonorariosJetons: parseDecimal(b.totalHonorariosJetons) || null,
          };
        })
        .filter(Boolean) as typeof servidoresRemuneracoes.$inferInsert[];

      if (remuneracoesToInsert.length > 0) {
        const servidorIds = [...new Set(remuneracoesToInsert.map(r => r.servidorId))];
        const anos = [...new Set(remuneracoesToInsert.map(r => r.ano))];
        const meses = [...new Set(remuneracoesToInsert.map(r => r.mes))];

        await silver_db.delete(servidoresRemuneracoes)
          .where(
            and(
              inArray(servidoresRemuneracoes.servidorId, servidorIds),
              inArray(servidoresRemuneracoes.ano, anos),
              inArray(servidoresRemuneracoes.mes, meses)
            )
          );

        log.info({ data: { count: remuneracoesToInsert.length } }, 'Inserindo servidores_remuneracoes');
        for (let i = 0; i < remuneracoesToInsert.length; i += 1000) {
          await silver_db.insert(servidoresRemuneracoes).values(remuneracoesToInsert.slice(i, i + 1000));
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento de Remunerações');
    throw error;
  }
}

export async function processServidores() {
  const log = logger.forMethod('processServidores');
  try {
    await processServidoresCadastro();
    await processAposentadosCadastro();
    await processPensionistasCadastro();
    await processServidoresAfastamentos();
    await processServidoresObservacoes();
    await processServidoresHonorariosAdvocaticios();
    await processServidoresJetons();
    await processServidoresRemuneracao();
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante a orquestração do módulo de Servidores');
    throw error;
  }
}