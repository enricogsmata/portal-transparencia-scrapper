import { sql, inArray, and } from 'drizzle-orm';
import { bronze_db } from '../../../database/bronze_connection.js';
import { silver_db } from '../../../database/silver_connection.js';
import { 
  RenunciasFiscais, 
  RenunciasFiscaisPorBeneficiario, 
  EmpresasImunesOuIsentas, 
  EmpresasHabilitadas 
} from '../../../database/bronze_schema.js';
import { 
  renunciasFiscais, 
  empresasHabilitadas, 
  pessoasJuridicas, 
  cnaes 
} from '../../../database/silver_schema.js';
import { formatCnpj, parseDate, parseDecimal } from '../shared/formatters.js';
import { MunicipioResolver } from '../shared/municipio-resolver.js';
import { new_logger } from '../../../services/logs.js';
import { BATCH_SIZE } from '../shared/constants.js';

const logger = new_logger(import.meta.url);

interface EmpresaBronzeItem {
  cnpj?: string | null;
  razaoSocial?: string | null;
  nomeFantasia?: string | null;
  codigoCnae?: string | null;
  cnae?: string | null;
  municipio?: string | null;
  uf?: string | null;
}

async function syncEmpresasDimensions(bronzeBatch: EmpresaBronzeItem[]): Promise<Map<string, number>> {
  // 1. Dimensão CNAEs
  const cnaesToUpsertMap = new Map<string, typeof cnaes.$inferInsert>();
  bronzeBatch.forEach((b) => {
    if (b.codigoCnae) {
      cnaesToUpsertMap.set(b.codigoCnae, {
        codigoSubclasse: b.codigoCnae,
        descricaoSubclasse: b.cnae || 'NÃO INFORMADO',
      });
    }
  });

  const cnaesToUpsert = Array.from(cnaesToUpsertMap.values());
  if (cnaesToUpsert.length > 0) {
    await silver_db.insert(cnaes)
      .values(cnaesToUpsert)
      .onDuplicateKeyUpdate({ set: { descricaoSubclasse: sql`VALUES(descricao_subclasse)` } });
  }

  const cnaesDb = cnaesToUpsert.length > 0
    ? await silver_db.select({ id: cnaes.id, codigoSubclasse: cnaes.codigoSubclasse })
        .from(cnaes)
        .where(inArray(cnaes.codigoSubclasse, cnaesToUpsert.map(c => c.codigoSubclasse)))
    : [];
  const cnaeIdMap = new Map(cnaesDb.map(c => [c.codigoSubclasse, c.id]));

  // 3. Dimensão Pessoas Jurídicas
  const pjsToUpsertMap = new Map<string, typeof pessoasJuridicas.$inferInsert>();
  bronzeBatch.forEach(b => {
    const cnpj = formatCnpj(b.cnpj);
    if (cnpj) {
      const cnaeId = b.codigoCnae ? cnaeIdMap.get(b.codigoCnae) || null : null;
      const municipioId = MunicipioResolver.resolve({
         nome: b.municipio,
         uf: b.uf
      });

      pjsToUpsertMap.set(cnpj, {
        cnpj,
        razaoSocial: b.razaoSocial || 'NÃO INFORMADO',
        nomeFantasia: b.nomeFantasia || null,
        cnaeId,
        municipioId,
        tipoPessoa: 'J',
      });
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

  return new Map(pjsDb.map(p => [p.cnpj, p.id]));
}

export async function processRenunciasFiscais() {
  const log = logger.forMethod('processRenunciasFiscais');
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento de Renúncias Fiscais');

  try {
    while (hasMore) {
      const bronzeData = await bronze_db.select()
        .from(RenunciasFiscais)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        log.info({ data: { offset } }, 'Processamento concluído. Nenhum registro adicional encontrado.');
        hasMore = false;
        break;
      }

      log.info({ data: { offset, batchSize: bronzeData.length } }, 'Lote extraído da tabela renuncias_fiscais');

      const pjIdMap = await syncEmpresasDimensions(bronzeData);

      const renunciasToInsert = bronzeData.map(b => {
        const cnpj = formatCnpj(b.cnpj);
        const beneficiarioId = cnpj ? pjIdMap.get(cnpj) : null;
        const anoCalendario = (b.anoCalendario || b.exercicioReferencia || '').trim().substring(0, 4);

        if (!beneficiarioId || !anoCalendario) return null;

        return {
          anoCalendario,
          beneficiarioId,
          tipoRenuncia: b.tipoRenuncia || null,
          beneficioFiscal: b.beneficioFiscal || 'NÃO INFORMADO',
          fundamentoLegal: b.fundamentoLegal || null,
          descricao: b.descricao || null,
          tributo: b.tributo || null,
          formaTributacao: b.formaTributacao || null,
          valorRenunciaFiscal: parseDecimal(b.valorRenunciaFiscal) || '0',
        };
      }).filter(Boolean) as typeof renunciasFiscais.$inferInsert[];

      if (renunciasToInsert.length > 0) {
        const anosLote = [...new Set(renunciasToInsert.map(r => r.anoCalendario))];
        const beneficiariosLote = [...new Set(renunciasToInsert.map(r => r.beneficiarioId))];

        await silver_db.delete(renunciasFiscais)
          .where(
            and(
              inArray(renunciasFiscais.anoCalendario, anosLote),
              inArray(renunciasFiscais.beneficiarioId, beneficiariosLote)
            )
          );

        log.info({ data: { count: renunciasToInsert.length } }, 'Inserindo renuncias_fiscais');
        for (let i = 0; i < renunciasToInsert.length; i += 1000) {
          await silver_db.insert(renunciasFiscais).values(renunciasToInsert.slice(i, i + 1000));
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento de Renúncias Fiscais');
    throw error;
  }
}

export async function processRenunciasFiscaisPorBeneficiario() {
  const log = logger.forMethod('processRenunciasFiscaisPorBeneficiario');
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento de Renúncias Fiscais por Beneficiário');

  try {
    while (hasMore) {
      const bronzeData = await bronze_db.select()
        .from(RenunciasFiscaisPorBeneficiario)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        hasMore = false;
        break;
      }

      log.info({ data: { offset, batchSize: bronzeData.length } }, 'Lote extraído da tabela renuncias_fiscais_por_beneficiario');

      const pjIdMap = await syncEmpresasDimensions(bronzeData);

      const renunciasToInsert = bronzeData.map(b => {
        const cnpj = formatCnpj(b.cnpj);
        const beneficiarioId = cnpj ? pjIdMap.get(cnpj) : null;
        const anoCalendario = (b.anoCalendario || b.exercicioReferencia || '').trim().substring(0, 4);

        if (!beneficiarioId || !anoCalendario) return null;

        return {
          anoCalendario,
          beneficiarioId,
          tipoRenuncia: 'POR BENEFICIARIO',
          beneficioFiscal: 'RENÚNCIA FISCAL POR BENEFICIÁRIO',
          fundamentoLegal: null,
          descricao: null,
          tributo: null,
          formaTributacao: null,
          valorRenunciaFiscal: parseDecimal(b.valorRenunciaFiscal) || '0',
        };
      }).filter(Boolean) as typeof renunciasFiscais.$inferInsert[];

      if (renunciasToInsert.length > 0) {
        const anosLote = [...new Set(renunciasToInsert.map(r => r.anoCalendario))];
        const beneficiariosLote = [...new Set(renunciasToInsert.map(r => r.beneficiarioId))];

        await silver_db.delete(renunciasFiscais)
          .where(
            and(
              inArray(renunciasFiscais.anoCalendario, anosLote),
              inArray(renunciasFiscais.beneficiarioId, beneficiariosLote),
              sql`${renunciasFiscais.tipoRenuncia} = 'POR BENEFICIARIO'`
            )
          );

        log.info({ data: { count: renunciasToInsert.length } }, 'Inserindo renuncias_fiscais (Por Beneficiário)');
        for (let i = 0; i < renunciasToInsert.length; i += 1000) {
          await silver_db.insert(renunciasFiscais).values(renunciasToInsert.slice(i, i + 1000));
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento de Renúncias Fiscais por Beneficiário');
    throw error;
  }
}

export async function processEmpresasImunesOuIsentas() {
  const log = logger.forMethod('processEmpresasImunesOuIsentas');
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento de Empresas Imunes ou Isentas');

  try {
    while (hasMore) {
      const bronzeData = await bronze_db.select()
        .from(EmpresasImunesOuIsentas)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        hasMore = false;
        break;
      }

      log.info({ data: { offset, batchSize: bronzeData.length } }, 'Lote extraído da tabela empresas_imunes_ou_isentas');

      const pjIdMap = await syncEmpresasDimensions(bronzeData);

      const habilitadasToInsert = bronzeData.map(b => {
        const cnpj = formatCnpj(b.cnpj);
        const empresaId = cnpj ? pjIdMap.get(cnpj) : null;
        if (!empresaId) return null;

        const ano = (b.anoCalendario || b.exercicioReferencia || '').trim().substring(0, 4);
        const inicioHabilitacao = ano ? `${ano}-01-01` : null;
        const fimHabilitacao = ano ? `${ano}-12-31` : null;

        return {
          empresaId,
          beneficioFiscal: b.beneficioFiscal || 'IMUNE OU ISENTA',
          baseLegal: null,
          descricao: b.tipoEntidade ? `Tipo de Entidade: ${b.tipoEntidade}` : 'EMPRESA IMUNE OU ISENTA',
          inicioHabilitacao,
          fimHabilitacao,
        };
      }).filter(Boolean) as typeof empresasHabilitadas.$inferInsert[];

      if (habilitadasToInsert.length > 0) {
        const empresasIds = [...new Set(habilitadasToInsert.map(h => h.empresaId))];

        await silver_db.delete(empresasHabilitadas)
          .where(
            and(
              inArray(empresasHabilitadas.empresaId, empresasIds),
              sql`${empresasHabilitadas.descricao} LIKE 'Tipo de Entidade:%' OR ${empresasHabilitadas.descricao} = 'EMPRESA IMUNE OU ISENTA'`
            )
          );

        log.info({ data: { count: habilitadasToInsert.length } }, 'Inserindo empresas_habilitadas (Imunes ou Isentas)');
        for (let i = 0; i < habilitadasToInsert.length; i += 1000) {
          await silver_db.insert(empresasHabilitadas).values(habilitadasToInsert.slice(i, i + 1000));
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento de Empresas Imunes ou Isentas');
    throw error;
  }
}

export async function processEmpresasHabilitadas() {
  const log = logger.forMethod('processEmpresasHabilitadas');
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento de Empresas Habilitadas');

  try {
    while (hasMore) {
      const bronzeData = await bronze_db.select()
        .from(EmpresasHabilitadas)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        hasMore = false;
        break;
      }

      log.info({ data: { offset, batchSize: bronzeData.length } }, 'Lote extraído da tabela empresas_habilitadas');

      const pjIdMap = await syncEmpresasDimensions(bronzeData);

      const habilitadasToInsert = bronzeData.map(b => {
        const cnpj = formatCnpj(b.cnpj);
        const empresaId = cnpj ? pjIdMap.get(cnpj) : null;
        if (!empresaId) return null;

        return {
          empresaId,
          beneficioFiscal: b.beneficioFiscal || 'NÃO INFORMADO',
          baseLegal: b.baseLegal || null,
          descricao: b.descricao || null,
          inicioHabilitacao: parseDate(b.inicioHabilitacao) || null,
          fimHabilitacao: parseDate(b.fimHabilitacao) || null,
        };
      }).filter(Boolean) as typeof empresasHabilitadas.$inferInsert[];

      if (habilitadasToInsert.length > 0) {
        const empresasIds = [...new Set(habilitadasToInsert.map(h => h.empresaId))];

        await silver_db.delete(empresasHabilitadas)
          .where(
            and(
              inArray(empresasHabilitadas.empresaId, empresasIds),
              sql`(${empresasHabilitadas.descricao} NOT LIKE 'Tipo de Entidade:%' AND ${empresasHabilitadas.descricao} != 'EMPRESA IMUNE OU ISENTA') OR ${empresasHabilitadas.descricao} IS NULL`
            )
          );

        log.info({ data: { count: habilitadasToInsert.length } }, 'Inserindo empresas_habilitadas');
        for (let i = 0; i < habilitadasToInsert.length; i += 1000) {
          await silver_db.insert(empresasHabilitadas).values(habilitadasToInsert.slice(i, i + 1000));
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento de Empresas Habilitadas');
    throw error;
  }
}

export async function processRenuncias() {
  const log = logger.forMethod('processRenuncias');
  try {
    await processRenunciasFiscais();
    await processRenunciasFiscaisPorBeneficiario();
    await processEmpresasImunesOuIsentas();
    await processEmpresasHabilitadas();
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante a orquestração do módulo de Renúncias');
    throw error;
  }
}