import { sql, inArray, and, eq } from 'drizzle-orm';
import { bronze_db } from '../../../database/bronze_connection.js';
import { silver_db } from '../../../database/silver_connection.js';
import { GasDoPovo } from '../../../database/bronze_schema.js';
import {
  beneficioPagamentos,
  pessoasFisicas,
  pessoasJuridicas,
  programasSociais
} from '../../../database/silver_schema.js';
import { formatCpf, formatCnpj, formatAnoMes, parseDate, parseDecimal } from '../shared/formatters.js';
import { MunicipioResolver } from '../shared/municipio-resolver.js';
import { new_logger } from '../../../services/logs.js';
import { BATCH_SIZE } from '../shared/constants.js';

const logger = new_logger(import.meta.url);

export async function processGasDoPovo() {
  const log = logger.forMethod('processGasDoPovo');
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento do módulo Gás do Povo');

  try {
    // 1. Resolução do programa social (Somente Leitura)
    const [programaDb] = await silver_db.select({ id: programasSociais.id })
      .from(programasSociais)
      .where(eq(programasSociais.codigo, 'GAS_DO_POVO'));

    if (!programaDb) {
      throw new Error('Programa social GAS_DO_POVO não cadastrado na dimensão programas_sociais.');
    }

    const programaId = programaDb.id;

    while (hasMore) {
      // 2. Extração da Camada Bronze
      const bronzeData = await bronze_db.select()
        .from(GasDoPovo)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        log.info({ data: { offset } }, 'Processamento concluído. Nenhum registro adicional encontrado.');
        hasMore = false;
        break;
      }

      log.info({ data: { offset, batchSize: bronzeData.length } }, 'Lote extraído da tabela gas_do_povo');

      // 4. Dimensão Pessoas Físicas (Beneficiários)
      const pfsLote = bronzeData.filter(b => formatCpf(b.cpfBeneficiario));
      const cpfsSet = [...new Set(pfsLote.map(p => formatCpf(p.cpfBeneficiario)!).filter(Boolean))];

      const cpfsExistentesDb = cpfsSet.length > 0
        ? await silver_db.select({ id: pessoasFisicas.id, cpf: pessoasFisicas.cpf })
          .from(pessoasFisicas)
          .where(inArray(pessoasFisicas.cpf, cpfsSet))
        : [];

      const cpfIdMap = new Map(cpfsExistentesDb.map(pf => [pf.cpf, pf.id]));
      const cpfsExistentesSet = new Set(cpfsExistentesDb.map(pf => pf.cpf));

      const pfToInsert = pfsLote
        .filter(p => !cpfsExistentesSet.has(formatCpf(p.cpfBeneficiario)!))
        .map(b => {
          const cpf = formatCpf(b.cpfBeneficiario)!;
          const municipioId = MunicipioResolver.resolve({
            ibge: b.codigoMunicipio,
            nome: b.nomeMunicipio,
            uf: b.uf
          });
          return {
            cpf,
            nome: b.nomeBeneficiario || 'NÃO INFORMADO',
            municipioId: municipioId || null,
          };
        });

      if (pfToInsert.length > 0) {
        const uniquePfMap = new Map<string, typeof pfToInsert[0]>();
        for (const pf of pfToInsert) uniquePfMap.set(pf.cpf, pf);
        const uniquePfs = Array.from(uniquePfMap.values());

        await silver_db.insert(pessoasFisicas).ignore().values(uniquePfs);

        const novosPfsDb = await silver_db.select({ id: pessoasFisicas.id, cpf: pessoasFisicas.cpf })
          .from(pessoasFisicas)
          .where(inArray(pessoasFisicas.cpf, uniquePfs.map(p => p.cpf)));

        novosPfsDb.forEach(pf => cpfIdMap.set(pf.cpf, pf.id));
      }

      // 5. Dimensão Pessoas Jurídicas (Estabelecimentos Comerciais/Revendedores)
      const pjsToUpsertMap = new Map<string, typeof pessoasJuridicas.$inferInsert>();
      bronzeData.forEach((b) => {
        const cnpj = formatCnpj(b.cnpjEstabelecimento);
        if (cnpj) {
          pjsToUpsertMap.set(cnpj, {
            cnpj,
            razaoSocial: b.nomeEstabelecimento || 'NÃO INFORMADO'
          });
        }
      });
      const pjsToUpsert = Array.from(pjsToUpsertMap.values());

      if (pjsToUpsert.length > 0) {
        await silver_db.insert(pessoasJuridicas)
          .values(pjsToUpsert)
          .onDuplicateKeyUpdate({ set: { id: sql`id` } });
      }

      // 6. Entidade de Pagamentos (Gás do Povo)
      const ocorrenciaMap = new Map<string, number>();

      const pagamentosToInsert = bronzeData
        .map(b => {
          const cpf = formatCpf(b.cpfBeneficiario);
          const beneficiarioId = cpf ? cpfIdMap.get(cpf) : null;

          const municipioId = MunicipioResolver.resolve({
            ibge: b.codigoMunicipio,
            nome: b.nomeMunicipio,
            uf: b.uf
          });

          const valor = parseDecimal(b.valorBeneficio);
          const mesCompetencia = formatAnoMes(b.mesReferencia);

          if (!beneficiarioId || !municipioId || !mesCompetencia || valor === null) return null;

          const key = `${programaId}|${beneficiarioId}|${mesCompetencia}|${valor}`;
          const ocorrencia = (ocorrenciaMap.get(key) || 0) + 1;
          ocorrenciaMap.set(key, ocorrencia);

          return {
            programaId,
            beneficiarioId,
            municipioId,
            mesCompetencia,
            mesReferencia: mesCompetencia,
            valor,
            ocorrencia,
            dataDisponibilizacao: parseDate(b.dataInicioVigenciaVale) || null,
            dataSaque: parseDate(b.dataRetiradaVale) || null,
          };
        })
        .filter(Boolean) as typeof beneficioPagamentos.$inferInsert[];

      if (pagamentosToInsert.length > 0) {
        // Garantir UPSERT seguro deletando sobreposições para este programa/competência/beneficiário
        const beneficiariosIdsLote = [...new Set(pagamentosToInsert.map(p => p.beneficiarioId))];
        const mesesCompetenciaLote = [...new Set(pagamentosToInsert.map(p => p.mesCompetencia))];

        if (beneficiariosIdsLote.length > 0 && mesesCompetenciaLote.length > 0) {
          await silver_db.delete(beneficioPagamentos)
            .where(
              and(
                eq(beneficioPagamentos.programaId, programaId),
                inArray(beneficioPagamentos.mesCompetencia, mesesCompetenciaLote),
                inArray(beneficioPagamentos.beneficiarioId, beneficiariosIdsLote)
              )
            );
        }

        log.info({ data: { count: pagamentosToInsert.length } }, 'Inserindo beneficio_pagamentos (Gás do Povo)');
        for (let i = 0; i < pagamentosToInsert.length; i += 1000) {
          await silver_db.insert(beneficioPagamentos)
            .values(pagamentosToInsert.slice(i, i + 1000))
            .onDuplicateKeyUpdate({
              set: {
                valor: sql`VALUES(valor)`,
                dataDisponibilizacao: sql`COALESCE(VALUES(data_disponibilizacao), beneficio_pagamentos.data_disponibilizacao)`,
                dataSaque: sql`COALESCE(VALUES(data_saque), beneficio_pagamentos.data_saque)`,
              },
            });
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento do Gás do Povo');
    throw error;
  }
}