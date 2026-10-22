import { inArray, and, eq, sql } from 'drizzle-orm';
import { bronze_db } from '../../../database/bronze_connection.js';
import { silver_db } from '../../../database/silver_connection.js';
import { PeDeMeia } from '../../../database/bronze_schema.js';
import {
  beneficioPagamentos,
  pessoasFisicas,
  programasSociais
} from '../../../database/silver_schema.js';
import { formatCpf, formatAnoMes, parseDate, parseDecimal } from '../shared/formatters.js';
import { MunicipioResolver } from '../shared/municipio-resolver.js';
import { new_logger } from '../../../services/logs.js';
import { BATCH_SIZE } from '../shared/constants.js';

const logger = new_logger(import.meta.url);

export async function processPeDeMeia() {
  const log = logger.forMethod('processPeDeMeia');
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento do módulo Pé-de-Meia');

  try {
    // 1. Resolução do programa social (Somente Leitura)
    const [programaDb] = await silver_db.select({ id: programasSociais.id })
      .from(programasSociais)
      .where(eq(programasSociais.codigo, 'PE_DE_MEIA'));

    if (!programaDb) {
      throw new Error('Programa social PE_DE_MEIA não cadastrado na dimensão programas_sociais.');
    }

    const programaId = programaDb.id;

    while (hasMore) {
      // 2. Extração da Camada Bronze
      const bronzeData = await bronze_db.select()
        .from(PeDeMeia)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        log.info({ data: { offset } }, 'Processamento concluído. Nenhum registro adicional encontrado.');
        hasMore = false;
        break;
      }

      log.info({ data: { offset, batchSize: bronzeData.length } }, 'Lote extraído da tabela pe_de_meia');

      // 4. Dimensão Pessoas Físicas (Beneficiários e Responsáveis)
      const pfsLote: { cpf: string, nis: string | null, nome: string, municipioId: number | null }[] = [];

      bronzeData.forEach(b => {
        const municipioId = MunicipioResolver.resolve({
          ibge: b.codigoMunicipio,
          nome: b.nomeMunicipio,
          uf: b.uf
        });

        const cpfBenef = formatCpf(b.cpfBeneficiario);
        if (cpfBenef) {
          pfsLote.push({
            cpf: cpfBenef,
            nis: b.nisBeneficiario || null,
            nome: b.nomeBeneficiario || 'NÃO INFORMADO',
            municipioId
          });
        }

        const cpfResp = formatCpf(b.cpfResponsavel);
        if (cpfResp) {
          pfsLote.push({
            cpf: cpfResp,
            nis: b.nisResponsavel || null,
            nome: b.nomeResponsavel || 'NÃO INFORMADO',
            municipioId
          });
        }
      });

      const cpfsSet = [...new Set(pfsLote.map(p => p.cpf))];

      const cpfsExistentesDb = cpfsSet.length > 0
        ? await silver_db.select({ id: pessoasFisicas.id, cpf: pessoasFisicas.cpf })
          .from(pessoasFisicas)
          .where(inArray(pessoasFisicas.cpf, cpfsSet))
        : [];

      const cpfIdMap = new Map(cpfsExistentesDb.map(pf => [pf.cpf, pf.id]));
      const cpfsExistentesSet = new Set(cpfsExistentesDb.map(pf => pf.cpf));

      const pfToInsert = pfsLote
        .filter(p => !cpfsExistentesSet.has(p.cpf))
        .map(p => ({
          cpf: p.cpf,
          nis: p.nis,
          nome: p.nome,
          municipioId: p.municipioId,
        }));

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

      // 5. Entidade de Pagamentos (Benefício)
      const ocorrenciaMap = new Map<string, number>();

      const pagamentosToInsert = bronzeData
        .map(b => {
          const cpfBeneficiario = formatCpf(b.cpfBeneficiario);
          const beneficiarioId = cpfBeneficiario ? cpfIdMap.get(cpfBeneficiario) : null;

          const cpfResponsavel = formatCpf(b.cpfResponsavel);
          const responsavelId = cpfResponsavel ? cpfIdMap.get(cpfResponsavel) : null;

          const municipioId = MunicipioResolver.resolve({
            ibge: b.codigoMunicipio,
            nome: b.nomeMunicipio,
            uf: b.uf
          });

          const valor = parseDecimal(b.valorParcela);
          const mesCompetencia = formatAnoMes(b.mesFolha);
          const mesReferencia = formatAnoMes(b.mesReferencia);

          if (!beneficiarioId || !municipioId || !mesCompetencia || valor === null) return null;

          // Combinando tipo_parcela e periodo_parcela para compor o campo parcela
          const tipoParcela = b.tipoParcela ? b.tipoParcela.trim() : '';
          const periodoParcela = b.periodoParcela ? b.periodoParcela.trim() : '';
          const parcela = [tipoParcela, periodoParcela].filter(Boolean).join(' - ').substring(0, 50);

          const key = `${programaId}|${beneficiarioId}|${mesCompetencia}|${mesReferencia ?? ''}|${parcela ?? ''}|${valor}`;
          const ocorrencia = (ocorrenciaMap.get(key) || 0) + 1;
          ocorrenciaMap.set(key, ocorrencia);

          return {
            programaId,
            beneficiarioId,
            responsavelId: responsavelId || null,
            municipioId,
            mesCompetencia,
            mesReferencia,
            parcela: parcela || null,
            valor,
            ocorrencia,
            dataDisponibilizacao: parseDate(b.dataPagamento) || null,
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

        log.info({ data: { count: pagamentosToInsert.length } }, 'Inserindo beneficio_pagamentos (Pé-de-Meia)');
        for (let i = 0; i < pagamentosToInsert.length; i += 1000) {
          await silver_db.insert(beneficioPagamentos)
            .values(pagamentosToInsert.slice(i, i + 1000))
            .onDuplicateKeyUpdate({
              set: {
                valor: sql`VALUES(valor)`,
                dataDisponibilizacao: sql`COALESCE(VALUES(data_disponibilizacao), beneficio_pagamentos.data_disponibilizacao)`,
              },
            });
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento do Pé-de-Meia');
    throw error;
  }
}