import { sql, inArray, eq, and } from 'drizzle-orm';
import { bronze_db } from '../../../database/bronze_connection.js';
import { silver_db } from '../../../database/silver_connection.js';
import { AuxilioEmergencial } from '../../../database/bronze_schema.js';
import {
  beneficioPagamentos,
  municipios,
  pessoasFisicas,
  programasSociais
} from '../../../database/silver_schema.js';
import { parseDecimal } from '../shared/formatters.js';
import { MunicipioResolver } from '../shared/municipio-resolver.js';
import { new_logger } from '../../../services/logs.js';

const logger = new_logger(import.meta.url);

function formatCpf(cpf?: string | null): string | null {
  if (!cpf || cpf.trim() === '' || cpf.trim().toUpperCase() === 'NÃO APLICÁVEL') return null;
  const numbers = cpf.replace(/\D/g, '');
  return numbers.length > 0 ? numbers.padStart(11, '0') : null;
}

export async function processAuxilioEmergencial() {
  const log = logger.forMethod('processAuxilioEmergencial');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento do módulo Auxílio Emergencial');

  try {
    // 1. Garantir existência do programa social (Dimensão Master)
    await silver_db.insert(programasSociais)
      .values({
        codigo: 'AUXILIO_EMERGENCIAL',
        nome: 'Auxílio Emergencial',
        descricao: 'Programa de concessões e parcelas do Auxílio Emergencial e extensões',
      })
      .onDuplicateKeyUpdate({
        set: { nome: sql`VALUES(nome)` },
      });

    const [programaDb] = await silver_db.select({ id: programasSociais.id })
      .from(programasSociais)
      .where(eq(programasSociais.codigo, 'AUXILIO_EMERGENCIAL'));

    const programaId = programaDb.id;

    while (hasMore) {
      // 2. Extração da Camada Bronze
      const bronzeData = await bronze_db.select()
        .from(AuxilioEmergencial)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        log.info({ data: { offset } }, 'Processamento concluído. Nenhum registro adicional encontrado.');
        hasMore = false;
        break;
      }

      log.info({ data: { offset, batchSize: bronzeData.length } }, 'Lote extraído da tabela auxilio_emergencial');

      // src/pipeline/silver/transformers/auxilio-emergencial.ts
      // 3. Dimensão Municípios (IBGE)
      const municipiosToUpsert = bronzeData
        .filter((b) => b.codigoMunicipioIbge && b.uf)
        .map((b) => ({
          codigoIbge: b.codigoMunicipioIbge!,
          nome: b.nomeMunicipio || 'NÃO INFORMADO',
          ufSigla: b.uf!.substring(0, 2).toUpperCase(),
        }));

      if (municipiosToUpsert.length > 0) {
        const uniqueMunMap = new Map<string, typeof municipiosToUpsert[0]>();
        for (const m of municipiosToUpsert) uniqueMunMap.set(m.codigoIbge, m);
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

      const codigosIbge = [...new Set(municipiosToUpsert.map(m => m.codigoIbge))];
      const municipiosDb = codigosIbge.length > 0
        ? await silver_db.select({ id: municipios.id, codigoIbge: municipios.codigoIbge })
          .from(municipios)
          .where(inArray(municipios.codigoIbge, codigosIbge))
        : [];
      const munIdMap = new Map(municipiosDb.map(m => [m.codigoIbge!, m.id]));

      // 4. Dimensão Pessoas Físicas (Beneficiários e Responsáveis)
      const pessoasMap = new Map<string, { cpf: string, nis: string | null, nome: string }>();

      for (const b of bronzeData) {
        const cpfBen = formatCpf(b.cpfBeneficiario);
        if (cpfBen) {
          pessoasMap.set(cpfBen, {
            cpf: cpfBen,
            nis: b.nisBeneficiario || null,
            nome: b.nomeBeneficiario || 'NÃO INFORMADO'
          });
        }

        const cpfResp = formatCpf(b.cpfResponsavel);
        if (cpfResp) {
          pessoasMap.set(cpfResp, {
            cpf: cpfResp,
            nis: b.nisResponsavel || null,
            nome: b.nomeResponsavel || 'NÃO INFORMADO'
          });
        }
      }

      const cpfsLote = Array.from(pessoasMap.keys());
      const cpfsExistentesDb = cpfsLote.length > 0
        ? await silver_db.select({ id: pessoasFisicas.id, cpf: pessoasFisicas.cpf })
          .from(pessoasFisicas)
          .where(inArray(pessoasFisicas.cpf, cpfsLote))
        : [];

      const cpfIdMap = new Map(cpfsExistentesDb.map(pf => [pf.cpf, pf.id]));
      const cpfsExistentesSet = new Set(cpfsExistentesDb.map(pf => pf.cpf));

      const pfToInsert = Array.from(pessoasMap.values())
        .filter(p => !cpfsExistentesSet.has(p.cpf))
        .map(p => ({
          cpf: p.cpf,
          nis: p.nis,
          nome: p.nome,
        }));

      if (pfToInsert.length > 0) {
        await silver_db.insert(pessoasFisicas).values(pfToInsert);

        const novosPfsDb = await silver_db.select({ id: pessoasFisicas.id, cpf: pessoasFisicas.cpf })
          .from(pessoasFisicas)
          .where(inArray(pessoasFisicas.cpf, pfToInsert.map(p => p.cpf)));

        novosPfsDb.forEach(pf => cpfIdMap.set(pf.cpf, pf.id));
      }

      // 5. Entidade de Pagamentos
      const pagamentosToInsert = bronzeData
        .map(b => {
          const beneficiarioCpf = formatCpf(b.cpfBeneficiario);
          const responsavelCpf = formatCpf(b.cpfResponsavel);

          const beneficiarioId = beneficiarioCpf ? cpfIdMap.get(beneficiarioCpf) : null;
          const responsavelId = responsavelCpf ? cpfIdMap.get(responsavelCpf) : null;
          const municipioId = MunicipioResolver.resolve({
            ibge: b.codigoMunicipioIbge,
            nome: b.nomeMunicipio,
            uf: b.uf,
          });
          const valor = parseDecimal(b.valorBeneficio);

          if (!beneficiarioId || !municipioId || !b.mesDisponibilizacao || valor === null) return null;

          return {
            programaId,
            beneficiarioId,
            responsavelId: responsavelId || null,
            municipioId,
            mesCompetencia: b.mesDisponibilizacao.substring(0, 7), // Tratamento seguro de tamanho
            mesReferencia: b.mesReferencia ? b.mesReferencia.substring(0, 7) : null,
            parcela: b.parcela || null,
            valor,
          };
        })
        .filter(Boolean) as typeof beneficioPagamentos.$inferInsert[];

      if (pagamentosToInsert.length > 0) {
        // Garantir UPSERT seguro/Idempotência deletando registros sobrepostos para este programa/competência/beneficiário
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

        log.info({ data: { count: pagamentosToInsert.length } }, 'Inserindo beneficio_pagamentos');
        for (let i = 0; i < pagamentosToInsert.length; i += 1000) {
          await silver_db.insert(beneficioPagamentos).values(pagamentosToInsert.slice(i, i + 1000));
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento do Auxílio Emergencial');
    throw error;
  }
}