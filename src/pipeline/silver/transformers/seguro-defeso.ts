import { inArray, and, eq, sql } from 'drizzle-orm';
import { bronze_db } from '../../../database/bronze_connection.js';
import { silver_db } from '../../../database/silver_connection.js';
import { SeguroDefeso } from '../../../database/bronze_schema.js';
import {
  beneficioPagamentos,
  pessoasFisicas,
  programasSociais
} from '../../../database/silver_schema.js';
import { formatCpf, formatAnoMes, parseDecimal, formatCpfByNis } from '../shared/formatters.js';
import { MunicipioResolver } from '../shared/municipio-resolver.js';
import { new_logger } from '../../../services/logs.js';
import { BATCH_SIZE } from '../shared/constants.js';

const logger = new_logger(import.meta.url);

export async function processSeguroDefeso() {
  const log = logger.forMethod('processSeguroDefeso');
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento do módulo Seguro-Defeso');

  try {
    // 1. Resolução do programa social (Somente Leitura)
    const [programaDb] = await silver_db.select({ id: programasSociais.id })
      .from(programasSociais)
      .where(eq(programasSociais.codigo, 'SEGURO_DEFESO'));

    if (!programaDb) {
      throw new Error('Programa social SEGURO_DEFESO não cadastrado na dimensão programas_sociais.');
    }

    const programaId = programaDb.id;

    while (hasMore) {
      // 2. Extração da Camada Bronze
      const bronzeData = await bronze_db.select()
        .from(SeguroDefeso)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        log.info({ data: { offset } }, 'Processamento concluído. Nenhum registro adicional encontrado.');
        hasMore = false;
        break;
      }

      log.info({ data: { offset, batchSize: bronzeData.length } }, 'Lote extraído da tabela seguro_defeso');

      // 4. Dimensão Pessoas Físicas (Beneficiários)
      const pfsLote = bronzeData
        .map(b => {
          const cpf = formatCpf(b.cpfFavorecido) || (b.nisFavorecido ? formatCpfByNis(b.nisFavorecido) : null);
          if (!cpf) return null;

          const municipioId = MunicipioResolver.resolve({
            siafi: b.codigoSiafiMunicipio,
            nome: b.nomeMunicipio,
            uf: b.uf
          });

          return {
            cpf,
            nis: b.nisFavorecido || null,
            nome: b.nomeFavorecido || 'NÃO INFORMADO',
            municipioId: municipioId || null,
          };
        })
        .filter(Boolean) as { cpf: string; nis: string | null; nome: string; municipioId: number | null }[];

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
          const cpf = formatCpf(b.cpfFavorecido) || (b.nisFavorecido ? formatCpfByNis(b.nisFavorecido) : null);
          const beneficiarioId = cpf ? cpfIdMap.get(cpf) : null;

          const municipioId = MunicipioResolver.resolve({
            siafi: b.codigoSiafiMunicipio,
            nome: b.nomeMunicipio,
            uf: b.uf
          });

          const valor = parseDecimal(b.valorParcela);
          const mesCompetencia = formatAnoMes(b.anoMesReferencia);
          const mesReferencia = b.mesReferencia
            ? (b.exercicioReferencia ? `${b.exercicioReferencia}-${b.mesReferencia.padStart(2, '0')}` : formatAnoMes(b.mesReferencia))
            : null;

          if (!beneficiarioId || !municipioId || !mesCompetencia || valor === null) return null;

          const numeroBeneficio = b.rgpFavorecido ? b.rgpFavorecido.trim().slice(0, 30) : null;
          const key = `${programaId}|${beneficiarioId}|${mesCompetencia}|${mesReferencia ?? ''}|${numeroBeneficio ?? ''}|${valor}`;
          const ocorrencia = (ocorrenciaMap.get(key) || 0) + 1;
          ocorrenciaMap.set(key, ocorrencia);

          return {
            programaId,
            beneficiarioId,
            municipioId,
            mesCompetencia,
            mesReferencia,
            valor,
            numeroBeneficio,
            ocorrencia,
          };
        })
        .filter(Boolean) as typeof beneficioPagamentos.$inferInsert[];

      if (pagamentosToInsert.length > 0) {
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

        log.info({ data: { count: pagamentosToInsert.length } }, 'Inserindo beneficio_pagamentos (Seguro-Defeso)');
        for (let i = 0; i < pagamentosToInsert.length; i += 1000) {
          await silver_db.insert(beneficioPagamentos)
            .values(pagamentosToInsert.slice(i, i + 1000))
            .onDuplicateKeyUpdate({
              set: {
                valor: sql`VALUES(valor)`,
                numeroBeneficio: sql`COALESCE(VALUES(numero_beneficio), beneficio_pagamentos.numero_beneficio)`,
              },
            });
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento do Seguro-Defeso');
    throw error;
  }
}