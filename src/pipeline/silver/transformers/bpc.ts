import { inArray, eq, and } from 'drizzle-orm';
import { bronze_db } from '../../../database/bronze_connection.js';
import { silver_db } from '../../../database/silver_connection.js';
import { Bpc } from '../../../database/bronze_schema.js';
import {
  beneficioPagamentos,
  pessoasFisicas,
  programasSociais
} from '../../../database/silver_schema.js';
import { MunicipioResolver } from '../shared/municipio-resolver.js';
import { new_logger } from '../../../services/logs.js';
import { parseDecimal } from '../shared/formatters.js';

const logger = new_logger(import.meta.url);

function formatCpf(cpf?: string | null): string | null {
  if (!cpf || cpf.trim() === '' || cpf.trim().toUpperCase() === 'NÃO APLICÁVEL') return null;
  const numbers = cpf.replace(/\D/g, '');
  return numbers.length > 0 ? numbers.padStart(11, '0') : null;
}

function formatAnoMes(anoMes?: string | null): string | null {
  if (!anoMes) return null;
  const cleaned = anoMes.replace(/\D/g, '');
  if (cleaned.length === 6) {
    return `${cleaned.substring(0, 4)}-${cleaned.substring(4, 6)}`;
  }
  return cleaned.substring(0, 7);
}

function parseConcedidoJudicialmente(valor?: string | null): boolean {
  if (!valor) return false;
  return valor.trim().toUpperCase() === 'SIM';
}

export async function processBpc() {
  const log = logger.forMethod('processBpc');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento do módulo BPC (Benefício de Prestação Continuada)');

  try {
    // 1. Resolução do programa social (Somente Leitura)
    const [programaDb] = await silver_db.select({ id: programasSociais.id })
      .from(programasSociais)
      .where(eq(programasSociais.codigo, 'BPC'));

    if (!programaDb) {
      throw new Error('Programa social BPC não cadastrado na dimensão programas_sociais.');
    }

    const programaId = programaDb.id;

    while (hasMore) {
      // 2. Extração da Camada Bronze
      const bronzeData = await bronze_db.select()
        .from(Bpc)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        log.info({ data: { offset } }, 'Processamento concluído. Nenhum registro adicional encontrado.');
        hasMore = false;
        break;
      }

      log.info({ data: { offset, batchSize: bronzeData.length } }, 'Lote extraído da tabela bpc');

      // 4. Dimensão Pessoas Físicas (Beneficiários e Representantes Legais)
      const pessoasMap = new Map<string, { cpf: string, nis: string | null, nome: string, munId: number | null }>();

      for (const b of bronzeData) {
        const munId = MunicipioResolver.resolve({
          siafi: b.codigoMunicipioSiafi,
          nome: b.nomeMunicipio,
          uf: b.uf,
        });

        const cpfBen = formatCpf(b.cpfBeneficiario);
        if (cpfBen) {
          pessoasMap.set(cpfBen, {
            cpf: cpfBen,
            nis: b.nisBeneficiario || null,
            nome: b.nomeBeneficiario || 'NÃO INFORMADO',
            munId
          });
        }

        const cpfResp = formatCpf(b.cpfRepresentanteLegal);
        if (cpfResp) {
          pessoasMap.set(cpfResp, {
            cpf: cpfResp,
            nis: b.nisRepresentanteLegal || null,
            nome: b.nomeRepresentanteLegal || 'NÃO INFORMADO',
            munId: null // O município costuma ser atrelado ao beneficiário primário
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
          municipioId: p.munId
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
          const responsavelCpf = formatCpf(b.cpfRepresentanteLegal);
          
          const beneficiarioId = beneficiarioCpf ? cpfIdMap.get(beneficiarioCpf) : null;
          const responsavelId = responsavelCpf ? cpfIdMap.get(responsavelCpf) : null;
          const municipioId = MunicipioResolver.resolve({
            siafi: b.codigoMunicipioSiafi,
            nome: b.nomeMunicipio,
            uf: b.uf,
          });
          
          const valor = parseDecimal(b.valorParcela);
          const mesCompetencia = formatAnoMes(b.mesCompetencia);
          const mesReferencia = formatAnoMes(b.mesReferencia);
          
          if (!beneficiarioId || !municipioId || !mesCompetencia || valor === null) return null;

          return {
            programaId,
            beneficiarioId,
            responsavelId: responsavelId || null,
            municipioId,
            mesCompetencia,
            mesReferencia,
            valor,
            concedidoJudicialmente: parseConcedidoJudicialmente(b.concedidoJudicialmente),
            numeroBeneficio: b.numeroBeneficio || null,
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

        log.info({ data: { count: pagamentosToInsert.length } }, 'Inserindo beneficio_pagamentos (BPC)');
        for (let i = 0; i < pagamentosToInsert.length; i += 1000) {
          await silver_db.insert(beneficioPagamentos).values(pagamentosToInsert.slice(i, i + 1000));
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento do BPC');
    throw error;
  }
}