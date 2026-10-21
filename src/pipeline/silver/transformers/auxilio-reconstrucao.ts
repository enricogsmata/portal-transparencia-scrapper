import { sql, inArray, eq, and } from 'drizzle-orm';
import { bronze_db } from '../../../database/bronze_connection.js';
import { silver_db } from '../../../database/silver_connection.js';
import { AuxilioReconstrucao } from '../../../database/bronze_schema.js';
import {
  beneficioPagamentos,
  municipios,
  pessoasFisicas,
  programasSociais
} from '../../../database/silver_schema.js';
import { parseDate, parseDecimal } from '../shared/formatters.js';
import { MunicipioResolver } from '../shared/municipio-resolver.js';
import { new_logger } from '../../../services/logs.js';

const logger = new_logger(import.meta.url);

function formatCpf(cpf?: string | null): string | null {
  if (!cpf || cpf.trim() === '') return null;
  const numbers = cpf.replace(/\D/g, '');
  return numbers.length > 0 ? numbers.padStart(11, '0') : null;
}

export async function processAuxilioReconstrucao() {
  const log = logger.forMethod('processAuxilioReconstrucao');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento do módulo Auxílio Reconstrução');

  try {
    // 1. Garantir existência do programa social (Dimensão Master)
    await silver_db.insert(programasSociais)
      .values({
        codigo: 'AUXILIO_RECONSTRUCAO',
        nome: 'Auxílio Reconstrução',
        descricao: 'Auxílio de reconstrução financeira para vítimas de calamidades',
      })
      .onDuplicateKeyUpdate({
        set: { nome: sql`VALUES(nome)` },
      });

    const [programaDb] = await silver_db.select({ id: programasSociais.id })
      .from(programasSociais)
      .where(eq(programasSociais.codigo, 'AUXILIO_RECONSTRUCAO'));

    const programaId = programaDb.id;

    while (hasMore) {
      // 2. Extração da Camada Bronze
      const bronzeData = await bronze_db.select()
        .from(AuxilioReconstrucao)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        log.info({ data: { offset } }, 'Processamento concluído. Nenhum registro adicional encontrado.');
        hasMore = false;
        break;
      }

      log.info({ data: { offset, batchSize: bronzeData.length } }, 'Lote extraído da tabela auxilio_reconstrucao');

      // 3. Dimensão Municípios
      // Como não temos IBGE/SIAFI nesta base, buscamos pelo Nome + UF
      const nomesLote = [...new Set(bronzeData.map(b => b.nomeMunicipio).filter(Boolean) as string[])];
      const municipiosDb = nomesLote.length > 0
        ? await silver_db.select({ id: municipios.id, nome: municipios.nome, ufSigla: municipios.ufSigla })
          .from(municipios)
          .where(inArray(municipios.nome, nomesLote))
        : [];

      const munIdMap = new Map(municipiosDb.map(m => [`${m.nome.toUpperCase()}-${m.ufSigla.toUpperCase()}`, m.id]));

      const municipiosToInsert = bronzeData
        .filter(b => b.nomeMunicipio && b.uf && !munIdMap.has(`${b.nomeMunicipio.toUpperCase()}-${b.uf.toUpperCase()}`))
        .map(b => ({
          nome: b.nomeMunicipio!,
          ufSigla: b.uf!.substring(0, 2).toUpperCase(),
        }));

      if (municipiosToInsert.length > 0) {
        const uniqueMunMap = new Map<string, typeof municipiosToInsert[0]>();
        for (const m of municipiosToInsert) uniqueMunMap.set(`${m.nome.toUpperCase()}-${m.ufSigla}`, m);
        const uniqueMunicipios = Array.from(uniqueMunMap.values());

        // Insere apenas os que realmente não existem
        await silver_db.insert(municipios).values(uniqueMunicipios).onDuplicateKeyUpdate({ set: { id: sql`id` } });

        // Atualiza o mapa com os recém-inseridos
        const novosMunicipiosDb = await silver_db.select({ id: municipios.id, nome: municipios.nome, ufSigla: municipios.ufSigla })
          .from(municipios)
          .where(inArray(municipios.nome, uniqueMunicipios.map(m => m.nome)));

        novosMunicipiosDb.forEach(m => munIdMap.set(`${m.nome.toUpperCase()}-${m.ufSigla.toUpperCase()}`, m.id));
      }

      // 4. Dimensão Pessoas Físicas
      const cpfsLote = [...new Set(bronzeData.map(b => formatCpf(b.cpfFavorecido)).filter(Boolean) as string[])];
      const cpfsExistentesDb = cpfsLote.length > 0
        ? await silver_db.select({ id: pessoasFisicas.id, cpf: pessoasFisicas.cpf })
          .from(pessoasFisicas)
          .where(inArray(pessoasFisicas.cpf, cpfsLote))
        : [];

      const cpfIdMap = new Map(cpfsExistentesDb.map(pf => [pf.cpf, pf.id]));
      const cpfsExistentesSet = new Set(cpfsExistentesDb.map(pf => pf.cpf));

      const pfToInsert = bronzeData
        .filter(b => b.cpfFavorecido && !cpfsExistentesSet.has(formatCpf(b.cpfFavorecido)!))
        .map(b => {
          const cpf = formatCpf(b.cpfFavorecido)!;
          const municipioId = MunicipioResolver.resolve({
            nome: b.nomeMunicipio,
            uf: b.uf,
          });

          return {
            cpf,
            nome: b.nomeFavorecido || 'NÃO INFORMADO',
            municipioId,
          };
        });

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

      // 5. Entidade de Pagamentos
      const pagamentosToInsert = bronzeData
        .map(b => {
          const beneficiarioCpf = formatCpf(b.cpfFavorecido);
          const beneficiarioId = beneficiarioCpf ? cpfIdMap.get(beneficiarioCpf) : null;
          const municipioId = MunicipioResolver.resolve({
            nome: b.nomeMunicipio,
            uf: b.uf,
          });
          const valor = parseDecimal(b.valor);
          const dataPagamentoIso = parseDate(b.dataPagamento);

          // Derivando mês de competência da data de pagamento se não existir explícito
          let mesCompetencia = b.mesReferencia && b.exercicioReferencia
            ? `${b.exercicioReferencia}-${b.mesReferencia.padStart(2, '0')}`
            : null;

          if (!mesCompetencia && dataPagamentoIso) {
            mesCompetencia = dataPagamentoIso.substring(0, 7);
          }

          if (!beneficiarioId || !municipioId || !mesCompetencia || valor === null) return null;

          return {
            programaId,
            beneficiarioId,
            municipioId,
            mesCompetencia,
            mesReferencia: mesCompetencia,
            valor,
            dataDisponibilizacao: dataPagamentoIso,
          };
        })
        .filter(Boolean) as typeof beneficioPagamentos.$inferInsert[];

      if (pagamentosToInsert.length > 0) {
        // Garantir UPSERT seguro deletando sobreposições pontuais 
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

        log.info({ data: { count: pagamentosToInsert.length } }, 'Inserindo beneficio_pagamentos (Auxílio Reconstrução)');
        for (let i = 0; i < pagamentosToInsert.length; i += 1000) {
          await silver_db.insert(beneficioPagamentos).values(pagamentosToInsert.slice(i, i + 1000));
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento do Auxílio Reconstrução');
    throw error;
  }
}