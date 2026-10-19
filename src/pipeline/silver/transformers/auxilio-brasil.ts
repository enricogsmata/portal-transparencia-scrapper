import { sql, inArray, eq, and } from 'drizzle-orm';
import { bronze_db } from '../../../database/bronze_connection.js';
import { silver_db } from '../../../database/silver_connection.js';
import { AuxilioBrasil } from '../../../database/bronze_schema.js';
import { 
  beneficioPagamentos, 
  pessoasFisicas, 
  municipios, 
  programasSociais 
} from '../../../database/silver_schema.js';
import { parseDecimal } from '../shared/formatters.js';
import { new_logger } from '../../../services/logs.js';

const logger = new_logger(import.meta.url);

function formatCpf(cpf?: string | null): string | null {
  if (!cpf) return null;
  return cpf.replace(/\D/g, '').padStart(11, '0');
}

export async function processAuxilioBrasil() {
  const log = logger.forMethod('processAuxilioBrasil');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento do módulo Auxílio Brasil');

  try {
    // 1. Garantir existência do programa social (Dimensão Master)
    await silver_db.insert(programasSociais)
      .values({
        codigo: 'AUXILIO_BRASIL',
        nome: 'Auxílio Brasil',
        descricao: 'Programa social de transferência de renda Auxílio Brasil',
      })
      .onDuplicateKeyUpdate({
        set: { nome: sql`VALUES(nome)` },
      });

    const [programaDb] = await silver_db.select({ id: programasSociais.id })
      .from(programasSociais)
      .where(eq(programasSociais.codigo, 'AUXILIO_BRASIL'));
    
    const programaId = programaDb.id;

    while (hasMore) {
      // 2. Extração da Camada Bronze
      const bronzeData = await bronze_db.select()
        .from(AuxilioBrasil)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        log.info({ data: { offset } }, 'Processamento concluído. Nenhum registro adicional encontrado.');
        hasMore = false;
        break;
      }

      log.info({ data: { offset, batchSize: bronzeData.length } }, 'Lote extraído da tabela auxilio_brasil');

      // 3. Dimensão Municípios (SIAFI)
      const municipiosToUpsert = bronzeData
        .filter((b) => b.codigoMunicipioSiafi && b.uf)
        .map((b) => ({
          codigoSiafi: b.codigoMunicipioSiafi!,
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
      const munIdMap = new Map(municipiosDb.map(m => [m.codigoSiafi, m.id]));

      // 4. Dimensão Pessoas Físicas
      // Como CPF não possui restrição UNIQUE nativa no schema, filtramos a existência manualmente para evitar duplicatas.
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
          const municipioId = b.codigoMunicipioSiafi ? munIdMap.get(b.codigoMunicipioSiafi) || null : null;
          return {
            cpf,
            nis: b.nisFavorecido || null,
            nome: b.nomeFavorecido || 'NÃO INFORMADO',
            municipioId,
          };
        });

      if (pfToInsert.length > 0) {
        const uniquePfMap = new Map<string, typeof pfToInsert[0]>();
        for (const pf of pfToInsert) uniquePfMap.set(pf.cpf, pf);
        const uniquePfs = Array.from(uniquePfMap.values());

        await silver_db.insert(pessoasFisicas).values(uniquePfs);

        // Atualizar o mapeamento de IDs com os recém-inseridos
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
          const municipioId = b.codigoMunicipioSiafi ? munIdMap.get(b.codigoMunicipioSiafi) : null;
          const valor = parseDecimal(b.valorParcela);
          
          if (!beneficiarioId || !municipioId || !b.mesCompetencia || valor === null) return null;

          return {
            programaId,
            beneficiarioId,
            municipioId,
            mesCompetencia: b.mesCompetencia.substring(0, 7),
            mesReferencia: b.mesReferencia ? b.mesReferencia.substring(0, 7) : null,
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
    log.error({ data: error }, 'Erro fatal durante o processamento do Auxílio Brasil');
    throw error;
  }
}