import { sql, inArray, and, eq } from 'drizzle-orm';
import { bronze_db } from '../../../database/bronze_connection.js';
import { silver_db } from '../../../database/silver_connection.js';
import { SeguroDefeso } from '../../../database/bronze_schema.js';
import { 
  beneficioPagamentos,
  pessoasFisicas,
  municipios,
  programasSociais,
  estados
} from '../../../database/silver_schema.js';
import { formatCpf, formatAnoMes, parseDecimal, formatCpfByNis } from '../shared/formatters.js';
import { new_logger } from '../../../services/logs.js';

const logger = new_logger(import.meta.url);

export async function processSeguroDefeso() {
  const log = logger.forMethod('processSeguroDefeso');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento do módulo Seguro-Defeso');

  try {
    // 1. Garantir existência do programa social
    await silver_db.insert(programasSociais)
      .values({
        codigo: 'SEGURO_DEFESO',
        nome: 'Seguro-Defeso',
        descricao: 'Seguro-desemprego concedido a pescadores artesanais no período de defeso',
      })
      .onDuplicateKeyUpdate({
        set: { nome: sql`VALUES(nome)` },
      });

    const [programaDb] = await silver_db.select({ id: programasSociais.id })
      .from(programasSociais)
      .where(eq(programasSociais.codigo, 'SEGURO_DEFESO'));
    
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

      // 3. Dimensão Municípios (SIAFI)
      const ufsLote = [...new Set(bronzeData.map(b => b.uf ? b.uf.substring(0, 2).toUpperCase() : null).filter(Boolean) as string[])];
      const estadosDb = ufsLote.length > 0 
        ? await silver_db.select({ sigla: estados.sigla }).from(estados).where(inArray(estados.sigla, ufsLote))
        : [];
      const ufsValidas = new Set(estadosDb.map(e => e.sigla));

      const municipiosToUpsert = bronzeData
        .filter(b => b.codigoSiafiMunicipio && b.uf && ufsValidas.has(b.uf.substring(0, 2).toUpperCase()))
        .map(b => ({
          codigoSiafi: b.codigoSiafiMunicipio!,
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
      const munIdMap = new Map(municipiosDb.map(m => [m.codigoSiafi!, m.id]));

      // 4. Dimensão Pessoas Físicas (Beneficiários)
      const pfsLote = bronzeData
        .map(b => {
          const cpf = formatCpf(b.cpfFavorecido) || (b.nisFavorecido ? formatCpfByNis(b.nisFavorecido) : null);
          if (!cpf) return null;
          return {
            cpf,
            nis: b.nisFavorecido || null,
            nome: b.nomeFavorecido || 'NÃO INFORMADO',
            codigoSiafiMunicipio: b.codigoSiafiMunicipio || null,
          };
        })
        .filter(Boolean) as { cpf: string; nis: string | null; nome: string; codigoSiafiMunicipio: string | null }[];

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
          municipioId: p.codigoSiafiMunicipio ? munIdMap.get(p.codigoSiafiMunicipio) || null : null,
        }));

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

      // 5. Entidade de Pagamentos (Benefício)
      const pagamentosToInsert = bronzeData
        .map(b => {
          const cpf = formatCpf(b.cpfFavorecido) || (b.nisFavorecido ? formatCpfByNis(b.nisFavorecido) : null);
          const beneficiarioId = cpf ? cpfIdMap.get(cpf) : null;
          const municipioId = b.codigoSiafiMunicipio ? munIdMap.get(b.codigoSiafiMunicipio) : null;
          
          const valor = parseDecimal(b.valorParcela);
          const mesCompetencia = formatAnoMes(b.anoMesReferencia);
          const mesReferencia = b.mesReferencia 
            ? (b.exercicioReferencia ? `${b.exercicioReferencia}-${b.mesReferencia.padStart(2, '0')}` : formatAnoMes(b.mesReferencia))
            : null;
          
          if (!beneficiarioId || !municipioId || !mesCompetencia || valor === null) return null;

          return {
            programaId,
            beneficiarioId,
            municipioId,
            mesCompetencia,
            mesReferencia,
            valor,
            numeroBeneficio: b.rgpFavorecido ? b.rgpFavorecido.trim().slice(0, 30) : null,
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
          await silver_db.insert(beneficioPagamentos).values(pagamentosToInsert.slice(i, i + 1000));
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento do Seguro-Defeso');
    throw error;
  }
}