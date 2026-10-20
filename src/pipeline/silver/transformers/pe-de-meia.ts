import { sql, inArray, and, eq } from 'drizzle-orm';
import { bronze_db } from '../../../database/bronze_connection.js';
import { silver_db } from '../../../database/silver_connection.js';
import { PeDeMeia } from '../../../database/bronze_schema.js';
import { 
  beneficioPagamentos,
  pessoasFisicas,
  municipios,
  programasSociais,
  estados
} from '../../../database/silver_schema.js';
import { formatCpf, formatAnoMes, parseDate, parseDecimal } from '../shared/formatters.js';
import { new_logger } from '../../../services/logs.js';

const logger = new_logger(import.meta.url);

export async function processPeDeMeia() {
  const log = logger.forMethod('processPeDeMeia');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento do módulo Pé-de-Meia');

  try {
    // 1. Garantir existência do programa social (Dimensão Master)
    await silver_db.insert(programasSociais)
      .values({
        codigo: 'PE_DE_MEIA',
        nome: 'Pé-de-Meia',
        descricao: 'Incentivo financeiro-educacional para estudantes do Ensino Médio',
      })
      .onDuplicateKeyUpdate({
        set: { nome: sql`VALUES(nome)` },
      });

    const [programaDb] = await silver_db.select({ id: programasSociais.id })
      .from(programasSociais)
      .where(eq(programasSociais.codigo, 'PE_DE_MEIA'));
    
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

      // 3. Dimensão Municípios (por IBGE, que costuma ser o 'codigoMunicipio' deste dataset)
      const ufsLote = [...new Set(bronzeData.map(b => b.uf ? b.uf.substring(0, 2).toUpperCase() : null).filter(Boolean) as string[])];
      const estadosDb = ufsLote.length > 0 
        ? await silver_db.select({ sigla: estados.sigla }).from(estados).where(inArray(estados.sigla, ufsLote))
        : [];
      const ufsValidas = new Set(estadosDb.map(e => e.sigla));

      const municipiosToUpsert = bronzeData
        .filter(b => b.codigoMunicipio && b.uf && ufsValidas.has(b.uf.substring(0, 2).toUpperCase()))
        .map(b => ({
          codigoIbge: b.codigoMunicipio!,
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
      const pfsLote: { cpf: string, nis: string | null, nome: string, codigoIbge: string | null }[] = [];
      
      bronzeData.forEach(b => {
        const cpfBenef = formatCpf(b.cpfBeneficiario);
        if (cpfBenef) {
          pfsLote.push({
            cpf: cpfBenef,
            nis: b.nisBeneficiario || null,
            nome: b.nomeBeneficiario || 'NÃO INFORMADO',
            codigoIbge: b.codigoMunicipio || null
          });
        }
        
        const cpfResp = formatCpf(b.cpfResponsavel);
        if (cpfResp) {
          pfsLote.push({
            cpf: cpfResp,
            nis: b.nisResponsavel || null,
            nome: b.nomeResponsavel || 'NÃO INFORMADO',
            codigoIbge: b.codigoMunicipio || null
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
          municipioId: p.codigoIbge ? munIdMap.get(p.codigoIbge) || null : null,
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
          const cpfBeneficiario = formatCpf(b.cpfBeneficiario);
          const beneficiarioId = cpfBeneficiario ? cpfIdMap.get(cpfBeneficiario) : null;
          
          const cpfResponsavel = formatCpf(b.cpfResponsavel);
          const responsavelId = cpfResponsavel ? cpfIdMap.get(cpfResponsavel) : null;
          
          const municipioId = b.codigoMunicipio ? munIdMap.get(b.codigoMunicipio) : null;
          
          const valor = parseDecimal(b.valorParcela);
          const mesCompetencia = formatAnoMes(b.mesFolha);
          const mesReferencia = formatAnoMes(b.mesReferencia);
          
          if (!beneficiarioId || !municipioId || !mesCompetencia || valor === null) return null;

          // Combinando tipo_parcela e periodo_parcela para compor o campo parcela
          const tipoParcela = b.tipoParcela ? b.tipoParcela.trim() : '';
          const periodoParcela = b.periodoParcela ? b.periodoParcela.trim() : '';
          const parcela = [tipoParcela, periodoParcela].filter(Boolean).join(' - ').substring(0, 50);

          return {
            programaId,
            beneficiarioId,
            responsavelId: responsavelId || null,
            municipioId,
            mesCompetencia,
            mesReferencia,
            parcela: parcela || null,
            valor,
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
          await silver_db.insert(beneficioPagamentos).values(pagamentosToInsert.slice(i, i + 1000));
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento do Pé-de-Meia');
    throw error;
  }
}