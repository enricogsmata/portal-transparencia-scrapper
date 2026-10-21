import { sql, inArray, and, eq } from 'drizzle-orm';
import { bronze_db } from '../../../database/bronze_connection.js';
import { silver_db } from '../../../database/silver_connection.js';
import { Ceis } from '../../../database/bronze_schema.js';
import {
  sancoesAdministrativas,
  pessoasFisicas,
  pessoasJuridicas
} from '../../../database/silver_schema.js';
import { formatCnpj, parseDate } from '../shared/formatters.js';
import { new_logger } from '../../../services/logs.js';

const logger = new_logger(import.meta.url);

function formatCpf(cpf?: string | null): string | null {
  if (!cpf || cpf.trim() === '') return null;
  const numbers = cpf.replace(/\D/g, '');
  return numbers.length > 0 ? numbers.padStart(11, '0') : null;
}

function getDocumentType(doc?: string | null): 'PF' | 'PJ' | null {
  if (!doc) return null;
  const numbers = doc.replace(/\D/g, '');
  if (numbers.length === 11) return 'PF';
  if (numbers.length === 14) return 'PJ';
  return null;
}

function parseEsfera(val?: string | null): 'Federal' | 'Estadual' | 'Municipal' | null {
  if (!val) return null;
  const v = val.toUpperCase();
  if (v.includes('FEDERAL')) return 'Federal';
  if (v.includes('ESTADUAL')) return 'Estadual';
  if (v.includes('MUNICIPAL')) return 'Municipal';
  return null;
}

export async function processCeis() {
  const log = logger.forMethod('processCeis');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento do módulo CEIS (Cadastro Nacional de Empresas Inidôneas e Suspensas)');

  try {
    while (hasMore) {
      // 1. Extração da Camada Bronze
      const bronzeData = await bronze_db.select()
        .from(Ceis)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        log.info({ data: { offset } }, 'Processamento concluído. Nenhum registro adicional encontrado.');
        hasMore = false;
        break;
      }

      log.info({ data: { offset, batchSize: bronzeData.length } }, 'Lote extraído da tabela ceis');

      // 2. Dimensão Pessoas Físicas (Sancionados PF)
      const pfsLote = bronzeData.filter(b => getDocumentType(b.cpfCnpjSancionado) === 'PF');
      const cpfsSet = [...new Set(pfsLote.map(b => formatCpf(b.cpfCnpjSancionado)).filter(Boolean) as string[])];

      const cpfsExistentesDb = cpfsSet.length > 0
        ? await silver_db.select({ id: pessoasFisicas.id, cpf: pessoasFisicas.cpf })
          .from(pessoasFisicas)
          .where(inArray(pessoasFisicas.cpf, cpfsSet))
        : [];

      const cpfIdMap = new Map(cpfsExistentesDb.map(pf => [pf.cpf, pf.id]));
      const cpfsExistentesSet = new Set(cpfsExistentesDb.map(pf => pf.cpf));

      const pfToInsert = pfsLote
        .filter(b => {
          const cpf = formatCpf(b.cpfCnpjSancionado);
          return cpf && !cpfsExistentesSet.has(cpf);
        })
        .map(b => ({
          cpf: formatCpf(b.cpfCnpjSancionado)!,
          nome: b.nomeSancionado || 'NÃO INFORMADO',
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

      // 3. Dimensão Pessoas Jurídicas (Sancionados PJ)
      const pjsLote = bronzeData.filter(b => getDocumentType(b.cpfCnpjSancionado) === 'PJ');
      const pjsToUpsert = pjsLote
        .map(b => ({
          cnpj: formatCnpj(b.cpfCnpjSancionado)!,
          razaoSocial: b.nomeSancionado || b.razaoSocialCadastroReceita || 'NÃO INFORMADO',
          nomeFantasia: b.nomeFantasiaCadastroReceita || null,
        }))
        .filter(b => b.cnpj);

      if (pjsToUpsert.length > 0) {
        const uniquePjMap = new Map<string, typeof pjsToUpsert[0]>();
        for (const pj of pjsToUpsert) uniquePjMap.set(pj.cnpj, pj);
        const uniquePjs = Array.from(uniquePjMap.values());

        await silver_db.insert(pessoasJuridicas)
          .values(uniquePjs)
          .onDuplicateKeyUpdate({
            set: {
              id: sql`id`,
            },
          });
      }

      const cnpjsSet = [...new Set(pjsToUpsert.map(p => p.cnpj))];
      const pjsExistentesDb = cnpjsSet.length > 0
        ? await silver_db.select({ id: pessoasJuridicas.id, cnpj: pessoasJuridicas.cnpj })
          .from(pessoasJuridicas)
          .where(inArray(pessoasJuridicas.cnpj, cnpjsSet))
        : [];

      const cnpjIdMap = new Map(pjsExistentesDb.map(pj => [pj.cnpj, pj.id]));

      // 4. Entidade de Sanções Administrativas
      const sancoesToInsert = bronzeData
        .map(b => {
          const docType = getDocumentType(b.cpfCnpjSancionado);
          let sancionadoPfId = null;
          let sancionadoPjId = null;

          if (docType === 'PF') {
            sancionadoPfId = cpfIdMap.get(formatCpf(b.cpfCnpjSancionado)!) || null;
          } else if (docType === 'PJ') {
            sancionadoPjId = cnpjIdMap.get(formatCnpj(b.cpfCnpjSancionado)!) || null;
          }

          if (!sancionadoPfId && !sancionadoPjId) return null;

          const numeroProcesso = b.numeroProcesso || null;
          const codigoSancao = b.codigoSancao?.trim() || `CEIS:${b.cpfCnpjSancionado || 'NA'}:${b.numeroProcesso || b.id}`;

          return {
            tipoCadastro: 'CEIS' as const,
            codigoSancao,
            sancionadoPfId,
            sancionadoPjId,
            numeroProcesso,
            categoriaSancao: b.categoriaSancao || null,
            dataInicioSancao: parseDate(b.dataInicioSancao) || null,
            dataFinalSancao: parseDate(b.dataFinalSancao) || null,
            dataPublicacao: parseDate(b.dataPublicacao) || null,
            dataTransitoJulgado: parseDate(b.dataTransitoJulgado) || null,
            orgaoSancionador: b.orgaoSancionador || b.nomeInformadoOrgaoSancionador || null,
            ufOrgaoSancionador: b.ufOrgaoSancionador ? b.ufOrgaoSancionador.substring(0, 2).toUpperCase() : null,
            esferaOrgaoSancionador: parseEsfera(b.esferaOrgaoSancionador),
            fundamentacaoLegal: b.fundamentacaoLegal || null,
            observacoes: b.observacoes || null,
          };
        })
        .filter(Boolean) as typeof sancoesAdministrativas.$inferInsert[];

      if (sancoesToInsert.length > 0) {
        const uniqueMap = new Map<string, typeof sancoesToInsert[0]>();
        for (const s of sancoesToInsert) uniqueMap.set(s.codigoSancao, s);
        const uniqueSancoes = Array.from(uniqueMap.values());

        const codigosSancaoLote = uniqueSancoes
          .map(s => s.codigoSancao)
          .filter((c): c is string => typeof c === 'string' && c.length > 0);

        if (codigosSancaoLote.length > 0) {
          await silver_db.delete(sancoesAdministrativas)
            .where(
              and(
                eq(sancoesAdministrativas.tipoCadastro, 'CEIS'),
                inArray(sancoesAdministrativas.codigoSancao, codigosSancaoLote)
              )
            );
        }

        log.info({ data: { count: uniqueSancoes.length } }, 'Inserindo sancoes_administrativas (CEIS)');
        for (let i = 0; i < uniqueSancoes.length; i += 1000) {
          await silver_db.insert(sancoesAdministrativas).values(uniqueSancoes.slice(i, i + 1000));
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento do CEIS');
    throw error;
  }
}