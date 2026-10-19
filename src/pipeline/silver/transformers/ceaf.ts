import { sql, inArray, and, eq } from 'drizzle-orm';
import { bronze_db } from '../../../database/bronze_connection.js';
import { silver_db } from '../../../database/silver_connection.js';
import { Ceaf } from '../../../database/bronze_schema.js';
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

export async function processCeaf() {
  const log = logger.forMethod('processCeaf');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento do módulo CEAF (Cadastro de Expulsões da Administração Federal)');

  try {
    while (hasMore) {
      // 1. Extração da Camada Bronze
      const bronzeData = await bronze_db.select()
        .from(Ceaf)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        log.info({ data: { offset } }, 'Processamento concluído. Nenhum registro adicional encontrado.');
        hasMore = false;
        break;
      }

      log.info({ data: { offset, batchSize: bronzeData.length } }, 'Lote extraído da tabela ceaf');

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
          razaoSocial: b.nomeSancionado || 'NÃO INFORMADO',
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
              razaoSocial: sql`VALUES(razao_social)`,
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

          const dataInicio = parseDate(b.dataInicioSancao);
          const numeroProcesso = b.numeroProcesso || 'NÃO INFORMADO';

          // Data de início e Processo são obrigatórios na 3FN de sancoes_administrativas
          if (!dataInicio || (!sancionadoPfId && !sancionadoPjId)) return null;

          return {
            tipoCadastro: 'CEAF' as const,
            codigoSancao: b.codigoSancao || null,
            sancionadoPfId,
            sancionadoPjId,
            numeroProcesso,
            categoriaSancao: b.categoriaSancao || null,
            dataInicioSancao: dataInicio,
            dataFinalSancao: parseDate(b.dataFinalSancao) || null,
            dataPublicacao: parseDate(b.dataPublicacao) || null,
            dataTransitoJulgado: parseDate(b.dataTransitoJulgado) || null,
            orgaoSancionador: b.orgaoSancionador || null,
            ufOrgaoSancionador: b.ufOrgaoSancionador ? b.ufOrgaoSancionador.substring(0, 2).toUpperCase() : null,
            esferaOrgaoSancionador: parseEsfera(b.esferaOrgaoSancionador),
            fundamentacaoLegal: b.fundamentacaoLegal || null,
            observacoes: b.observacoes || null,
          };
        })
        .filter(Boolean) as typeof sancoesAdministrativas.$inferInsert[];

      if (sancoesToInsert.length > 0) {
        // Garantir UPSERT seguro deletando sobreposições para CEAF + Processos do lote
        const processosLote = [...new Set(sancoesToInsert.map(s => s.numeroProcesso))];

        if (processosLote.length > 0) {
          await silver_db.delete(sancoesAdministrativas)
            .where(
              and(
                eq(sancoesAdministrativas.tipoCadastro, 'CEAF'),
                inArray(sancoesAdministrativas.numeroProcesso, processosLote)
              )
            );
        }

        log.info({ data: { count: sancoesToInsert.length } }, 'Inserindo sancoes_administrativas (CEAF)');
        for (let i = 0; i < sancoesToInsert.length; i += 1000) {
          await silver_db.insert(sancoesAdministrativas).values(sancoesToInsert.slice(i, i + 1000));
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento do CEAF');
    throw error;
  }
}