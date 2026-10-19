import { sql, inArray, and, eq } from 'drizzle-orm';
import { bronze_db } from '../../../database/bronze_connection.js';
import { silver_db } from '../../../database/silver_connection.js';
import { Cepim } from '../../../database/bronze_schema.js';
import { 
  sancoesAdministrativas, 
  pessoasJuridicas 
} from '../../../database/silver_schema.js';
import { formatCnpj } from '../shared/formatters.js';
import { new_logger } from '../../../services/logs.js';

const logger = new_logger(import.meta.url);

export async function processCepim() {
  const log = logger.forMethod('processCepim');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento do módulo CEPIM (Cadastro de Entidades Privadas Sem Fins Lucrativos Impedidas)');

  try {
    while (hasMore) {
      // 1. Extração da Camada Bronze
      const bronzeData = await bronze_db.select()
        .from(Cepim)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        log.info({ data: { offset } }, 'Processamento concluído. Nenhum registro adicional encontrado.');
        hasMore = false;
        break;
      }

      log.info({ data: { offset, batchSize: bronzeData.length } }, 'Lote extraído da tabela cepim');

      // 2. Dimensão Pessoas Jurídicas (Entidades Impedidas)
      const pjsToUpsert = bronzeData
        .map(b => ({
          cnpj: formatCnpj(b.cnpjEntidade)!,
          razaoSocial: b.nomeEntidade || 'NÃO INFORMADO',
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

      // 3. Entidade de Sanções Administrativas
      const sancoesToInsert = bronzeData
        .map(b => {
          const cnpjFormatado = formatCnpj(b.cnpjEntidade);
          const sancionadoPjId = cnpjFormatado ? cnpjIdMap.get(cnpjFormatado) : null;
          
          if (!sancionadoPjId) return null;

          const numeroProcesso = b.numeroConvenio || 'NÃO INFORMADO';
          
          // Data de Início é obrigatória na entidade sancoes_administrativas.
          // Derivando da data de extração/referência do arquivo caso não exista na fonte.
          const dataInicio = b.exercicioReferencia 
            ? `${b.exercicioReferencia}-${(b.mesReferencia || '01').padStart(2, '0')}-${(b.diaReferencia || '01').padStart(2, '0')}`
            : '1970-01-01';

          return {
            tipoCadastro: 'CEPIM' as const,
            sancionadoPjId,
            numeroProcesso,
            categoriaSancao: 'IMPEDIMENTO',
            dataInicioSancao: dataInicio,
            orgaoSancionador: b.orgaoConcedente || null,
            observacoes: b.motivoImpedimento || null,
          };
        })
        .filter(Boolean) as typeof sancoesAdministrativas.$inferInsert[];

      if (sancoesToInsert.length > 0) {
        // Garantir UPSERT seguro deletando sobreposições para CEPIM + Convênios (Processos) do lote
        const processosLote = [...new Set(sancoesToInsert.map(s => s.numeroProcesso))];

        if (processosLote.length > 0) {
          await silver_db.delete(sancoesAdministrativas)
            .where(
              and(
                eq(sancoesAdministrativas.tipoCadastro, 'CEPIM'),
                inArray(sancoesAdministrativas.numeroProcesso, processosLote)
              )
            );
        }

        log.info({ data: { count: sancoesToInsert.length } }, 'Inserindo sancoes_administrativas (CEPIM)');
        for (let i = 0; i < sancoesToInsert.length; i += 1000) {
          await silver_db.insert(sancoesAdministrativas).values(sancoesToInsert.slice(i, i + 1000));
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento do CEPIM');
    throw error;
  }
}