import { sql, inArray } from 'drizzle-orm';
import { bronze_db } from '../../../database/bronze_connection.js';
import { silver_db } from '../../../database/silver_connection.js';
import { ApoiamentosEmendasParlamentares } from '../../../database/bronze_schema.js';
import { emendasApoiamentos, emendasParlamentares, pessoasFisicas } from '../../../database/silver_schema.js';
import { formatCpf, parseDate, parseDecimal } from '../shared/formatters.js';
import { new_logger } from '../../../services/logs.js';
import { BATCH_SIZE } from '../shared/constants.js';

const logger = new_logger(import.meta.url);

export async function processApoiamentosEmendasParlamentares() {
  const log = logger.forMethod('processApoiamentosEmendasParlamentares');
  let offset = 0;
  let hasMore = true;
  const processedEmendas = new Set<number>();

  log.info('Iniciando processamento de Apoiamentos a Emendas Parlamentares');

  try {
    while (hasMore) {
      // 1. Extração de Entidades Bronze
      const bronzeData = await bronze_db.select()
        .from(ApoiamentosEmendasParlamentares)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        log.info({ data: { offset } }, 'Processamento concluído. Nenhum registro adicional encontrado na camada bronze.');
        hasMore = false;
        break;
      }

      log.info({ data: { offset, batchSize: bronzeData.length } }, 'Lote extraído da camada bronze');

      // 2. Recuperação de Chaves Estrangeiras (Emendas)
      const codigosEmendas = [...new Set(bronzeData.map((b) => b.codigoEmenda).filter(Boolean) as string[])];
      const emendasDb = codigosEmendas.length > 0
        ? await silver_db.select({ id: emendasParlamentares.id, codigoEmenda: emendasParlamentares.codigoEmenda })
          .from(emendasParlamentares)
          .where(inArray(emendasParlamentares.codigoEmenda, codigosEmendas))
        : [];
      const emendaIdMap = new Map(emendasDb.map((e) => [e.codigoEmenda, e.id]));

      // 3. Recuperação de Chaves Estrangeiras (Pessoas Físicas / Apoiadores - JIT Stubbing Não-Destrutivo)
      const pfsToUpsert = bronzeData
        .filter((b) => b.codigoApoiador && formatCpf(b.codigoApoiador))
        .map((b) => ({
          cpf: formatCpf(b.codigoApoiador)!,
          nome: b.apoiador || 'NÃO INFORMADO',
        }));

      const uniquePfMap = new Map<string, typeof pfsToUpsert[0]>();
      for (const pf of pfsToUpsert) uniquePfMap.set(pf.cpf, pf);
      const uniquePfs = Array.from(uniquePfMap.values());

      if (uniquePfs.length > 0) {
        log.info({ data: { count: uniquePfs.length } }, 'Realizando stubbing não-destrutivo na dimensão pessoas_fisicas');
        await silver_db.insert(pessoasFisicas)
          .values(uniquePfs)
          .onDuplicateKeyUpdate({
            set: {
              id: sql`id`,
            },
          });
      }

      const cpfsApoiadores = uniquePfs.map((pf) => pf.cpf);
      const pessoasFisicasDb = cpfsApoiadores.length > 0
        ? await silver_db.select({ id: pessoasFisicas.id, cpf: pessoasFisicas.cpf })
          .from(pessoasFisicas)
          .where(inArray(pessoasFisicas.cpf, cpfsApoiadores))
        : [];
      const pfIdMap = new Map(pessoasFisicasDb.map((pf) => [pf.cpf, pf.id]));

      // 4. Mapeamento da Entidade Principal de Apoiamentos (3FN e 1FN)
      const apoiamentosToInsert = bronzeData
        .map((b) => {
          const emendaId = b.codigoEmenda ? emendaIdMap.get(b.codigoEmenda) : null;

          if (!emendaId) {
            if (b.codigoEmenda) {
              log.warn({ data: { codigoEmenda: b.codigoEmenda } }, 'Emenda parlamentar pai não encontrada no Silver. Apoiamento descartado.');
            }
            return null;
          }

          if (!b.apoiador || !b.dataApoio) return null;

          const cpfFormatado = formatCpf(b.codigoApoiador);
          const apoiadorPfId = cpfFormatado ? pfIdMap.get(cpfFormatado) : null;

          return {
            emendaId,
            apoiadorPfId: apoiadorPfId || null,
            apoiadorNome: b.apoiador,
            dataApoio: parseDate(b.dataApoio)!,
            dataRetiradaApoio: parseDate(b.dataRetiradaApoio) || null,
            valorEmpenhado: parseDecimal(b.valorEmpenhado) || null,
            valorCancelado: parseDecimal(b.valorCancelado) || null,
            valorPago: parseDecimal(b.valorPago) || null,
          };
        })
        .filter(Boolean) as typeof emendasApoiamentos.$inferInsert[];

      if (apoiamentosToInsert.length > 0) {
        // Idempotência segura através do rastreamento global de emendas afetadas nesta execução
        const silverEmendasIds = [...new Set(apoiamentosToInsert.map((a) => a.emendaId))];
        const emendasToDelete = silverEmendasIds.filter(id => !processedEmendas.has(id));

        if (emendasToDelete.length > 0) {
          log.info({ data: { count: emendasToDelete.length } }, 'Limpando apoiamentos antigos da(s) emenda(s) alvo para garantir idempotência do lote');
          await silver_db.delete(emendasApoiamentos)
            .where(inArray(emendasApoiamentos.emendaId, emendasToDelete));

          emendasToDelete.forEach(id => processedEmendas.add(id));
        }

        log.info({ data: { count: apoiamentosToInsert.length } }, 'Realizando insert na entidade emendas_apoiamentos');
        for (let i = 0; i < apoiamentosToInsert.length; i += 1000) {
          await silver_db.insert(emendasApoiamentos).values(apoiamentosToInsert.slice(i, i + 1000));
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento de Apoiamentos de Emendas Parlamentares');
    throw error;
  }
}