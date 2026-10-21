import { inArray } from 'drizzle-orm';
import { bronze_db } from '../../../database/bronze_connection.js';
import { silver_db } from '../../../database/silver_connection.js';
import { Pep } from '../../../database/bronze_schema.js';
import { 
  pep,
  pessoasFisicas,
} from '../../../database/silver_schema.js';
import { formatCpf, parseDate } from '../shared/formatters.js';
import { new_logger } from '../../../services/logs.js';

const logger = new_logger(import.meta.url);

export async function processPep() {
  const log = logger.forMethod('processPep');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento do módulo PEP (Pessoas Expostas Politicamente)');

  try {
    while (hasMore) {
      const bronzeData = await bronze_db.select()
        .from(Pep)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        log.info({ data: { offset } }, 'Processamento concluído. Nenhum registro adicional encontrado.');
        hasMore = false;
        break;
      }

      log.info({ data: { offset, batchSize: bronzeData.length } }, 'Lote extraído da tabela pep');

      // 1. Dimensão Pessoas Físicas
      const pfsLote = bronzeData.filter(b => b.cpf).map(b => ({
        cpf: formatCpf(b.cpf)!,
        nome: b.nomePep || 'NÃO INFORMADO'
      })).filter(p => p.cpf);

      const cpfsSet = [...new Set(pfsLote.map(p => p.cpf))];
      const cpfsExistentesDb = cpfsSet.length > 0
        ? await silver_db.select({ id: pessoasFisicas.id, cpf: pessoasFisicas.cpf })
            .from(pessoasFisicas)
            .where(inArray(pessoasFisicas.cpf, cpfsSet))
        : [];
      
      const cpfIdMap = new Map(cpfsExistentesDb.map(pf => [pf.cpf, pf.id]));
      const cpfsExistentesSet = new Set(cpfsExistentesDb.map(pf => pf.cpf));

      const pfToInsert = pfsLote.filter(p => !cpfsExistentesSet.has(p.cpf));

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

      // 2. Entidade PEP
      const pepsToInsert = bronzeData.map(b => {
        const cpfFormatado = formatCpf(b.cpf);
        const pessoaFisicaId = cpfFormatado ? cpfIdMap.get(cpfFormatado) : null;
        
        if (!pessoaFisicaId || !b.descricaoFuncao || !b.nomeOrgao || !b.dataInicioExercicio) return null;

        return {
          pessoaFisicaId,
          siglaFuncao: b.siglaFuncao || null,
          descricaoFuncao: b.descricaoFuncao,
          nivelFuncao: b.nivelFuncao || null,
          nomeOrgao: b.nomeOrgao,
          dataInicioExercicio: parseDate(b.dataInicioExercicio) || '1970-01-01',
          dataFimExercicio: parseDate(b.dataFimExercicio) || null,
          dataFimCarencia: parseDate(b.dataFimCarencia) || null,
        };
      }).filter(Boolean) as typeof pep.$inferInsert[];

      if (pepsToInsert.length > 0) {
        // Garantindo unicidade na memória do lote atual por pessoa/órgão/função/dataInicio
        const batchMap = new Map<string, typeof pepsToInsert[0]>();
        for (const p of pepsToInsert) {
          const key = `${p.pessoaFisicaId}-${p.nomeOrgao}-${p.descricaoFuncao}-${p.dataInicioExercicio}`;
          batchMap.set(key, p);
        }
        const uniquePeps = Array.from(batchMap.values());

        // Deleção segura por pessoa para manter idempotência do lote
        const pfIds = [...new Set(uniquePeps.map(p => p.pessoaFisicaId))];
        await silver_db.delete(pep).where(inArray(pep.pessoaFisicaId, pfIds));

        log.info({ data: { count: uniquePeps.length } }, 'Inserindo pep');
        for (let i = 0; i < uniquePeps.length; i += 1000) {
          await silver_db.insert(pep).values(uniquePeps.slice(i, i + 1000));
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento do cadastro PEP');
    throw error;
  }
}