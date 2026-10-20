import { sql, inArray, and, eq } from 'drizzle-orm';
import { bronze_db } from '../../../database/bronze_connection.js';
import { silver_db } from '../../../database/silver_connection.js';
import { 
  ImoveisFuncionais, 
  ImoveisFuncionaisPermissionarios 
} from '../../../database/bronze_schema.js';
import { 
  imoveisFuncionais, 
  imoveisFuncionaisPermissionarios, 
  pessoasFisicas, 
  orgaosSubordinados 
} from '../../../database/silver_schema.js';
import { formatCpf, parseDate } from '../shared/formatters.js';
import { new_logger } from '../../../services/logs.js';

const logger = new_logger(import.meta.url);

export async function processImoveisFuncionais() {
  const log = logger.forMethod('processImoveisFuncionais');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento de Imóveis Funcionais');

  try {
    while (hasMore) {
      const bronzeData = await bronze_db.select()
        .from(ImoveisFuncionais)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        log.info({ data: { offset } }, 'Processamento concluído. Nenhum registro adicional encontrado.');
        hasMore = false;
        break;
      }

      log.info({ data: { offset, batchSize: bronzeData.length } }, 'Lote extraído da tabela imoveis_funcionais');

      // 1. Resolver Órgãos Responsáveis (Apenas lookup por nome, já que não temos hierarquia completa para inserir)
      const orgaosNomesLote = [...new Set(bronzeData.map(b => b.orgaoResponsavel).filter(Boolean) as string[])];
      const orgaosDb = orgaosNomesLote.length > 0 
        ? await silver_db.select({ id: orgaosSubordinados.id, nome: orgaosSubordinados.nome })
            .from(orgaosSubordinados)
            .where(inArray(orgaosSubordinados.nome, orgaosNomesLote))
        : [];
      const orgaoIdMap = new Map(orgaosDb.map(o => [o.nome.toUpperCase(), o.id]));

      // 2. Preparar e Inserir Imóveis
      const imoveisMapped = bronzeData.map(b => {
        if (!b.endereco) return null;

        const orgaoId = b.orgaoResponsavel ? orgaoIdMap.get(b.orgaoResponsavel.toUpperCase()) || null : null;

        return {
          endereco: b.endereco,
          orgaoResponsavelId: orgaoId,
          situacao: b.situacao || null,
        };
      }).filter(Boolean) as typeof imoveisFuncionais.$inferInsert[];

      if (imoveisMapped.length > 0) {
        // Garantir unicidade no lote (endereco + orgao)
        const batchMap = new Map<string, typeof imoveisMapped[0]>();
        for (const i of imoveisMapped) {
          const key = `${i.endereco}-${i.orgaoResponsavelId}`;
          batchMap.set(key, i);
        }
        const uniqueImoveis = Array.from(batchMap.values());

        // Buscar existentes para evitar duplicidade (já que não há constraint unique em endereco)
        const enderecosLote = [...new Set(uniqueImoveis.map(i => i.endereco))];
        const existingImoveis = await silver_db.select({ id: imoveisFuncionais.id, endereco: imoveisFuncionais.endereco, orgaoResponsavelId: imoveisFuncionais.orgaoResponsavelId })
          .from(imoveisFuncionais)
          .where(inArray(imoveisFuncionais.endereco, enderecosLote));
        
        const existingSet = new Set(existingImoveis.map(i => `${i.endereco}-${i.orgaoResponsavelId}`));

        const toInsert = uniqueImoveis.filter(i => !existingSet.has(`${i.endereco}-${i.orgaoResponsavelId}`));

        if (toInsert.length > 0) {
          log.info({ data: { count: toInsert.length } }, 'Inserindo novos imoveis_funcionais');
          for (let i = 0; i < toInsert.length; i += 1000) {
            await silver_db.insert(imoveisFuncionais).values(toInsert.slice(i, i + 1000));
          }
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento de Imóveis Funcionais');
    throw error;
  }
}

export async function processImoveisFuncionaisPermissionarios() {
  const log = logger.forMethod('processImoveisFuncionaisPermissionarios');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento de Permissionários de Imóveis Funcionais');

  try {
    while (hasMore) {
      const bronzeData = await bronze_db.select()
        .from(ImoveisFuncionaisPermissionarios)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        hasMore = false;
        break;
      }

      log.info({ data: { offset, batchSize: bronzeData.length } }, 'Lote extraído da tabela imoveis_funcionais_permissionarios');

      // 1. Dimensão Pessoas Físicas (Permissionários)
      const pfsLote = bronzeData.filter(b => b.cpf).map(b => ({
        cpf: formatCpf(b.cpf)!,
        nome: b.nomePermissionario || 'NÃO INFORMADO'
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

      // 2. Órgãos de Exercício (Lookup)
      const orgaosNomesLote = [...new Set(bronzeData.map(b => b.orgaoExercicio).filter(Boolean) as string[])];
      const orgaosDb = orgaosNomesLote.length > 0 
        ? await silver_db.select({ id: orgaosSubordinados.id, nome: orgaosSubordinados.nome })
            .from(orgaosSubordinados)
            .where(inArray(orgaosSubordinados.nome, orgaosNomesLote))
        : [];
      const orgaoIdMap = new Map(orgaosDb.map(o => [o.nome.toUpperCase(), o.id]));

      // 3. Entidade de Vínculo Permissionário
      const permissionariosMapped = bronzeData.map(b => {
        const cpfFormatado = formatCpf(b.cpf);
        const permissionarioId = cpfFormatado ? cpfIdMap.get(cpfFormatado) : null;
        
        if (!permissionarioId) return null;

        const orgaoExercicioId = b.orgaoExercicio ? orgaoIdMap.get(b.orgaoExercicio.toUpperCase()) || null : null;

        return {
          imovelId: null, // Sem chave forte de imóvel no payload isolado de permissionários da Bronze
          permissionarioId,
          cargoFuncao: b.cargoFuncao || null,
          orgaoExercicioId,
          dataOcupacao: parseDate(b.dataOcupacao) || null,
        };
      }).filter(Boolean) as typeof imoveisFuncionaisPermissionarios.$inferInsert[];

      if (permissionariosMapped.length > 0) {
        // Garantindo unicidade na memória do lote atual
        const batchMap = new Map<string, typeof permissionariosMapped[0]>();
        for (const p of permissionariosMapped) {
          const key = `${p.permissionarioId}-${p.dataOcupacao || 'null'}-${p.orgaoExercicioId || 'null'}`;
          batchMap.set(key, p);
        }
        const uniquePermissionarios = Array.from(batchMap.values());

        // Deleção segura por permissionário para manter idempotência
        const permissionarioIds = [...new Set(uniquePermissionarios.map(p => p.permissionarioId))];
        await silver_db.delete(imoveisFuncionaisPermissionarios)
          .where(inArray(imoveisFuncionaisPermissionarios.permissionarioId, permissionarioIds));

        log.info({ data: { count: uniquePermissionarios.length } }, 'Inserindo imoveis_funcionais_permissionarios');
        for (let i = 0; i < uniquePermissionarios.length; i += 1000) {
          await silver_db.insert(imoveisFuncionaisPermissionarios).values(uniquePermissionarios.slice(i, i + 1000));
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento de Permissionários de Imóveis Funcionais');
    throw error;
  }
}