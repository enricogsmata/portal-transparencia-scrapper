import { sql, inArray, and } from 'drizzle-orm';
import { bronze_db } from '../../../database/bronze_connection.js';
import { silver_db } from '../../../database/silver_connection.js';
import { RecursosTransferidos } from '../../../database/bronze_schema.js';
import {
  recursosTransferidos,
  unidadesGestoras,
  acoesOrcamentarias,
  pessoasFisicas,
  pessoasJuridicas
} from '../../../database/silver_schema.js';
import {
  formatCnpj,
  formatCpf,
  getDocumentType,
  formatAnoMes,
  parseDecimal
} from '../shared/formatters.js';
import { MunicipioResolver } from '../shared/municipio-resolver.js';
import { new_logger } from '../../../services/logs.js';

const logger = new_logger(import.meta.url);

export async function processRecursosTransferidos() {
  const log = logger.forMethod('processRecursosTransferidos');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento do módulo Transferências (Recursos Transferidos)');

  try {
    while (hasMore) {
      // 1. Extração da Camada Bronze
      const bronzeData = await bronze_db.select()
        .from(RecursosTransferidos)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        log.info({ data: { offset } }, 'Processamento concluído. Nenhum registro adicional encontrado.');
        hasMore = false;
        break;
      }

      log.info({ data: { offset, batchSize: bronzeData.length } }, 'Lote extraído da tabela recursos_transferidos');

      // 2. Dimensão Unidades Gestoras
      const ugsToUpsertMap = new Map<string, typeof unidadesGestoras.$inferInsert>();
      bronzeData.forEach((b) => {
        if (b.codigoUnidadeGestora) {
          ugsToUpsertMap.set(b.codigoUnidadeGestora, {
            codigo: b.codigoUnidadeGestora,
            nome: b.nomeUnidadeGestora || 'NÃO INFORMADO'
          });
        }
      });
      const ugsToUpsert = Array.from(ugsToUpsertMap.values());

      if (ugsToUpsert.length > 0) {
        await silver_db.insert(unidadesGestoras)
          .values(ugsToUpsert)
          .onDuplicateKeyUpdate({ set: { nome: sql`VALUES(nome)` } });
      }

      const ugsDb = ugsToUpsert.length > 0
        ? await silver_db.select({ id: unidadesGestoras.id, codigo: unidadesGestoras.codigo })
          .from(unidadesGestoras)
          .where(inArray(unidadesGestoras.codigo, ugsToUpsert.map(u => u.codigo)))
        : [];
      const ugIdMap = new Map(ugsDb.map(ug => [ug.codigo, ug.id]));

      // 4. Dimensão Ações Orçamentárias
      const acoesToUpsertMap = new Map<string, typeof acoesOrcamentarias.$inferInsert>();
      bronzeData.forEach((b) => {
        if (b.codigoAcao) {
          acoesToUpsertMap.set(b.codigoAcao, {
            codigo: b.codigoAcao,
            nome: b.nomeAcao || 'NÃO INFORMADO',
            linguagemCidada: b.linguagemCidada || null,
          });
        }
      });
      const acoesToUpsert = Array.from(acoesToUpsertMap.values());

      if (acoesToUpsert.length > 0) {
        const acoesEx = await silver_db.select({ id: acoesOrcamentarias.id, codigo: acoesOrcamentarias.codigo })
          .from(acoesOrcamentarias)
          .where(inArray(acoesOrcamentarias.codigo, acoesToUpsert.map(a => a.codigo)));
        const acoesExSet = new Set(acoesEx.map(a => a.codigo));
        const toInsert = acoesToUpsert.filter(a => !acoesExSet.has(a.codigo));
        if (toInsert.length > 0) {
          await silver_db.insert(acoesOrcamentarias).values(toInsert);
        }
      }

      const acoesDb = acoesToUpsert.length > 0
        ? await silver_db.select({ id: acoesOrcamentarias.id, codigo: acoesOrcamentarias.codigo })
          .from(acoesOrcamentarias)
          .where(inArray(acoesOrcamentarias.codigo, acoesToUpsert.map(a => a.codigo)))
        : [];
      const acaoIdMap = new Map(acoesDb.map(a => [a.codigo, a.id]));

      // 5. Dimensões Pessoas Físicas e Pessoas Jurídicas (Favorecidos)
      const pfMap = new Map<string, typeof pessoasFisicas.$inferInsert>();
      const pjMap = new Map<string, typeof pessoasJuridicas.$inferInsert>();

      bronzeData.forEach((b) => {
        const type = getDocumentType(b.codigoFavorecido);
        const municipioId = MunicipioResolver.resolve({
          siafi: b.codigoMunicipioSiafi,
          nome: b.nomeMunicipio,
          uf: b.uf
        });

        if (type === 'PF') {
          const cpf = formatCpf(b.codigoFavorecido)!;
          pfMap.set(cpf, {
            cpf,
            nome: b.nomeFavorecido || 'NÃO INFORMADO',
            municipioId,
          });
        } else if (type === 'PJ') {
          const cnpj = formatCnpj(b.codigoFavorecido)!;
          pjMap.set(cnpj, {
            cnpj,
            razaoSocial: b.nomeFavorecido || 'NÃO INFORMADO',
            municipioId,
            tipoPessoa: 'J',
          });
        }
      });

      const pjToUpsert = Array.from(pjMap.values());
      if (pjToUpsert.length > 0) {
        await silver_db.insert(pessoasJuridicas)
          .values(pjToUpsert)
          .onDuplicateKeyUpdate({
            set: {
              razaoSocial: sql`VALUES(razao_social)`,
              municipioId: sql`COALESCE(VALUES(municipio_id), pessoas_juridicas.municipio_id)`,
            },
          });
      }

      const pfToUpsert = Array.from(pfMap.values());
      if (pfToUpsert.length > 0) {
        const exists = await silver_db.select({ cpf: pessoasFisicas.cpf })
          .from(pessoasFisicas)
          .where(inArray(pessoasFisicas.cpf, pfToUpsert.map(p => p.cpf)));
        const eSet = new Set(exists.map(e => e.cpf));
        const toInsert = pfToUpsert.filter(p => !eSet.has(p.cpf));
        if (toInsert.length > 0) {
          await silver_db.insert(pessoasFisicas).values(toInsert);
        }
      }

      const pfDb = pfToUpsert.length > 0
        ? await silver_db.select({ id: pessoasFisicas.id, cpf: pessoasFisicas.cpf }).from(pessoasFisicas).where(inArray(pessoasFisicas.cpf, pfToUpsert.map(p => p.cpf)))
        : [];
      const pjDb = pjToUpsert.length > 0
        ? await silver_db.select({ id: pessoasJuridicas.id, cnpj: pessoasJuridicas.cnpj }).from(pessoasJuridicas).where(inArray(pessoasJuridicas.cnpj, pjToUpsert.map(p => p.cnpj)))
        : [];
      const pfIdMap = new Map(pfDb.map(p => [p.cpf, p.id]));
      const pjIdMap = new Map(pjDb.map(p => [p.cnpj, p.id]));

      // 6. Entidade de Recursos Transferidos
      const transferenciasToInsert = bronzeData
        .map((b) => {
          const anoMes = formatAnoMes(b.anoMes);
          const valorTransferido = parseDecimal(b.valorTransferido);
          if (!anoMes || valorTransferido === null) return null;

          const unidadeGestoraId = b.codigoUnidadeGestora ? ugIdMap.get(b.codigoUnidadeGestora) || null : null;
          const municipioId = MunicipioResolver.resolve({
            siafi: b.codigoMunicipioSiafi,
            nome: b.nomeMunicipio,
            uf: b.uf
          });
          const acaoOrcamentariaId = b.codigoAcao ? acaoIdMap.get(b.codigoAcao) || null : null;

          let favorecidoPfId = null;
          let favorecidoPjId = null;
          const docType = getDocumentType(b.codigoFavorecido);

          if (docType === 'PF') {
            const cpf = formatCpf(b.codigoFavorecido);
            favorecidoPfId = cpf ? pfIdMap.get(cpf) || null : null;
          } else if (docType === 'PJ') {
            const cnpj = formatCnpj(b.codigoFavorecido);
            favorecidoPjId = cnpj ? pjIdMap.get(cnpj) || null : null;
          }

          return {
            anoMes,
            tipoTransferencia: b.tipoTransferencia || 'NÃO INFORMADO',
            municipioId,
            unidadeGestoraId,
            acaoOrcamentariaId,
            favorecidoPfId,
            favorecidoPjId,
            valorTransferido,
          };
        })
        .filter(Boolean) as typeof recursosTransferidos.$inferInsert[];

      if (transferenciasToInsert.length > 0) {
        const anoMeses = [...new Set(transferenciasToInsert.map(t => t.anoMes))];
        const ugIds = [...new Set(transferenciasToInsert.map(t => t.unidadeGestoraId).filter(Boolean) as number[])];

        if (anoMeses.length > 0 && ugIds.length > 0) {
          await silver_db.delete(recursosTransferidos)
            .where(
              and(
                inArray(recursosTransferidos.anoMes, anoMeses),
                inArray(recursosTransferidos.unidadeGestoraId, ugIds)
              )
            );
        }

        log.info({ data: { count: transferenciasToInsert.length } }, 'Inserindo recursos_transferidos');
        for (let i = 0; i < transferenciasToInsert.length; i += 1000) {
          await silver_db.insert(recursosTransferidos).values(transferenciasToInsert.slice(i, i + 1000));
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento de Recursos Transferidos');
    throw error;
  }
}