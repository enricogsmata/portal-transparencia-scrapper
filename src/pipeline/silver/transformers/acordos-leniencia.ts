import { sql, inArray } from 'drizzle-orm';
import { bronze_db } from '../../../database/bronze_connection.js';
import { silver_db } from '../../../database/silver_connection.js';
import { AcordosLeniencia, AcordosLenienciaEfeitos } from '../../../database/bronze_schema.js';
import { acordosLeniencia, acordoLenienciaEfeitos, pessoasJuridicas } from '../../../database/silver_schema.js';
import { formatCnpj, parseDate } from '../shared/formatters.js';
import { new_logger } from '../../../services/logs.js';

const logger = new_logger(import.meta.url);

export async function processAcordosLeniencia() {
  const log = logger.forMethod('processAcordosLeniencia');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento de Acordos de Leniência');

  try {
    while (hasMore) {
      // 1. Extração de Entidades Bronze
      const acordosBronze = await bronze_db.select()
        .from(AcordosLeniencia)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (acordosBronze.length === 0) {
        log.info({ data: { offset } }, 'Processamento concluído. Nenhum registro adicional encontrado na camada bronze.');
        hasMore = false;
        break;
      }

      log.info({ data: { offset, batchSize: acordosBronze.length } }, 'Lote extraído da camada bronze');

      // 2. Extração, Deduplicação e Upsert na Dimensão de Pessoas Jurídicas (3FN)
      const empresasToUpsert = acordosBronze
        .filter((a) => a.cnpjSancionado && formatCnpj(a.cnpjSancionado))
        .map((a) => ({
          cnpj: formatCnpj(a.cnpjSancionado!)!,
          razaoSocial: a.razaoSocialCadastroReceita || 'NÃO INFORMADO',
          nomeFantasia: a.nomeFantasiaCadastroReceita,
        }));

      const uniqueEmpresasMap = new Map<string, typeof empresasToUpsert[0]>();
      for (const e of empresasToUpsert) uniqueEmpresasMap.set(e.cnpj, e);
      const uniqueEmpresas = Array.from(uniqueEmpresasMap.values());

      if (uniqueEmpresas.length > 0) {
        log.info({ data: { count: uniqueEmpresas.length } }, 'Realizando upsert na dimensão pessoas_juridicas');
        await silver_db.insert(pessoasJuridicas)
          .values(uniqueEmpresas)
          .onDuplicateKeyUpdate({
            set: {
              razaoSocial: sql`VALUES(razao_social)`,
              nomeFantasia: sql`COALESCE(VALUES(nome_fantasia), pessoas_juridicas.nome_fantasia)`,
            },
          });
      }

      // 3. Recuperação de Chaves Estrangeiras (Empresas)
      const cnpjs = uniqueEmpresas.map((e) => e.cnpj);
      const empresasDb = cnpjs.length > 0
        ? await silver_db.select({ id: pessoasJuridicas.id, cnpj: pessoasJuridicas.cnpj })
          .from(pessoasJuridicas)
          .where(inArray(pessoasJuridicas.cnpj, cnpjs))
        : [];
      const empresaIdMap = new Map(empresasDb.map((e) => [e.cnpj, e.id]));

      // 4. Mapeamento, Deduplicação e Upsert da Entidade Principal de Acordos
      const acordosToInsert = acordosBronze
        .filter((a) => a.idAcordo && a.cnpjSancionado && a.dataInicioAcordo)
        .map((a) => {
          const cnpjFormatado = formatCnpj(a.cnpjSancionado!);
          const empresaId = cnpjFormatado ? empresaIdMap.get(cnpjFormatado) : null;

          if (!empresaId) return null;

          return {
            idAcordo: a.idAcordo!,
            empresaId,
            numeroProcesso: a.numeroProcesso || 'NÃO INFORMADO',
            dataInicioAcordo: parseDate(a.dataInicioAcordo)!,
            dataConclusaoAcordo: parseDate(a.dataConclusaoAcordo),
            orgaoSancionador: a.orgaoSancionador,
            situacaoAcordo: a.situacaoAcordo,
            termosAcordo: a.termosAcordo,
          };
        })
        .filter(Boolean) as typeof acordosLeniencia.$inferInsert[];

      if (acordosToInsert.length > 0) {
        const uniqueAcordosMap = new Map<string, typeof acordosToInsert[0]>();
        for (const a of acordosToInsert) uniqueAcordosMap.set(a.idAcordo, a);
        const uniqueAcordos = Array.from(uniqueAcordosMap.values());

        log.info({ data: { count: uniqueAcordos.length } }, 'Realizando upsert na entidade acordos_leniencia');
        await silver_db.insert(acordosLeniencia)
          .values(uniqueAcordos)
          .onDuplicateKeyUpdate({
            set: {
              situacaoAcordo: sql`VALUES(situacao_acordo)`,
              termosAcordo: sql`VALUES(termos_acordo)`,
              dataConclusaoAcordo: sql`VALUES(data_conclusao_acordo)`,
            },
          });
      }

      // 5. Processamento, Resiliência e Inserção de Efeitos Dependentes (2FN)
      const acordosIds = acordosBronze.map((a) => a.idAcordo).filter(Boolean) as string[];
      if (acordosIds.length > 0) {
        const efeitosBronze = await bronze_db.select()
          .from(AcordosLenienciaEfeitos)
          .where(inArray(AcordosLenienciaEfeitos.idAcordo, acordosIds));

        if (efeitosBronze.length > 0) {
          const acordosDb = await silver_db.select({ id: acordosLeniencia.id, idAcordo: acordosLeniencia.idAcordo })
            .from(acordosLeniencia)
            .where(inArray(acordosLeniencia.idAcordo, acordosIds));
          const acordoIdMap = new Map(acordosDb.map((a) => [a.idAcordo, a.id]));

          const efeitosToInsert = efeitosBronze
            .map((e) => {
              const acordoId = acordoIdMap.get(e.idAcordo!);
              if (!acordoId || !e.efeitoAcordo) return null;
              return {
                acordoId,
                efeitoAcordo: e.efeitoAcordo,
                complemento: e.complemento,
              };
            })
            .filter(Boolean) as typeof acordoLenienciaEfeitos.$inferInsert[];

          if (efeitosToInsert.length > 0) {
            const silverAcordosIds = Array.from(new Set(efeitosToInsert.map((e) => e.acordoId)));

            log.info({ data: { count: efeitosToInsert.length, acordosAfetados: silverAcordosIds.length } }, 'Inserindo acordo_leniencia_efeitos');

            // Garante idempotência: remove efeitos anteriores desta exata remessa para evitar duplicatas sem dependência de unique keys múltiplas
            await silver_db.delete(acordoLenienciaEfeitos)
              .where(inArray(acordoLenienciaEfeitos.acordoId, silverAcordosIds));

            for (let i = 0; i < efeitosToInsert.length; i += 1000) {
              await silver_db.insert(acordoLenienciaEfeitos).values(efeitosToInsert.slice(i, i + 1000));
            }
          }
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento de Acordos de Leniência');
    throw error;
  }
}