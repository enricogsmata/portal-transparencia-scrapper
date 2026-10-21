import { sql, inArray } from 'drizzle-orm';
import { bronze_db } from '../../../database/bronze_connection.js';
import { silver_db } from '../../../database/silver_connection.js';
import {
  NotaFiscal,
  NotaFiscalEvento,
  NotaFiscalItem
} from '../../../database/bronze_schema.js';
import {
  notasFiscais,
  notaFiscalEventos,
  notaFiscalItens,
  pessoasFisicas,
  pessoasJuridicas
} from '../../../database/silver_schema.js';
import {
  formatCnpj,
  formatCpf,
  getDocumentType,
  parseDate,
  parseDecimal
} from '../shared/formatters.js';
import { MunicipioResolver } from '../shared/municipio-resolver.js';
import { new_logger } from '../../../services/logs.js';

const logger = new_logger(import.meta.url);

export async function processNotaFiscal() {
  const log = logger.forMethod('processNotaFiscal');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento da tabela principal de Notas Fiscais');

  try {
    while (hasMore) {
      const bronzeData = await bronze_db.select()
        .from(NotaFiscal)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        log.info({ data: { offset } }, 'Processamento concluído. Nenhum registro adicional encontrado.');
        hasMore = false;
        break;
      }

      log.info({ data: { offset, batchSize: bronzeData.length } }, 'Lote extraído da tabela nota_fiscal');

      // 2. Pessoas Jurídicas (Emitente e Destinatário PJ)
      const pjMap = new Map<string, typeof pessoasJuridicas.$inferInsert>();

      bronzeData.forEach(b => {
        // Emitente (geralmente PJ)
        const tipoEmi = getDocumentType(b.cpfCnpjEmitente);
        if (tipoEmi === 'PJ') {
          const cnpjEmi = formatCnpj(b.cpfCnpjEmitente);
          if (cnpjEmi) {
            const municipioId = MunicipioResolver.resolve({
              nome: b.municipioEmitente,
              uf: b.ufEmitente,
            });
            pjMap.set(cnpjEmi, {
              cnpj: cnpjEmi,
              razaoSocial: b.razaoSocialEmitente || 'NÃO INFORMADO',
              municipioId: municipioId || null
            });
          }
        }

        // Destinatário
        const tipoDest = getDocumentType(b.cnpjDestinatario); // Apesar do nome da coluna ser cnpjDestinatario, pode conter CPF
        if (tipoDest === 'PJ') {
          const cnpjDest = formatCnpj(b.cnpjDestinatario);
          if (cnpjDest && !pjMap.has(cnpjDest)) {
            pjMap.set(cnpjDest, {
              cnpj: cnpjDest,
              razaoSocial: b.nomeDestinatario || 'NÃO INFORMADO'
            });
          }
        }
      });

      if (pjMap.size > 0) {
        await silver_db.insert(pessoasJuridicas).values(Array.from(pjMap.values())).onDuplicateKeyUpdate({ set: { id: sql`id` } });
      }
      const pjDb = pjMap.size > 0 ? await silver_db.select({ id: pessoasJuridicas.id, cnpj: pessoasJuridicas.cnpj }).from(pessoasJuridicas).where(inArray(pessoasJuridicas.cnpj, Array.from(pjMap.keys()))) : [];
      const pjIdMap = new Map(pjDb.map(p => [p.cnpj, p.id]));

      // 3. Pessoas Físicas (Destinatário PF)
      const pfMap = new Map<string, typeof pessoasFisicas.$inferInsert>();

      bronzeData.forEach(b => {
        const tipoDest = getDocumentType(b.cnpjDestinatario);
        if (tipoDest === 'PF') {
          const cpfDest = formatCpf(b.cnpjDestinatario);
          if (cpfDest) {
            pfMap.set(cpfDest, {
              cpf: cpfDest,
              nome: b.nomeDestinatario || 'NÃO INFORMADO'
            });
          }
        }
      });

      if (pfMap.size > 0) {
        const exists = await silver_db.select({ cpf: pessoasFisicas.cpf }).from(pessoasFisicas).where(inArray(pessoasFisicas.cpf, Array.from(pfMap.keys())));
        const eSet = new Set(exists.map(e => e.cpf));
        const toInsert = Array.from(pfMap.values()).filter(p => !eSet.has(p.cpf));
        if (toInsert.length > 0) await silver_db.insert(pessoasFisicas).values(toInsert);
      }
      const pfDb = pfMap.size > 0 ? await silver_db.select({ id: pessoasFisicas.id, cpf: pessoasFisicas.cpf }).from(pessoasFisicas).where(inArray(pessoasFisicas.cpf, Array.from(pfMap.keys()))) : [];
      const pfIdMap = new Map(pfDb.map(p => [p.cpf, p.id]));

      // 4. Entidade de Notas Fiscais
      const nfToInsert = bronzeData.map(b => {
        if (!b.chaveAcesso || b.chaveAcesso.length !== 44) return null;

        const emitenteCnpj = formatCnpj(b.cpfCnpjEmitente);
        const emitenteId = emitenteCnpj ? pjIdMap.get(emitenteCnpj) : null;

        if (!emitenteId) return null;

        const tipoDest = getDocumentType(b.cnpjDestinatario);
        let destinatarioPfId = null;
        let destinatarioPjId = null;

        if (tipoDest === 'PF') destinatarioPfId = pfIdMap.get(formatCpf(b.cnpjDestinatario)!) || null;
        else if (tipoDest === 'PJ') destinatarioPjId = pjIdMap.get(formatCnpj(b.cnpjDestinatario)!) || null;

        return {
          chaveAcesso: b.chaveAcesso,
          modelo: b.modelo || null,
          serie: b.serie || null,
          numero: b.numero || '0',
          naturezaOperacao: b.naturezaOperacao || null,
          dataEmissao: parseDate(b.dataEmissao) || '1970-01-01',
          emitenteId,
          destinatarioPfId,
          destinatarioPjId,
          consumidorFinal: b.consumidorFinal === '1',
          presencaComprador: b.presencaComprador || null,
          valorNotaFiscal: parseDecimal(b.valorNotaFiscal) || '0',
          exercicioReferencia: b.exercicioReferencia || null
        };
      }).filter(Boolean) as typeof notasFiscais.$inferInsert[];

      if (nfToInsert.length > 0) {
        // Garantindo unicidade na memória do lote atual
        const batchMap = new Map<string, typeof nfToInsert[0]>();
        for (const n of nfToInsert) batchMap.set(n.chaveAcesso, n);
        const uniqueNfs = Array.from(batchMap.values());

        // Inserção com IGNORE/OnDuplicate seguro (chaves únicas não devem ser duplicadas)
        log.info({ data: { count: uniqueNfs.length } }, 'Inserindo notas_fiscais');

        const existingNfs = await silver_db.select({ chaveAcesso: notasFiscais.chaveAcesso })
          .from(notasFiscais)
          .where(inArray(notasFiscais.chaveAcesso, uniqueNfs.map(n => n.chaveAcesso)));

        const existingSet = new Set(existingNfs.map(n => n.chaveAcesso));
        const toInsertFinal = uniqueNfs.filter(n => !existingSet.has(n.chaveAcesso));

        if (toInsertFinal.length > 0) {
          for (let i = 0; i < toInsertFinal.length; i += 1000) {
            await silver_db.insert(notasFiscais).values(toInsertFinal.slice(i, i + 1000));
          }
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento de Notas Fiscais');
    throw error;
  }
}

export async function processNotaFiscalItem() {
  const log = logger.forMethod('processNotaFiscalItem');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento de Itens de Nota Fiscal');

  try {
    while (hasMore) {
      const bronzeData = await bronze_db.select()
        .from(NotaFiscalItem)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        hasMore = false;
        break;
      }

      const chavesLote = [...new Set(bronzeData.map(b => b.chaveAcesso).filter(c => c && c.length === 44) as string[])];
      const nfsDb = chavesLote.length > 0
        ? await silver_db.select({ id: notasFiscais.id, chaveAcesso: notasFiscais.chaveAcesso })
          .from(notasFiscais)
          .where(inArray(notasFiscais.chaveAcesso, chavesLote))
        : [];
      const nfIdMap = new Map(nfsDb.map(n => [n.chaveAcesso, n.id]));

      const itensToInsert = bronzeData.map(b => {
        if (!b.chaveAcesso || b.chaveAcesso.length !== 44) return null;

        const notaFiscalId = nfIdMap.get(b.chaveAcesso);
        if (!notaFiscalId || !b.numeroProduto || !b.descricaoProdutoServico) return null;

        return {
          notaFiscalId,
          numeroProduto: b.numeroProduto,
          descricaoProdutoServico: b.descricaoProdutoServico,
          codigoNcmSh: b.codigoNcmSh || null,
          cfop: b.cfop || null,
          quantidade: parseDecimal(b.quantidade) || '0',
          unidade: b.unidade || 'UN',
          valorUnitario: parseDecimal(b.valorUnitario) || '0',
          valorTotal: parseDecimal(b.valorTotal) || '0'
        };
      }).filter(Boolean) as typeof notaFiscalItens.$inferInsert[];

      if (itensToInsert.length > 0) {
        // Idempotência por NF no lote atual
        const nfIds = [...new Set(itensToInsert.map(i => i.notaFiscalId))];
        await silver_db.delete(notaFiscalItens).where(inArray(notaFiscalItens.notaFiscalId, nfIds));

        log.info({ data: { count: itensToInsert.length } }, 'Inserindo nota_fiscal_itens');
        for (let i = 0; i < itensToInsert.length; i += 1000) {
          await silver_db.insert(notaFiscalItens).values(itensToInsert.slice(i, i + 1000));
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento de Itens de Nota Fiscal');
    throw error;
  }
}

export async function processNotaFiscalEvento() {
  const log = logger.forMethod('processNotaFiscalEvento');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento de Eventos de Nota Fiscal');

  try {
    while (hasMore) {
      const bronzeData = await bronze_db.select()
        .from(NotaFiscalEvento)
        .limit(BATCH_SIZE)
        .offset(offset);

      if (bronzeData.length === 0) {
        hasMore = false;
        break;
      }

      const chavesLote = [...new Set(bronzeData.map(b => b.chaveAcesso).filter(c => c && c.length === 44) as string[])];
      const nfsDb = chavesLote.length > 0
        ? await silver_db.select({ id: notasFiscais.id, chaveAcesso: notasFiscais.chaveAcesso })
          .from(notasFiscais)
          .where(inArray(notasFiscais.chaveAcesso, chavesLote))
        : [];
      const nfIdMap = new Map(nfsDb.map(n => [n.chaveAcesso, n.id]));

      const eventosToInsert = bronzeData.map(b => {
        if (!b.chaveAcesso || b.chaveAcesso.length !== 44) return null;

        const notaFiscalId = nfIdMap.get(b.chaveAcesso);
        // dataHoraEvento necessita de formatação adequada caso venha do SIASG
        let dhFormatada = b.dataHoraEvento ? parseDate(b.dataHoraEvento.split(' ')[0]) : null;
        if (!dhFormatada) dhFormatada = '1970-01-01 00:00:00';
        else dhFormatada = b.dataHoraEvento; // Preserva a hora se o banco aceitar string ISO

        if (!notaFiscalId || !b.evento) return null;

        return {
          notaFiscalId,
          evento: b.evento,
          dataHoraEvento: dhFormatada,
          descricaoEvento: b.descricaoEvento || null,
          motivoEvento: b.motivoEvento || null
        };
      }).filter(Boolean) as typeof notaFiscalEventos.$inferInsert[];

      if (eventosToInsert.length > 0) {
        const nfIds = [...new Set(eventosToInsert.map(e => e.notaFiscalId))];
        await silver_db.delete(notaFiscalEventos).where(inArray(notaFiscalEventos.notaFiscalId, nfIds));

        log.info({ data: { count: eventosToInsert.length } }, 'Inserindo nota_fiscal_eventos');
        for (let i = 0; i < eventosToInsert.length; i += 1000) {
          await silver_db.insert(notaFiscalEventos).values(eventosToInsert.slice(i, i + 1000));
        }
      }

      offset += BATCH_SIZE;
    }
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante o processamento de Eventos de Nota Fiscal');
    throw error;
  }
}

export async function processNotasFiscais() {
  const log = logger.forMethod('processNotasFiscais');
  try {
    await processNotaFiscal();
    await processNotaFiscalItem();
    await processNotaFiscalEvento();
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante a orquestração do módulo de Notas Fiscais');
    throw error;
  }
}