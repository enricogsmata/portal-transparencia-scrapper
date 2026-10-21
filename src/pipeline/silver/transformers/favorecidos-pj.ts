import { sql, inArray } from 'drizzle-orm';
import { bronze_db } from '../../../database/bronze_connection.js';
import { silver_db } from '../../../database/silver_connection.js';
import {
  FavorecidosPjCnpj,
  FavorecidosPjCnae,
  FavorecidosPjNaturezaJuridica
} from '../../../database/bronze_schema.js';
import {
  pessoasJuridicas,
  cnaes,
  naturezasJuridicas
} from '../../../database/silver_schema.js';
import { formatCep, formatCnpj } from '../shared/formatters.js';
import { MunicipioResolver } from '../shared/municipio-resolver.js';
import { new_logger } from '../../../services/logs.js';

const logger = new_logger(import.meta.url);

export async function processFavorecidosPjCnae() {
  const log = logger.forMethod('processFavorecidosPjCnae');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento de Favorecidos PJ - CNAEs');

  while (hasMore) {
    const bronzeData = await bronze_db.select()
      .from(FavorecidosPjCnae)
      .limit(BATCH_SIZE)
      .offset(offset);

    if (bronzeData.length === 0) {
      log.info({ data: { offset } }, 'Processamento de CNAEs concluído.');
      hasMore = false;
      break;
    }

    const cnaesToUpsert = bronzeData
      .filter((c) => c.codigoSubclasse)
      .map((c) => ({
        codigoSecao: c.codigoSecao || null,
        descricaoSecao: c.descricaoSecao || null,
        codigoSubclasse: c.codigoSubclasse!,
        descricaoSubclasse: c.descricaoSubclasse || null,
      }));

    if (cnaesToUpsert.length > 0) {
      const uniqueMap = new Map<string, typeof cnaesToUpsert[0]>();
      for (const item of cnaesToUpsert) uniqueMap.set(item.codigoSubclasse, item);
      const uniqueItems = Array.from(uniqueMap.values());

      log.info({ data: { count: uniqueItems.length } }, 'Realizando upsert na tabela cnaes');
      await silver_db.insert(cnaes)
        .values(uniqueItems)
        .onDuplicateKeyUpdate({
          set: {
            descricaoSubclasse: sql`VALUES(descricao_subclasse)`,
            codigoSecao: sql`COALESCE(VALUES(codigo_secao), cnaes.codigo_secao)`,
            descricaoSecao: sql`COALESCE(VALUES(descricao_secao), cnaes.descricao_secao)`,
          },
        });
    }
    offset += BATCH_SIZE;
  }
}

export async function processFavorecidosPjNaturezaJuridica() {
  const log = logger.forMethod('processFavorecidosPjNaturezaJuridica');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento de Favorecidos PJ - Naturezas Jurídicas');

  while (hasMore) {
    const bronzeData = await bronze_db.select()
      .from(FavorecidosPjNaturezaJuridica)
      .limit(BATCH_SIZE)
      .offset(offset);

    if (bronzeData.length === 0) {
      log.info({ data: { offset } }, 'Processamento de Naturezas Jurídicas concluído.');
      hasMore = false;
      break;
    }

    const naturezasToUpsert = bronzeData
      .filter((n) => n.codigoNaturezaJuridica)
      .map((n) => ({
        codigo: n.codigoNaturezaJuridica!,
        descricao: n.descricaoNaturezaJuridica || 'NÃO INFORMADO',
        codigoTipo: n.codigoTipoNaturezaJuridica || null,
        descricaoTipo: n.descricaoTipoNaturezaJuridica || null,
      }));

    if (naturezasToUpsert.length > 0) {
      const uniqueMap = new Map<string, typeof naturezasToUpsert[0]>();
      for (const item of naturezasToUpsert) uniqueMap.set(item.codigo, item);
      const uniqueItems = Array.from(uniqueMap.values());

      log.info({ data: { count: uniqueItems.length } }, 'Realizando upsert na tabela naturezas_juridicas');
      await silver_db.insert(naturezasJuridicas)
        .values(uniqueItems)
        .onDuplicateKeyUpdate({
          set: {
            descricao: sql`VALUES(descricao)`,
            codigoTipo: sql`COALESCE(VALUES(codigo_tipo), naturezas_juridicas.codigo_tipo)`,
            descricaoTipo: sql`COALESCE(VALUES(descricao_tipo), naturezas_juridicas.descricao_tipo)`,
          },
        });
    }
    offset += BATCH_SIZE;
  }
}

export async function processFavorecidosPjCnpj() {
  const log = logger.forMethod('processFavorecidosPjCnpj');
  const BATCH_SIZE = 1000;
  let offset = 0;
  let hasMore = true;

  log.info('Iniciando processamento de Favorecidos PJ - CNPJs');

  while (hasMore) {
    const bronzeData = await bronze_db.select()
      .from(FavorecidosPjCnpj)
      .limit(BATCH_SIZE)
      .offset(offset);

    if (bronzeData.length === 0) {
      log.info({ data: { offset } }, 'Processamento de CNPJs concluído.');
      hasMore = false;
      break;
    }

    const codigosCnae = [...new Set(bronzeData.map(b => b.codigoCnae).filter(Boolean) as string[])];
    const cnaesDb = codigosCnae.length > 0
      ? await silver_db.select({ id: cnaes.id, codigoSubclasse: cnaes.codigoSubclasse }).from(cnaes).where(inArray(cnaes.codigoSubclasse, codigosCnae))
      : [];
    const cnaeIdMap = new Map(cnaesDb.map(c => [c.codigoSubclasse, c.id]));

    const codigosNatJur = [...new Set(bronzeData.map(b => b.codigoNaturezaJuridica).filter(Boolean) as string[])];
    const natJurDb = codigosNatJur.length > 0
      ? await silver_db.select({ id: naturezasJuridicas.id, codigo: naturezasJuridicas.codigo }).from(naturezasJuridicas).where(inArray(naturezasJuridicas.codigo, codigosNatJur))
      : [];
    const natJurIdMap = new Map(natJurDb.map(n => [n.codigo, n.id]));

    const pjToUpsert = bronzeData
      .filter((p) => p.cnpj && formatCnpj(p.cnpj))
      .map((p) => {
        const cnpjFormatado = formatCnpj(p.cnpj!);
        const cnaeId = p.codigoCnae ? cnaeIdMap.get(p.codigoCnae) || null : null;
        const naturezaJuridicaId = p.codigoNaturezaJuridica ? natJurIdMap.get(p.codigoNaturezaJuridica) || null : null;

        let tipoPessoaRaw = p.tipoPessoa?.toUpperCase() || null;
        if (tipoPessoaRaw && !['F', 'J', 'FISICA', 'JURIDICA'].includes(tipoPessoaRaw)) {
          tipoPessoaRaw = null;
        }

        const municipioId = MunicipioResolver.resolve({
          nome: p.municipio,
          uf: p.uf
        });

        return {
          cnpj: cnpjFormatado!,
          razaoSocial: p.razaoSocial || 'NÃO INFORMADO',
          nomeFantasia: p.nomeFantasia || null,
          cnaeId,
          naturezaJuridicaId,
          tipoPessoa: tipoPessoaRaw as 'F' | 'J' | 'FISICA' | 'JURIDICA' | null,
          logradouro: p.logradouro || null,
          numero: p.numero || null,
          complemento: p.complemento || null,
          cep: formatCep(p.cep),
          bairro: p.bairro || null,
          municipioId: municipioId || null,
        };
      });

    if (pjToUpsert.length > 0) {
      const uniqueMap = new Map<string, typeof pjToUpsert[0]>();
      for (const item of pjToUpsert) uniqueMap.set(item.cnpj, item);
      const uniqueItems = Array.from(uniqueMap.values());

      log.info({ data: { count: uniqueItems.length } }, 'Realizando upsert na tabela pessoas_juridicas');
      await silver_db.insert(pessoasJuridicas)
        .values(uniqueItems)
        .onDuplicateKeyUpdate({
          set: {
            razaoSocial: sql`COALESCE(NULLIF(VALUES(razao_social), 'NÃO INFORMADO'), pessoas_juridicas.razao_social)`,
            nomeFantasia: sql`COALESCE(VALUES(nome_fantasia), pessoas_juridicas.nome_fantasia)`,
            cnaeId: sql`COALESCE(VALUES(cnae_id), pessoas_juridicas.cnae_id)`,
            naturezaJuridicaId: sql`COALESCE(VALUES(natureza_juridica_id), pessoas_juridicas.natureza_juridica_id)`,
            tipoPessoa: sql`COALESCE(VALUES(tipo_pessoa), pessoas_juridicas.tipo_pessoa)`,
            logradouro: sql`COALESCE(VALUES(logradouro), pessoas_juridicas.logradouro)`,
            numero: sql`COALESCE(VALUES(numero), pessoas_juridicas.numero)`,
            complemento: sql`COALESCE(VALUES(complemento), pessoas_juridicas.complemento)`,
            cep: sql`COALESCE(VALUES(cep), pessoas_juridicas.cep)`,
            bairro: sql`COALESCE(VALUES(bairro), pessoas_juridicas.bairro)`,
            municipioId: sql`COALESCE(VALUES(municipio_id), pessoas_juridicas.municipio_id)`,
          },
        });
    }
    offset += BATCH_SIZE;
  }
}

export async function processFavorecidosPj() {
  const log = logger.forMethod('processFavorecidosPj');
  try {
    await processFavorecidosPjCnae();
    await processFavorecidosPjNaturezaJuridica();
    await processFavorecidosPjCnpj();
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante a orquestração do módulo Favorecidos PJ');
    throw error;
  }
}