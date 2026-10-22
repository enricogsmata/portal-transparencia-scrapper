import {
  mysqlTable,
  bigint,
  int,
  varchar,
  char,
  decimal,
  date,
  datetime,
  timestamp,
  boolean,
  text,
  json,
  mysqlEnum,
  index,
  uniqueIndex,
  foreignKey,
  binary,
} from 'drizzle-orm/mysql-core';
import { relations, sql } from 'drizzle-orm';

/**
 * Chave natural calculada pelo próprio MySQL (coluna gerada STORED, SHA-256 em BINARY(32)).
 *
 * Por que existe: UNIQUE em MySQL aceita vários NULLs, então uma chave composta com colunas
 * anuláveis NÃO deduplica e o onDuplicateKeyUpdate nunca dispara. Cada parte anulável deve ser
 * passada já envolvida em IFNULL(...) (CONCAT_WS ignora NULLs, o que geraria ambiguidade).
 * Como é gerada pelo banco, o transformer não calcula nem grava essa coluna.
 */
const chaveNaturalHash = (...partes: string[]) =>
  binary('chave_natural', { length: 32 }).generatedAlwaysAs(
    sql.raw(`UNHEX(SHA2(CONCAT_WS('|', ${partes.join(', ')}), 256))`),
    { mode: 'stored' },
  );

// ============================================================================
// 1. SISTEMA E ORQUESTRAÇÃO DE PIPELINE (METADADOS DE CARGA)
// ============================================================================

export const logs = mysqlTable(
  'logs',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    category: varchar('category', { length: 50 }).notNull().default(''),
    module: varchar('module', { length: 100 }).notNull().default(''),
    context: varchar('context', { length: 150 }).notNull().default(''),
    message: varchar('message', { length: 500 }).notNull().default(''),
    data: json('data'),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_logs_category').on(table.category),
    index('idx_logs_module').on(table.module),
    index('idx_logs_created_at').on(table.createdAt),
  ]
);

// ============================================================================
// 2. DIMENSÕES GEOGRÁFICAS E TERRITORIAIS (3FN)
// ============================================================================

export const estados = mysqlTable(
  'estados',
  {
    sigla: char('sigla', { length: 2 }).primaryKey(),
    nome: varchar('nome', { length: 50 }).notNull(),
    codigoIbge: char('codigo_ibge', { length: 2 }).notNull().unique(),
    regiao: varchar('regiao', { length: 20 }).notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  }
);

export const municipios = mysqlTable(
  'municipios',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    ufSigla: char('uf_sigla', { length: 2 }).notNull().references(() => estados.sigla),
    codigoSiafi: varchar('codigo_siafi', { length: 10 }).unique(),
    codigoIbge: varchar('codigo_ibge', { length: 10 }).unique(),
    nome: varchar('nome', { length: 150 }).notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_municipios_uf').on(table.ufSigla),
    index('idx_municipios_codigo_siafi').on(table.codigoSiafi),
    index('idx_municipios_codigo_ibge').on(table.codigoIbge),
    index('idx_municipios_nome').on(table.nome),
  ]
);

// ============================================================================
// 3. HIERARQUIA INSTITUCIONAL GOVERNAMENTAL (3FN)
// ============================================================================

export const orgaosSuperiores = mysqlTable(
  'orgaos_superiores',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    // Códigos SIAFI e SIAPE podem colidir com significados diferentes: a unicidade é por (sistema, código).
    sistemaOrigem: mysqlEnum('sistema_origem', ['SIAFI', 'SIAPE']).notNull().default('SIAFI'),
    codigo: varchar('codigo', { length: 30 }).notNull(),
    nome: varchar('nome', { length: 255 }).notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_orgaos_sup_codigo').on(table.codigo),
    uniqueIndex('uq_orgaos_sup_sist_codigo').on(table.sistemaOrigem, table.codigo),
  ]
);

export const orgaosSubordinados = mysqlTable(
  'orgaos_subordinados',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    orgaoSuperiorId: bigint('orgao_superior_id', { mode: 'number' }).notNull().references(() => orgaosSuperiores.id),
    // Códigos SIAFI e SIAPE podem colidir com significados diferentes: a unicidade é por (sistema, código).
    sistemaOrigem: mysqlEnum('sistema_origem', ['SIAFI', 'SIAPE']).notNull().default('SIAFI'),
    codigo: varchar('codigo', { length: 30 }).notNull(),
    nome: varchar('nome', { length: 255 }).notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_orgaos_sub_superior').on(table.orgaoSuperiorId),
    index('idx_orgaos_sub_codigo').on(table.codigo),
    uniqueIndex('uq_orgaos_sub_sist_codigo').on(table.sistemaOrigem, table.codigo),
  ]
);

export const unidadesGestoras = mysqlTable(
  'unidades_gestoras',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    orgaoSubordinadoId: bigint('orgao_subordinado_id', { mode: 'number' }).references(() => orgaosSubordinados.id),
    codigo: varchar('codigo', { length: 30 }).notNull().unique(),
    nome: varchar('nome', { length: 255 }).notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_ug_orgao_sub').on(table.orgaoSubordinadoId),
    index('idx_ug_codigo').on(table.codigo),
  ]
);

// Unidade Orçamentária (UO) é um conceito distinto de Unidade Gestora (UG): códigos e semântica diferentes.
export const unidadesOrcamentarias = mysqlTable(
  'unidades_orcamentarias',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    orgaoSubordinadoId: bigint('orgao_subordinado_id', { mode: 'number' }),
    codigo: varchar('codigo', { length: 30 }).notNull().unique(),
    nome: varchar('nome', { length: 255 }).notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_uo_orgao_sub').on(table.orgaoSubordinadoId),
    foreignKey({
      name: 'fk_uo_orgao_sub',
      columns: [table.orgaoSubordinadoId],
      foreignColumns: [orgaosSubordinados.id],
    }),
  ]
);

export const gestoes = mysqlTable(
  'gestoes',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    codigo: varchar('codigo', { length: 30 }).notNull().unique(),
    nome: varchar('nome', { length: 255 }).notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  }
);

// ============================================================================
// 4. PESSOAS, EMPRESAS E CADASTROS BASE (3FN)
// ============================================================================

export const cnaes = mysqlTable(
  'cnaes',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    codigoSecao: varchar('codigo_secao', { length: 10 }),
    descricaoSecao: varchar('descricao_secao', { length: 255 }),
    codigoSubclasse: varchar('codigo_subclasse', { length: 20 }).notNull().unique(),
    descricaoSubclasse: varchar('descricao_subclasse', { length: 255 }),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_cnae_subclasse').on(table.codigoSubclasse),
  ]
);

export const naturezasJuridicas = mysqlTable(
  'naturezas_juridicas',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    codigo: varchar('codigo', { length: 10 }).notNull().unique(),
    descricao: varchar('descricao', { length: 255 }).notNull(),
    codigoTipo: varchar('codigo_tipo', { length: 10 }),
    descricaoTipo: varchar('descricao_tipo', { length: 255 }),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_nat_juridica_codigo').on(table.codigo),
  ]
);

export const pessoasFisicas = mysqlTable(
  'pessoas_fisicas',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    cpf: varchar('cpf', { length: 14 }).notNull(),
    nis: varchar('nis', { length: 15 }),
    // Identidade = CPF (possivelmente mascarado) + nome normalizado. Gerada pelo banco.
    chaveNatural: chaveNaturalHash('cpf', "UPPER(TRIM(IFNULL(nome, '')))"),
    nome: varchar('nome', { length: 255 }),
    municipioId: bigint('municipio_id', { mode: 'number' }).references(() => municipios.id),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_pf_cpf').on(table.cpf),
    index('idx_pf_nis').on(table.nis),
    index('idx_pf_municipio').on(table.municipioId),
    uniqueIndex('uq_pf_chave_natural').on(table.chaveNatural),
  ]
);

export const pessoasJuridicas = mysqlTable(
  'pessoas_juridicas',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    cnpj: varchar('cnpj', { length: 18 }).notNull().unique(),
    razaoSocial: varchar('razao_social', { length: 255 }).notNull(),
    nomeFantasia: varchar('nome_fantasia', { length: 255 }),
    cnaeId: bigint('cnae_id', { mode: 'number' }).references(() => cnaes.id),
    naturezaJuridicaId: bigint('natureza_juridica_id', { mode: 'number' }).references(() => naturezasJuridicas.id),
    tipoPessoa: mysqlEnum('tipo_pessoa', ['F', 'J', 'FISICA', 'JURIDICA']),
    logradouro: varchar('logradouro', { length: 255 }),
    numero: varchar('numero', { length: 50 }),
    complemento: varchar('complemento', { length: 150 }),
    cep: varchar('cep', { length: 9 }),
    bairro: varchar('bairro', { length: 100 }),
    municipioId: bigint('municipio_id', { mode: 'number' }).references(() => municipios.id),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_pj_cnpj').on(table.cnpj),
    index('idx_pj_municipio').on(table.municipioId),
    index('idx_pj_cnae').on(table.cnaeId),
    index('idx_pj_natureza').on(table.naturezaJuridicaId),
  ]
);

// ============================================================================
// 5. ESTRUTURA E CLASSIFICAÇÃO ORÇAMENTÁRIA FEDERAL (3FN)
// ============================================================================

export const funcoesOrcamentarias = mysqlTable(
  'funcoes_orcamentarias',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    codigo: varchar('codigo', { length: 10 }).notNull().unique(),
    nome: varchar('nome', { length: 150 }).notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  }
);

export const subfuncoesOrcamentarias = mysqlTable(
  'subfuncoes_orcamentarias',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    codigo: varchar('codigo', { length: 10 }).notNull().unique(),
    nome: varchar('nome', { length: 150 }).notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  }
);

export const programasOrcamentarios = mysqlTable(
  'programas_orcamentarios',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    codigo: varchar('codigo', { length: 20 }).notNull().unique(),
    nome: varchar('nome', { length: 255 }).notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  }
);

export const acoesOrcamentarias = mysqlTable(
  'acoes_orcamentarias',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    programaId: bigint('programa_id', { mode: 'number' }).references(() => programasOrcamentarios.id),
    codigo: varchar('codigo', { length: 20 }).notNull(),
    nome: varchar('nome', { length: 255 }).notNull(),
    linguagemCidada: text('linguagem_cidada'),
    chaveNatural: chaveNaturalHash('IFNULL(programa_id, 0)', 'codigo'),
    createdAt: timestamp('created_at', { fsp: 3 })
      .default(sql`CURRENT_TIMESTAMP(3)`)
      .notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 })
      .default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`)
      .notNull(),
  },
  (table) => [
    index('idx_acoes_programa').on(table.programaId),
    index('idx_acoes_codigo').on(table.codigo),
    uniqueIndex('uq_acoes_chave_natural').on(table.chaveNatural),
  ]
);

export const planosOrcamentarios = mysqlTable(
  'planos_orcamentarios',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    codigo: varchar('codigo', { length: 30 }).notNull().unique(),
    nome: varchar('nome', { length: 255 }).notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  }
);

export const subtitulosLocalizadores = mysqlTable(
  'subtitulos_localizadores',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    codigo: varchar('codigo', { length: 30 }).notNull(),
    nome: varchar('nome', { length: 255 }).notNull(),
    sigla: varchar('sigla', { length: 50 }),
    descricaoComplementar: text('descricao_complementar'),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  }
);

export const categoriasDespesa = mysqlTable(
  'categorias_despesa',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    codigo: varchar('codigo', { length: 10 }).notNull().unique(),
    nome: varchar('nome', { length: 100 }).notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  }
);

export const gruposDespesa = mysqlTable(
  'grupos_despesa',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    codigo: varchar('codigo', { length: 10 }).notNull().unique(),
    nome: varchar('nome', { length: 100 }).notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  }
);

export const modalidadesAplicacao = mysqlTable(
  'modalidades_aplicacao',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    codigo: varchar('codigo', { length: 10 }).notNull().unique(),
    nome: varchar('nome', { length: 255 }).notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  }
);

export const elementosDespesa = mysqlTable(
  'elementos_despesa',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    codigo: varchar('codigo', { length: 10 }).notNull().unique(),
    nome: varchar('nome', { length: 150 }).notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  }
);

export const subelementosDespesa = mysqlTable(
  'subelementos_despesa',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    elementoDespesaId: bigint('elemento_despesa_id', { mode: 'number' }).references(() => elementosDespesa.id),
    codigo: varchar('codigo', { length: 10 }).notNull(),
    chaveNatural: chaveNaturalHash('IFNULL(elemento_despesa_id, 0)', 'codigo'),
    nome: varchar('nome', { length: 150 }).notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_subelemento_elemento').on(table.elementoDespesaId),
    index('idx_subelemento_codigo').on(table.codigo),
    uniqueIndex('uq_subelemento_chave_natural').on(table.chaveNatural),
  ]
);

export const fontesRecursos = mysqlTable(
  'fontes_recursos',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    codigoGrupo: varchar('codigo_grupo', { length: 20 }),
    grupo: varchar('grupo', { length: 100 }),
    codigoFonte: varchar('codigo_fonte', { length: 30 }).notNull().unique(),
    nome: varchar('nome', { length: 150 }).notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  }
);

// ============================================================================
// 6. DESPESAS PÚBLICAS (SIAFI) - RELACIONAL 1:N E N:N
// ============================================================================

export const despesasEmpenhos = mysqlTable(
  'despesas_empenhos',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    codigoEmpenho: varchar('codigo_empenho', { length: 50 }).notNull().unique(),
    dataEmissao: date('data_emissao', { mode: 'string' }).notNull(),
    unidadeGestoraId: bigint('unidade_gestora_id', { mode: 'number' }).notNull().references(() => unidadesGestoras.id),
    gestaoId: bigint('gestao_id', { mode: 'number' }).references(() => gestoes.id),
    favorecidoPfId: bigint('favorecido_pf_id', { mode: 'number' }).references(() => pessoasFisicas.id),
    favorecidoPjId: bigint('favorecido_pj_id', { mode: 'number' }).references(() => pessoasJuridicas.id),
    funcaoId: bigint('funcao_id', { mode: 'number' }).references(() => funcoesOrcamentarias.id),
    subfuncaoId: bigint('subfuncao_id', { mode: 'number' }).references(() => subfuncoesOrcamentarias.id),
    acaoOrcamentariaId: bigint('acao_orcamentaria_id', { mode: 'number' }).references(() => acoesOrcamentarias.id),

    // Removido .references() destas duas colunas:
    planoOrcamentarioId: bigint('plano_orcamentario_id', { mode: 'number' }),

    categoriaDespesaId: bigint('categoria_despesa_id', { mode: 'number' }).references(() => categoriasDespesa.id),
    grupoDespesaId: bigint('grupo_despesa_id', { mode: 'number' }).references(() => gruposDespesa.id),

    // Removido .references() destas duas colunas:
    modalidadeAplicacaoId: bigint('modalidade_aplicacao_id', { mode: 'number' }),

    elementoDespesaId: bigint('elemento_despesa_id', { mode: 'number' }).references(() => elementosDespesa.id),
    fonteRecursoId: bigint('fonte_recurso_id', { mode: 'number' }).references(() => fontesRecursos.id),
    tipoEmpenho: varchar('tipo_empenho', { length: 100 }),
    especieEmpenho: varchar('especie_empenho', { length: 100 }),
    processo: varchar('processo', { length: 60 }),
    modalidadeLicitacao: varchar('modalidade_licitacao', { length: 100 }),
    observacao: text('observacao'),
    valorOriginalEmpenho: decimal('valor_original_empenho', { precision: 18, scale: 2 }).notNull(),
    valorEmpenhoConvertido: decimal('valor_empenho_convertido', { precision: 18, scale: 2 }),
    exercicioReferencia: char('exercicio_referencia', { length: 4 }).notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_desp_emp_ug').on(table.unidadeGestoraId),
    index('idx_desp_emp_data').on(table.dataEmissao),
    index('idx_desp_emp_fav_pf').on(table.favorecidoPfId),
    index('idx_desp_emp_fav_pj').on(table.favorecidoPjId),
    index('idx_desp_emp_exercicio').on(table.exercicioReferencia),

    // Chaves estrangeiras com nomes curtos
    foreignKey({
      name: 'fk_de_plano_orc',
      columns: [table.planoOrcamentarioId],
      foreignColumns: [planosOrcamentarios.id],
    }),
    foreignKey({
      name: 'fk_de_mod_aplicacao',
      columns: [table.modalidadeAplicacaoId],
      foreignColumns: [modalidadesAplicacao.id],
    }),
  ]
);

export const despesasEmpenhoItens = mysqlTable(
  'despesas_empenho_itens',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    empenhoId: bigint('empenho_id', { mode: 'number' }).notNull().references(() => despesasEmpenhos.id),
    sequencial: int('sequencial').notNull(),
    // Removido o .references() para evitar o nome longo
    subelementoDespesaId: bigint('subelemento_despesa_id', { mode: 'number' }),
    descricao: text('descricao').notNull(),
    quantidade: decimal('quantidade', { precision: 14, scale: 4 }).notNull(),
    valorUnitario: decimal('valor_unitario', { precision: 15, scale: 2 }).notNull(),
    valorTotal: decimal('valor_total', { precision: 15, scale: 2 }).notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_emp_itens_empenho').on(table.empenhoId),
    index('idx_emp_itens_subelemento').on(table.subelementoDespesaId),
    uniqueIndex('uq_emp_itens_nk').on(table.empenhoId, table.sequencial),
    // Chave estrangeira declarada com nome curto
    foreignKey({
      name: 'fk_dei_subelemento',
      columns: [table.subelementoDespesaId],
      foreignColumns: [subelementosDespesa.id],
    }),
  ]
);

export const despesasLiquidacoes = mysqlTable(
  'despesas_liquidacoes',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    codigoLiquidacao: varchar('codigo_liquidacao', { length: 50 }).notNull().unique(),
    dataEmissao: date('data_emissao', { mode: 'string' }).notNull(),
    unidadeGestoraId: bigint('unidade_gestora_id', { mode: 'number' }).notNull().references(() => unidadesGestoras.id),
    gestaoId: bigint('gestao_id', { mode: 'number' }).references(() => gestoes.id),
    favorecidoPfId: bigint('favorecido_pf_id', { mode: 'number' }).references(() => pessoasFisicas.id),
    favorecidoPjId: bigint('favorecido_pj_id', { mode: 'number' }).references(() => pessoasJuridicas.id),
    observacao: text('observacao'),
    exercicioReferencia: char('exercicio_referencia', { length: 4 }).notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_liq_ug').on(table.unidadeGestoraId),
    index('idx_liq_data').on(table.dataEmissao),
    index('idx_liq_fav_pf').on(table.favorecidoPfId),
    index('idx_liq_fav_pj').on(table.favorecidoPjId),
  ]
);

export const despesasLiquidacaoEmpenhos = mysqlTable(
  'despesas_liquidacao_empenhos',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    // Removido .references()
    liquidacaoId: bigint('liquidacao_id', { mode: 'number' }).notNull(),
    empenhoId: bigint('empenho_id', { mode: 'number' }).notNull(),
    naturezaDespesaCompleta: varchar('natureza_despesa_completa', { length: 30 }),
    subitem: varchar('subitem', { length: 30 }),
    valorLiquidado: decimal('valor_liquidado', { precision: 18, scale: 2 }).notNull(),
    valorRestosAPagarInscritos: decimal('valor_restos_a_pagar_inscritos', { precision: 18, scale: 2 }),
    valorRestosAPagarCancelado: decimal('valor_restos_a_pagar_cancelado', { precision: 18, scale: 2 }),
    valorRestosAPagarPagos: decimal('valor_restos_a_pagar_pagos', { precision: 18, scale: 2 }),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_liq_emp_liquidacao').on(table.liquidacaoId),
    index('idx_liq_emp_empenho').on(table.empenhoId),
    // Nomes curtos para as chaves estrangeiras
    foreignKey({
      name: 'fk_dle_liquidacao',
      columns: [table.liquidacaoId],
      foreignColumns: [despesasLiquidacoes.id],
    }),
    foreignKey({
      name: 'fk_dle_empenho',
      columns: [table.empenhoId],
      foreignColumns: [despesasEmpenhos.id],
    }),
  ]
);

export const despesasPagamentos = mysqlTable(
  'despesas_pagamentos',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    codigoPagamento: varchar('codigo_pagamento', { length: 50 }).notNull().unique(),
    dataEmissao: date('data_emissao', { mode: 'string' }).notNull(),
    unidadeGestoraId: bigint('unidade_gestora_id', { mode: 'number' }).notNull().references(() => unidadesGestoras.id),
    gestaoId: bigint('gestao_id', { mode: 'number' }).references(() => gestoes.id),
    favorecidoPfId: bigint('favorecido_pf_id', { mode: 'number' }).references(() => pessoasFisicas.id),
    favorecidoPjId: bigint('favorecido_pj_id', { mode: 'number' }).references(() => pessoasJuridicas.id),
    tipoOb: varchar('tipo_ob', { length: 50 }),
    extraorcamentario: boolean('extraorcamentario').default(false).notNull(),
    processo: varchar('processo', { length: 60 }),
    observacao: text('observacao'),
    valorOriginalPagamento: decimal('valor_original_pagamento', { precision: 18, scale: 2 }).notNull(),
    valorPagamentoConvertido: decimal('valor_pagamento_convertido', { precision: 18, scale: 2 }),
    exercicioReferencia: char('exercicio_referencia', { length: 4 }).notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_pag_ug').on(table.unidadeGestoraId),
    index('idx_pag_data').on(table.dataEmissao),
    index('idx_pag_fav_pf').on(table.favorecidoPfId),
    index('idx_pag_fav_pj').on(table.favorecidoPjId),
  ]
);

export const despesasPagamentoEmpenhos = mysqlTable(
  'despesas_pagamento_empenhos',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    // Removido .references()
    pagamentoId: bigint('pagamento_id', { mode: 'number' }).notNull(),
    empenhoId: bigint('empenho_id', { mode: 'number' }).notNull(),
    naturezaDespesaCompleta: varchar('natureza_despesa_completa', { length: 30 }),
    subitem: varchar('subitem', { length: 30 }),
    valorPago: decimal('valor_pago', { precision: 18, scale: 2 }).notNull(),
    valorRestosAPagarInscritos: decimal('valor_restos_a_pagar_inscritos', { precision: 18, scale: 2 }),
    valorRestosAPagarCancelado: decimal('valor_restos_a_pagar_cancelado', { precision: 18, scale: 2 }),
    valorRestosAPagarPagos: decimal('valor_restos_a_pagar_pagos', { precision: 18, scale: 2 }),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_pag_emp_pagamento').on(table.pagamentoId),
    index('idx_pag_emp_empenho').on(table.empenhoId),
    // Nomes curtos para as chaves estrangeiras
    foreignKey({
      name: 'fk_dpe_pagamento',
      columns: [table.pagamentoId],
      foreignColumns: [despesasPagamentos.id],
    }),
    foreignKey({
      name: 'fk_dpe_empenho',
      columns: [table.empenhoId],
      foreignColumns: [despesasEmpenhos.id],
    }),
  ]
);

// ============================================================================
// 7. ORÇAMENTO, RECEITAS E EXECUÇÃO GERAL (3FN)
// ============================================================================

export const orcamentoDespesa = mysqlTable(
  'orcamento_despesa',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    exercicio: char('exercicio', { length: 4 }).notNull(),

    // Removido .references() para evitar o erro de limite de caracteres
    unidadeOrcamentariaId: bigint('unidade_orcamentaria_id', { mode: 'number' }),

    acaoOrcamentariaId: bigint('acao_orcamentaria_id', { mode: 'number' }).references(() => acoesOrcamentarias.id),
    categoriaDespesaId: bigint('categoria_despesa_id', { mode: 'number' }).references(() => categoriasDespesa.id),
    grupoDespesaId: bigint('grupo_despesa_id', { mode: 'number' }).references(() => gruposDespesa.id),
    elementoDespesaId: bigint('elemento_despesa_id', { mode: 'number' }).references(() => elementosDespesa.id),
    funcaoId: bigint('funcao_id', { mode: 'number' }).references(() => funcoesOrcamentarias.id),
    subfuncaoId: bigint('subfuncao_id', { mode: 'number' }).references(() => subfuncoesOrcamentarias.id),
    chaveNatural: chaveNaturalHash(
      'exercicio',
      'IFNULL(unidade_orcamentaria_id, 0)',
      'IFNULL(acao_orcamentaria_id, 0)',
      'IFNULL(funcao_id, 0)',
      'IFNULL(subfuncao_id, 0)',
      'IFNULL(categoria_despesa_id, 0)',
      'IFNULL(grupo_despesa_id, 0)',
      'IFNULL(elemento_despesa_id, 0)',
    ),
    orcamentoInicial: decimal('orcamento_inicial', { precision: 18, scale: 2 }).notNull(),
    orcamentoAtualizado: decimal('orcamento_atualizado', { precision: 18, scale: 2 }).notNull(),
    orcamentoEmpenhado: decimal('orcamento_empenhado', { precision: 18, scale: 2 }).notNull(),
    orcamentoRealizado: decimal('orcamento_realizado', { precision: 18, scale: 2 }).notNull(),
    percentualRealizado: decimal('percentual_realizado', { precision: 7, scale: 2 }).notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_orc_desp_exercicio').on(table.exercicio),
    index('idx_orc_desp_uo').on(table.unidadeOrcamentariaId),
    index('idx_orc_desp_acao').on(table.acaoOrcamentariaId),
    uniqueIndex('uq_orc_desp_chave_natural').on(table.chaveNatural),

    // Declarando a chave estrangeira manualmente com nome curto
    foreignKey({
      name: 'fk_orc_desp_uo',
      columns: [table.unidadeOrcamentariaId],
      foreignColumns: [unidadesOrcamentarias.id],
    }),
  ]
);

export const receitas = mysqlTable(
  'receitas',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    exercicio: char('exercicio', { length: 4 }).notNull(),
    unidadeGestoraId: bigint('unidade_gestora_id', { mode: 'number' }).references(() => unidadesGestoras.id),
    categoriaEconomica: varchar('categoria_economica', { length: 100 }),
    origem: varchar('origem', { length: 150 }),
    especieReceita: varchar('especie_receita', { length: 150 }),
    detalhamento: text('detalhamento'),
    chaveNatural: chaveNaturalHash(
      'exercicio',
      'IFNULL(unidade_gestora_id, 0)',
      "IFNULL(categoria_economica, '')",
      "IFNULL(origem, '')",
      "IFNULL(especie_receita, '')",
      "IFNULL(detalhamento, '')",
      "IFNULL(data_lancamento, '')",
    ),
    valorPrevisto: decimal('valor_previsto', { precision: 18, scale: 2 }).notNull(),
    valorLancado: decimal('valor_lancado', { precision: 18, scale: 2 }).notNull(),
    valorRealizado: decimal('valor_realizado', { precision: 18, scale: 2 }).notNull(),
    percentualRealizado: decimal('percentual_realizado', { precision: 7, scale: 2 }).notNull(),
    dataLancamento: date('data_lancamento', { mode: 'string' }),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_receitas_exercicio').on(table.exercicio),
    index('idx_receitas_ug').on(table.unidadeGestoraId),
    uniqueIndex('uq_receitas_chave_natural').on(table.chaveNatural),
  ]
);

export const recursosTransferidos = mysqlTable(
  'recursos_transferidos',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    anoMes: varchar('ano_mes', { length: 7 }).notNull(),
    tipoTransferencia: varchar('tipo_transferencia', { length: 100 }).notNull(),
    municipioId: bigint('municipio_id', { mode: 'number' }).references(() => municipios.id),
    unidadeGestoraId: bigint('unidade_gestora_id', { mode: 'number' }).references(() => unidadesGestoras.id),

    // Removido .references() para evitar o erro de limite de caracteres
    acaoOrcamentariaId: bigint('acao_orcamentaria_id', { mode: 'number' }),

    favorecidoPfId: bigint('favorecido_pf_id', { mode: 'number' }).references(() => pessoasFisicas.id),
    favorecidoPjId: bigint('favorecido_pj_id', { mode: 'number' }).references(() => pessoasJuridicas.id),
    chaveNatural: chaveNaturalHash(
      'ano_mes',
      'tipo_transferencia',
      'IFNULL(municipio_id, 0)',
      'IFNULL(unidade_gestora_id, 0)',
      'IFNULL(acao_orcamentaria_id, 0)',
      'IFNULL(favorecido_pf_id, 0)',
      'IFNULL(favorecido_pj_id, 0)',
    ),
    valorTransferido: decimal('valor_transferido', { precision: 18, scale: 2 }).notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_rec_transf_ano_mes').on(table.anoMes),
    index('idx_rec_transf_municipio').on(table.municipioId),
    index('idx_rec_transf_ug').on(table.unidadeGestoraId),
    uniqueIndex('uq_rec_transf_chave_natural').on(table.chaveNatural),

    // Declarando a chave estrangeira manualmente com nome curto
    foreignKey({
      name: 'fk_rec_transf_acao',
      columns: [table.acaoOrcamentariaId],
      foreignColumns: [acoesOrcamentarias.id],
    }),
  ]
);

// ============================================================================
// 8. LICITAÇÕES, COMPRAS E CONTRATOS (3FN - RELAÇÃO 1:N)
// ============================================================================

export const licitacoes = mysqlTable(
  'licitacoes',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    numeroLicitacao: varchar('numero_licitacao', { length: 50 }).notNull(),
    unidadeGestoraId: bigint('unidade_gestora_id', { mode: 'number' }).notNull().references(() => unidadesGestoras.id),
    modalidadeCompra: varchar('modalidade_compra', { length: 100 }).notNull(),
    numeroProcesso: varchar('numero_processo', { length: 60 }),
    objeto: text('objeto').notNull(),
    situacaoLicitacao: varchar('situacao_licitacao', { length: 100 }),
    municipioId: bigint('municipio_id', { mode: 'number' }).references(() => municipios.id),
    dataResultadoCompra: date('data_resultado_compra', { mode: 'string' }),
    dataAbertura: date('data_abertura', { mode: 'string' }),
    valorLicitacao: decimal('valor_licitacao', { precision: 15, scale: 2 }),
    exercicioReferencia: char('exercicio_referencia', { length: 4 }),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_licitacao_ug').on(table.unidadeGestoraId),
    index('idx_licitacao_numero').on(table.numeroLicitacao),
    index('idx_licitacao_processo').on(table.numeroProcesso),
    uniqueIndex('uq_licitacao_nk').on(table.unidadeGestoraId, table.modalidadeCompra, table.numeroLicitacao),
  ]
);

export const licitacaoItens = mysqlTable(
  'licitacao_itens',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    licitacaoId: bigint('licitacao_id', { mode: 'number' }).notNull().references(() => licitacoes.id),
    codigoItemCompra: varchar('codigo_item_compra', { length: 30 }).notNull(),
    descricaoItemCompra: text('descricao_item_compra').notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_lic_itens_licitacao').on(table.licitacaoId),
    uniqueIndex('uq_lic_itens_nk').on(table.licitacaoId, table.codigoItemCompra),
  ]
);

export const licitacaoParticipantes = mysqlTable(
  'licitacao_participantes',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    licitacaoId: bigint('licitacao_id', { mode: 'number' }).notNull().references(() => licitacoes.id),
    licitacaoItemId: bigint('licitacao_item_id', { mode: 'number' }).references(() => licitacaoItens.id),
    participanteId: bigint('participante_id', { mode: 'number' }).notNull().references(() => pessoasJuridicas.id),
    flagVencedor: boolean('flag_vencedor').default(false).notNull(),
    chaveNatural: chaveNaturalHash('licitacao_id', 'IFNULL(licitacao_item_id, 0)', 'participante_id'),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_lic_part_licitacao').on(table.licitacaoId),
    index('idx_lic_part_empresa').on(table.participanteId),
    uniqueIndex('uq_lic_part_chave_natural').on(table.chaveNatural),
  ]
);

export const contratos = mysqlTable(
  'contratos',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    numeroContrato: varchar('numero_contrato', { length: 50 }).notNull(),
    unidadeGestoraId: bigint('unidade_gestora_id', { mode: 'number' }).notNull().references(() => unidadesGestoras.id),
    licitacaoId: bigint('licitacao_id', { mode: 'number' }).references(() => licitacoes.id),
    contratadoId: bigint('contratado_id', { mode: 'number' }).notNull().references(() => pessoasJuridicas.id),
    objeto: text('objeto').notNull(),
    fundamentoLegal: varchar('fundamento_legal', { length: 255 }),
    modalidadeCompra: varchar('modalidade_compra', { length: 100 }),
    situacaoContrato: varchar('situacao_contrato', { length: 100 }),
    dataAssinatura: date('data_assinatura', { mode: 'string' }),
    dataPublicacaoDou: date('data_publicacao_dou', { mode: 'string' }),
    dataInicioVigencia: date('data_inicio_vigencia', { mode: 'string' }),
    dataFimVigencia: date('data_fim_vigencia', { mode: 'string' }),
    valorInicialCompra: decimal('valor_inicial_compra', { precision: 15, scale: 2 }).notNull(),
    valorFinalCompra: decimal('valor_final_compra', { precision: 15, scale: 2 }).notNull(),
    exercicioReferencia: char('exercicio_referencia', { length: 4 }),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_contratos_ug').on(table.unidadeGestoraId),
    index('idx_contratos_numero').on(table.numeroContrato),
    index('idx_contratos_contratado').on(table.contratadoId),
    index('idx_contratos_licitacao').on(table.licitacaoId),
    uniqueIndex('uq_contratos_nk').on(table.unidadeGestoraId, table.numeroContrato),
  ]
);

export const contratoItens = mysqlTable(
  'contrato_itens',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    contratoId: bigint('contrato_id', { mode: 'number' }).notNull().references(() => contratos.id),
    codigoItemCompra: varchar('codigo_item_compra', { length: 30 }).notNull(),
    descricaoItemCompra: text('descricao_item_compra').notNull(),
    quantidade: decimal('quantidade', { precision: 14, scale: 4 }).notNull(),
    valorItem: decimal('valor_item', { precision: 15, scale: 2 }).notNull(),
    // Sequência determinística (1..n, ordenada pelo id do bronze) para linhas repetidas do mesmo item.
    ocorrencia: int('ocorrencia').notNull().default(1),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_contrato_itens_contrato').on(table.contratoId),
    uniqueIndex('uq_contrato_itens_nk').on(table.contratoId, table.codigoItemCompra, table.ocorrencia),
  ]
);

export const contratoTermosAditivos = mysqlTable(
  'contrato_termos_aditivos',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    contratoId: bigint('contrato_id', { mode: 'number' }).notNull().references(() => contratos.id),
    numeroTermoAditivo: varchar('numero_termo_aditivo', { length: 50 }).notNull(),
    dataPublicacao: date('data_publicacao', { mode: 'string' }).notNull(),
    objeto: text('objeto').notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_termo_aditivo_contrato').on(table.contratoId),
    uniqueIndex('uq_termo_aditivo_nk').on(table.contratoId, table.numeroTermoAditivo),
  ]
);

export const contratoApostilamentos = mysqlTable(
  'contrato_apostilamentos',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    contratoId: bigint('contrato_id', { mode: 'number' }).notNull().references(() => contratos.id),
    numeroApostilamento: varchar('numero_apostilamento', { length: 50 }).notNull(),
    descricao: text('descricao'),
    valorApostilamento: decimal('valor_apostilamento', { precision: 15, scale: 2 }),
    dataInclusao: date('data_inclusao', { mode: 'string' }),
    situacaoApostilamento: varchar('situacao_apostilamento', { length: 100 }),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_apostilamento_contrato').on(table.contratoId),
    uniqueIndex('uq_apostilamento_nk').on(table.contratoId, table.numeroApostilamento),
  ]
);

// ============================================================================
// 9. CONVÊNIOS E ORDENS BANCÁRIAS (3FN)
// ============================================================================

export const convenios = mysqlTable(
  'convenios',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    numeroConvenio: varchar('numero_convenio', { length: 30 }).notNull().unique(),
    municipioId: bigint('municipio_id', { mode: 'number' }).references(() => municipios.id),
    situacaoConvenio: varchar('situacao_convenio', { length: 100 }),
    numeroOriginal: varchar('numero_original', { length: 50 }),
    numeroProcesso: varchar('numero_processo', { length: 60 }),
    objeto: text('objeto').notNull(),
    unidadeGestoraConcedenteId: bigint('ug_concedente_id', { mode: 'number' }).references(() => unidadesGestoras.id),
    convenenteId: bigint('convenente_id', { mode: 'number' }).notNull().references(() => pessoasJuridicas.id),
    tipoConvenente: varchar('tipo_convenente', { length: 100 }),
    tipoInstrumento: varchar('tipo_instrumento', { length: 100 }),
    valorConvenio: decimal('valor_convenio', { precision: 15, scale: 2 }).notNull(),
    valorLiberado: decimal('valor_liberado', { precision: 15, scale: 2 }),
    valorContrapartida: decimal('valor_contrapartida', { precision: 15, scale: 2 }),
    dataPublicacao: date('data_publicacao', { mode: 'string' }),
    dataInicioVigencia: date('data_inicio_vigencia', { mode: 'string' }),
    dataFimVigencia: date('data_fim_vigencia', { mode: 'string' }),
    dataUltimaLiberacao: date('data_ultima_liberacao', { mode: 'string' }),
    valorUltimaLiberacao: decimal('valor_ultima_liberacao', { precision: 15, scale: 2 }),
    exercicioReferencia: char('exercicio_referencia', { length: 4 }),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_convenios_ug').on(table.unidadeGestoraConcedenteId),
    index('idx_convenios_convenente').on(table.convenenteId),
    index('idx_convenios_municipio').on(table.municipioId),
  ]
);

export const convenioOrdensBancarias = mysqlTable(
  'convenio_ordens_bancarias',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    convenioId: bigint('convenio_id', { mode: 'number' }).notNull().references(() => convenios.id),
    numeroOrdemBancaria: varchar('numero_ordem_bancaria', { length: 50 }).notNull(),
    dataEmissaoOb: date('data_emissao_ob', { mode: 'string' }).notNull(),
    valorLiberado: decimal('valor_liberado', { precision: 15, scale: 2 }).notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_convenio_ob_convenio').on(table.convenioId),
    index('idx_convenio_ob_data').on(table.dataEmissaoOb),
    uniqueIndex('uq_convenio_ob_nk').on(table.convenioId, table.numeroOrdemBancaria),
  ]
);

// ============================================================================
// 10. CARTÕES DE PAGAMENTO DO GOVERNO (CPGF, CPCC, DEFESA CIVIL)
// ============================================================================

export const cartaoPagamentoCpgf = mysqlTable(
  'cartao_pagamento_cpgf',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    unidadeGestoraId: bigint('unidade_gestora_id', { mode: 'number' }).notNull().references(() => unidadesGestoras.id),
    portadorId: bigint('portador_id', { mode: 'number' }).notNull().references(() => pessoasFisicas.id),
    favorecidoPfId: bigint('favorecido_pf_id', { mode: 'number' }).references(() => pessoasFisicas.id),
    favorecidoPjId: bigint('favorecido_pj_id', { mode: 'number' }).references(() => pessoasJuridicas.id),
    anoExtrato: char('ano_extrato', { length: 4 }).notNull(),
    mesExtrato: char('mes_extrato', { length: 2 }).notNull(),
    transacao: varchar('transacao', { length: 100 }).notNull(),
    dataTransacao: date('data_transacao', { mode: 'string' }).notNull(),
    valorTransacao: decimal('valor_transacao', { precision: 15, scale: 2 }).notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_cpgf_ug').on(table.unidadeGestoraId),
    index('idx_cpgf_portador').on(table.portadorId),
    index('idx_cpgf_data').on(table.dataTransacao),
  ]
);

export const cartaoPagamentoCpcc = mysqlTable(
  'cartao_pagamento_cpcc',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    unidadeGestoraId: bigint('unidade_gestora_id', { mode: 'number' }).notNull().references(() => unidadesGestoras.id),
    portadorId: bigint('portador_id', { mode: 'number' }).references(() => pessoasFisicas.id),
    favorecidoPjId: bigint('favorecido_pj_id', { mode: 'number' }).references(() => pessoasJuridicas.id),
    anoExtrato: char('ano_extrato', { length: 4 }).notNull(),
    mesExtrato: char('mes_extrato', { length: 2 }).notNull(),
    transacao: varchar('transacao', { length: 100 }).notNull(),
    dataTransacao: date('data_transacao', { mode: 'string' }).notNull(),
    valorTransacao: decimal('valor_transacao', { precision: 15, scale: 2 }).notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_cpcc_ug').on(table.unidadeGestoraId),
    index('idx_cpcc_portador').on(table.portadorId),
  ]
);

export const cartaoPagamentoDefesaCivil = mysqlTable(
  'cartao_pagamento_defesa_civil',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    ufSigla: char('uf_sigla', { length: 2 }).references(() => estados.sigla),
    convenioId: bigint('convenio_id', { mode: 'number' }).references(() => convenios.id),

    // Remova o .references() destas duas colunas:
    convenenteId: bigint('convenente_id', { mode: 'number' }),
    favorecidoPjId: bigint('favorecido_pj_id', { mode: 'number' }),

    portadorId: bigint('portador_id', { mode: 'number' }).notNull().references(() => pessoasFisicas.id),
    anoExtrato: char('ano_extrato', { length: 4 }).notNull(),
    mesExtrato: char('mes_extrato', { length: 2 }).notNull(),
    transacao: varchar('transacao', { length: 100 }).notNull(),
    dataTransacao: date('data_transacao', { mode: 'string' }).notNull(),
    valorTransacao: decimal('valor_transacao', { precision: 15, scale: 2 }).notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_cpdc_uf').on(table.ufSigla),
    index('idx_cpdc_convenio').on(table.convenioId),
    index('idx_cpdc_portador').on(table.portadorId),

    // Defina as chaves estrangeiras com nomes curtos aqui:
    foreignKey({
      name: 'fk_cpdc_convenente',
      columns: [table.convenenteId],
      foreignColumns: [pessoasJuridicas.id],
    }),
    foreignKey({
      name: 'fk_cpdc_fav_pj',
      columns: [table.favorecidoPjId],
      foreignColumns: [pessoasJuridicas.id],
    }),
  ]
);

// ============================================================================
// 11. NOTAS FISCAIS ELETRÔNICAS (NF-E) (1:N)
// ============================================================================

export const notasFiscais = mysqlTable(
  'notas_fiscais',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    chaveAcesso: char('chave_acesso', { length: 44 }).notNull().unique(),
    modelo: varchar('modelo', { length: 100 }),
    serie: varchar('serie', { length: 10 }),
    numero: varchar('numero', { length: 30 }).notNull(),
    naturezaOperacao: varchar('natureza_operacao', { length: 255 }),
    dataEmissao: date('data_emissao', { mode: 'string' }).notNull(),
    emitenteId: bigint('emitente_id', { mode: 'number' }).notNull().references(() => pessoasJuridicas.id),
    destinatarioPjId: bigint('destinatario_pj_id', { mode: 'number' }).references(() => pessoasJuridicas.id),
    destinatarioPfId: bigint('destinatario_pf_id', { mode: 'number' }).references(() => pessoasFisicas.id),
    consumidorFinal: boolean('consumidor_final').default(false).notNull(),
    presencaComprador: varchar('presenca_comprador', { length: 50 }),
    valorNotaFiscal: decimal('valor_nota_fiscal', { precision: 15, scale: 2 }).notNull(),
    exercicioReferencia: char('exercicio_referencia', { length: 4 }),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_nfe_emitente').on(table.emitenteId),
    index('idx_nfe_destinatario_pj').on(table.destinatarioPjId),
    index('idx_nfe_destinatario_pf').on(table.destinatarioPfId),
    index('idx_nfe_data_emissao').on(table.dataEmissao),
  ]
);

export const notaFiscalEventos = mysqlTable(
  'nota_fiscal_eventos',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    notaFiscalId: bigint('nota_fiscal_id', { mode: 'number' }).notNull().references(() => notasFiscais.id),
    evento: varchar('evento', { length: 100 }).notNull(),
    dataHoraEvento: datetime('data_hora_evento', { fsp: 3, mode: 'string' }).notNull(),
    descricaoEvento: text('descricao_evento'),
    motivoEvento: text('motivo_evento'),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_nfe_eventos_nf').on(table.notaFiscalId),
    index('idx_nfe_eventos_data_hora').on(table.dataHoraEvento),
  ]
);

export const notaFiscalItens = mysqlTable(
  'nota_fiscal_itens',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    notaFiscalId: bigint('nota_fiscal_id', { mode: 'number' }).notNull().references(() => notasFiscais.id),
    numeroProduto: varchar('numero_produto', { length: 50 }).notNull(),
    descricaoProdutoServico: text('descricao_produto_servico').notNull(),
    codigoNcmSh: varchar('codigo_ncm_sh', { length: 20 }),
    cfop: varchar('cfop', { length: 10 }),
    quantidade: decimal('quantidade', { precision: 14, scale: 4 }).notNull(),
    unidade: varchar('unidade', { length: 20 }).notNull(),
    valorUnitario: decimal('valor_unitario', { precision: 15, scale: 2 }).notNull(),
    valorTotal: decimal('valor_total', { precision: 15, scale: 2 }).notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_nfe_itens_nf').on(table.notaFiscalId),
    index('idx_nfe_itens_ncm').on(table.codigoNcmSh),
    index('idx_nfe_itens_cfop').on(table.cfop),
  ]
);

// ============================================================================
// 12. VIAGENS A SERVIÇO (PCDP) (3FN)
// ============================================================================

export const viagens = mysqlTable(
  'viagens',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    identificadorProcessoViagem: varchar('identificador_processo_viagem', { length: 25 }).notNull().unique(),
    situacao: varchar('situacao', { length: 100 }).notNull(),
    unidadeGestoraPagadoraId: bigint('ug_pagadora_id', { mode: 'number' }).references(() => unidadesGestoras.id),
    viajanteId: bigint('viajante_id', { mode: 'number' }).notNull().references(() => pessoasFisicas.id),
    cargo: varchar('cargo', { length: 255 }),
    periodoDataInicio: date('periodo_data_inicio', { mode: 'string' }).notNull(),
    periodoDataFim: date('periodo_data_fim', { mode: 'string' }).notNull(),
    destinos: text('destinos'),
    motivo: text('motivo').notNull(),
    valorDiarias: decimal('valor_diarias', { precision: 15, scale: 2 }).notNull(),
    valorPassagens: decimal('valor_passagens', { precision: 15, scale: 2 }).notNull(),
    valorOutrosGastos: decimal('valor_outros_gastos', { precision: 15, scale: 2 }).notNull(),
    exercicioReferencia: char('exercicio_referencia', { length: 4 }),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_viagens_viajante').on(table.viajanteId),
    index('idx_viagens_ug_pagadora').on(table.unidadeGestoraPagadoraId),
    index('idx_viagens_periodo_inicio').on(table.periodoDataInicio),
  ]
);

export const viagemTrechos = mysqlTable(
  'viagem_trechos',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    viagemId: bigint('viagem_id', { mode: 'number' }).notNull().references(() => viagens.id),
    sequenciaTrecho: int('sequencia_trecho').notNull(),
    origemData: date('origem_data', { mode: 'string' }).notNull(),
    origemPais: varchar('origem_pais', { length: 100 }),
    origemUf: varchar('origem_uf', { length: 50 }),
    origemCidade: varchar('origem_cidade', { length: 150 }),
    destinoData: date('destino_data', { mode: 'string' }).notNull(),
    destinoPais: varchar('destino_pais', { length: 100 }),
    destinoUf: varchar('destino_uf', { length: 50 }),
    destinoCidade: varchar('destino_cidade', { length: 150 }),
    meioTransporte: varchar('meio_transporte', { length: 50 }).notNull(),
    numeroDiarias: int('numero_diarias').notNull(),
    missao: boolean('missao').default(false).notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_viagem_trechos_viagem').on(table.viagemId),
    index('idx_viagem_trechos_origem_data').on(table.origemData),
    uniqueIndex('uq_viagem_trechos_nk').on(table.viagemId, table.sequenciaTrecho),
  ]
);

export const viagemPassagens = mysqlTable(
  'viagem_passagens',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    viagemId: bigint('viagem_id', { mode: 'number' }).notNull().references(() => viagens.id),
    meioTransporte: varchar('meio_transporte', { length: 50 }).notNull(),
    origemUf: varchar('origem_uf', { length: 50 }),
    origemCidade: varchar('origem_cidade', { length: 150 }),
    destinoUf: varchar('destino_uf', { length: 50 }),
    destinoCidade: varchar('destino_cidade', { length: 150 }),
    valorPassagem: decimal('valor_passagem', { precision: 15, scale: 2 }).notNull(),
    taxaServico: decimal('taxa_servico', { precision: 15, scale: 2 }),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_viagem_passagens_viagem').on(table.viagemId),
  ]
);

// ============================================================================
// 13. SERVIDORES PÚBLICOS E REMUNERAÇÃO (3FN)
// ============================================================================

export const servidores = mysqlTable(
  'servidores',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    idServidorPortal: varchar('id_servidor_portal', { length: 30 }).notNull().unique(),
    pessoaFisicaId: bigint('pessoa_fisica_id', { mode: 'number' }).notNull().references(() => pessoasFisicas.id),
    matricula: varchar('matricula', { length: 30 }),
    descricaoCargo: varchar('descricao_cargo', { length: 255 }),
    classeCargo: varchar('classe_cargo', { length: 20 }),
    padraoCargo: varchar('padrao_cargo', { length: 20 }),
    nivelCargo: varchar('nivel_cargo', { length: 20 }),
    funcao: varchar('funcao', { length: 255 }),
    orgaoLotacaoId: bigint('orgao_lotacao_id', { mode: 'number' }).references(() => orgaosSubordinados.id),
    orgaoExercicioId: bigint('orgao_exercicio_id', { mode: 'number' }).references(() => orgaosSubordinados.id),
    ufExercicioSigla: char('uf_exercicio_sigla', { length: 2 }).references(() => estados.sigla),
    tipoVinculo: varchar('tipo_vinculo', { length: 100 }),
    situacaoVinculo: varchar('situacao_vinculo', { length: 100 }),
    regimeJuridico: varchar('regime_juridico', { length: 100 }),
    jornadaDeTrabalho: varchar('jornada_de_trabalho', { length: 50 }),
    dataIngressoOrgao: date('data_ingresso_orgao', { mode: 'string' }),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_servidores_pf').on(table.pessoaFisicaId),
    index('idx_servidores_orgao_lot').on(table.orgaoLotacaoId),
    index('idx_servidores_orgao_ex').on(table.orgaoExercicioId),
    index('idx_servidores_uf').on(table.ufExercicioSigla),
  ]
);

export const servidoresRemuneracoes = mysqlTable(
  'servidores_remuneracoes',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    servidorId: bigint('servidor_id', { mode: 'number' }).notNull().references(() => servidores.id),
    ano: char('ano', { length: 4 }).notNull(),
    mes: char('mes', { length: 2 }).notNull(),
    remuneracaoBasicaBrutaRs: decimal('remuneracao_basica_bruta_rs', { precision: 15, scale: 2 }).notNull(),
    abateTetoRs: decimal('abate_teto_rs', { precision: 15, scale: 2 }),
    gratificacaoNatalinaRs: decimal('gratificacao_natalina_rs', { precision: 15, scale: 2 }),
    feriasRs: decimal('ferias_rs', { precision: 15, scale: 2 }),
    outrasRemuneracoesEventuaisRs: decimal('outras_remuneracoes_eventuais_rs', { precision: 15, scale: 2 }),
    irrfRs: decimal('irrf_rs', { precision: 15, scale: 2 }),
    pssRpgsRs: decimal('pss_rpgs_rs', { precision: 15, scale: 2 }),
    demaisDeducoesRs: decimal('demais_deducoes_rs', { precision: 15, scale: 2 }),
    remuneracaoAposDeducoesRs: decimal('remuneracao_apos_deducoes_rs', { precision: 15, scale: 2 }).notNull(),
    totalVerbasIndenizatoriasRs: decimal('total_verbas_indenizatorias_rs', { precision: 15, scale: 2 }),
    totalHonorariosJetons: decimal('total_honorarios_jetons', { precision: 15, scale: 2 }),
    abateTetoGratificacaoNatalinaRs: decimal('abate_teto_gratificacao_natalina_rs', { precision: 15, scale: 2 }),
    pensaoMilitarRs: decimal('pensao_militar_rs', { precision: 15, scale: 2 }),
    fundoSaudeRs: decimal('fundo_saude_rs', { precision: 15, scale: 2 }),
    taxaOcupacaoImovelFuncionalRs: decimal('taxa_ocupacao_imovel_funcional_rs', { precision: 15, scale: 2 }),
    verbasIndenizatoriasCivilRs: decimal('verbas_indenizatorias_civil_rs', { precision: 15, scale: 2 }),
    verbasIndenizatoriasMilitarRs: decimal('verbas_indenizatorias_militar_rs', { precision: 15, scale: 2 }),
    verbasIndenizatoriasPdvRs: decimal('verbas_indenizatorias_pdv_rs', { precision: 15, scale: 2 }),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_serv_remun_servidor').on(table.servidorId),
    index('idx_serv_remun_competencia').on(table.ano, table.mes),
    uniqueIndex('uq_serv_remun_nk').on(table.servidorId, table.ano, table.mes),
  ]
);

export const servidoresAfastamentos = mysqlTable(
  'servidores_afastamentos',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    servidorId: bigint('servidor_id', { mode: 'number' }).notNull().references(() => servidores.id),
    dataInicioAfastamento: date('data_inicio_afastamento', { mode: 'string' }).notNull(),
    dataFimAfastamento: date('data_fim_afastamento', { mode: 'string' }),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_serv_afast_servidor').on(table.servidorId),
    index('idx_serv_afast_inicio').on(table.dataInicioAfastamento),
    uniqueIndex('uq_serv_afast_nk').on(table.servidorId, table.dataInicioAfastamento),
  ]
);

export const servidoresJetons = mysqlTable(
  'servidores_jetons',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    servidorId: bigint('servidor_id', { mode: 'number' }).notNull().references(() => servidores.id),
    ano: char('ano', { length: 4 }).notNull(),
    mes: char('mes', { length: 2 }).notNull(),
    empresa: varchar('empresa', { length: 255 }).notNull(),
    valor: decimal('valor', { precision: 15, scale: 2 }).notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_jetons_servidor').on(table.servidorId),
    uniqueIndex('uq_jetons_nk').on(table.servidorId, table.ano, table.mes, table.empresa),
  ]
);

export const servidoresHonorarios = mysqlTable(
  'servidores_honorarios',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    servidorId: bigint('servidor_id', { mode: 'number' }).notNull().references(() => servidores.id),
    ano: char('ano', { length: 4 }).notNull(),
    mes: char('mes', { length: 2 }).notNull(),
    observacoes: text('observacoes'),
    valor: decimal('valor', { precision: 15, scale: 2 }).notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_honorarios_servidor').on(table.servidorId),
  ]
);

export const servidoresObservacoes = mysqlTable(
  'servidores_observacoes',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    servidorId: bigint('servidor_id', { mode: 'number' }).notNull(),
    ano: char('ano', { length: 4 }).notNull(),
    mes: char('mes', { length: 2 }).notNull(),
    observacao: text('observacao').notNull(),
    // Identidade = servidor + competência + texto (todas as partes NOT NULL). Gerada pelo banco.
    chaveNatural: chaveNaturalHash('servidor_id', 'ano', 'mes', 'observacao'),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_serv_obs_servidor').on(table.servidorId),
    index('idx_serv_obs_competencia').on(table.ano, table.mes),
    uniqueIndex('uq_serv_obs_chave_natural').on(table.chaveNatural),
    foreignKey({
      name: 'fk_so_servidor',
      columns: [table.servidorId],
      foreignColumns: [servidores.id],
    }),
  ]
);

export const servidoresObservacoesRelations = relations(servidoresObservacoes, ({ one }) => ({
  servidor: one(servidores, {
    fields: [servidoresObservacoes.servidorId],
    references: [servidores.id],
  }),
}));

// ============================================================================
// 14. BENEFÍCIOS AO CIDADÃO (CONSOLIDAÇÃO NORMALIZADA EM 3FN)
// ============================================================================

export const programasSociais = mysqlTable(
  'programas_sociais',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    codigo: varchar('codigo', { length: 50 }).notNull().unique(),
    nome: varchar('nome', { length: 150 }).notNull(),
    descricao: text('descricao'),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  }
);

export const beneficioPagamentos = mysqlTable(
  'beneficio_pagamentos',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    programaId: bigint('programa_id', { mode: 'number' }).notNull().references(() => programasSociais.id),
    beneficiarioId: bigint('beneficiario_id', { mode: 'number' }).notNull().references(() => pessoasFisicas.id),
    responsavelId: bigint('responsavel_id', { mode: 'number' }).references(() => pessoasFisicas.id),
    municipioId: bigint('municipio_id', { mode: 'number' }).notNull().references(() => municipios.id),
    mesCompetencia: varchar('mes_competencia', { length: 7 }).notNull(),
    mesReferencia: varchar('mes_referencia', { length: 7 }),
    parcela: varchar('parcela', { length: 50 }),
    valor: decimal('valor', { precision: 15, scale: 2 }).notNull(),
    dataDisponibilizacao: date('data_disponibilizacao', { mode: 'string' }),
    dataSaque: date('data_saque', { mode: 'string' }),
    concedidoJudicialmente: boolean('concedido_judicialmente'),
    numeroBeneficio: varchar('numero_beneficio', { length: 30 }),
    // Sequência determinística (1..n, ordenada pelo id do bronze) para linhas idênticas na mesma chave.
    ocorrencia: int('ocorrencia').notNull().default(1),
    chaveNatural: chaveNaturalHash(
      'programa_id',
      'beneficiario_id',
      'mes_competencia',
      "IFNULL(mes_referencia, '')",
      "IFNULL(parcela, '')",
      "IFNULL(numero_beneficio, '')",
      'valor',
      'ocorrencia',
    ),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_ben_pag_programa').on(table.programaId),
    index('idx_ben_pag_beneficiario').on(table.beneficiarioId),
    index('idx_ben_pag_municipio').on(table.municipioId),
    index('idx_ben_pag_competencia').on(table.mesCompetencia),
    index('idx_ben_pag_data_disp').on(table.dataDisponibilizacao),
    uniqueIndex('uq_ben_pag_chave_natural').on(table.chaveNatural),
  ]
);

// Detalhe 1:1 específico do Gás do Povo (estabelecimento e vale), sem poluir beneficio_pagamentos.
export const beneficioGasDoPovoDetalhes = mysqlTable(
  'beneficio_gas_do_povo_detalhes',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    beneficioPagamentoId: bigint('beneficio_pagamento_id', { mode: 'number' }).notNull().unique(),
    estabelecimentoId: bigint('estabelecimento_id', { mode: 'number' }),
    quantidadePessoasFamilia: int('quantidade_pessoas_familia'),
    validadeVale: varchar('validade_vale', { length: 30 }),
    dataInicioVigenciaVale: date('data_inicio_vigencia_vale', { mode: 'string' }),
    dataFimVigenciaVale: date('data_fim_vigencia_vale', { mode: 'string' }),
    dataRetiradaVale: date('data_retirada_vale', { mode: 'string' }),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_bgpd_estabelecimento').on(table.estabelecimentoId),
    foreignKey({
      name: 'fk_bgpd_beneficio',
      columns: [table.beneficioPagamentoId],
      foreignColumns: [beneficioPagamentos.id],
    }),
    foreignKey({
      name: 'fk_bgpd_estab',
      columns: [table.estabelecimentoId],
      foreignColumns: [pessoasJuridicas.id],
    }),
  ]
);

// ============================================================================
// 15. SANÇÕES E INTEGRIDADE PÚBLICA (3FN)
// ============================================================================

export const sancoesAdministrativas = mysqlTable(
  'sancoes_administrativas',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    tipoCadastro: mysqlEnum('tipo_cadastro', ['CEIS', 'CNEP', 'CEAF', 'CEPIM']).notNull(),
    // Obrigatório: CEIS/CNEP/CEAF trazem o código; para CEPIM o transformer gera 'CEPIM:{cnpj}:{numeroConvenio}'.
    codigoSancao: varchar('codigo_sancao', { length: 80 }).notNull(),
    sancionadoPjId: bigint('sancionado_pj_id', { mode: 'number' }).references(() => pessoasJuridicas.id),
    sancionadoPfId: bigint('sancionado_pf_id', { mode: 'number' }).references(() => pessoasFisicas.id),
    // CEPIM referencia um convênio e não possui número de processo.
    convenioId: bigint('convenio_id', { mode: 'number' }).references(() => convenios.id),
    numeroProcesso: varchar('numero_processo', { length: 60 }),
    categoriaSancao: varchar('categoria_sancao', { length: 150 }),
    valorMulta: decimal('valor_multa', { precision: 15, scale: 2 }),
    dataInicioSancao: date('data_inicio_sancao', { mode: 'string' }),
    dataFinalSancao: date('data_final_sancao', { mode: 'string' }),
    dataPublicacao: date('data_publicacao', { mode: 'string' }),
    dataTransitoJulgado: date('data_transito_julgado', { mode: 'string' }),
    orgaoSancionador: varchar('orgao_sancionador', { length: 255 }),
    ufOrgaoSancionador: char('uf_orgao_sancionador', { length: 2 }).references(() => estados.sigla),
    esferaOrgaoSancionador: mysqlEnum('esfera_orgao_sancionador', ['Federal', 'Estadual', 'Municipal']),
    fundamentacaoLegal: text('fundamentacao_legal'),
    observacoes: text('observacoes'),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_sancoes_tipo').on(table.tipoCadastro),
    index('idx_sancoes_pj').on(table.sancionadoPjId),
    index('idx_sancoes_pf').on(table.sancionadoPfId),
    index('idx_sancoes_processo').on(table.numeroProcesso),
    index('idx_sancoes_data_inicio').on(table.dataInicioSancao),
    index('idx_sancoes_convenio').on(table.convenioId),
    uniqueIndex('uq_sancoes_nk').on(table.tipoCadastro, table.codigoSancao),
  ]
);

export const acordosLeniencia = mysqlTable(
  'acordos_leniencia',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    idAcordo: varchar('id_acordo', { length: 50 }).notNull().unique(),
    empresaId: bigint('empresa_id', { mode: 'number' }).notNull().references(() => pessoasJuridicas.id),
    numeroProcesso: varchar('numero_processo', { length: 60 }).notNull(),
    dataInicioAcordo: date('data_inicio_acordo', { mode: 'string' }).notNull(),
    dataConclusaoAcordo: date('data_conclusao_acordo', { mode: 'string' }),
    orgaoSancionador: varchar('orgao_sancionador', { length: 255 }),
    situacaoAcordo: varchar('situacao_acordo', { length: 100 }),
    termosAcordo: text('termos_acordo'),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_acordo_empresa').on(table.empresaId),
    index('idx_acordo_processo').on(table.numeroProcesso),
  ]
);

export const acordoLenienciaEfeitos = mysqlTable(
  'acordo_leniencia_efeitos',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    acordoId: bigint('acordo_id', { mode: 'number' }).notNull().references(() => acordosLeniencia.id),
    efeitoAcordo: varchar('efeito_acordo', { length: 255 }).notNull(),
    complemento: text('complemento'),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_acordo_efeitos_acordo').on(table.acordoId),
  ]
);

export const pep = mysqlTable(
  'pep',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    pessoaFisicaId: bigint('pessoa_fisica_id', { mode: 'number' }).notNull().references(() => pessoasFisicas.id),
    siglaFuncao: varchar('sigla_funcao', { length: 30 }),
    descricaoFuncao: varchar('descricao_funcao', { length: 255 }).notNull(),
    nivelFuncao: varchar('nivel_funcao', { length: 30 }),
    nomeOrgao: varchar('nome_orgao', { length: 255 }).notNull(),
    dataInicioExercicio: date('data_inicio_exercicio', { mode: 'string' }).notNull(),
    dataFimExercicio: date('data_fim_exercicio', { mode: 'string' }),
    dataFimCarencia: date('data_fim_carencia', { mode: 'string' }),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_pep_pf').on(table.pessoaFisicaId),
    index('idx_pep_data_inicio').on(table.dataInicioExercicio),
  ]
);

// ============================================================================
// 16. EMENDAS PARLAMENTARES (3FN)
// ============================================================================

export const emendasParlamentares = mysqlTable(
  'emendas_parlamentares',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    codigoEmenda: varchar('codigo_emenda', { length: 30 }).notNull().unique(),
    anoEmenda: char('ano_emenda', { length: 4 }).notNull(),
    tipoEmenda: varchar('tipo_emenda', { length: 100 }).notNull(),
    autorEmenda: varchar('autor_emenda', { length: 255 }).notNull(),
    numeroEmenda: varchar('numero_emenda', { length: 30 }),
    municipioId: bigint('municipio_id', { mode: 'number' }).references(() => municipios.id),
    funcaoId: bigint('funcao_id', { mode: 'number' }).references(() => funcoesOrcamentarias.id),

    // Removido .references()
    subfuncaoId: bigint('subfuncao_id', { mode: 'number' }),

    programaId: bigint('programa_id', { mode: 'number' }).references(() => programasOrcamentarios.id),
    acaoId: bigint('acao_id', { mode: 'number' }).references(() => acoesOrcamentarias.id),

    // Removido .references()
    planoOrcamentarioId: bigint('plano_orcamentario_id', { mode: 'number' }),

    valorEmpenhado: decimal('valor_empenhado', { precision: 18, scale: 2 }).notNull(),
    valorLiquidado: decimal('valor_liquidado', { precision: 18, scale: 2 }).notNull(),
    valorPago: decimal('valor_pago', { precision: 18, scale: 2 }).notNull(),
    valorRestosAPagarInscritos: decimal('valor_restos_a_pagar_inscritos', { precision: 18, scale: 2 }),
    valorRestosAPagarCancelados: decimal('valor_restos_a_pagar_cancelados', { precision: 18, scale: 2 }),
    valorRestosAPagarPagos: decimal('valor_restos_a_pagar_pagos', { precision: 18, scale: 2 }),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_emendas_ano').on(table.anoEmenda),
    index('idx_emendas_autor').on(table.autorEmenda),
    index('idx_emendas_municipio').on(table.municipioId),

    // Chaves estrangeiras com nomes curtos
    foreignKey({
      name: 'fk_ep_subfuncao',
      columns: [table.subfuncaoId],
      foreignColumns: [subfuncoesOrcamentarias.id],
    }),
    foreignKey({
      name: 'fk_ep_plano_orc',
      columns: [table.planoOrcamentarioId],
      foreignColumns: [planosOrcamentarios.id],
    }),
  ]
);

export const emendasApoiamentos = mysqlTable(
  'emendas_apoiamentos',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    emendaId: bigint('emenda_id', { mode: 'number' }).notNull().references(() => emendasParlamentares.id),
    apoiadorPfId: bigint('apoiador_pf_id', { mode: 'number' }).references(() => pessoasFisicas.id),
    apoiadorNome: varchar('apoiador_nome', { length: 255 }).notNull(),
    dataApoio: date('data_apoio', { mode: 'string' }).notNull(),
    dataRetiradaApoio: date('data_retirada_apoio', { mode: 'string' }),
    valorEmpenhado: decimal('valor_empenhado', { precision: 18, scale: 2 }),
    valorCancelado: decimal('valor_cancelado', { precision: 18, scale: 2 }),
    valorPago: decimal('valor_pago', { precision: 18, scale: 2 }),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_apoio_emenda').on(table.emendaId),
    index('idx_apoio_pf').on(table.apoiadorPfId),
  ]
);

// ============================================================================
// 17. IMÓVEIS FUNCIONAIS (3FN)
// ============================================================================

export const imoveisFuncionais = mysqlTable(
  'imoveis_funcionais',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    endereco: varchar('endereco', { length: 255 }).notNull(),

    // Removido o .references() para evitar o nome longo automático
    orgaoResponsavelId: bigint('orgao_responsavel_id', { mode: 'number' }),

    situacao: varchar('situacao', { length: 100 }),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_imoveis_orgao').on(table.orgaoResponsavelId),

    // Declarando a chave estrangeira manualmente com nome curto
    foreignKey({
      name: 'fk_imoveis_orgao_resp',
      columns: [table.orgaoResponsavelId],
      foreignColumns: [orgaosSubordinados.id],
    }),
  ]
);

export const imoveisFuncionaisPermissionarios = mysqlTable(
  'imoveis_funcionais_permissionarios',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    imovelId: bigint('imovel_id', { mode: 'number' }),
    permissionarioId: bigint('permissionario_id', { mode: 'number' }).notNull(),
    cargoFuncao: varchar('cargo_funcao', { length: 255 }),
    orgaoExercicioId: bigint('orgao_exercicio_id', { mode: 'number' }),
    dataOcupacao: date('data_ocupacao', { mode: 'string' }),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_perm_imovel').on(table.imovelId),
    index('idx_perm_permissionario').on(table.permissionarioId),
    // Nomes curtos para as chaves estrangeiras
    foreignKey({
      name: 'fk_ifp_imovel',
      columns: [table.imovelId],
      foreignColumns: [imoveisFuncionais.id],
    }),
    foreignKey({
      name: 'fk_ifp_permissionario',
      columns: [table.permissionarioId],
      foreignColumns: [pessoasFisicas.id],
    }),
    foreignKey({
      name: 'fk_ifp_orgao',
      columns: [table.orgaoExercicioId],
      foreignColumns: [orgaosSubordinados.id],
    }),
  ]
);

// ============================================================================
// 18. RENÚNCIAS FISCAIS E DESONERAÇÕES (3FN)
// ============================================================================

export const renunciasFiscais = mysqlTable(
  'renuncias_fiscais',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    anoCalendario: char('ano_calendario', { length: 4 }).notNull(),
    beneficiarioId: bigint('beneficiario_id', { mode: 'number' }).notNull().references(() => pessoasJuridicas.id),
    tipoRenuncia: varchar('tipo_renuncia', { length: 100 }),
    beneficioFiscal: varchar('beneficio_fiscal', { length: 255 }).notNull(),
    fundamentoLegal: varchar('fundamento_legal', { length: 255 }),
    descricao: text('descricao'),
    tributo: varchar('tributo', { length: 100 }),
    formaTributacao: varchar('forma_tributacao', { length: 100 }),
    valorRenunciaFiscal: decimal('valor_renuncia_fiscal', { precision: 15, scale: 2 }).notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_renuncias_ano').on(table.anoCalendario),
    index('idx_renuncias_beneficiario').on(table.beneficiarioId),
  ]
);

export const empresasHabilitadas = mysqlTable(
  'empresas_habilitadas',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    empresaId: bigint('empresa_id', { mode: 'number' }).notNull().references(() => pessoasJuridicas.id),
    beneficioFiscal: varchar('beneficio_fiscal', { length: 255 }).notNull(),
    baseLegal: varchar('base_legal', { length: 255 }),
    descricao: text('descricao'),
    inicioHabilitacao: date('inicio_habilitacao', { mode: 'string' }),
    fimHabilitacao: date('fim_habilitacao', { mode: 'string' }),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_emp_hab_empresa').on(table.empresaId),
  ]
);

// ============================================================================
// 19. RELAÇÕES DRIZZLE ORM (RELATIONAL API)
// ============================================================================

export const estadosRelations = relations(estados, ({ many }) => ({
  municipios: many(municipios),
}));

export const municipiosRelations = relations(municipios, ({ one, many }) => ({
  estado: one(estados, {
    fields: [municipios.ufSigla],
    references: [estados.sigla],
  }),
  pessoasFisicas: many(pessoasFisicas),
  pessoasJuridicas: many(pessoasJuridicas),
}));

export const orgaosSuperioresRelations = relations(orgaosSuperiores, ({ many }) => ({
  subordinados: many(orgaosSubordinados),
}));

export const orgaosSubordinadosRelations = relations(orgaosSubordinados, ({ one, many }) => ({
  orgaoSuperior: one(orgaosSuperiores, {
    fields: [orgaosSubordinados.orgaoSuperiorId],
    references: [orgaosSuperiores.id],
  }),
  unidadesGestoras: many(unidadesGestoras),
}));

export const unidadesGestorasRelations = relations(unidadesGestoras, ({ one, many }) => ({
  orgaoSubordinado: one(orgaosSubordinados, {
    fields: [unidadesGestoras.orgaoSubordinadoId],
    references: [orgaosSubordinados.id],
  }),
  despesasEmpenhos: many(despesasEmpenhos),
  licitacoes: many(licitacoes),
  contratos: many(contratos),
}));

export const despesasEmpenhosRelations = relations(despesasEmpenhos, ({ one, many }) => ({
  unidadeGestora: one(unidadesGestoras, {
    fields: [despesasEmpenhos.unidadeGestoraId],
    references: [unidadesGestoras.id],
  }),
  favorecidoPf: one(pessoasFisicas, {
    fields: [despesasEmpenhos.favorecidoPfId],
    references: [pessoasFisicas.id],
  }),
  favorecidoPj: one(pessoasJuridicas, {
    fields: [despesasEmpenhos.favorecidoPjId],
    references: [pessoasJuridicas.id],
  }),
  itens: many(despesasEmpenhoItens),
  liquidacoes: many(despesasLiquidacaoEmpenhos),
  pagamentos: many(despesasPagamentoEmpenhos),
}));

export const despesasEmpenhoItensRelations = relations(despesasEmpenhoItens, ({ one }) => ({
  empenho: one(despesasEmpenhos, {
    fields: [despesasEmpenhoItens.empenhoId],
    references: [despesasEmpenhos.id],
  }),
}));

export const licitacoesRelations = relations(licitacoes, ({ one, many }) => ({
  unidadeGestora: one(unidadesGestoras, {
    fields: [licitacoes.unidadeGestoraId],
    references: [unidadesGestoras.id],
  }),
  itens: many(licitacaoItens),
  participantes: many(licitacaoParticipantes),
  contratos: many(contratos),
}));

export const contratosRelations = relations(contratos, ({ one, many }) => ({
  unidadeGestora: one(unidadesGestoras, {
    fields: [contratos.unidadeGestoraId],
    references: [unidadesGestoras.id],
  }),
  licitacao: one(licitacoes, {
    fields: [contratos.licitacaoId],
    references: [licitacoes.id],
  }),
  contratado: one(pessoasJuridicas, {
    fields: [contratos.contratadoId],
    references: [pessoasJuridicas.id],
  }),
  itens: many(contratoItens),
  termosAditivos: many(contratoTermosAditivos),
}));

export const viagensRelations = relations(viagens, ({ one, many }) => ({
  viajante: one(pessoasFisicas, {
    fields: [viagens.viajanteId],
    references: [pessoasFisicas.id],
  }),
  unidadeGestoraPagadora: one(unidadesGestoras, {
    fields: [viagens.unidadeGestoraPagadoraId],
    references: [unidadesGestoras.id],
  }),
  trechos: many(viagemTrechos),
  passagens: many(viagemPassagens),
}));

export const notasFiscaisRelations = relations(notasFiscais, ({ one, many }) => ({
  emitente: one(pessoasJuridicas, {
    fields: [notasFiscais.emitenteId],
    references: [pessoasJuridicas.id],
  }),
  destinatarioPj: one(pessoasJuridicas, {
    fields: [notasFiscais.destinatarioPjId],
    references: [pessoasJuridicas.id],
  }),
  destinatarioPf: one(pessoasFisicas, {
    fields: [notasFiscais.destinatarioPfId],
    references: [pessoasFisicas.id],
  }),
  itens: many(notaFiscalItens),
  eventos: many(notaFiscalEventos),
}));

export const servidoresRelations = relations(servidores, ({ one, many }) => ({
  pessoaFisica: one(pessoasFisicas, {
    fields: [servidores.pessoaFisicaId],
    references: [pessoasFisicas.id],
  }),
  orgaoLotacao: one(orgaosSubordinados, {
    fields: [servidores.orgaoLotacaoId],
    references: [orgaosSubordinados.id],
  }),
  orgaoExercicio: one(orgaosSubordinados, {
    fields: [servidores.orgaoExercicioId],
    references: [orgaosSubordinados.id],
  }),
  remuneracoes: many(servidoresRemuneracoes),
}));

export const beneficioPagamentosRelations = relations(beneficioPagamentos, ({ one }) => ({
  programa: one(programasSociais, {
    fields: [beneficioPagamentos.programaId],
    references: [programasSociais.id],
  }),
  beneficiario: one(pessoasFisicas, {
    fields: [beneficioPagamentos.beneficiarioId],
    references: [pessoasFisicas.id],
  }),
  responsavel: one(pessoasFisicas, {
    fields: [beneficioPagamentos.responsavelId],
    references: [pessoasFisicas.id],
  }),
  municipio: one(municipios, {
    fields: [beneficioPagamentos.municipioId],
    references: [municipios.id],
  }),
}));

// ============================================================================
// DIMENSÃO DE INSTITUIÇÕES FINANCEIRAS (3FN)
// ============================================================================

export const bancos = mysqlTable(
  'bancos',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    codigo: varchar('codigo', { length: 20 }).notNull().unique(),
    nome: varchar('nome', { length: 255 }).notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_bancos_codigo').on(table.codigo),
  ]
);

// ============================================================================
// HISTÓRICO DE ITENS DE EMPENHO (3FN)
// ============================================================================

export const despesasEmpenhoItemHistorico = mysqlTable(
  'despesas_empenho_item_historico',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    empenhoItemId: bigint('empenho_item_id', { mode: 'number' }).notNull(),
    tipoOperacao: varchar('tipo_operacao', { length: 100 }).notNull(),
    dataOperacao: date('data_operacao', { mode: 'string' }).notNull(),
    quantidadeItem: decimal('quantidade_item', { precision: 14, scale: 4 }).notNull(),
    valorUnitarioItem: decimal('valor_unitario_item', { precision: 15, scale: 2 }).notNull(),
    valorTotalItem: decimal('valor_total_item', { precision: 15, scale: 2 }).notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_deih_item').on(table.empenhoItemId),
    index('idx_deih_data').on(table.dataOperacao),
    index('idx_deih_tipo_op').on(table.tipoOperacao),
    foreignKey({
      name: 'fk_deih_empenho_item',
      columns: [table.empenhoItemId],
      foreignColumns: [despesasEmpenhoItens.id],
    }),
  ]
);

// ============================================================================
// LISTAS E DETALHAMENTOS DE PAGAMENTOS (3FN)
// ============================================================================

export const despesasPagamentoFavorecidosFinais = mysqlTable(
  'despesas_pagamento_favorecidos_finais',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    pagamentoId: bigint('pagamento_id', { mode: 'number' }).notNull(),
    codigoLista: varchar('codigo_lista', { length: 50 }).notNull(),
    favorecidoPfId: bigint('favorecido_pf_id', { mode: 'number' }),
    favorecidoPjId: bigint('favorecido_pj_id', { mode: 'number' }),
    valorPagamento: decimal('valor_pagamento', { precision: 18, scale: 2 }).notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_dpff_pagamento').on(table.pagamentoId),
    index('idx_dpff_codigo_lista').on(table.codigoLista),
    index('idx_dpff_fav_pf').on(table.favorecidoPfId),
    index('idx_dpff_fav_pj').on(table.favorecidoPjId),
    foreignKey({
      name: 'fk_dpff_pagamento',
      columns: [table.pagamentoId],
      foreignColumns: [despesasPagamentos.id],
    }),
    foreignKey({
      name: 'fk_dpff_fav_pf',
      columns: [table.favorecidoPfId],
      foreignColumns: [pessoasFisicas.id],
    }),
    foreignKey({
      name: 'fk_dpff_fav_pj',
      columns: [table.favorecidoPjId],
      foreignColumns: [pessoasJuridicas.id],
    }),
  ]
);

export const despesasPagamentoListaBancos = mysqlTable(
  'despesas_pagamento_lista_bancos',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    pagamentoId: bigint('pagamento_id', { mode: 'number' }).notNull(),
    codigoLista: varchar('codigo_lista', { length: 50 }).notNull(),
    bancoId: bigint('banco_id', { mode: 'number' }).notNull(),
    agencia: varchar('agencia', { length: 30 }).notNull(),
    valor: decimal('valor', { precision: 18, scale: 2 }).notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_dplb_pagamento').on(table.pagamentoId),
    index('idx_dplb_lista').on(table.codigoLista),
    index('idx_dplb_banco').on(table.bancoId),
    foreignKey({
      name: 'fk_dplb_pagamento',
      columns: [table.pagamentoId],
      foreignColumns: [despesasPagamentos.id],
    }),
    foreignKey({
      name: 'fk_dplb_banco',
      columns: [table.bancoId],
      foreignColumns: [bancos.id],
    }),
  ]
);

export const despesasPagamentoListaFaturas = mysqlTable(
  'despesas_pagamento_lista_faturas',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    pagamentoId: bigint('pagamento_id', { mode: 'number' }).notNull(),
    codigoLista: varchar('codigo_lista', { length: 50 }).notNull(),
    sequencialFatura: int('sequencial_fatura').notNull(),
    valorLancamento: decimal('valor_lancamento', { precision: 18, scale: 2 }).notNull(),
    valorDesconto: decimal('valor_desconto', { precision: 18, scale: 2 }).default('0.00').notNull(),
    valorJuros: decimal('valor_juros', { precision: 18, scale: 2 }).default('0.00').notNull(),
    valorDeducao: decimal('valor_deducao', { precision: 18, scale: 2 }).default('0.00').notNull(),
    valorAcrescimo: decimal('valor_acrescimo', { precision: 18, scale: 2 }).default('0.00').notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_dplf_pagamento').on(table.pagamentoId),
    index('idx_dplf_lista').on(table.codigoLista),
    index('idx_dplf_seq').on(table.sequencialFatura),
    foreignKey({
      name: 'fk_dplf_pagamento',
      columns: [table.pagamentoId],
      foreignColumns: [despesasPagamentos.id],
    }),
  ]
);

export const despesasPagamentoListaPrecatorios = mysqlTable(
  'despesas_pagamento_lista_precatorios',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    pagamentoId: bigint('pagamento_id', { mode: 'number' }).notNull(),
    codigoLista: varchar('codigo_lista', { length: 50 }).notNull(),
    numeroParcela: varchar('numero_parcela', { length: 30 }).notNull(),
    valorPrecatorio: decimal('valor_precatorio', { precision: 18, scale: 2 }).notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_dplp_pagamento').on(table.pagamentoId),
    index('idx_dplp_lista').on(table.codigoLista),
    index('idx_dplp_parcela').on(table.numeroParcela),
    foreignKey({
      name: 'fk_dplp_pagamento',
      columns: [table.pagamentoId],
      foreignColumns: [despesasPagamentos.id],
    }),
  ]
);

// ============================================================================
// EXECUÇÃO DA DESPESA (FATO AGREGADA MENSAL)
// ============================================================================

export const execucaoDespesas = mysqlTable(
  'execucao_despesas',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    anoMesLancamento: varchar('ano_mes_lancamento', { length: 7 }).notNull(),
    unidadeGestoraId: bigint('unidade_gestora_id', { mode: 'number' }).references(() => unidadesGestoras.id),
    acaoOrcamentariaId: bigint('acao_orcamentaria_id', { mode: 'number' }).references(() => acoesOrcamentarias.id),
    municipioId: bigint('municipio_id', { mode: 'number' }).references(() => municipios.id),

    // Dimensões Orçamentárias
    categoriaDespesaId: bigint('categoria_despesa_id', { mode: 'number' }).references(() => categoriasDespesa.id),
    grupoDespesaId: bigint('grupo_despesa_id', { mode: 'number' }).references(() => gruposDespesa.id),
    modalidadeAplicacaoId: bigint('modalidade_aplicacao_id', { mode: 'number' }),
    elementoDespesaId: bigint('elemento_despesa_id', { mode: 'number' }).references(() => elementosDespesa.id),

    // Atributos textuais residuais da dimensão da Emenda (caso não haja tabela específica)
    codigoAutorEmenda: varchar('codigo_autor_emenda', { length: 50 }),
    chaveNatural: chaveNaturalHash(
      'ano_mes_lancamento',
      'IFNULL(unidade_gestora_id, 0)',
      'IFNULL(acao_orcamentaria_id, 0)',
      'IFNULL(municipio_id, 0)',
      'IFNULL(categoria_despesa_id, 0)',
      'IFNULL(grupo_despesa_id, 0)',
      'IFNULL(modalidade_aplicacao_id, 0)',
      'IFNULL(elemento_despesa_id, 0)',
      "IFNULL(codigo_autor_emenda, '')",
    ),
    nomeAutorEmenda: varchar('nome_autor_emenda', { length: 255 }),

    // Métricas Financeiras (1FN)
    valorEmpenhado: decimal('valor_empenhado', { precision: 18, scale: 2 }).notNull(),
    valorLiquidado: decimal('valor_liquidado', { precision: 18, scale: 2 }).notNull(),
    valorPago: decimal('valor_pago', { precision: 18, scale: 2 }).notNull(),
    valorRestosAPagarInscritos: decimal('valor_restos_a_pagar_inscritos', { precision: 18, scale: 2 }),
    valorRestosAPagarCancelado: decimal('valor_restos_a_pagar_cancelado', { precision: 18, scale: 2 }),
    valorRestosAPagarPagos: decimal('valor_restos_a_pagar_pagos', { precision: 18, scale: 2 }),

    exercicioReferencia: char('exercicio_referencia', { length: 4 }).notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_exec_desp_competencia').on(table.anoMesLancamento),
    index('idx_exec_desp_ug').on(table.unidadeGestoraId),
    index('idx_exec_desp_acao').on(table.acaoOrcamentariaId),
    index('idx_exec_desp_municipio').on(table.municipioId),
    uniqueIndex('uq_exec_desp_chave_natural').on(table.chaveNatural),
    // Declaração de FK com nome curto para evitar ultrapassar o limite do MySQL
    foreignKey({
      name: 'fk_exec_desp_mod_aplic',
      columns: [table.modalidadeAplicacaoId],
      foreignColumns: [modalidadesAplicacao.id],
    }),
  ]
);

// ============================================================================
// RELAÇÕES (RELATIONAL API)
// ============================================================================

export const execucaoDespesasRelations = relations(execucaoDespesas, ({ one }) => ({
  unidadeGestora: one(unidadesGestoras, {
    fields: [execucaoDespesas.unidadeGestoraId],
    references: [unidadesGestoras.id],
  }),
  acaoOrcamentaria: one(acoesOrcamentarias, {
    fields: [execucaoDespesas.acaoOrcamentariaId],
    references: [acoesOrcamentarias.id],
  }),
  municipio: one(municipios, {
    fields: [execucaoDespesas.municipioId],
    references: [municipios.id],
  }),
  categoriaDespesa: one(categoriasDespesa, {
    fields: [execucaoDespesas.categoriaDespesaId],
    references: [categoriasDespesa.id],
  }),
  grupoDespesa: one(gruposDespesa, {
    fields: [execucaoDespesas.grupoDespesaId],
    references: [gruposDespesa.id],
  }),
  modalidadeAplicacao: one(modalidadesAplicacao, {
    fields: [execucaoDespesas.modalidadeAplicacaoId],
    references: [modalidadesAplicacao.id],
  }),
  elementoDespesa: one(elementosDespesa, {
    fields: [execucaoDespesas.elementoDespesaId],
    references: [elementosDespesa.id],
  }),
}));

// ============================================================================
// CONVÊNIOS E FAVORECIDOS DE EMENDAS PARLAMENTARES (3FN)
// ============================================================================

export const emendasParlamentaresConvenios = mysqlTable(
  'emendas_parlamentares_convenios',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    emendaId: bigint('emenda_id', { mode: 'number' }).notNull(),
    convenioId: bigint('convenio_id', { mode: 'number' }),
    funcaoId: bigint('funcao_id', { mode: 'number' }),
    subfuncaoId: bigint('subfuncao_id', { mode: 'number' }),
    localidadeGasto: varchar('localidade_gasto', { length: 255 }),
    dataPublicacaoConvenio: date('data_publicacao_convenio', { mode: 'string' }),
    valorConvenio: decimal('valor_convenio', { precision: 18, scale: 2 }).notNull(),
    exercicioReferencia: char('exercicio_referencia', { length: 4 }).notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_epc_emenda').on(table.emendaId),
    index('idx_epc_convenio').on(table.convenioId),
    index('idx_epc_funcao').on(table.funcaoId),
    // Declaração de FKs com nomes curtos para evitar limites do MySQL
    foreignKey({
      name: 'fk_epc_emenda',
      columns: [table.emendaId],
      foreignColumns: [emendasParlamentares.id],
    }),
    foreignKey({
      name: 'fk_epc_convenio',
      columns: [table.convenioId],
      foreignColumns: [convenios.id],
    }),
    foreignKey({
      name: 'fk_epc_funcao',
      columns: [table.funcaoId],
      foreignColumns: [funcoesOrcamentarias.id],
    }),
    foreignKey({
      name: 'fk_epc_subfuncao',
      columns: [table.subfuncaoId],
      foreignColumns: [subfuncoesOrcamentarias.id],
    }),
  ]
);

export const emendasParlamentaresFavorecidos = mysqlTable(
  'emendas_parlamentares_favorecidos',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    emendaId: bigint('emenda_id', { mode: 'number' }).notNull(),
    favorecidoPfId: bigint('favorecido_pf_id', { mode: 'number' }),
    favorecidoPjId: bigint('favorecido_pj_id', { mode: 'number' }),
    municipioId: bigint('municipio_id', { mode: 'number' }),
    anoMes: varchar('ano_mes', { length: 7 }),
    valorRecebido: decimal('valor_recebido', { precision: 18, scale: 2 }).notNull(),
    exercicioReferencia: char('exercicio_referencia', { length: 4 }).notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_epf_emenda').on(table.emendaId),
    index('idx_epf_fav_pf').on(table.favorecidoPfId),
    index('idx_epf_fav_pj').on(table.favorecidoPjId),
    index('idx_epf_municipio').on(table.municipioId),
    // Declaração de FKs com nomes curtos para evitar limites do MySQL
    foreignKey({
      name: 'fk_epf_emenda',
      columns: [table.emendaId],
      foreignColumns: [emendasParlamentares.id],
    }),
    foreignKey({
      name: 'fk_epf_fav_pf',
      columns: [table.favorecidoPfId],
      foreignColumns: [pessoasFisicas.id],
    }),
    foreignKey({
      name: 'fk_epf_fav_pj',
      columns: [table.favorecidoPjId],
      foreignColumns: [pessoasJuridicas.id],
    }),
    foreignKey({
      name: 'fk_epf_municipio',
      columns: [table.municipioId],
      foreignColumns: [municipios.id],
    }),
  ]
);

export const emendasParlamentaresConveniosRelations = relations(emendasParlamentaresConvenios, ({ one }) => ({
  emenda: one(emendasParlamentares, {
    fields: [emendasParlamentaresConvenios.emendaId],
    references: [emendasParlamentares.id],
  }),
  convenio: one(convenios, {
    fields: [emendasParlamentaresConvenios.convenioId],
    references: [convenios.id],
  }),
  funcao: one(funcoesOrcamentarias, {
    fields: [emendasParlamentaresConvenios.funcaoId],
    references: [funcoesOrcamentarias.id],
  }),
  subfuncao: one(subfuncoesOrcamentarias, {
    fields: [emendasParlamentaresConvenios.subfuncaoId],
    references: [subfuncoesOrcamentarias.id],
  }),
}));

export const emendasParlamentaresFavorecidosRelations = relations(emendasParlamentaresFavorecidos, ({ one }) => ({
  emenda: one(emendasParlamentares, {
    fields: [emendasParlamentaresFavorecidos.emendaId],
    references: [emendasParlamentares.id],
  }),
  favorecidoPf: one(pessoasFisicas, {
    fields: [emendasParlamentaresFavorecidos.favorecidoPfId],
    references: [pessoasFisicas.id],
  }),
  favorecidoPj: one(pessoasJuridicas, {
    fields: [emendasParlamentaresFavorecidos.favorecidoPjId],
    references: [pessoasJuridicas.id],
  }),
  municipio: one(municipios, {
    fields: [emendasParlamentaresFavorecidos.municipioId],
    references: [municipios.id],
  }),
}));


// ============================================================================
// COMPLEMENTOS: DESTINOS QUE FALTAVAM PARA DADOS DO BRONZE
// ============================================================================

// Recebimentos agregados por favorecido/mês (bronze: recebimento_recursos_favorecido).
// NÃO deve ir para despesas_pagamentos: é um agregado, e duplicaria valores dos pagamentos reais.
export const recebimentosFavorecido = mysqlTable(
  'recebimentos_favorecido',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    anoMesLancamento: varchar('ano_mes_lancamento', { length: 7 }).notNull(),
    unidadeGestoraId: bigint('unidade_gestora_id', { mode: 'number' }),
    municipioId: bigint('municipio_id', { mode: 'number' }),
    favorecidoPfId: bigint('favorecido_pf_id', { mode: 'number' }),
    favorecidoPjId: bigint('favorecido_pj_id', { mode: 'number' }),
    // Preserva o identificador original quando o favorecido não é PF nem PJ (ex.: estrangeiro).
    codigoFavorecido: varchar('codigo_favorecido', { length: 30 }).notNull(),
    nomeFavorecido: varchar('nome_favorecido', { length: 255 }),
    valorRecebido: decimal('valor_recebido', { precision: 18, scale: 2 }).notNull(),
    exercicioReferencia: char('exercicio_referencia', { length: 4 }).notNull(),
    chaveNatural: chaveNaturalHash(
      'ano_mes_lancamento',
      'IFNULL(unidade_gestora_id, 0)',
      'IFNULL(municipio_id, 0)',
      'codigo_favorecido',
    ),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_rf_ano_mes').on(table.anoMesLancamento),
    index('idx_rf_ug').on(table.unidadeGestoraId),
    index('idx_rf_fav_pf').on(table.favorecidoPfId),
    index('idx_rf_fav_pj').on(table.favorecidoPjId),
    uniqueIndex('uq_rf_chave_natural').on(table.chaveNatural),
    foreignKey({ name: 'fk_rf_ug', columns: [table.unidadeGestoraId], foreignColumns: [unidadesGestoras.id] }),
    foreignKey({ name: 'fk_rf_municipio', columns: [table.municipioId], foreignColumns: [municipios.id] }),
    foreignKey({ name: 'fk_rf_fav_pf', columns: [table.favorecidoPfId], foreignColumns: [pessoasFisicas.id] }),
    foreignKey({ name: 'fk_rf_fav_pj', columns: [table.favorecidoPjId], foreignColumns: [pessoasJuridicas.id] }),
  ]
);

// Vínculo licitação <-> empenho (bronze: licitacao_empenhos_relacionados).
export const licitacaoEmpenhos = mysqlTable(
  'licitacao_empenhos',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    licitacaoId: bigint('licitacao_id', { mode: 'number' }).notNull(),
    empenhoId: bigint('empenho_id', { mode: 'number' }).notNull(),
    valorEmpenho: decimal('valor_empenho', { precision: 18, scale: 2 }),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_lic_emp_empenho').on(table.empenhoId),
    uniqueIndex('uq_lic_emp_nk').on(table.licitacaoId, table.empenhoId),
    foreignKey({ name: 'fk_le_licitacao', columns: [table.licitacaoId], foreignColumns: [licitacoes.id] }),
    foreignKey({ name: 'fk_le_empenho', columns: [table.empenhoId], foreignColumns: [despesasEmpenhos.id] }),
  ]
);

// Documentos de despesa ligados a emendas (bronze: emendas_parlamentares_documentos_despesa).
// Mantém o vínculo emenda <-> documento sem criar empenhos/pagamentos "stub".
export const emendasParlamentaresDocumentos = mysqlTable(
  'emendas_parlamentares_documentos',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    emendaId: bigint('emenda_id', { mode: 'number' }).notNull(),
    codigoDocumento: varchar('codigo_documento', { length: 50 }).notNull(),
    faseDespesa: varchar('fase_despesa', { length: 30 }).notNull(),
    dataDocumento: date('data_documento', { mode: 'string' }),
    // Preenchidos quando o documento já existe nas tabelas de despesa (resolvidos após a carga de despesas).
    empenhoId: bigint('empenho_id', { mode: 'number' }),
    liquidacaoId: bigint('liquidacao_id', { mode: 'number' }),
    pagamentoId: bigint('pagamento_id', { mode: 'number' }),
    unidadeGestoraId: bigint('unidade_gestora_id', { mode: 'number' }),
    favorecidoPfId: bigint('favorecido_pf_id', { mode: 'number' }),
    favorecidoPjId: bigint('favorecido_pj_id', { mode: 'number' }),
    municipioAplicacaoId: bigint('municipio_aplicacao_id', { mode: 'number' }),
    valorEmpenhado: decimal('valor_empenhado', { precision: 18, scale: 2 }),
    valorPago: decimal('valor_pago', { precision: 18, scale: 2 }),
    exercicioReferencia: char('exercicio_referencia', { length: 4 }).notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_epd_emenda').on(table.emendaId),
    index('idx_epd_empenho').on(table.empenhoId),
    index('idx_epd_pagamento').on(table.pagamentoId),
    uniqueIndex('uq_epd_nk').on(table.emendaId, table.codigoDocumento, table.faseDespesa),
    foreignKey({ name: 'fk_epd_emenda', columns: [table.emendaId], foreignColumns: [emendasParlamentares.id] }),
    foreignKey({ name: 'fk_epd_empenho', columns: [table.empenhoId], foreignColumns: [despesasEmpenhos.id] }),
    foreignKey({ name: 'fk_epd_liquidacao', columns: [table.liquidacaoId], foreignColumns: [despesasLiquidacoes.id] }),
    foreignKey({ name: 'fk_epd_pagamento', columns: [table.pagamentoId], foreignColumns: [despesasPagamentos.id] }),
    foreignKey({ name: 'fk_epd_ug', columns: [table.unidadeGestoraId], foreignColumns: [unidadesGestoras.id] }),
    foreignKey({ name: 'fk_epd_fav_pf', columns: [table.favorecidoPfId], foreignColumns: [pessoasFisicas.id] }),
    foreignKey({ name: 'fk_epd_fav_pj', columns: [table.favorecidoPjId], foreignColumns: [pessoasJuridicas.id] }),
    foreignKey({ name: 'fk_epd_municipio', columns: [table.municipioAplicacaoId], foreignColumns: [municipios.id] }),
  ]
);

// Empresas imunes ou isentas (bronze: empresas_imunes_ou_isentas).
export const empresasImunesIsentas = mysqlTable(
  'empresas_imunes_isentas',
  {
    id: bigint('id', { mode: 'number' }).primaryKey().autoincrement(),
    empresaId: bigint('empresa_id', { mode: 'number' }).notNull().references(() => pessoasJuridicas.id),
    anoCalendario: char('ano_calendario', { length: 4 }).notNull(),
    tipoEntidade: varchar('tipo_entidade', { length: 150 }),
    beneficioFiscal: varchar('beneficio_fiscal', { length: 255 }).notNull(),
    createdAt: timestamp('created_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3)`).notNull(),
    updatedAt: timestamp('updated_at', { fsp: 3 }).default(sql`CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)`).notNull(),
  },
  (table) => [
    index('idx_imunes_empresa').on(table.empresaId),
    uniqueIndex('uq_imunes_nk').on(table.empresaId, table.anoCalendario, table.beneficioFiscal),
  ]
);