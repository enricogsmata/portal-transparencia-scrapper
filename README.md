# Portal da Transparência — Scrapper & Pipeline de Dados

> Pipeline ETL completo para extração, transformação e carga dos dados públicos do [Portal da Transparência do Governo Federal Brasileiro](https://portaldatransparencia.gov.br).

---

## Sumário

- [Visão Geral](#visão-geral)
- [Arquitetura da Solução](#arquitetura-da-solução)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Pré-requisitos](#pré-requisitos)
- [Configuração do Ambiente](#configuração-do-ambiente)
- [Comandos Disponíveis](#comandos-disponíveis)
- [Fluxo do Pipeline (ETL)](#fluxo-do-pipeline-etl)
  - [Etapa 1 — Scrapper (Camada Bronze)](#etapa-1--scrapper-camada-bronze)
  - [Etapa 2 — Processadores (Camada Bronze)](#etapa-2--processadores-camada-bronze)
  - [Etapa 3 — Transformadores (Camada Silver)](#etapa-3--transformadores-camada-silver)
- [Módulos de Dados](#módulos-de-dados)
- [Banco de Dados](#banco-de-dados)
- [Serviços de Suporte](#serviços-de-suporte)
- [Controle de Reexecução e Idempotência](#controle-de-reexecução-e-idempotência)
- [Anti-bloqueio WAF](#anti-bloqueio-waf)
- [Convenções de Nomenclatura de Arquivos](#convenções-de-nomenclatura-de-arquivos)
- [Tecnologias Utilizadas](#tecnologias-utilizadas)

---

## Visão Geral

Este projeto implementa um **pipeline ETL (Extract, Transform, Load)** para consumir sistematicamente os dados abertos disponibilizados pelo Portal da Transparência do Governo Federal. Os dados brutos são extraídos como arquivos ZIP contendo CSVs, persistidos em uma camada **Bronze** (dados crus, sem transformação), e posteriormente normalizados, tipificados e relacionados em uma camada **Silver** (dados limpos e prontos para análise ou consumo por aplicações).

A solução cobre **44 módulos temáticos** — de benefícios sociais a contratos públicos, servidores, sanções e renúncias fiscais — totalizando mais de 80 entidades de destino na camada Silver.

---

## Arquitetura da Solução

```
Portal da Transparência (portaldatransparencia.gov.br)
        │
        │  (Puppeteer + Axios + Cheerio)
        ▼
┌───────────────────────────────────────────┐
│          SCRAPPER ORCHESTRATOR            │
│  Descobre módulos, navega nas páginas,    │
│  resolve filtros (ano/mês/origem/dia)     │
│  e baixa os arquivos ZIP                  │
└──────────────────────┬────────────────────┘
                       │  ZIPs em src/files/<módulo>/
                       ▼
┌───────────────────────────────────────────┐
│         PROCESSOR ORCHESTRATOR            │
│  Descompacta ZIPs, lê CSVs (UTF-8/Latin) │
│  e persiste os dados brutos no banco      │
│  BRONZE (MySQL) via Drizzle ORM           │
└──────────────────────┬────────────────────┘
                       │  Camada Bronze (dados crus)
                       ▼
┌───────────────────────────────────────────┐
│     SILVER ORCHESTRATOR (12 Fases)        │
│  Lê da Bronze, normaliza, tipifica,       │
│  resolve FKs, e persiste na camada        │
│  SILVER (MySQL) com esquema relacional    │
└───────────────────────────────────────────┘
                       │  Camada Silver (dados prontos)
                       ▼
              Aplicações / Analytics
```

O pipeline segue o padrão **Medallion Architecture** (Bronze → Silver), onde cada camada tem responsabilidade exclusiva:

| Camada     | Responsabilidade                              | Banco de Dados         |
|------------|-----------------------------------------------|------------------------|
| **Bronze** | Dados brutos, sem transformação, fiel à fonte | `BRONZE_DATABASE_URL`  |
| **Silver** | Dados normalizados, tipificados e relacionados | `SILVER_DATABASE_URL`  |

---

## Estrutura do Projeto

```
portal-transparencia-scrapper/
├── src/
│   ├── index.ts                        # Entry point automatizado (execução completa)
│   ├── index-cli.ts                    # Entry point interativo com CLI (Commander + Inquirer)
│   │
│   ├── services/
│   │   └── logs.ts                     # Serviço de logging dual (Pino → console + banco Bronze)
│   │
│   ├── database/
│   │   ├── bronze_connection.ts        # Pool de conexão MySQL — camada Bronze
│   │   ├── silver_connection.ts        # Pool de conexão MySQL — camada Silver
│   │   ├── bronze_schema.ts            # Esquema Drizzle da camada Bronze (~61 KB)
│   │   ├── silver_schema.ts            # Esquema Drizzle da camada Silver (~120 KB)
│   │   ├── schema.ts                   # Aliases e reexportações consolidadas
│   │   └── bronze_to_silver_mapping.json  # Mapeamento declarativo Bronze → Silver
│   │
│   ├── pipeline/
│   │   ├── bronze/
│   │   │   ├── scrapper_orchestrator.ts    # Orquestrador de scraping (Puppeteer + Axios)
│   │   │   ├── processor_orchestrator.ts   # Orquestrador dos processadores Bronze
│   │   │   ├── shared.ts                   # Utilitários compartilhados (paths, metadata, normalize)
│   │   │   └── processors/                 # 45 processadores — um por módulo temático
│   │   │       ├── despesas.ts
│   │   │       ├── contratos.ts
│   │   │       ├── servidores.ts
│   │   │       └── ...
│   │   │
│   │   └── silver/
│   │       ├── transformers.orchestrator.ts  # Orquestrador Silver com 12 fases ordenadas
│   │       ├── seed/                          # Seeds estruturais (estados, programas sociais)
│   │       │   ├── estados.seed.ts
│   │       │   └── seed_programas_sociais.ts
│   │       ├── shared/                        # Helpers compartilhados pelos transformers
│   │       │   ├── formatters.ts              # Formatadores de CNPJ, CPF, datas, decimais, CEP
│   │       │   ├── historicos.ts              # Helpers para módulos históricos (ensure* helpers)
│   │       │   ├── municipio-resolver.ts      # Cache em memória para resolução de municípios
│   │       │   └── constants.ts               # Constantes globais (BATCH_SIZE, etc.)
│   │       └── transformers/                  # 43+ transformers Silver — um por entidade
│   │           ├── despesas.ts
│   │           ├── servidores.ts
│   │           └── ...
│   │
│   └── files/                          # Diretório de ZIPs baixados (gitignored)
│       ├── despesas/
│       ├── contratos/
│       ├── servidores/
│       └── ...
│
├── drizzle.bronze.config.ts            # Configuração Drizzle Kit — Bronze
├── drizzle.silver.config.ts            # Configuração Drizzle Kit — Silver
├── tsconfig.json
└── package.json
```

---

## Pré-requisitos

- **Node.js** >= 18 (recomendado 20+ LTS)
- **MySQL** 8.x (dois bancos distintos: Bronze e Silver, ou um único banco com schemas separados)
- **Chromium / Google Chrome** — necessário para o Puppeteer realizar os downloads via navegador headless

---

## Configuração do Ambiente

Crie um arquivo `.env` na raiz do projeto com as seguintes variáveis:

```env
# URL de conexão para o banco Bronze (dados brutos)
BRONZE_DATABASE_URL=mysql://usuario:senha@localhost:3306/bronze_db

# URL de conexão para o banco Silver (dados normalizados)
# Caso omitido, o Silver reutilizará a mesma conexão do Bronze
SILVER_DATABASE_URL=mysql://usuario:senha@localhost:3306/silver_db

# Nível de log (pino): trace | debug | info | warn | error | fatal
LOG_LEVEL=info
```

### Aplicando as migrações do banco de dados

```bash
# Gera as migrações a partir do schema (bronze)
npm run bronze-generate

# Aplica as migrações no banco Bronze
npm run bronze-migrate

# Gera as migrações a partir do schema (silver)
npm run silver-generate

# Aplica as migrações no banco Silver
npm run silver-migrate
```

> **Alternativa — Push direto** (sem migrations): Use `npm run bronze-push` e `npm run silver-push` para sincronizar o schema diretamente com o banco em ambientes de desenvolvimento.

---

## Comandos Disponíveis

| Comando                   | Descrição                                                                 |
|---------------------------|---------------------------------------------------------------------------|
| `npm run start`           | Executa o pipeline completo de forma não-interativa                       |
| `npm run script`          | Alias para `start`                                                        |
| `npm run cli`             | Executa com menu interativo (seleciona modo de execução e opções)         |
| `npm run cli -- -o`       | Baixa apenas **1 arquivo por módulo** (ideal para testes rápidos)         |
| `npm run cli -- -p`       | Inclui a etapa de processadores Bronze após o scraping                    |
| `npm run bronze-generate` | Gera arquivos de migração Drizzle para o banco Bronze                     |
| `npm run bronze-migrate`  | Executa as migrações no banco Bronze                                      |
| `npm run bronze-push`     | Sincroniza o schema Bronze diretamente (sem migrations)                   |
| `npm run silver-generate` | Gera arquivos de migração Drizzle para o banco Silver                     |
| `npm run silver-migrate`  | Executa as migrações no banco Silver                                      |
| `npm run silver-push`     | Sincroniza o schema Silver diretamente (sem migrations)                   |

### CLI Interativo

Ao executar `npm run cli` sem flags, um menu interativo é apresentado:

```
========================================
    Pipeline de Dados - Camada Bronze
========================================

? Selecione o modo de execução:
  ❯ Baixar tudo (Modo padrão)
    Baixar apenas 1 arquivo por módulo

? Deseja rodar os processadores após a raspagem? (y/N)
```

### Flags da CLI

| Flag               | Atalho | Descrição                                           |
|--------------------|--------|-----------------------------------------------------|
| `--one-per-module` | `-o`   | Baixa apenas o primeiro arquivo pendente por módulo |
| `--processors`     | `-p`   | Executa os processadores Bronze após o scraper      |

---

## Fluxo do Pipeline (ETL)

### Etapa 1 — Scrapper (Camada Bronze)

**Arquivo:** `src/pipeline/bronze/scrapper_orchestrator.ts`

O scrapper opera em quatro sub-etapas sequenciais:

#### 1.1 — Descoberta de Módulos (`fetchPortalData`)

Realiza uma requisição HTTP via **Axios** diretamente à página `https://portaldatransparencia.gov.br/download-de-dados`. O HTML é parseado com **Cheerio**, extraindo da estrutura do acordeão `#accordion1` os módulos disponíveis (nome, frequência de atualização e links).

```
Portal → HTTP GET → Cheerio parse → Lista de ModuleData[]
```

#### 1.2 — Dados Históricos (`fetchHistoricalData`)

Descobre e extrai arquivos históricos (dados **anteriores a 2013**) em uma etapa paralela não-bloqueante. Em caso de falha, o fluxo principal continua normalmente com os módulos regulares. Os arquivos históricos recebem o prefixo `historico-` no nome do diretório para evitar colisão com os módulos regulares. Como dados históricos não são mais atualizados pelo governo, a frequência é marcada como `"sob demanda"` (baixados apenas uma vez).

#### 1.3 — Criação da Estrutura de Diretórios (`ensureDirectories`)

Para cada módulo/arquivo descoberto, cria recursivamente o diretório de destino em `src/files/<folderName>/` caso ainda não exista.

#### 1.4 — Download dos Arquivos (`fetchFilesForModule`)

Esta é a etapa mais complexa do scrapper. Utiliza **Puppeteer** em modo headless com anti-detecção de automação (`navigator.webdriver = false`) para navegar nas páginas de detalhes de cada arquivo.

O algoritmo de download resolve a hierarquia de filtros do portal de forma adaptativa:

```
Para cada módulo → Para cada arquivo:
  │
  ├─ #arquivo-unico visível?
  │    └─ Sim → Download direto (arquivo único, sem filtros)
  │
  └─ Não → Itera origens (#origens / #links-origens)
              └─ Itera anos (#anos / #links-anos)
                   │
                   ├─ Tem meses (#meses)?
                   │    └─ Itera meses → Tem dias (#dias)?
                   │         ├─ Sim → Itera dias → processDownload(ano+mes+dia+origem)
                   │         └─ Não → Tem origens-mes (#origens-mes)?
                   │              ├─ Sim → Itera origens-mes → processDownload(ano+mes+origemMes)
                   │              └─ Não → processDownload(ano+mes+origem)
                   │
                   └─ Não tem meses → resolve link dinâmico (#link ou #anos a)
                        └─ processDownload(linkDinamico)
```

**Controle de validade dos downloads (`isFileValid`):**

O scrapper verifica a tabela `FileDownloadHistory` no banco Bronze antes de cada download. Um arquivo já baixado é ignorado se:
1. Existe um registro de download no banco **E**
2. O arquivo físico ainda existe no disco **E**
3. A data de download está dentro do prazo de validade conforme a frequência:

| Frequência            | Validade   |
|-----------------------|------------|
| `mensal`              | 30 dias    |
| `semanal`             | 7 dias     |
| `diário`              | 1 dia      |
| `a cada quatro horas` | 4 horas    |
| `sob demanda`         | Permanente |

**Mecanismo de download via CDP (`downloadZip`):**

O Puppeteer utiliza o **Chrome DevTools Protocol (CDP)** com `Page.setDownloadBehavior` para redirecionar os downloads para um diretório temporário único por UUID. Após o término (detectado pela ausência do sufixo `.crdownload`), o arquivo é movido para o destino final. O diretório temporário é sempre removido ao final, com ou sem erro.

---

### Etapa 2 — Processadores (Camada Bronze)

**Arquivo:** `src/pipeline/bronze/processor_orchestrator.ts`

O `runProcessors()` executa sequencialmente **44 processadores**, um para cada módulo temático. Cada processador:

1. Varre o diretório `src/files/<módulo>/` em busca de arquivos ZIP
2. **Descompacta** o ZIP em memória (via **yauzl**) sem extrair para disco
3. Detecta a **codificação** do CSV (predominantemente Latin-1 / ISO-8859-1) e converte para UTF-8 com **iconv-lite**
4. **Parseia** o CSV linha a linha com **csv-parser** em modo streaming
5. Acumula registros em batches de **2.000 linhas** (padrão `BATCH_SIZE`)
6. Persiste cada batch no banco Bronze com `INSERT ... ON DUPLICATE KEY UPDATE` (upsert idempotente via Drizzle ORM)
7. Extrai metadados de referência temporal (exercício, mês, dia) do nome do arquivo ZIP usando `extractMetadataFromFileName`

Os processadores são organizados por domínio temático:

| Domínio                       | Processadores                                                                                                             |
|-------------------------------|---------------------------------------------------------------------------------------------------------------------------|
| **Benefícios ao Cidadão**     | Auxílio Brasil, Auxílio Emergencial, Auxílio Reconstrução, Bolsa Família (pagamentos e saques), Novo Bolsa Família, Garantia Safra, Gás do Povo, Seguro Defeso, PETI, Pé-de-Meia, BPC |
| **Cartão de Pagamento**       | CPGF, CPCC, Cartão Defesa Civil                                                                                           |
| **CNPJ / Favorecidos PJ**     | Favorecidos Pessoas Jurídicas                                                                                             |
| **Convênios**                 | Convênios e Instrumentos Congêneres                                                                                       |
| **Despesas Públicas**         | Despesas (empenho/liquidação/pagamento), Execução de Despesa, Recursos Transferidos, Recebimento por Favorecido           |
| **Emendas Parlamentares**     | Emendas, Documentos de Despesa, Apoiamentos                                                                               |
| **Imóveis Funcionais**        | Imóveis Funcionais da União                                                                                               |
| **Licitações e Contratos**    | Licitações, Contratos                                                                                                     |
| **Notas Fiscais**             | Notas Fiscais Eletrônicas                                                                                                 |
| **Orçamento Público**         | Orçamento de Despesa                                                                                                      |
| **Receitas Públicas**         | Receitas                                                                                                                  |
| **Renúncias Fiscais**         | Renúncias Fiscais                                                                                                         |
| **Sanções**                   | CEIS, CEPIM, CNEP, Acordos de Leniência                                                                                   |
| **Servidores**                | Servidores, CEAF, PEP                                                                                                     |
| **Viagens a Serviço**         | Viagens                                                                                                                   |
| **Histórico (antes de 2013)** | Diárias e Passagens, Licitações Hist., Contratos Hist., Execução Hist.                                                    |

---

### Etapa 3 — Transformadores (Camada Silver)

**Arquivo:** `src/pipeline/silver/transformers.orchestrator.ts`

O `runSilverOrchestrator()` executa **80+ tarefas** organizadas em **12 fases** com dependência explícita de ordem. A execução é totalmente sequencial, garantindo que dimensões mestras existam antes de registros dependentes.

#### Fase 0 — Seeds Estruturais de Infraestrutura

Executada **antes de qualquer transformação**. Popula as tabelas de referência geográfica e social que serão usadas como chaves estrangeiras ao longo de todo o pipeline.

| Tarefa                   | Descrição                                                              |
|--------------------------|------------------------------------------------------------------------|
| `siafi-municipios`       | Scrapia o sistema SIAFI para obter os códigos de municípios            |
| `seed-estados`           | Insere os 27 estados brasileiros com códigos IBGE e região             |
| `seed-programas-sociais` | Insere o catálogo dos 12 programas sociais monitorados                 |
| `municipios-seed`        | Gera o seed de municípios a partir dos dados do SIAFI e IBGE           |
| `init-municipios`        | Persiste os municípios no banco Silver                                 |
| `municipio-resolver`     | Carrega toda a tabela de municípios em memória para resolução por SIAFI, IBGE ou nome/UF |

#### Fase 1 — Cadastros Base e Dimensões Mestras Independentes

Processa os cadastros de **Pessoas Jurídicas (CNPJ)** com seus CNAEs e Naturezas Jurídicas — entidades referenciadas por praticamente todos os outros módulos.

#### Fase 2 — Estrutura Institucional, Gestão e Orçamento

Processa Orçamento de Despesa, Receitas e Execução de Despesa — estrutura orçamentária que serve de base para os ciclos de despesa.

#### Fase 3 — Ciclo Central da Despesa Federal (SIAFI)

Processa o fluxo completo da despesa federal em sequência:

```
Empenho → Itens de Empenho → Histórico de Itens → Liquidação
→ Empenhos Impactados (Liquidação) → Pagamento → Empenhos Impactados (Pagamento)
→ Favorecidos Finais → Lista de Bancos → Lista de Faturas
→ Lista de Precatórios → Recebimento por Favorecido
```

#### Fase 4 — Convênios e Transferências da União

Convênios federais e suas respectivas Ordens Bancárias, além de Recursos Transferidos a estados, municípios e entidades privadas.

#### Fase 5 — Compras Públicas, Licitações e Notas Fiscais

Licitações (com itens, participantes e empenhos relacionados), Contratos (com itens, termos aditivos e apostilamentos) e Notas Fiscais Eletrônicas (com itens e eventos).

#### Fase 6 — Cartões de Pagamento e Viagens a Serviço

Cartões CPGF, CPCC e Defesa Civil; Viagens a serviço com trechos, passagens e pagamentos associados.

#### Fase 7 — Recursos Humanos, Servidores Públicos e Imóveis Funcionais

Cadastro de Servidores, Aposentados e Pensionistas; Remuneração, Afastamentos, Jetons, Honorários Advocatícios e Observações; Imóveis Funcionais e seus permissionários.

#### Fase 8 — Emendas Parlamentares e Vínculos

Emendas Parlamentares com seus convênios associados, beneficiários e documentos de despesa vinculados.

#### Fase 9 — Programas Sociais e Benefícios ao Cidadão

Todos os 12 programas sociais: Auxílio Brasil, Auxílio Emergencial, Auxílio Reconstrução, Bolsa Família (pagamentos e saques), Novo Bolsa Família, Garantia Safra, Gás do Povo, Pé-de-Meia, PETI, BPC e Seguro Defeso.

#### Fase 10 — Integridade, Sanções, PEP e Acordos de Leniência

PEP (Pessoas Expostas Politicamente), Acordos de Leniência e seus efeitos, CEIS (Cadastro de Empresas Inidôneas e Suspensas), CNEP (Cadastro Nacional de Empresas Punidas), CEAF (Cadastro de Expulsos), CEPIM (Cadastro de Entidades Privadas Sem Fins Lucrativos Impedidas).

#### Fase 11 — Renúncias Fiscais e Desonerações Tributárias

Renúncias fiscais gerais, renúncias por beneficiário, empresas imunes/isentas e empresas habilitadas.

#### Fase 12 — Arquivos Históricos

Licitações, Execução Orçamentária e Contratos históricos (dados anteriores a 2013).

---

## Módulos de Dados

Os 44 módulos extraídos do Portal da Transparência cobrem:

| Módulo                              | Diretório de Arquivos                           |
|-------------------------------------|-------------------------------------------------|
| Acordos de Leniência                | `src/files/acordos-leniencia/`                  |
| Apoiamento de Emendas Parlamentares | `src/files/apoiamento-emendas-parlamentares-documentos/` |
| Auxílio Brasil                      | `src/files/auxilio-brasil/`                     |
| Auxílio Emergencial                 | `src/files/auxilio-emergencial/`                |
| Auxílio Reconstrução                | `src/files/auxilio-reconstrucao/`               |
| Bolsa Família — Pagamentos          | `src/files/bolsa-familia-pagamentos/`           |
| Bolsa Família — Saques              | `src/files/bolsa-familia-saques/`               |
| BPC / LOAS                          | `src/files/bpc/`                                |
| CEAF                                | `src/files/ceaf/`                               |
| CEIS                                | `src/files/ceis/`                               |
| CEPIM                               | `src/files/cepim/`                              |
| CNEP                                | `src/files/cnep/`                               |
| Compras / Contratos                 | `src/files/compras/`                            |
| Convênios                           | `src/files/convenios/`                          |
| CPCC                                | `src/files/cpcc/`                               |
| CPDC (Defesa Civil)                 | `src/files/cpdc/`                               |
| CPGF                                | `src/files/cpgf/`                               |
| Despesas                            | `src/files/despesas/`                           |
| Despesas — Execução                 | `src/files/despesas-execucao/`                  |
| Despesas — Favorecidos              | `src/files/despesas-favorecidos/`               |
| Emendas Parlamentares               | `src/files/emendas-parlamentares/`              |
| Emendas Parlamentares — Documentos  | `src/files/emendas-parlamentares-documentos/`   |
| Favorecidos PJ                      | `src/files/favorecidos-pj/`                     |
| Garantia Safra                      | `src/files/garantia-safra/`                     |
| Gás do Povo                         | `src/files/gas-do-povo/`                        |
| Histórico — Contratos               | `src/files/historico-contratos/`                |
| Histórico — Diárias e Passagens     | `src/files/historico-diarias-e-passagens/`      |
| Histórico — Execução Orçamentária   | `src/files/historico-execucao-orcamentaria/`    |
| Histórico — Licitações              | `src/files/historico-licitacoes/`               |
| Imóveis Funcionais                  | `src/files/imoveis-funcionais/`                 |
| Licitações                          | `src/files/licitacoes/`                         |
| Municípios                          | `src/files/municipios/`                         |
| Notas Fiscais                       | `src/files/notas-fiscais/`                      |
| Novo Bolsa Família                  | `src/files/novo-bolsa-familia/`                 |
| Orçamento — Despesa                 | `src/files/orcamento-despesa/`                  |
| Pé-de-Meia                          | `src/files/pe-de-meia/`                         |
| PEP                                 | `src/files/pep/`                                |
| PETI                                | `src/files/peti/`                               |
| Receitas                            | `src/files/receitas/`                           |
| Renúncias Fiscais                   | `src/files/renuncias/`                          |
| Seguro Defeso                       | `src/files/seguro-defeso/`                      |
| Servidores                          | `src/files/servidores/`                         |
| Transferências                      | `src/files/transferencias/`                     |
| Viagens                             | `src/files/viagens/`                            |

---

## Banco de Dados

### Camada Bronze

O banco Bronze é gerenciado pelo arquivo `src/database/bronze_schema.ts`. Além das tabelas de dados brutos para cada módulo, contém tabelas de infraestrutura:

| Tabela                    | Propósito                                                       |
|---------------------------|-----------------------------------------------------------------|
| `file_download_history`   | Controle de downloads realizados com timestamp e frequência     |
| `logs`                    | Registro persistente de eventos de warning/error/fatal do pipeline |

### Camada Silver

O banco Silver é gerenciado pelo arquivo `src/database/silver_schema.ts` (~120 KB). Implementa um esquema relacional normalizado com tabelas de dimensão e fato:

**Dimensões Geográficas:**
- `estados`, `municipios`

**Dimensões Institucionais:**
- `orgaos_superiores`, `orgaos_subordinados`, `unidades_orcamentarias`, `unidades_gestoras`

**Dimensões Cadastrais:**
- `pessoas_juridicas`, `programas_sociais`

**Fatos Financeiros:**
- `despesas_empenho`, `despesas_liquidacao`, `despesas_pagamento` (e suas sub-tabelas de itens, histórico, bancos, faturas, precatórios)
- `convenios`, `recursos_transferidos`
- `licitacoes`, `contratos`
- `notas_fiscais`

**Programas Sociais:**
- `auxilio_brasil`, `auxilio_emergencial`, `auxilio_reconstrucao`
- `bolsa_familia_pagamentos`, `bolsa_familia_saques`, `novo_bolsa_familia`
- `garantia_safra`, `gas_do_povo`, `pe_de_meia`, `peti`, `bpc`, `seguro_defeso`

**Sanções e Integridade:**
- `ceis`, `cnep`, `cepim`, `ceaf`, `acordos_leniencia`, `pep`

**Transparência:**
- `servidores`, `aposentados`, `pensionistas`, `remuneracao`, `imoveis_funcionais`
- `emendas_parlamentares`, `viagens`
- `renuncias_fiscais`, `orcamento_despesa`, `receitas`

### Mapeamento Bronze → Silver

O arquivo `src/database/bronze_to_silver_mapping.json` é um documento declarativo que mapeia cada coluna das tabelas Bronze para sua contraparte na Silver, incluindo o tipo de transformação aplicada (renomeação, formatação de CNPJ/CPF, conversão de data, parse de decimal, etc.).

---

## Serviços de Suporte

### Sistema de Logging

**Arquivo:** `src/services/logs.ts`

O sistema de log utiliza **Pino** com saída dual simultânea via `pino.multistream`:

1. **Console (pino-pretty):** Logs coloridos e formatados para desenvolvimento, com timestamp no formato `YYYY-MM-DD HH:MM:ss`
2. **Banco de dados Bronze:** Persiste automaticamente todos os eventos de nível **warn (40) ou superior** na tabela `logs`, incluindo module, context, data e message

Cada logger é instanciado por arquivo com `new_logger(import.meta.url)` e pode criar sub-loggers por método com `logger.forMethod("nomeDoMetodo")`, gerando contexto estruturado em todos os registros.

### MunicipioResolver

**Arquivo:** `src/pipeline/silver/shared/municipio-resolver.ts`

Singleton com cache em memória que indexa todos os municípios da tabela Silver em **três índices simultâneos** para resolução eficiente de FK:

| Índice      | Chave                       | Uso                                    |
|-------------|-----------------------------|----------------------------------------|
| `bySiafi`   | Código SIAFI                | Primário para despesas e transferências |
| `byIbge`    | Código IBGE (6 ou 7 dígitos) | Dados do IBGE e geocódigos            |
| `byNomeUf`  | `"NOME SANITIZADO\|UF"`     | Fallback para dados sem código         |

### Formatters Silver

**Arquivo:** `src/pipeline/silver/shared/formatters.ts`

Funções puras de formatação e normalização aplicadas durante as transformações Bronze → Silver:

| Função         | Entrada                              | Saída                    |
|----------------|--------------------------------------|--------------------------|
| `formatCnpj`   | String com ou sem máscara            | `XX.XXX.XXX/XXXX-XX`     |
| `formatCpf`    | String com ou sem máscara            | 11 dígitos com padding   |
| `parseDate`    | Datas em DD/MM/YYYY, YYYY-MM-DD, etc. | `YYYY-MM-DD`            |
| `parseDecimal` | `1.234,56` (pt-BR)                   | `1234.56` (SQL/decimal)  |
| `formatCep`    | CEP com ou sem traço                 | `XXXXX-XXX`              |
| `formatAnoMes` | Múltiplos formatos                   | `YYYY-MM`                |
| `getDocumentType` | CPF ou CNPJ                       | `'PF'` ou `'PJ'`         |

---

## Controle de Reexecução e Idempotência

O pipeline foi projetado para ser **totalmente idempotente** em todas as etapas:

1. **Scrapper:** Verifica `FileDownloadHistory` + existência física do arquivo antes de qualquer download. Arquivos dentro da validade são ignorados automaticamente.

2. **Processadores Bronze:** Todos os `INSERT` usam `ON DUPLICATE KEY UPDATE` (upsert), garantindo que reprocessar o mesmo CSV nunca gere duplicatas.

3. **Transformadores Silver:** Igualmente usam upsert (`onDuplicateKeyUpdate`) para todas as inserções, além dos helpers `ensure*` (ex: `ensureUnidadesGestoras`, `ensurePessoasJuridicas`) que criam entidades referenciadas apenas quando não existem.

---

## Anti-bloqueio WAF

O Portal da Transparência é protegido pelo **AWS WAF (Web Application Firewall)**. O scrapper implementa múltiplas estratégias de mitigação:

| Estratégia                      | Implementação                                                          |
|---------------------------------|------------------------------------------------------------------------|
| **User-Agent realista**         | Header `Mozilla/5.0 Chrome/120` em todas as requisições Axios          |
| **Desativação do `webdriver`**  | `navigator.webdriver = false` via `evaluateOnNewDocument`              |
| **Desativação de AutomationControlled** | Flag `--disable-blink-features=AutomationControlled` no Puppeteer |
| **Pausa entre downloads**       | 2.500 ms entre cada download para evitar rate limiting                 |
| **Retry com espera crescente**  | `loadFilePage` tenta até 3x com espera de `3000ms × tentativa`         |
| **Reconexão automática**        | Detecta browser desconectado e recria a instância Puppeteer            |

---

## Convenções de Nomenclatura de Arquivos

A função `extractMetadataFromFileName` em `src/pipeline/bronze/shared.ts` parseia o nome do arquivo ZIP para extrair metadados de referência temporal:

| Padrão do Nome        | exercicioReferencia | mesReferencia | diaReferencia | dadosAdicionais |
|-----------------------|---------------------|---------------|---------------|-----------------|
| `20240115_ORIGEM.zip` | `2024`              | `01`          | `15`          | `ORIGEM`        |
| `20240115.zip`        | `2024`              | `01`          | `15`          | `null`          |
| `202401_ORIGEM.zip`   | `2024`              | `01`          | `null`        | `ORIGEM`        |
| `202401.zip`          | `2024`              | `01`          | `null`        | `null`          |
| `2024.zip`            | `2024`              | `null`        | `null`        | `null`          |
| `ARQUIVO_UNICO.zip`   | `null`              | `null`        | `null`        | `ARQUIVO_UNICO` |

Esses metadados são persistidos em todas as tabelas Bronze como campos de auditoria e usados nos transformadores Silver para determinar o período de referência de cada registro.

---

## Tecnologias Utilizadas

| Tecnologia            | Versão  | Uso                                               |
|-----------------------|---------|---------------------------------------------------|
| **TypeScript**        | ^7.0    | Linguagem principal com tipagem estrita            |
| **tsx**               | ^4.23   | Execução TypeScript sem build step                 |
| **Puppeteer**         | ^25.13  | Automação de browser headless para downloads       |
| **Axios**             | ^1.20   | Requisições HTTP para descoberta de módulos        |
| **Cheerio**           | ^1.2    | Parse de HTML no servidor                          |
| **Drizzle ORM**       | ^0.45   | ORM type-safe com suporte a MySQL e migrations     |
| **MySQL2**            | ^3.24   | Driver MySQL com suporte a Pool de conexões        |
| **yauzl**             | ^3.4    | Descompressão de ZIPs em streaming                 |
| **iconv-lite**        | ^0.7    | Conversão de encoding Latin-1/ISO-8859-1 → UTF-8  |
| **csv-parser**        | ^3.2    | Parse de CSV em streaming                          |
| **Pino**              | ^10.4   | Logger estruturado de alta performance             |
| **pino-pretty**       | ^13.2   | Formatação colorida dos logs no console            |
| **date-fns**          | ^4.4    | Cálculo de datas para validação de validade        |
| **Commander**         | ^15.0   | Parsing de argumentos da CLI                       |
| **@inquirer/prompts** | ^8.7    | Menu interativo para o modo CLI                    |
| **adm-zip**           | ^0.6    | Utilitário auxiliar de ZIP                         |
| **dotenv**            | ^18.0   | Carregamento de variáveis de ambiente              |
| **drizzle-kit**       | ^0.31   | CLI para geração e execução de migrations          |

---

## Licença

Este projeto não possui licença declarada. Todos os dados extraídos são de domínio público, disponibilizados pelo Governo Federal Brasileiro sob a [Lei de Acesso à Informação (Lei nº 12.527/2011)](https://www.planalto.gov.br/ccivil_03/_ato2011-2014/2011/lei/l12527.htm).
