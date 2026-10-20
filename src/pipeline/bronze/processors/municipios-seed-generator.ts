import 'dotenv/config';
import fs from 'node:fs';
import path from 'node:path';
import { new_logger } from '../../../services/logs.js';

const logger = new_logger(import.meta.url);
const log = logger.forMethod('generateMunicipiosSeed');

const CSV_FILE_PATH = path.join('src', 'files', 'municipios', 'tabmun.csv');
const OUT_FILE_PATH = path.join('src', 'files', 'municipios', 'municipios_seed.json');
const IBGE_API_URL = 'https://servicodados.ibge.gov.br/api/v1/localidades/municipios';

const UFS_VALIDAS = new Set([
  'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS',
  'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC',
  'SP', 'SE', 'TO'
]);

interface IbgeMunicipioPayload {
  id: number;
  nome: string;
  microrregiao?: {
    mesorregiao?: {
      UF?: {
        sigla: string;
      };
    };
  } | null;
  'regiao-imediata'?: {
    'regiao-intermediaria'?: {
      UF?: {
        sigla: string;
      };
    };
  } | null;
}

interface MunicipioConsolidado {
  ufSigla: string;
  codigoIbge: string;
  codigoSiafi: string;
  nome: string;
}

function removeAccents(str: string): string {
  return str.normalize('NFD').replace(/[\u0300-\u036f]/g, '');
}

function cleanName(str: string): string {
  return removeAccents(str).toUpperCase().replace(/\s+/g, ' ').trim();
}

export async function generateMunicipiosSeed(): Promise<void> {
  log.info('Iniciando geração do seed de municípios...');

  try {
    if (!fs.existsSync(CSV_FILE_PATH)) {
      throw new Error(`Arquivo CSV do SIAFI não encontrado no diretório: ${CSV_FILE_PATH}`);
    }

    const csvContent = fs.readFileSync(CSV_FILE_PATH, 'utf-8');
    const lines = csvContent.split(/\r?\n/).filter((line) => line.trim().length > 0);

    const tesouroByIbge = new Map<string, string>();
    const tesouroByNameUf = new Map<string, string>();

    log.info('Processando e higienizando dados do Tesouro Nacional (CSV)...');
    for (const line of lines) {
      let rawSiafi = '';
      let rawNome = '';
      let ufSigla = '';
      let rawIbge = '';

      // Tenta usar regex flexível para capturar os dados (suporta separadores como vírgula, ponto-e-vírgula, tabulação ou espaços)
      const match = line.trim().match(/^['"]?(\d{4})['"]?[\s;,]+['"]?(\d+)['"]?[\s;,]+(.+?)[\s;,]+['"]?([A-Z]{2})['"]?[\s;,]+['"]?(\d{7})['"]?$/);

      if (match) {
        rawSiafi = match[1];
        rawNome = match[3].replace(/^['"]+|['"]+$/g, '').trim();
        ufSigla = match[4];
        rawIbge = match[5];
      } else {
        // Tenta split tradicional por ponto-e-vírgula, tabulação ou vírgula
        const separator = line.includes(';') ? ';' : (line.includes('\t') ? '\t' : ',');
        const parts = line.split(separator);

        if (parts.length >= 5) {
          rawSiafi = parts[0].replace(/^['"]+|['"]+$/g, '').trim();
          rawIbge = parts[parts.length - 1].replace(/^['"]+|['"]+$/g, '').trim();
          ufSigla = parts[parts.length - 2].replace(/^['"]+|['"]+$/g, '').trim().toUpperCase();
          rawNome = parts.slice(2, parts.length - 2).join(' ').replace(/^['"]+|['"]+$/g, '').trim();
        } else {
          continue;
        }
      }

      if (!/^\d{4}$/.test(rawSiafi) || !/^\d{7}$/.test(rawIbge)) continue;
      if (!UFS_VALIDAS.has(ufSigla)) continue;

      const codigoSiafi = rawSiafi;
      const codigoIbge = rawIbge;
      const normalizedNome = cleanName(rawNome);

      tesouroByIbge.set(codigoIbge, codigoSiafi);
      tesouroByNameUf.set(`${normalizedNome}-${ufSigla}`, codigoSiafi);
    }

    log.info('Buscando dados oficiais na API de localidades do IBGE...');
    const ibgeResponse = await fetch(IBGE_API_URL);
    if (!ibgeResponse.ok) {
      throw new Error(`Falha ao buscar dados da API do IBGE. Status: ${ibgeResponse.status}`);
    }

    const ibgeData = (await ibgeResponse.json()) as IbgeMunicipioPayload[];

    const resultados: MunicipioConsolidado[] = [];
    const siafiSet = new Set<string>();
    const ibgeSet = new Set<string>();

    log.info('Cruzando bases e validando chaves únicas e integridade...');
    for (const ibge of ibgeData) {
      const codigoIbge = String(ibge.id).padStart(7, '0');
      const nomeOficial = ibge.nome.replace(/\s+/g, ' ').trim();

      const ufSigla = ibge.microrregiao?.mesorregiao?.UF?.sigla
        ?? ibge['regiao-imediata']?.['regiao-intermediaria']?.UF?.sigla;

      if (!ufSigla) {
        log.warn(`Não foi possível determinar a UF para o município IBGE: ${codigoIbge} - ${nomeOficial}`);
        continue;
      }

      const normalizedNome = cleanName(nomeOficial);

      // Junção primária
      let codigoSiafi = tesouroByIbge.get(codigoIbge);

      // Resolução de divergência (Junção secundária)
      if (!codigoSiafi) {
        codigoSiafi = tesouroByNameUf.get(`${normalizedNome}-${ufSigla}`);
      }

      if (!codigoSiafi) {
        log.warn(`Código SIAFI não encontrado para o município IBGE: ${codigoIbge} - ${nomeOficial} (${ufSigla})`);
        continue;
      }

      if (siafiSet.has(codigoSiafi)) {
        log.warn(`Violação de duplicidade ignorada - SIAFI duplicado: ${codigoSiafi} no IBGE: ${codigoIbge}`);
        continue;
      }

      if (ibgeSet.has(codigoIbge)) {
        log.warn(`Violação de duplicidade ignorada - IBGE duplicado: ${codigoIbge}`);
        continue;
      }

      siafiSet.add(codigoSiafi);
      ibgeSet.add(codigoIbge);

      resultados.push({
        ufSigla,
        codigoIbge,
        codigoSiafi,
        nome: nomeOficial,
      });
    }

    fs.writeFileSync(OUT_FILE_PATH, JSON.stringify(resultados, null, 2), 'utf-8');
    log.info(`Seed consolidado com sucesso! Salvo em ${OUT_FILE_PATH} com ${resultados.length} registros válidos.`);
  } catch (error) {
    log.error({ err: error }, 'Falha fatal durante a geração do seed de municípios.');
    process.exitCode = 1;
  }
}