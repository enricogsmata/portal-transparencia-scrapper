import 'dotenv/config';
import fs from 'node:fs';
import path from 'node:path';
import { new_logger } from '../../../services/logs.js';

const logger = new_logger(import.meta.url);
const log = logger.forMethod('scrapeSiafiMunicipios');

const BASE_URL = 'https://www.tesourotransparente.gov.br';
const DATASET_URL = `${BASE_URL}/ckan/dataset/lista-de-municipios-do-siafi`;
const DESTINATION_DIR = path.join('src', 'files', 'municipios');

async function fetchWithRetry(url: string, retries = 3, delayMs = 2000): Promise<Response> {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const response = await fetch(url);
      if (!response.ok) {
        throw new Error(`Falha HTTP: ${response.status} ${response.statusText}`);
      }
      return response;
    } catch (error) {
      if (attempt === retries) throw error;
      log.warn(`Falha na requisição para ${url}. Tentativa ${attempt} de ${retries}. Retentando em ${delayMs}ms...`);
      await new Promise(resolve => setTimeout(resolve, delayMs));
    }
  }
  throw new Error('Unreachable');
}

export async function scrapeSiafiMunicipios(): Promise<void> {
  log.info('Iniciando scrapper para o dataset Lista de municípios do SIAFI...');

  try {
    log.info(`Acessando a página do dataset diretamente em: ${DATASET_URL}`);
    const datasetResponse = await fetchWithRetry(DATASET_URL);
    const datasetHtml = await datasetResponse.text();

    const aTags = datasetHtml.match(/<a\s[^>]+>/gi) || [];
    const downloadTag = aTags.find(tag => tag.includes('resource-url-analytics') && tag.includes('.csv'));
    const downloadUrlMatch = downloadTag?.match(/href="([^"]+)"/i);

    if (!downloadUrlMatch || !downloadUrlMatch[1]) {
      throw new Error('Link de download do arquivo CSV não encontrado no HTML do dataset.');
    }

    const downloadUrl = downloadUrlMatch[1];
    log.info(`URL de download extraída: ${downloadUrl}`);

    log.info('Iniciando o download do arquivo CSV...');
    const fileResponse = await fetchWithRetry(downloadUrl);

    const arrayBuffer = await fileResponse.arrayBuffer();
    const buffer = Buffer.from(arrayBuffer);

    if (!fs.existsSync(DESTINATION_DIR)) {
      fs.mkdirSync(DESTINATION_DIR, { recursive: true });
    }

    const fileName = downloadUrl.split('/').pop() || 'tabmun.csv';
    const filePath = path.join(DESTINATION_DIR, fileName);

    fs.writeFileSync(filePath, buffer);
    log.info(`Arquivo baixado e salvo com sucesso em: ${filePath}`);

  } catch (error) {
    log.error({ err: error }, 'Falha durante o processamento do scrapper.');
    process.exitCode = 1;
  }
}