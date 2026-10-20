import 'dotenv/config';
import fs from 'node:fs';
import path from 'node:path';
import pino from 'pino';
import { new_logger } from '../../../services/logs.js';

const logger = new_logger(import.meta.url);
const log = logger.forMethod('scrapeSiafiMunicipios');

const BASE_URL = 'https://www.tesourotransparente.gov.br';
const DATASETS_URL = `${BASE_URL}/ckan/dataset`;
const DESTINATION_DIR = path.join('src', 'files', 'municipios');

export async function scrapeSiafiMunicipios(): Promise<void> {
  log.info('Iniciando scrapper para o dataset Lista de municípios do SIAFI...');

  try {
    const datasetsResponse = await fetch(DATASETS_URL);
    if (!datasetsResponse.ok) {
      throw new Error(`Falha ao buscar página de datasets: ${datasetsResponse.status}`);
    }
    
    const datasetsHtml = await datasetsResponse.text();
    const datasetLinkMatch = datasetsHtml.match(/href="(\/ckan\/dataset\/lista-de-municipios-do-siafi)"/i);
    
    if (!datasetLinkMatch) {
      throw new Error('Dataset "Lista de municípios do SIAFI" não localizado na página principal.');
    }

    const datasetUrl = `${BASE_URL}${datasetLinkMatch[1]}`;
    log.info(`Dataset localizado em: ${datasetUrl}`);

    const datasetResponse = await fetch(datasetUrl);
    if (!datasetResponse.ok) {
      throw new Error(`Falha ao buscar página do dataset: ${datasetResponse.status}`);
    }

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
    const fileResponse = await fetch(downloadUrl);
    if (!fileResponse.ok) {
      throw new Error(`Falha ao baixar o arquivo: ${fileResponse.status}`);
    }

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