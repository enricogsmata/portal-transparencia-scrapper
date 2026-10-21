// src\pipeline\silver\transformers.orchestrator.ts
import { new_logger } from '../../services/logs.js';
import { generateMunicipiosSeed } from '../bronze/processors/municipios-seed-generator.js';
import { scrapeSiafiMunicipios } from '../bronze/processors/siafi-scrapper.js';
import { generateEstadosSeed } from './seed/estados.seed.js';
import { generateProgramasSociaisSeed } from './seed/seed_programas_sociais.js';
import { MunicipioResolver } from './shared/municipio-resolver.js';
import { initMunicipios } from './transformers/init-municipios.js';

const logger = new_logger(import.meta.url);

interface TransformerTask {
  name: string;
  execute: () => Promise<void>;
}

export async function runSilverOrchestrator() {
  const log = logger.forMethod('runSilverOrchestrator');

  const tasks: TransformerTask[] = [
    { name: 'siafi-municipios', execute: scrapeSiafiMunicipios },
    { name: 'seed-estados', execute: generateEstadosSeed },
    { name: 'municipios-seed', execute: generateMunicipiosSeed },
    { name: 'init-municipios', execute: initMunicipios },
    { name: 'municipio-resolver', execute: MunicipioResolver.load },
    { name: 'seed-programas-sociais', execute: generateProgramasSociaisSeed }
  ];

  log.info({ data: { totalTasks: tasks.length } }, 'Iniciando orquestração da camada Silver');

  for (const task of tasks) {
    log.info(`Iniciando processamento: ${task.name}`);
    const start = Date.now();

    try {
      await task.execute();
      const durationMs = Date.now() - start;
      log.info({ data: { durationMs } }, `Processamento concluído com sucesso: ${task.name}`);
    } catch (error) {
      log.error({ data: error }, `Erro crítico ao processar: ${task.name}`);
      throw error;
    }
  }

  log.info('Orquestração da camada Silver finalizada com sucesso');
}

await runSilverOrchestrator();