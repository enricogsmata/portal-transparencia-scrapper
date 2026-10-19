import { new_logger } from '../../services/logs.js';
import { processAcordosLeniencia } from './transformers/acordos-leniencia.js';

const logger = new_logger(import.meta.url);

interface TransformerTask {
  name: string;
  execute: () => Promise<void>;
}

export async function runSilverOrchestrator() {
  const log = logger.forMethod('runSilverOrchestrator');

  const tasks: TransformerTask[] = [
    { name: 'acordos-leniencia', execute: processAcordosLeniencia },
    // Adicione os próximos transformers aqui conforme forem implementados
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