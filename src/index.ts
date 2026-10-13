import { runProcessors } from "./pipeline/bronze/orchestrator.js";
import { new_logger } from "./services/logs.js";

const logger = new_logger(import.meta.url);

async function run() {
    const log = logger.forMethod('run');
    await runProcessors();
    return;
}

run();