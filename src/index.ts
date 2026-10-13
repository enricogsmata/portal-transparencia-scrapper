import { runProcessors } from "./pipeline/bronze/processor_orchestrator.js";
import { runScrapper } from "./pipeline/bronze/scrapper_orchestrator.js";
import { new_logger } from "./services/logs.js";

const logger = new_logger(import.meta.url);

async function run() {
    const log = logger.forMethod('run');
    await
        //await runProcessors();
        await runScrapper().catch(err => {
            logger.logger.fatal(err);
            process.exit(1);
        });;
        
    return;
}

run();