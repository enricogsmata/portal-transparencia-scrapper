import { new_logger } from "./services/logs.js";

const logger = new_logger(import.meta.url);

function run() {
    const log = logger.forMethod('run');
    log.info('Deu certo!');

    return;
}

run();