import { Command } from "commander";
import { select, confirm } from "@inquirer/prompts";
import { runProcessors } from "./pipeline/bronze/processor_orchestrator.js";
import { runScrapper } from "./pipeline/bronze/scrapper_orchestrator.js";
import { new_logger } from "./services/logs.js";

const logger = new_logger(import.meta.url);
const program = new Command();

interface CliOptions {
    onePerModule?: boolean;
    processors?: boolean;
}

async function executePipeline(options: CliOptions) {
    const log = logger.forMethod("run");

    if (options.onePerModule) {
        log.info("Modo ativado: Baixando apenas o primeiro arquivo pendente por módulo (--one-per-module)");
    }

    try {
        log.info("Iniciando o orquestrador do scrapper...");
        await runScrapper({ oneFilePerModule: !!options.onePerModule });

        if (options.processors) {
            log.info("Iniciando processadores...");
            await runProcessors();
        }

        log.info("Processo finalizado com sucesso.");
    } catch (err) {
        logger.logger.fatal({ data: err }, "Erro fatal na execução do pipeline:");
        process.exit(1);
    }
}

async function runInteractiveMenu(): Promise<CliOptions> {
    console.clear();
    console.log("\x1b[36m%s\x1b[0m", "========================================");
    console.log("\x1b[1m%s\x1b[0m", "    Pipeline de Dados - Camada Bronze");
    console.log("\x1b[36m%s\x1b[0m", "========================================\n");

    const mode = await select({
        message: "Selecione o modo de execução:",
        choices: [
            {
                name: "Baixar tudo (Modo padrão)",
                value: "full",
                description: "Processa todos os arquivos pendentes de todos os módulos",
            },
            {
                name: "Baixar apenas 1 arquivo por módulo",
                value: "one-per-module",
                description: "Útil para testes rápidos (--one-per-module)",
            },
        ],
    });

    const runProc = await confirm({
        message: "Deseja rodar os processadores após a raspagem?",
        default: false,
    });

    return {
        onePerModule: mode === "one-per-module",
        processors: runProc,
    };
}

async function main() {
    program
        .name("npm run cli --")
        .description("Orquestrador do Pipeline de Dados - Camada Bronze")
        .version("1.0.0")
        .option("-o, --one-per-module", "Baixa apenas o primeiro arquivo pendente de cada módulo")
        .option("-p, --processors", "Executa os processadores após o scrapper");

    program.parse(process.argv);

    const rawOptions = program.opts<CliOptions>();

    // Verifica se alguma flag foi passada como true via terminal
    const hasFlagsPassed = Object.values(rawOptions).some((val) => val === true);

    const options: CliOptions = !hasFlagsPassed
        ? await runInteractiveMenu()
        : rawOptions;

    await executePipeline(options);
}

main();