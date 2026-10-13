import fs from "node:fs/promises";
import path from "node:path";
import axios from "axios";
import * as cheerio from "cheerio";
import { ZIP_FILES_DIRECTORY } from "./shared.js";
import { new_logger } from "../../services/logs.js";

const logger = new_logger(import.meta.url);
const BASE_URL = "https://portaldatransparencia.gov.br/download-de-dados";

interface FileMetadata {
    name: string;
    href: string;
    folderName: string;
    frequency: string;
}

interface ModuleData {
    moduleName: string;
    files: FileMetadata[];
}

async function fetchPortalData(): Promise<ModuleData[]> {
    const log = logger.forMethod("fetchPortalData");
    log.info(`Buscando dados da página: ${BASE_URL}`);

    const { data } = await axios.get(BASE_URL, {
        headers: {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
    });
    const $ = cheerio.load(data);
    const modules: ModuleData[] = [];

    $("#accordion1 .item").each((_, el) => {
        const moduleName = $(el).find(".title").text().trim();
        const controlsId = $(el).find("button").attr("aria-controls");

        if (controlsId) {
            const files: FileMetadata[] = [];
            const contentDiv = $(`#${controlsId}`);

            contentDiv.find("tbody tr").each((_, tr) => {
                const $tds = $(tr).find("td");
                const $link = $tds.first().find("a");
                const name = $link.text().trim();
                const href = $link.attr("href") || "";
                const frequency = $tds.eq(1).text().trim();

                if (href) {
                    const folderName = href.split("/").pop() || "";
                    if (folderName) {
                        files.push({ name, href, folderName, frequency });
                    }
                }
            });

            if (files.length > 0) {
                modules.push({ moduleName, files });
            }
        }
    });

    log.info(`Extração concluída. ${modules.length} módulos encontrados.`);
    return modules;
}

async function ensureDirectories(modules: ModuleData[]): Promise<void> {
    const log = logger.forMethod("ensureDirectories");
    log.info("Iniciando geração de diretórios de destino");

    for (const mod of modules) {
        for (const file of mod.files) {
            const dirPath = path.join(ZIP_FILES_DIRECTORY, file.folderName);
            await fs.mkdir(dirPath, { recursive: true });
        }
    }

    log.info("Estrutura de diretórios validada e gerada com sucesso.");
}

export async function runScrapper(): Promise<ModuleData[]> {
    const log = logger.forMethod("runScrapper");
    try {
        log.info("Iniciando orquestração do scrapper");

        const modules = await fetchPortalData();
        await ensureDirectories(modules);

        log.info("Orquestração do scrapper finalizada com sucesso");
        return modules;
    } catch (error) {
        log.error({ data: error }, "Erro fatal durante a execução do scrapper");
        throw error;
    }
}