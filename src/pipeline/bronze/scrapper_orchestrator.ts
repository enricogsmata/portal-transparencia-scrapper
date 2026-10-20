import fs from "node:fs/promises";
import path from "node:path";
import axios from "axios";
import * as cheerio from "cheerio";
import { ZIP_FILES_DIRECTORY } from "./shared.js";
import { new_logger } from "../../services/logs.js";
import puppeteer, { Page } from "puppeteer";
import { randomUUID } from "node:crypto";
import { eq } from "drizzle-orm";
import { addMonths, addDays, addHours, isAfter } from "date-fns";
import { bronze_db } from "../../database/bronze_connection.js";
import { FileDownloadHistory } from "../../database/bronze_schema.js";
import { scrapeSiafiMunicipios } from "./processors/siafi-scrapper.js";
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

async function downloadZip(page: Page, url: string, destPath: string): Promise<void> {
    const log = logger.forMethod("downloadZip");
    const tempDir = path.join(process.cwd(), '.temp_downloads', randomUUID());
    await fs.mkdir(tempDir, { recursive: true });

    try {
        const client = await page.createCDPSession();
        await client.send('Page.setDownloadBehavior', {
            behavior: 'allow',
            downloadPath: tempDir,
        });

        await page.evaluate((downloadUrl) => {
            const a = document.createElement('a');
            a.href = downloadUrl;
            a.download = '';
            document.body.appendChild(a);
            a.click();
            a.remove();
        }, url);

        let downloadedFile = '';
        let attempts = 0;
        while (attempts < 600) {
            const files = await fs.readdir(tempDir);
            const finishedFile = files.find(f => !f.endsWith('.crdownload'));
            if (finishedFile) {
                downloadedFile = path.join(tempDir, finishedFile);
                break;
            }
            await new Promise(r => setTimeout(r, 1000));
            attempts++;
        }

        if (!downloadedFile) {
            throw new Error("Timeout ao esperar o download do arquivo");
        }

        await fs.rename(downloadedFile, destPath);
    } catch (error) {
        log.error({ data: error }, `Erro ao baixar arquivo da URL: ${url}`);
        throw error;
    } finally {
        await fs.rm(tempDir, { recursive: true, force: true }).catch(() => { });
    }
}

function isFileValid(lastDownloadDate: Date, frequency: string, log: any): boolean {
    const now = new Date();
    let expirationDate: Date;

    switch (frequency.toLowerCase()) {
        case 'mensal':
            expirationDate = addMonths(lastDownloadDate, 1);
            break;
        case 'semanal':
            expirationDate = addDays(lastDownloadDate, 7);
            break;
        case 'diário':
            expirationDate = addDays(lastDownloadDate, 1);
            break;
        case 'a cada quatro horas':
            expirationDate = addHours(lastDownloadDate, 4);
            break;
        case 'sob demanda':
            return true;
        default:
            log.warn({ data: { frequency } }, `Periodicidade de atualização não mapeada: ${frequency}`);
            return false;
    }

    return isAfter(expirationDate, now);
}

async function processDownload(downloadUrl: string, destPath: string, uniqueFileName: string, file: FileMetadata, moduleName: string, page: Page, log: any): Promise<boolean> {
    const records = await bronze_db.select().from(FileDownloadHistory).where(eq(FileDownloadHistory.fileName, uniqueFileName)).limit(1);

    let fileExistsOnDisk = false;
    try {
        await fs.access(destPath);
        fileExistsOnDisk = true;
    } catch {
        fileExistsOnDisk = false;
    }

    if (records.length > 0 && isFileValid(records[0].downloadedAt, file.frequency, log) && fileExistsOnDisk) {
        log.info(`Arquivo ${uniqueFileName} ignorado (dentro da validade e presente no disco).`);
        return true;
    }

    // Garante que qualquer arquivo corrompido/parcial seja limpo antes do novo download
    await fs.rm(destPath, { force: true }).catch(() => { });

    log.info(`Baixando: ${downloadUrl} para ${destPath}`);
    await downloadZip(page, downloadUrl, destPath);

    // Pausa entre downloads para evitar rate limit do AWS WAF
    await new Promise(r => setTimeout(r, 2500));

    await bronze_db.delete(FileDownloadHistory).where(eq(FileDownloadHistory.fileName, uniqueFileName));
    await bronze_db.insert(FileDownloadHistory).values({
        fileName: uniqueFileName,
        moduleName,
        frequency: file.frequency,
        downloadedAt: new Date()
    });

    return true;
}

async function fetchFilesForModule(modules: ModuleData[], options?: { oneFilePerModule?: boolean }): Promise<void> {
    const log = logger.forMethod("fetchFilesForModule");
    log.info("Iniciando extração de arquivos por exercício e mês (Etapa 2)");

    let browser = await puppeteer.launch({
        headless: true,
        args: ['--disable-blink-features=AutomationControlled']
    });
    let page = await browser.newPage();

    await page.evaluateOnNewDocument(() => {
        Object.defineProperty(navigator, 'webdriver', { get: () => false });
    });

    for (const mod of modules) {
        for (const file of mod.files) {
            let fileDone = false; // Controle individual por item/arquivo do agrupamento

            if (!browser.connected) {
                log.warn("Conexão com o Puppeteer perdida. Recriando instância...");
                browser = await puppeteer.launch({
                    headless: true,
                    args: ['--disable-blink-features=AutomationControlled']
                });
                page = await browser.newPage();
                await page.evaluateOnNewDocument(() => {
                    Object.defineProperty(navigator, 'webdriver', { get: () => false });
                });
            }

            const pageUrl = `https://portaldatransparencia.gov.br${file.href}`;
            log.info(`Acessando página do arquivo: ${pageUrl}`);

            try {
                await page.goto(pageUrl, { waitUntil: "networkidle2" });

                const isSingleLinkVisible = await page.evaluate(() => {
                    const el = document.querySelector('#arquivo-unico');
                    return el && window.getComputedStyle(el).display !== 'none';
                });

                if (isSingleLinkVisible) {
                    const singleHref = await page.$eval('#link-unico a', el => el.getAttribute('href'));
                    if (singleHref) {
                        const fileName = singleHref.split('/').pop() + '.zip';
                        const destPath = path.join(ZIP_FILES_DIRECTORY, file.folderName, fileName);
                        const uniqueFileName = `${file.folderName}/${fileName}`;
                        const downloadUrl = `https://portaldatransparencia.gov.br${singleHref}`;

                        await processDownload(downloadUrl, destPath, uniqueFileName, file, mod.moduleName, page, log);
                    }
                    continue;
                }

                const hasOrigens = await page.evaluate(() => {
                    const el = document.querySelector('#origens');
                    return el && window.getComputedStyle(el).display !== 'none';
                });

                const origens = hasOrigens
                    ? await page.$$eval('#links-origens option', options => options.map(o => (o as HTMLOptionElement).value)) : [null]; for (const origem of origens) {
                        if (fileDone) break; if (origem) { await page.select('#links-origens', origem); await new Promise(r => setTimeout(r, 1500)); } const hasAnos = await page.evaluate(() => { const el = document.querySelector('#anos'); return el && window.getComputedStyle(el).display !== 'none'; }); if (hasAnos) {
                            const anos = await page.$$eval('#links-anos option', options => options.map(o => (o as HTMLOptionElement).value));

                            for (const ano of anos) {
                                if (fileDone) break;

                                await page.select('#links-anos', ano);
                                await new Promise(r => setTimeout(r, 1500));

                                const hasMeses = await page.evaluate(() => {
                                    const el = document.querySelector('#meses');
                                    const select = document.querySelector('#links-meses') as HTMLSelectElement | null;
                                    const isVisible = el && window.getComputedStyle(el).display !== 'none';
                                    return Boolean(isVisible && select && select.options.length > 0);
                                });

                                if (hasMeses) {
                                    const meses = await page.$$eval('#links-meses option', options => options.map(o => (o as HTMLOptionElement).value));
                                    for (const mes of meses) {
                                        if (fileDone) break;

                                        await page.select('#links-meses', mes); await new Promise(r => setTimeout(r, 1500));
                                        const hasDias = await page.evaluate(() => {
                                            const el = document.querySelector('#dias');
                                            const select = document.querySelector('#links-dias') as HTMLSelectElement || null;
                                            const isVisible = el && window.getComputedStyle(el).display !== 'none';
                                            return Boolean(isVisible && select && select.options.length > 0);
                                        });

                                        if (hasDias) {
                                            const dias = await page.$$eval('#links-dias option', options => options.map(o => (o as HTMLOptionElement).value));

                                            for (const dia of dias) {
                                                if (fileDone) break;

                                                await page.select('#links-dias', dia);
                                                await new Promise(r => setTimeout(r, 1000));

                                                const downloadPath = `${file.href}/${ano}${mes}${dia}${origem ? '_' + origem : ''}`;
                                                const downloadUrl = `https://portaldatransparencia.gov.br${downloadPath}`;
                                                const fileName = downloadPath.split('/').pop() + '.zip';
                                                const destPath = path.join(ZIP_FILES_DIRECTORY, file.folderName, fileName);
                                                const uniqueFileName = `${file.folderName}/${fileName}`;

                                                const success = await processDownload(downloadUrl, destPath, uniqueFileName, file, mod.moduleName, page, log);
                                                if (success && options?.oneFilePerModule) fileDone = true;
                                            }
                                        } else {
                                            const hasOrigensMes = await page.evaluate(() => {
                                                const el = document.querySelector('#origens-mes');
                                                const select = document.querySelector('#links-origens-mes') as HTMLSelectElement | null;
                                                const isVisible = el && window.getComputedStyle(el).display !== 'none';
                                                return Boolean(isVisible && select && select.options.length > 0);
                                            });

                                            if (hasOrigensMes) {
                                                const origensMes = await page.$$eval('#links-origens-mes option', options => options.map(o => (o as HTMLOptionElement).value));

                                                for (const origemMes of origensMes) {
                                                    if (fileDone) break;

                                                    await page.select('#links-origens-mes', origemMes);
                                                    await new Promise(r => setTimeout(r, 1000));

                                                    const downloadPath = `${file.href}/${ano}${mes}_${origemMes}`;
                                                    const downloadUrl = `https://portaldatransparencia.gov.br${downloadPath}`;
                                                    const fileName = downloadPath.split('/').pop() + '.zip';
                                                    const destPath = path.join(ZIP_FILES_DIRECTORY, file.folderName, fileName);
                                                    const uniqueFileName = `${file.folderName}/${fileName}`;

                                                    const success = await processDownload(downloadUrl, destPath, uniqueFileName, file, mod.moduleName, page, log);
                                                    if (success && options?.oneFilePerModule) fileDone = true;
                                                }
                                            } else {
                                                const downloadPath = `${file.href}/${ano}${mes}${origem ? '_' + origem : ''}`;
                                                const downloadUrl = `https://portaldatransparencia.gov.br${downloadPath}`;
                                                const fileName = downloadPath.split('/').pop() + '.zip';
                                                const destPath = path.join(ZIP_FILES_DIRECTORY, file.folderName, fileName);
                                                const uniqueFileName = `${file.folderName}/${fileName}`;

                                                const success = await processDownload(downloadUrl, destPath, uniqueFileName, file, mod.moduleName, page, log);
                                                if (success && options?.oneFilePerModule) fileDone = true;
                                            }
                                        }
                                    }
                                } else {
                                    const dynamicHref = await page.$eval('#link', el => el.getAttribute('href')).catch(() => null);
                                    let downloadPath = dynamicHref;

                                    if (!downloadPath) {
                                        const linkHref = await page.evaluate(() => {
                                            const a = document.querySelector('#anos a') as HTMLAnchorElement | null;
                                            return a ? a.getAttribute('href') : null;
                                        });
                                        downloadPath = linkHref || `${file.href}/${ano}${origem ? '_' + origem : ''}`;
                                    }

                                    const downloadUrl = `https://portaldatransparencia.gov.br${downloadPath}`;
                                    const fileName = downloadPath.split('/').pop() + '.zip';
                                    const destPath = path.join(ZIP_FILES_DIRECTORY, file.folderName, fileName);
                                    const uniqueFileName = `${file.folderName}/${fileName}`;

                                    const success = await processDownload(downloadUrl, destPath, uniqueFileName, file, mod.moduleName, page, log);
                                    if (success && options?.oneFilePerModule) fileDone = true;
                                }
                            }
                        }
                    }
            } catch (error) {
                log.error({ data: error }, `Erro ao processar o arquivo ${file.name}`);
            }
        }
    }

    if (browser.connected) {
        await browser.close();
    }
    log.info("Etapa 2 concluída com sucesso.");
}

export async function runScrapper(options?: { oneFilePerModule?: boolean }) {
    const log = logger.forMethod("runScrapper");
    try {
        log.info("Iniciando orquestração do scrapper");

        const modules = await fetchPortalData();
        await ensureDirectories(modules);
        await fetchFilesForModule(modules, options);

        log.info("Orquestração do scrapper finalizada com sucesso");
        return;
    } catch (error) {
        log.error({ data: error }, "Erro fatal durante a execução do scrapper");
        throw error;
    }
}