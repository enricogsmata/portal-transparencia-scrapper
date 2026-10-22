/**
 * @file scrapper_orchestrator.ts
 * @description Orquestrador principal da etapa de scraping (Camada Bronze).
 *
 * Responsabilidades:
 *  1. Descobrir os módulos de dados disponíveis no Portal da Transparência via HTTP + parse HTML (Cheerio).
 *  2. Descobrir e indexar dados históricos (antes de 2013) em uma etapa adicional não-bloqueante.
 *  3. Garantir a existência dos diretórios de destino para cada módulo.
 *  4. Navegar nas páginas de detalhes de cada arquivo com Puppeteer (browser headless) e realizar
 *     o download dos ZIPs resolvendo a hierarquia de filtros: origem → ano → mês → dia.
 *  5. Controlar a validade dos downloads via banco Bronze (FileDownloadHistory) para evitar
 *     downloads redundantes dentro do prazo de atualização de cada fonte.
 *
 * Estratégias anti-WAF (AWS Web Application Firewall):
 *  - User-Agent realista em todas as requisições HTTP
 *  - Desativação da propriedade `navigator.webdriver` via evaluateOnNewDocument
 *  - Flag `--disable-blink-features=AutomationControlled` no Puppeteer
 *  - Pausas programáticas entre downloads (2.500 ms)
 *  - Retry com espera crescente ao carregar páginas de download
 */
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
const logger = new_logger(import.meta.url);
const PORTAL_ORIGIN = "https://portaldatransparencia.gov.br";
const BASE_URL = `${PORTAL_ORIGIN}/download-de-dados`;
// Fallback caso o link do "Histórico de Dados" não seja encontrado na página principal
const HISTORICO_FALLBACK_PATH = "/download-de-dados/historico/paginas-de-transparencia";
const HISTORICO_MODULE_NAME = "Histórico de Dados (antes de 2013)";
const HISTORICO_FOLDER_PREFIX = "historico-";
// User-Agent de navegador real para mitigar bloqueios do AWS WAF
const DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

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

/**
 * Etapa 1 do Scrapper — Descoberta de Módulos
 *
 * Realiza um GET na página principal de downloads do Portal da Transparência e parseia
 * o HTML com Cheerio para extrair a lista de módulos disponíveis.
 *
 * A página é estruturada como um acordeão (`#accordion1`) onde cada `.item` representa
 * um módulo (ex: Despesas, Contratos, Servidores). Dentro de cada módulo, uma tabela
 * `tbody tr` lista os arquivos disponíveis com nome, link e frequência de atualização.
 *
 * @returns Lista de ModuleData com todos os módulos e seus respectivos arquivos.
 */
async function fetchPortalData(): Promise<ModuleData[]> {
    const log = logger.forMethod("fetchPortalData");
    log.info(`Buscando dados da página: ${BASE_URL}`);

    const { data } = await axios.get(BASE_URL, {
        headers: {
            "User-Agent": DEFAULT_USER_AGENT
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

    if (modules.length === 0) {
        log.warn("Nenhum módulo encontrado na página principal (possível bloqueio do WAF ou mudança de layout).");
    }

    log.info(`Extração concluída. ${modules.length} módulos encontrados.`);
    return modules;
}

/**
 * Normaliza um href (absoluto ou relativo) para um path relativo à origem do portal.
 */
function toPortalPath(href: string): string {
    if (href.startsWith("http")) {
        return new URL(href).pathname;
    }
    return href.startsWith("/") ? href : `/${href}`;
}

/**
 * Step adicional: descobre, na página principal, o link de "Histórico de Dados (antes de 2013)"
 * e lista os arquivos históricos (execução orçamentária, licitações, contratos, diárias e passagens).
 * Cada arquivo histórico é um arquivo único (#arquivo-unico), sem filtros de ano/mês.
 */
async function fetchHistoricalData(): Promise<ModuleData[]> {
    const log = logger.forMethod("fetchHistoricalData");
    log.info("Iniciando extração dos dados históricos (Etapa 1.1)");

    const headers = { "User-Agent": DEFAULT_USER_AGENT };

    // 1. Descobre o link do histórico na página principal
    let historicoPath = HISTORICO_FALLBACK_PATH;
    try {
        const { data: mainPage } = await axios.get(BASE_URL, { headers });
        const $main = cheerio.load(mainPage);
        const historicoHref = $main("a")
            .filter((_, el) => {
                const href = $main(el).attr("href") || "";
                const text = $main(el).text().toLowerCase();
                return href.includes("/download-de-dados/historico/") || text.includes("histórico de dados");
            })
            .first()
            .attr("href");

        if (historicoHref) {
            historicoPath = toPortalPath(historicoHref);
        } else {
            log.warn(`Link do histórico não encontrado na página principal. Usando fallback: ${HISTORICO_FALLBACK_PATH}`);
        }
    } catch (error) {
        log.warn({ data: error }, `Falha ao ler a página principal para localizar o histórico. Usando fallback: ${HISTORICO_FALLBACK_PATH}`);
    }

    // 2. Lista os arquivos históricos disponíveis
    const historicoUrl = `${PORTAL_ORIGIN}${historicoPath}`;
    log.info(`Buscando arquivos históricos em: ${historicoUrl}`);
    const { data } = await axios.get(historicoUrl, { headers });
    const $ = cheerio.load(data);

    const files: FileMetadata[] = [];
    const seen = new Set<string>();

    $(".tabela-receptor tbody tr td a").each((_, el) => {
        const name = $(el).text().trim();
        const href = $(el).attr("href") || "";
        if (!href) return;

        const hrefPath = toPortalPath(href);
        if (seen.has(hrefPath)) return;
        seen.add(hrefPath);

        const slug = hrefPath.split("/").filter(Boolean).pop() || "";
        if (!slug) return;

        files.push({
            name,
            href: hrefPath,
            // Prefixo evita colisão de diretório/chave de histórico com os módulos regulares
            folderName: `${HISTORICO_FOLDER_PREFIX}${slug}`,
            // Dados anteriores a 2013 não são mais atualizados: baixa uma vez e reaproveita
            frequency: "sob demanda"
        });
    });

    if (files.length === 0) {
        log.warn("Nenhum arquivo histórico encontrado na página de histórico.");
        return [];
    }

    log.info(`Extração histórica concluída. ${files.length} arquivos encontrados.`);
    return [{ moduleName: HISTORICO_MODULE_NAME, files }];
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

/**
 * Valida se um arquivo já baixado ainda está dentro do prazo de validade
 * conforme a frequência de atualização declarada pelo Portal da Transparência.
 *
 * Tabela de validade:
 *  - mensal              → 30 dias a partir do último download
 *  - semanal             → 7 dias a partir do último download
 *  - diário              → 1 dia a partir do último download
 *  - a cada quatro horas → 4 horas a partir do último download
 *  - sob demanda         → sempre válido (dados históricos imutáveis)
 *
 * @param lastDownloadDate Data do último download registrado no banco Bronze
 * @param frequency        Frequência declarada pelo portal (ex: "mensal", "diário")
 * @param log              Logger contextualizado do chamador
 * @returns true se o arquivo ainda está válido e não precisa ser rebaixado
 */
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

/**
 * Controla o ciclo completo de um download individual:
 *  1. Consulta o banco Bronze (FileDownloadHistory) para verificar se o arquivo já foi baixado.
 *  2. Verifica se o arquivo físico ainda existe no disco.
 *  3. Se ambas as condições forem verdadeiras E o arquivo estiver dentro da validade → ignora.
 *  4. Caso contrário, remove qualquer arquivo corrompido/parcial, executa o download via Puppeteer
 *     e registra/atualiza o histórico no banco Bronze.
 *
 * Este método garante a idempotência da etapa de scraping:
 * re-executar o pipeline nunca baixa um arquivo desnecessariamente.
 *
 * @param downloadUrl    URL completa do arquivo ZIP a ser baixado
 * @param destPath       Caminho local de destino para o ZIP
 * @param uniqueFileName Chave única do arquivo no formato `<folderName>/<fileName>`
 * @param file           Metadados do arquivo (frequência, folderName, etc.)
 * @param moduleName     Nome do módulo temático (para log e histórico)
 * @param page           Instância ativa do Puppeteer Page
 * @param log            Logger contextualizado do chamador
 * @returns true se o arquivo foi baixado ou já estava válido
 */
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

/**
 * Abre a página de detalhes de um arquivo e só retorna quando algum dos blocos de download
 * (arquivo único, origens ou anos) estiver visível. Se o conteúdo não aparecer (ex.: página de
 * desafio do AWS WAF na primeira requisição), recarrega a página com espera crescente.
 */
async function loadFilePage(page: Page, pageUrl: string, log: any, maxAttempts = 3): Promise<void> {
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        await page.goto(pageUrl, { waitUntil: "networkidle2" });

        try {
            await page.waitForFunction(() => {
                return ['#arquivo-unico', '#origens', '#anos'].some(selector => {
                    const el = document.querySelector(selector);
                    return el && window.getComputedStyle(el).display !== 'none';
                });
            }, { timeout: 15000 });
            return;
        } catch {
            log.warn(`Conteúdo de download não apareceu em ${pageUrl} (tentativa ${attempt}/${maxAttempts}).`);
            if (attempt < maxAttempts) {
                await new Promise(r => setTimeout(r, 3000 * attempt));
            }
        }
    }

    throw new Error(`Página sem opções de download após ${maxAttempts} tentativas: ${pageUrl}`);
}

/**
 * Etapa 2 do Scrapper — Download dos Arquivos por Módulo
 *
 * Para cada módulo e arquivo descoberto na Etapa 1, acessa a página de detalhes no portal
 * via Puppeteer e resolve adaptativamente a hierarquia de filtros disponíveis:
 *
 *   #arquivo-unico → download direto sem filtros
 *   #origens       → seletor de origem dos dados
 *   #anos          → seletor de exercício (ano)
 *   #meses         → seletor de mês de referência
 *   #dias          → seletor de dia (para dados diários)
 *   #origens-mes   → seletor de origem por mês (ex: CPGF)
 *
 * Para cada combinação válida de filtros, constrói a URL de download e delega a
 * `processDownload` que executa o controle de idempotência e o download via CDP.
 *
 * Estratégias anti-detecção de automação ativas nesta etapa:
 *  - Flag `--disable-blink-features=AutomationControlled` na inicialização do Puppeteer
 *  - Override de `navigator.webdriver` para `false` via `evaluateOnNewDocument`
 *  - Reconexão automática da instância do browser em caso de desconexão
 *
 * @param modules  Lista completa de módulos (regulares + históricos) com seus arquivos
 * @param options  Opções de execução: `oneFilePerModule` limita a 1 download por módulo (modo teste)
 */
async function fetchFilesForModule(modules: ModuleData[], options?: { oneFilePerModule?: boolean }): Promise<void> {
    const log = logger.forMethod("fetchFilesForModule");
    log.info("Iniciando extração de arquivos por exercício e mês (Etapa 2)");

    let browser = await puppeteer.launch({
        headless: true,
        args: ['--disable-blink-features=AutomationControlled']
    });
    let page = await browser.newPage();

    // Desativa a flag `navigator.webdriver` para mitigar detecção de automação pelo WAF
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
                await loadFilePage(page, pageUrl, log);

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

/**
 * Ponto de entrada público do Scrapper — Orquestração Completa da Etapa de Extração
 *
 * Executa as quatro sub-etapas do scrapper em sequência:
 *  1. `fetchPortalData`       → Descobre módulos regulares via HTTP + Cheerio
 *  2. `fetchHistoricalData`   → Descobre arquivos históricos (falha não-bloqueante)
 *  3. `ensureDirectories`     → Cria diretórios de destino para todos os módulos
 *  4. `fetchFilesForModule`   → Navega, resolve filtros e baixa os ZIPs via Puppeteer
 *
 * Em caso de falha fatal (etapas 1, 3 ou 4), o erro é relançado para que o chamador
 * (index.ts ou index-cli.ts) possa tomar a ação apropriada (ex: encerrar o processo).
 *
 * @param options.oneFilePerModule Se true, baixa apenas o primeiro arquivo pendente
 *                                 de cada módulo — útil para validações rápidas.
 */
export async function runScrapper(options?: { oneFilePerModule?: boolean }) {
    const log = logger.forMethod("runScrapper");
    try {
        log.info("Iniciando orquestração do scrapper");

        // Etapa 1: Descoberta dos módulos regulares via página principal do portal
        const modules = await fetchPortalData();

        // Step adicional: dados históricos (antes de 2013). Falha aqui não deve derrubar o fluxo regular.
        let historicalModules: ModuleData[] = [];
        try {
            historicalModules = await fetchHistoricalData();
        } catch (error) {
            log.error({ data: error }, "Erro ao extrair dados históricos. Seguindo apenas com os módulos regulares.");
        }

        // Combina módulos regulares e históricos em uma única lista para processamento unificado
        const allModules = [...modules, ...historicalModules];

        // Etapa 3: Garante que todos os diretórios de destino existam antes de iniciar downloads
        await ensureDirectories(allModules);
        // Etapa 4: Navega nas páginas de detalhes e executa os downloads resolvendo filtros hierárquicos
        await fetchFilesForModule(allModules, options);

        log.info("Orquestração do scrapper finalizada com sucesso");
        return;
    } catch (error) {
        log.error({ data: error }, "Erro fatal durante a execução do scrapper");
        throw error;
    }
}