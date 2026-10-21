import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { sql } from 'drizzle-orm';
import { silver_db } from '../../../database/silver_connection.js';
import { municipios } from '../../../database/silver_schema.js';
import { MunicipioResolver } from '../shared/municipio-resolver.js';
import { new_logger } from '../../../services/logs.js';

const logger = new_logger(import.meta.url);

interface MunicipioSeedItem {
  ufSigla: string;
  codigoIbge?: string | null;
  codigoSiafi?: string | null;
  nome: string;
}

export async function initMunicipios(): Promise<void> {
  const log = logger.forMethod('initMunicipios');
  log.info('Iniciando carga e upsert da dimensão municípios a partir do seed estático');

  try {
    const __dirname = path.dirname(fileURLToPath(import.meta.url));
    const seedFilePath = path.resolve(__dirname, '../../../files/municipios/municipios_seed.json');

    const fileContent = await fs.readFile(seedFilePath, 'utf-8');
    const seedData: MunicipioSeedItem[] = JSON.parse(fileContent);

    log.info({ data: { totalRegistros: seedData.length, seedFilePath } }, 'Arquivo seed de municípios lido com sucesso');

    const BATCH_SIZE = 1000;
    for (let i = 0; i < seedData.length; i += BATCH_SIZE) {
      const chunk = seedData.slice(i, i + BATCH_SIZE).map((m) => ({
        ufSigla: m.ufSigla.trim().toUpperCase(),
        codigoIbge: m.codigoIbge ? m.codigoIbge.trim() : null,
        codigoSiafi: m.codigoSiafi ? m.codigoSiafi.trim() : null,
        nome: m.nome.trim(),
      }));

      await silver_db
        .insert(municipios)
        .values(chunk)
        .onDuplicateKeyUpdate({
          set: {
            nome: sql`VALUES(nome)`,
            ufSigla: sql`VALUES(uf_sigla)`,
            codigoSiafi: sql`COALESCE(VALUES(codigo_siafi), municipios.codigo_siafi)`,
            codigoIbge: sql`COALESCE(VALUES(codigo_ibge), municipios.codigo_ibge)`,
          },
        });
    }

    log.info('Upsert da tabela municipios finalizado. Inicializando MunicipioResolver em memória');
    await MunicipioResolver.load(silver_db);
    log.info('MunicipioResolver carregado com sucesso');
  } catch (error) {
    log.error({ data: error }, 'Erro fatal durante a inicialização da dimensão municípios');
    throw error;
  }
}