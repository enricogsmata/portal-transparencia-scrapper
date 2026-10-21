import { silver_db } from '../../../database/silver_connection.js';
import { municipios } from '../../../database/silver_schema.js';
import { new_logger } from '../../../services/logs.js';

const logger = new_logger(import.meta.url);

export class MunicipioResolver {
  private static bySiafi = new Map<string, number>();
  private static byIbge = new Map<string, number>();
  private static byNomeUf = new Map<string, number>();

  static async load(db: typeof silver_db = silver_db): Promise<void> {
    const log = logger.forMethod('MunicipioResolver.load');
    log.info('Carregando tabela de municípios em memória para indexação rápida');

    const todos = await db.select().from(municipios);

    this.bySiafi.clear();
    this.byIbge.clear();
    this.byNomeUf.clear();

    for (const m of todos) {
      if (m.codigoSiafi) {
        this.bySiafi.set(m.codigoSiafi.trim(), m.id);
      }
      if (m.codigoIbge) {
        const ibge = m.codigoIbge.trim();
        this.byIbge.set(ibge, m.id);
        if (ibge.length === 7) {
          this.byIbge.set(ibge.substring(0, 6), m.id);
        }
      }
      const chaveNome = `${this.sanitizar(m.nome)}|${m.ufSigla.trim().toUpperCase()}`;
      this.byNomeUf.set(chaveNome, m.id);
    }

    log.info({ data: { totalIndexed: todos.length } }, 'Índices de municípios carregados com sucesso');
  }

  static sanitizar(texto: string): string {
    return texto
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .replace(/[^a-zA-Z0-9 ]/g, '')
      .replace(/\s+/g, ' ')
      .trim()
      .toUpperCase();
  }

  static resolve(params: { siafi?: string | null; ibge?: string | null; nome?: string | null; uf?: string | null }): number | null {
    if (params.siafi) {
      const id = this.bySiafi.get(params.siafi.trim());
      if (id) return id;
    }

    if (params.ibge) {
      const id = this.byIbge.get(params.ibge.trim());
      if (id) return id;
    }

    if (params.nome && params.uf) {
      const chave = `${this.sanitizar(params.nome)}|${params.uf.trim().toUpperCase()}`;
      const id = this.byNomeUf.get(chave);
      if (id) return id;
    }

    return null;
  }
}