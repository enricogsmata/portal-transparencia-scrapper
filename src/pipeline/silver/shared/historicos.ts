// src/pipeline/silver/shared/historicos.ts
// Helpers compartilhados pelos transformers dos módulos históricos.
import { sql, inArray } from 'drizzle-orm';
import { silver_db } from '../../../database/silver_connection.js';
import {
  unidadesGestoras,
  pessoasJuridicas,
  orgaosSuperiores,
  orgaosSubordinados,
  unidadesOrcamentarias,
} from '../../../database/silver_schema.js';
import { formatCnpj } from './formatters.js';
import { BATCH_SIZE } from './constants.js';

export const CHUNK = BATCH_SIZE;

export function chunk<T>(arr: T[], size = CHUNK): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

export function soDigitos(v?: string | null): string {
  return (v ?? '').replace(/\D/g, '');
}

/** Retorna o CNPJ formatado apenas se o documento tiver 14 dígitos (CPF/mascarado/ausente => null). */
export function cnpjOuNull(v?: string | null): string | null {
  const d = soDigitos(v);
  if (d.length !== 14) return null;
  return formatCnpj(v);
}

/** Aceita '1', '01', '2024-03', '03/2024', '2024/03'. Retorna 'YYYY-MM' ou null. */
export function toAnoMes(ano?: string | null, mes?: string | null): string | null {
  const a = soDigitos(ano);
  const m = parseInt(soDigitos(mes), 10);
  if (a.length !== 4 || !Number.isFinite(m) || m < 1 || m > 12) return null;
  return `${a}-${String(m).padStart(2, '0')}`;
}

/** Valor monetário: parseDecimal já devolve string normalizada; aqui só garantimos fallback. */
export function nz(v: string | null | undefined, fallback = '0'): string {
  return v && v.length > 0 ? v : fallback;
}

/** Normaliza texto usado como parte de chave em memória (o MySQL compara case-insensitive). */
export function k(v?: string | null): string {
  return (v ?? '').trim().toUpperCase();
}

export async function ensureUnidadesGestoras(
  entries: { codigo: string; nome: string }[],
): Promise<Map<string, number>> {
  const validEntries = entries
    .map((e) => ({
      codigo: (e.codigo ?? '').trim(),
      nome: (e.nome ?? '').trim() || 'NÃO INFORMADO',
    }))
    .filter((e) => e.codigo.length > 0 && e.codigo.length <= 30);

  const uniq = new Map(validEntries.map((e) => [e.codigo, { codigo: e.codigo, nome: e.nome.substring(0, 255) }]));
  const list = Array.from(uniq.values());
  if (list.length === 0) return new Map();

  for (const part of chunk(list)) {
    await silver_db.insert(unidadesGestoras).values(part).onDuplicateKeyUpdate({ set: { id: sql`id` } });
  }
  const rows = await silver_db
    .select({ id: unidadesGestoras.id, codigo: unidadesGestoras.codigo })
    .from(unidadesGestoras)
    .where(inArray(unidadesGestoras.codigo, list.map((u) => u.codigo)));
  return new Map(rows.map((r) => [r.codigo, r.id]));
}

export async function ensurePessoasJuridicas(
  entries: { cnpj: string; razaoSocial: string }[],
): Promise<Map<string, number>> {
  const uniq = new Map(entries.map((e) => [e.cnpj, e]));
  const list = Array.from(uniq.values());
  if (list.length === 0) return new Map();

  for (const part of chunk(list)) {
    await silver_db.insert(pessoasJuridicas).values(part).onDuplicateKeyUpdate({ set: { id: sql`id` } });
  }
  const rows = await silver_db
    .select({ id: pessoasJuridicas.id, cnpj: pessoasJuridicas.cnpj })
    .from(pessoasJuridicas)
    .where(inArray(pessoasJuridicas.cnpj, list.map((p) => p.cnpj)));
  return new Map(rows.map((r) => [r.cnpj, r.id]));
}

export interface UoInput {
  codigoOrgaoSuperior?: string | null;
  nomeOrgaoSuperior?: string | null;
  codigoOrgao?: string | null;
  nomeOrgao?: string | null;
  codigoUnidadeOrcamentaria?: string | null;
  nomeUnidadeOrcamentaria?: string | null;
}

/**
 * Garante órgão superior -> órgão subordinado -> unidade orçamentária e devolve codigoUO -> id.
 * Cria apenas o que faltar (no-op em duplicata), então não sobrescreve cadastros mais ricos.
 * Órgão superior/subordinado são tratados como SIAFI (mesma unicidade (sistema, código) do silver).
 */
export async function ensureUnidadesOrcamentarias(rows: UoInput[]): Promise<Map<string, number>> {
  // 1. Órgãos superiores
  const supMap = new Map<string, typeof orgaosSuperiores.$inferInsert>();
  for (const r of rows) {
    if (r.codigoOrgaoSuperior) {
      supMap.set(r.codigoOrgaoSuperior, {
        sistemaOrigem: 'SIAFI',
        codigo: r.codigoOrgaoSuperior,
        nome: r.nomeOrgaoSuperior || 'NÃO INFORMADO',
      });
    }
  }
  const sups = Array.from(supMap.values());
  for (const part of chunk(sups)) {
    await silver_db.insert(orgaosSuperiores).values(part).onDuplicateKeyUpdate({ set: { id: sql`id` } });
  }
  const supDb = sups.length
    ? await silver_db
      .select({ id: orgaosSuperiores.id, codigo: orgaosSuperiores.codigo })
      .from(orgaosSuperiores)
      .where(inArray(orgaosSuperiores.codigo, sups.map((s) => s.codigo)))
    : [];
  const supIdMap = new Map(supDb.map((s) => [s.codigo, s.id]));

  // 2. Órgãos subordinados (exigem superior)
  const subMap = new Map<string, typeof orgaosSubordinados.$inferInsert>();
  for (const r of rows) {
    const supId = r.codigoOrgaoSuperior ? supIdMap.get(r.codigoOrgaoSuperior) : null;
    if (r.codigoOrgao && supId) {
      subMap.set(r.codigoOrgao, {
        orgaoSuperiorId: supId,
        sistemaOrigem: 'SIAFI',
        codigo: r.codigoOrgao,
        nome: r.nomeOrgao || 'NÃO INFORMADO',
      });
    }
  }
  const subs = Array.from(subMap.values());
  for (const part of chunk(subs)) {
    await silver_db.insert(orgaosSubordinados).values(part).onDuplicateKeyUpdate({ set: { id: sql`id` } });
  }
  const subDb = subs.length
    ? await silver_db
      .select({ id: orgaosSubordinados.id, codigo: orgaosSubordinados.codigo })
      .from(orgaosSubordinados)
      .where(inArray(orgaosSubordinados.codigo, subs.map((s) => s.codigo)))
    : [];
  const subIdMap = new Map(subDb.map((s) => [s.codigo, s.id]));

  // 3. Unidades orçamentárias
  const uoMap = new Map<string, typeof unidadesOrcamentarias.$inferInsert>();
  for (const r of rows) {
    if (r.codigoUnidadeOrcamentaria) {
      uoMap.set(r.codigoUnidadeOrcamentaria, {
        codigo: r.codigoUnidadeOrcamentaria,
        nome: r.nomeUnidadeOrcamentaria || 'NÃO INFORMADO',
        orgaoSubordinadoId: r.codigoOrgao ? subIdMap.get(r.codigoOrgao) ?? null : null,
      });
    }
  }
  const uos = Array.from(uoMap.values());
  for (const part of chunk(uos)) {
    await silver_db.insert(unidadesOrcamentarias).values(part).onDuplicateKeyUpdate({ set: { id: sql`id` } });
  }
  const uoDb = uos.length
    ? await silver_db
      .select({ id: unidadesOrcamentarias.id, codigo: unidadesOrcamentarias.codigo })
      .from(unidadesOrcamentarias)
      .where(inArray(unidadesOrcamentarias.codigo, uos.map((u) => u.codigo)))
    : [];
  return new Map(uoDb.map((u) => [u.codigo, u.id]));
}