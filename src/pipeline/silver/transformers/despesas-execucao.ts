import { sql, inArray, and, eq } from 'drizzle-orm';
import { bronze_db } from '../../../database/bronze_connection.js';
import { silver_db } from '../../../database/silver_connection.js';
import { ExecucaoDespesa } from '../../../database/bronze_schema.js';
import {
    unidadesGestoras,
    acoesOrcamentarias,
    categoriasDespesa,
    gruposDespesa,
    elementosDespesa,
    modalidadesAplicacao,
    execucaoDespesas
} from '../../../database/silver_schema.js';
import { new_logger } from '../../../services/logs.js';
import { parseDecimal } from '../shared/formatters.js';
import { MunicipioResolver } from '../shared/municipio-resolver.js';

const logger = new_logger(import.meta.url);

function formatAnoMes(anoMes?: string | null): string | null {
    if (!anoMes) return null;
    const cleaned = anoMes.replace(/\D/g, '');
    if (cleaned.length === 6) {
        return `${cleaned.substring(0, 4)}-${cleaned.substring(4, 6)}`;
    }
    return cleaned.substring(0, 7);
}

export async function processExecucaoDespesa() {
    const log = logger.forMethod('processExecucaoDespesa');
    const BATCH_SIZE = 1000;
    let offset = 0;
    let hasMore = true;

    log.info('Iniciando processamento do módulo Despesas - Execução (Agregada Mensal)');

    try {
        while (hasMore) {
            const bronzeData = await bronze_db.select()
                .from(ExecucaoDespesa)
                .limit(BATCH_SIZE)
                .offset(offset);

            if (bronzeData.length === 0) {
                log.info({ data: { offset } }, 'Processamento concluído. Nenhum registro adicional encontrado.');
                hasMore = false;
                break;
            }

            log.info({ data: { offset, batchSize: bronzeData.length } }, 'Lote extraído da tabela execucao_despesa');

            // 1. Dimensão Unidades Gestoras
            const ugToUpsertMap = new Map<string, typeof unidadesGestoras.$inferInsert>();
            bronzeData.forEach(b => {
                if (b.codigoUnidadeGestora) {
                    ugToUpsertMap.set(b.codigoUnidadeGestora, {
                        codigo: b.codigoUnidadeGestora,
                        nome: b.nomeUnidadeGestora || 'NÃO INFORMADO'
                    });
                }
            });
            const ugToUpsert = Array.from(ugToUpsertMap.values());
            if (ugToUpsert.length > 0) {
                await silver_db.insert(unidadesGestoras)
                    .values(ugToUpsert)
                    .onDuplicateKeyUpdate({ set: { id: sql`id` } });
            }

            const ugsDb = ugToUpsert.length > 0
                ? await silver_db.select({ id: unidadesGestoras.id, codigo: unidadesGestoras.codigo })
                    .from(unidadesGestoras)
                    .where(inArray(unidadesGestoras.codigo, ugToUpsert.map(u => u.codigo)))
                : [];
            const ugIdMap = new Map(ugsDb.map(ug => [ug.codigo, ug.id]));

            // 2. Dimensão Ações Orçamentárias
            const acoesToUpsertMap = new Map<string, typeof acoesOrcamentarias.$inferInsert>();
            bronzeData.forEach(b => {
                if (b.codigoAcao) {
                    acoesToUpsertMap.set(b.codigoAcao, {
                        codigo: b.codigoAcao,
                        nome: b.nomeAcao || 'NÃO INFORMADO',
                        linguagemCidada: null
                    });
                }
            });
            const acoesToUpsert = Array.from(acoesToUpsertMap.values());
            if (acoesToUpsert.length > 0) {
                const acoesEx = await silver_db.select({ codigo: acoesOrcamentarias.codigo }).from(acoesOrcamentarias).where(inArray(acoesOrcamentarias.codigo, acoesToUpsert.map(a => a.codigo)));
                const acoesExSet = new Set(acoesEx.map(a => a.codigo));
                const toInsert = acoesToUpsert.filter(a => !acoesExSet.has(a.codigo));
                if (toInsert.length > 0) await silver_db.insert(acoesOrcamentarias).values(toInsert);
            }

            const acoesDb = acoesToUpsert.length > 0
                ? await silver_db.select({ id: acoesOrcamentarias.id, codigo: acoesOrcamentarias.codigo })
                    .from(acoesOrcamentarias)
                    .where(inArray(acoesOrcamentarias.codigo, acoesToUpsert.map(a => a.codigo)))
                : [];
            const acaoIdMap = new Map(acoesDb.map(a => [a.codigo, a.id]));

            // 3. Dimensão Categorias
            const catToUpsertMap = new Map<string, typeof categoriasDespesa.$inferInsert>();
            bronzeData.forEach(b => {
                if (b.codigoCategoriaEconomica) catToUpsertMap.set(b.codigoCategoriaEconomica, { codigo: b.codigoCategoriaEconomica, nome: b.nomeCategoriaEconomica || 'NÃO INFORMADO' });
            });
            const catToUpsert = Array.from(catToUpsertMap.values());
            if (catToUpsert.length > 0) await silver_db.insert(categoriasDespesa).values(catToUpsert).onDuplicateKeyUpdate({ set: { nome: sql`VALUES(nome)` } });
            const catDb = catToUpsert.length > 0 ? await silver_db.select({ id: categoriasDespesa.id, codigo: categoriasDespesa.codigo }).from(categoriasDespesa).where(inArray(categoriasDespesa.codigo, catToUpsert.map(f => f.codigo))) : [];
            const catIdMap = new Map(catDb.map(f => [f.codigo, f.id]));

            // 4. Dimensão Grupos
            const grupToUpsertMap = new Map<string, typeof gruposDespesa.$inferInsert>();
            bronzeData.forEach(b => {
                if (b.codigoGrupoDespesa) grupToUpsertMap.set(b.codigoGrupoDespesa, { codigo: b.codigoGrupoDespesa, nome: b.nomeGrupoDespesa || 'NÃO INFORMADO' });
            });
            const grupToUpsert = Array.from(grupToUpsertMap.values());
            if (grupToUpsert.length > 0) await silver_db.insert(gruposDespesa).values(grupToUpsert).onDuplicateKeyUpdate({ set: { nome: sql`VALUES(nome)` } });
            const grupDb = grupToUpsert.length > 0 ? await silver_db.select({ id: gruposDespesa.id, codigo: gruposDespesa.codigo }).from(gruposDespesa).where(inArray(gruposDespesa.codigo, grupToUpsert.map(f => f.codigo))) : [];
            const grupIdMap = new Map(grupDb.map(f => [f.codigo, f.id]));

            // 5. Dimensão Elementos
            const elemToUpsertMap = new Map<string, typeof elementosDespesa.$inferInsert>();
            bronzeData.forEach(b => {
                if (b.codigoElementoDespesa) elemToUpsertMap.set(b.codigoElementoDespesa, { codigo: b.codigoElementoDespesa, nome: b.nomeElementoDespesa || 'NÃO INFORMADO' });
            });
            const elemToUpsert = Array.from(elemToUpsertMap.values());
            if (elemToUpsert.length > 0) await silver_db.insert(elementosDespesa).values(elemToUpsert).onDuplicateKeyUpdate({ set: { nome: sql`VALUES(nome)` } });
            const elemDb = elemToUpsert.length > 0 ? await silver_db.select({ id: elementosDespesa.id, codigo: elementosDespesa.codigo }).from(elementosDespesa).where(inArray(elementosDespesa.codigo, elemToUpsert.map(f => f.codigo))) : [];
            const elemIdMap = new Map(elemDb.map(f => [f.codigo, f.id]));

            // 6. Dimensão Modalidades
            const modToUpsertMap = new Map<string, typeof modalidadesAplicacao.$inferInsert>();
            bronzeData.forEach(b => {
                if (b.codigoModalidadeDespesa) {
                    modToUpsertMap.set(b.codigoModalidadeDespesa, {
                        codigo: b.codigoModalidadeDespesa,
                        nome: b.modalidadeDespesa ? b.modalidadeDespesa.slice(0, 255) : 'NÃO INFORMADO',
                    });
                }
            });
            const modToUpsert = Array.from(modToUpsertMap.values());
            if (modToUpsert.length > 0) await silver_db.insert(modalidadesAplicacao).values(modToUpsert).onDuplicateKeyUpdate({ set: { nome: sql`VALUES(nome)` } });
            const modDb = modToUpsert.length > 0 ? await silver_db.select({ id: modalidadesAplicacao.id, codigo: modalidadesAplicacao.codigo }).from(modalidadesAplicacao).where(inArray(modalidadesAplicacao.codigo, modToUpsert.map(f => f.codigo))) : [];
            const modIdMap = new Map(modDb.map(f => [f.codigo, f.id]));

            // 8. Entidade de Execução de Despesas
            const execucoesToInsert = bronzeData.map(b => {
                const anoMesLancamento = formatAnoMes(b.anoMesLancamento);
                const unidadeGestoraId = b.codigoUnidadeGestora ? ugIdMap.get(b.codigoUnidadeGestora) : null;
                const acaoOrcamentariaId = b.codigoAcao ? acaoIdMap.get(b.codigoAcao) : null;

                const municipioId = MunicipioResolver.resolve({
                    nome: b.municipio,
                    uf: b.uf,
                });

                if (!anoMesLancamento) return null;

                return {
                    anoMesLancamento,
                    unidadeGestoraId: unidadeGestoraId || null,
                    acaoOrcamentariaId: acaoOrcamentariaId || null,
                    municipioId: municipioId || null,
                    categoriaDespesaId: b.codigoCategoriaEconomica ? catIdMap.get(b.codigoCategoriaEconomica) || null : null,
                    grupoDespesaId: b.codigoGrupoDespesa ? grupIdMap.get(b.codigoGrupoDespesa) || null : null,
                    modalidadeAplicacaoId: b.codigoModalidadeDespesa ? modIdMap.get(b.codigoModalidadeDespesa) || null : null,
                    elementoDespesaId: b.codigoElementoDespesa ? elemIdMap.get(b.codigoElementoDespesa) || null : null,
                    codigoAutorEmenda: b.codigoAutorEmenda || null,
                    nomeAutorEmenda: b.nomeAutorEmenda || null,
                    valorEmpenhado: parseDecimal(b.valorEmpenhado) || '0',
                    valorLiquidado: parseDecimal(b.valorLiquidado) || '0',
                    valorPago: parseDecimal(b.valorPago) || '0',
                    valorRestosAPagarInscritos: parseDecimal(b.valorRestosAPagarInscritos) || null,
                    valorRestosAPagarCancelado: parseDecimal(b.valorRestosAPagarCancelado) || null,
                    valorRestosAPagarPagos: parseDecimal(b.valorRestosAPagarPagos) || null,
                    exercicioReferencia: b.exercicioReferencia || anoMesLancamento.substring(0, 4)
                };
            }).filter(Boolean) as typeof execucaoDespesas.$inferInsert[];

            if (execucoesToInsert.length > 0) {
                const batchMap = new Map<string, typeof execucoesToInsert[0]>();
                for (const e of execucoesToInsert) {
                    const key = `${e.anoMesLancamento}|${e.unidadeGestoraId ?? 0}|${e.acaoOrcamentariaId ?? 0}|${e.municipioId ?? 0}|${e.categoriaDespesaId ?? 0}|${e.grupoDespesaId ?? 0}|${e.modalidadeAplicacaoId ?? 0}|${e.elementoDespesaId ?? 0}|${e.codigoAutorEmenda ?? ''}`;
                    const existing = batchMap.get(key);
                    if (existing) {
                        existing.valorEmpenhado = (Number(existing.valorEmpenhado) + Number(e.valorEmpenhado)).toFixed(2);
                        existing.valorLiquidado = (Number(existing.valorLiquidado) + Number(e.valorLiquidado)).toFixed(2);
                        existing.valorPago = (Number(existing.valorPago) + Number(e.valorPago)).toFixed(2);
                        if (e.valorRestosAPagarInscritos !== null) {
                            existing.valorRestosAPagarInscritos = ((Number(existing.valorRestosAPagarInscritos) || 0) + Number(e.valorRestosAPagarInscritos)).toFixed(2);
                        }
                        if (e.valorRestosAPagarCancelado !== null) {
                            existing.valorRestosAPagarCancelado = ((Number(existing.valorRestosAPagarCancelado) || 0) + Number(e.valorRestosAPagarCancelado)).toFixed(2);
                        }
                        if (e.valorRestosAPagarPagos !== null) {
                            existing.valorRestosAPagarPagos = ((Number(existing.valorRestosAPagarPagos) || 0) + Number(e.valorRestosAPagarPagos)).toFixed(2);
                        }
                        if (!existing.nomeAutorEmenda && e.nomeAutorEmenda) {
                            existing.nomeAutorEmenda = e.nomeAutorEmenda;
                        }
                    } else {
                        batchMap.set(key, { ...e });
                    }
                }
                const uniqueExecucoes = Array.from(batchMap.values());

                const anoMesLote = [...new Set(uniqueExecucoes.map(e => e.anoMesLancamento))];
                const ugIdsLote = [...new Set(uniqueExecucoes.map(e => e.unidadeGestoraId).filter(Boolean) as number[])];

                if (anoMesLote.length > 0 && ugIdsLote.length > 0) {
                    await silver_db.delete(execucaoDespesas)
                        .where(
                            and(
                                inArray(execucaoDespesas.anoMesLancamento, anoMesLote),
                                inArray(execucaoDespesas.unidadeGestoraId, ugIdsLote)
                            )
                        );
                }

                log.info({ data: { count: uniqueExecucoes.length } }, 'Inserindo execucao_despesas');
                for (let i = 0; i < uniqueExecucoes.length; i += 1000) {
                    await silver_db.insert(execucaoDespesas)
                        .values(uniqueExecucoes.slice(i, i + 1000))
                        .onDuplicateKeyUpdate({
                            set: {
                                valorEmpenhado: sql`VALUES(valor_empenhado)`,
                                valorLiquidado: sql`VALUES(valor_liquidado)`,
                                valorPago: sql`VALUES(valor_pago)`,
                                valorRestosAPagarInscritos: sql`VALUES(valor_restos_a_pagar_inscritos)`,
                                valorRestosAPagarCancelado: sql`VALUES(valor_restos_a_pagar_cancelado)`,
                                valorRestosAPagarPagos: sql`VALUES(valor_restos_a_pagar_pagos)`,
                                nomeAutorEmenda: sql`COALESCE(VALUES(nome_autor_emenda), execucao_despesas.nome_autor_emenda)`,
                                exercicioReferencia: sql`VALUES(exercicio_referencia)`,
                            },
                        });
                }
            }

            offset += BATCH_SIZE;
        }
    } catch (error: any) {
        const errorCode = error.code || 'UNKNOWN_CODE';
        const errorMessage = error.sqlMessage || error.message || 'Erro desconhecido';

        const shortSql = error.sql ? `${error.sql.substring(0, 150)}... [TRUNCATED]` : undefined;

        log.error({
            data: {
                code: errorCode,
                message: errorMessage,
                sqlSnippet: shortSql
            }
        }, 'Erro fatal durante o processamento de Execucao de Despesa');

        if (error.sql) {
            error.sql = shortSql;
        }
        if (error.cause && error.cause.sql) {
            error.cause.sql = shortSql;
        }

        throw error;
    }
}