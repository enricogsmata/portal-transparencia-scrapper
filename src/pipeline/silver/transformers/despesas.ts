// src/pipeline/silver/transformers/despesas.ts

import { sql, inArray, and, eq } from 'drizzle-orm';
import { bronze_db } from '../../../database/bronze_connection.js';
import { silver_db } from '../../../database/silver_connection.js';
import {
    DespesasEmpenho,
    DespesasLiquidacao,
    DespesasPagamento,
    DespesasItemEmpenho,
    DespesasItemEmpenhoHistorico,
    DespesasLiquidacaoEmpenhosImpactados,
    DespesasPagamentoEmpenhosImpactados,
    DespesasPagamentoFavorecidosFinais,
    DespesasPagamentoListaBancos,
    DespesasPagamentoListaFaturas,
    DespesasPagamentoListaPrecatorios
} from '../../../database/bronze_schema.js';
import {
    despesasEmpenhos,
    despesasLiquidacoes,
    despesasPagamentos,
    despesasEmpenhoItens,
    despesasLiquidacaoEmpenhos,
    despesasPagamentoEmpenhos,
    unidadesGestoras,
    gestoes,
    pessoasFisicas,
    pessoasJuridicas,
    bancos,
    funcoesOrcamentarias,
    subfuncoesOrcamentarias,
    programasOrcamentarios,
    acoesOrcamentarias,
    planosOrcamentarios,
    categoriasDespesa,
    gruposDespesa,
    modalidadesAplicacao,
    elementosDespesa,
    subelementosDespesa,
    fontesRecursos,
    despesasEmpenhoItemHistorico,
    despesasPagamentoFavorecidosFinais,
    despesasPagamentoListaBancos,
    despesasPagamentoListaFaturas,
    despesasPagamentoListaPrecatorios
} from '../../../database/silver_schema.js';
import { formatCnpj, parseDate, parseDecimal } from '../shared/formatters.js';
import { new_logger } from '../../../services/logs.js';

const logger = new_logger(import.meta.url);

function getDocType(doc?: string | null): 'PF' | 'PJ' | null {
    if (!doc || doc.trim().toUpperCase() === 'SIGILOSO' || doc.trim().toUpperCase() === 'NÃO APLICÁVEL') return null;
    const numbers = doc.replace(/\D/g, '');
    if (numbers.length === 11) return 'PF';
    if (numbers.length > 11) return 'PJ';
    return null;
}

function formatCpf(cpf?: string | null): string | null {
    if (!cpf) return null;
    const numbers = cpf.replace(/\D/g, '');
    return numbers.length > 0 ? numbers.padStart(11, '0') : null;
}

export async function processDespesasEmpenho() {
    const log = logger.forMethod('processDespesasEmpenho');
    const BATCH_SIZE = 1000;
    let offset = 0;
    let hasMore = true;

    log.info('Iniciando processamento de Despesas - Empenhos');

    try {
        while (hasMore) {
            const bronzeData = await bronze_db.select()
                .from(DespesasEmpenho)
                .limit(BATCH_SIZE)
                .offset(offset);

            if (bronzeData.length === 0) {
                log.info({ data: { offset } }, 'Processamento concluído.');
                hasMore = false;
                break;
            }

            // 1. Unidades Gestoras
            const ugToUpsertMap = new Map<string, typeof unidadesGestoras.$inferInsert>();
            bronzeData.forEach(b => {
                if (b.codigoUnidadeGestora) ugToUpsertMap.set(b.codigoUnidadeGestora, { codigo: b.codigoUnidadeGestora, nome: b.unidadeGestora || 'NÃO INFORMADO' });
            });
            const ugToUpsert = Array.from(ugToUpsertMap.values());
            if (ugToUpsert.length > 0) {
                await silver_db.insert(unidadesGestoras).values(ugToUpsert).onDuplicateKeyUpdate({ set: { id: sql`id` } });
            }
            const ugDb = ugToUpsert.length > 0 ? await silver_db.select({ id: unidadesGestoras.id, codigo: unidadesGestoras.codigo }).from(unidadesGestoras).where(inArray(unidadesGestoras.codigo, ugToUpsert.map(u => u.codigo))) : [];
            const ugIdMap = new Map(ugDb.map(u => [u.codigo, u.id]));

            // 2. Gestões
            const gesToUpsertMap = new Map<string, typeof gestoes.$inferInsert>();
            bronzeData.forEach(b => {
                if (b.codigoGestao) gesToUpsertMap.set(b.codigoGestao, { codigo: b.codigoGestao, nome: b.gestao || 'NÃO INFORMADO' });
            });
            const gesToUpsert = Array.from(gesToUpsertMap.values());
            if (gesToUpsert.length > 0) {
                await silver_db.insert(gestoes).values(gesToUpsert).onDuplicateKeyUpdate({ set: { nome: sql`VALUES(nome)` } });
            }
            const gesDb = gesToUpsert.length > 0 ? await silver_db.select({ id: gestoes.id, codigo: gestoes.codigo }).from(gestoes).where(inArray(gestoes.codigo, gesToUpsert.map(g => g.codigo))) : [];
            const gesIdMap = new Map(gesDb.map(g => [g.codigo, g.id]));

            // 3. Favorecidos (PF/PJ)
            const pfMap = new Map<string, typeof pessoasFisicas.$inferInsert>();
            const pjMap = new Map<string, typeof pessoasJuridicas.$inferInsert>();
            bronzeData.forEach(b => {
                const type = getDocType(b.codigoFavorecido);
                if (type === 'PF') {
                    const cpf = formatCpf(b.codigoFavorecido)!;
                    pfMap.set(cpf, { cpf, nome: b.favorecido || 'NÃO INFORMADO' });
                } else if (type === 'PJ') {
                    const cnpj = formatCnpj(b.codigoFavorecido)!;
                    pjMap.set(cnpj, { cnpj, razaoSocial: b.favorecido || 'NÃO INFORMADO' });
                }
            });
            const pfToUpsert = Array.from(pfMap.values());
            if (pfToUpsert.length > 0) {
                // Ignora duplicatas se já existe (CPF não é unique constraint mas para simplificar vamos tentar inserir novos)
                const cpfsExistentes = await silver_db.select({ cpf: pessoasFisicas.cpf }).from(pessoasFisicas).where(inArray(pessoasFisicas.cpf, pfToUpsert.map(p => p.cpf)));
                const cpfsSet = new Set(cpfsExistentes.map(c => c.cpf));
                const pfToInsert = pfToUpsert.filter(p => !cpfsSet.has(p.cpf));
                if (pfToInsert.length > 0) await silver_db.insert(pessoasFisicas).values(pfToInsert);
            }
            const pfDb = pfToUpsert.length > 0 ? await silver_db.select({ id: pessoasFisicas.id, cpf: pessoasFisicas.cpf }).from(pessoasFisicas).where(inArray(pessoasFisicas.cpf, pfToUpsert.map(p => p.cpf))) : [];
            const pfIdMap = new Map(pfDb.map(p => [p.cpf, p.id]));

            const pjToUpsert = Array.from(pjMap.values());
            if (pjToUpsert.length > 0) {
                await silver_db.insert(pessoasJuridicas).values(pjToUpsert).onDuplicateKeyUpdate({ set: { id: sql`id` } });
            }
            const pjDb = pjToUpsert.length > 0 ? await silver_db.select({ id: pessoasJuridicas.id, cnpj: pessoasJuridicas.cnpj }).from(pessoasJuridicas).where(inArray(pessoasJuridicas.cnpj, pjToUpsert.map(p => p.cnpj))) : [];
            const pjIdMap = new Map(pjDb.map(p => [p.cnpj, p.id]));

            // 4. Funções
            const funToUpsertMap = new Map<string, typeof funcoesOrcamentarias.$inferInsert>();
            bronzeData.forEach(b => { if (b.codigoFuncao) funToUpsertMap.set(b.codigoFuncao, { codigo: b.codigoFuncao, nome: b.funcao || 'NÃO INFORMADO' }); });
            const funToUpsert = Array.from(funToUpsertMap.values());
            if (funToUpsert.length > 0) await silver_db.insert(funcoesOrcamentarias).values(funToUpsert).onDuplicateKeyUpdate({ set: { nome: sql`VALUES(nome)` } });
            const funDb = funToUpsert.length > 0 ? await silver_db.select({ id: funcoesOrcamentarias.id, codigo: funcoesOrcamentarias.codigo }).from(funcoesOrcamentarias).where(inArray(funcoesOrcamentarias.codigo, funToUpsert.map(f => f.codigo))) : [];
            const funIdMap = new Map(funDb.map(f => [f.codigo, f.id]));

            // 5. Subfunções
            const sfunToUpsertMap = new Map<string, typeof subfuncoesOrcamentarias.$inferInsert>();
            bronzeData.forEach(b => { if (b.codigoSubfuncao) sfunToUpsertMap.set(b.codigoSubfuncao, { codigo: b.codigoSubfuncao, nome: b.subfuncao || 'NÃO INFORMADO' }); });
            const sfunToUpsert = Array.from(sfunToUpsertMap.values());
            if (sfunToUpsert.length > 0) await silver_db.insert(subfuncoesOrcamentarias).values(sfunToUpsert).onDuplicateKeyUpdate({ set: { nome: sql`VALUES(nome)` } });
            const sfunDb = sfunToUpsert.length > 0 ? await silver_db.select({ id: subfuncoesOrcamentarias.id, codigo: subfuncoesOrcamentarias.codigo }).from(subfuncoesOrcamentarias).where(inArray(subfuncoesOrcamentarias.codigo, sfunToUpsert.map(f => f.codigo))) : [];
            const sfunIdMap = new Map(sfunDb.map(f => [f.codigo, f.id]));

            // 6. Programas
            const progToUpsertMap = new Map<string, typeof programasOrcamentarios.$inferInsert>();
            bronzeData.forEach(b => { if (b.codigoPrograma) progToUpsertMap.set(b.codigoPrograma, { codigo: b.codigoPrograma, nome: b.programa || 'NÃO INFORMADO' }); });
            const progToUpsert = Array.from(progToUpsertMap.values());
            if (progToUpsert.length > 0) await silver_db.insert(programasOrcamentarios).values(progToUpsert).onDuplicateKeyUpdate({ set: { nome: sql`VALUES(nome)` } });
            const progDb = progToUpsert.length > 0 ? await silver_db.select({ id: programasOrcamentarios.id, codigo: programasOrcamentarios.codigo }).from(programasOrcamentarios).where(inArray(programasOrcamentarios.codigo, progToUpsert.map(f => f.codigo))) : [];
            const progIdMap = new Map(progDb.map(f => [f.codigo, f.id]));

            // 7. Ações
            const acaoToUpsertMap = new Map<string, typeof acoesOrcamentarias.$inferInsert>();
            bronzeData.forEach(b => {
                if (b.codigoAcao) {
                    const programaId = b.codigoPrograma ? progIdMap.get(b.codigoPrograma) : null;
                    acaoToUpsertMap.set(b.codigoAcao, { codigo: b.codigoAcao, nome: b.acao || 'NÃO INFORMADO', programaId: programaId || null, linguagemCidada: b.linguagemCidada || null });
                }
            });
            const acaoToUpsert = Array.from(acaoToUpsertMap.values());
            if (acaoToUpsert.length > 0) {
                // Insert and handle duplicates.
                const existAcoes = await silver_db.select({ id: acoesOrcamentarias.id, codigo: acoesOrcamentarias.codigo }).from(acoesOrcamentarias).where(inArray(acoesOrcamentarias.codigo, acaoToUpsert.map(a => a.codigo)));
                const acoesMapSet = new Map(existAcoes.map(a => [a.codigo, a.id]));
                const acaoToInsert = acaoToUpsert.filter(a => !acoesMapSet.has(a.codigo));
                if (acaoToInsert.length > 0) await silver_db.insert(acoesOrcamentarias).values(acaoToInsert);
            }
            const acaoDb = acaoToUpsert.length > 0 ? await silver_db.select({ id: acoesOrcamentarias.id, codigo: acoesOrcamentarias.codigo }).from(acoesOrcamentarias).where(inArray(acoesOrcamentarias.codigo, acaoToUpsert.map(a => a.codigo))) : [];
            const acaoIdMap = new Map(acaoDb.map(a => [a.codigo, a.id]));

            // 8. Planos Orcamentarios
            const poToUpsertMap = new Map<string, typeof planosOrcamentarios.$inferInsert>();
            bronzeData.forEach(b => { if (b.codigoPlanoOrcamentario) poToUpsertMap.set(b.codigoPlanoOrcamentario, { codigo: b.codigoPlanoOrcamentario, nome: b.planoOrcamentario || 'NÃO INFORMADO' }); });
            const poToUpsert = Array.from(poToUpsertMap.values());
            if (poToUpsert.length > 0) await silver_db.insert(planosOrcamentarios).values(poToUpsert).onDuplicateKeyUpdate({ set: { nome: sql`VALUES(nome)` } });
            const poDb = poToUpsert.length > 0 ? await silver_db.select({ id: planosOrcamentarios.id, codigo: planosOrcamentarios.codigo }).from(planosOrcamentarios).where(inArray(planosOrcamentarios.codigo, poToUpsert.map(f => f.codigo))) : [];
            const poIdMap = new Map(poDb.map(f => [f.codigo, f.id]));

            // 9. Categorias
            const catToUpsertMap = new Map<string, typeof categoriasDespesa.$inferInsert>();
            bronzeData.forEach(b => { if (b.codigoCategoriaDespesa) catToUpsertMap.set(b.codigoCategoriaDespesa, { codigo: b.codigoCategoriaDespesa, nome: b.categoriaDespesa || 'NÃO INFORMADO' }); });
            const catToUpsert = Array.from(catToUpsertMap.values());
            if (catToUpsert.length > 0) await silver_db.insert(categoriasDespesa).values(catToUpsert).onDuplicateKeyUpdate({ set: { nome: sql`VALUES(nome)` } });
            const catDb = catToUpsert.length > 0 ? await silver_db.select({ id: categoriasDespesa.id, codigo: categoriasDespesa.codigo }).from(categoriasDespesa).where(inArray(categoriasDespesa.codigo, catToUpsert.map(f => f.codigo))) : [];
            const catIdMap = new Map(catDb.map(f => [f.codigo, f.id]));

            // 10. Grupos
            const grupToUpsertMap = new Map<string, typeof gruposDespesa.$inferInsert>();
            bronzeData.forEach(b => { if (b.codigoGrupoDespesa) grupToUpsertMap.set(b.codigoGrupoDespesa, { codigo: b.codigoGrupoDespesa, nome: b.grupoDespesa || 'NÃO INFORMADO' }); });
            const grupToUpsert = Array.from(grupToUpsertMap.values());
            if (grupToUpsert.length > 0) await silver_db.insert(gruposDespesa).values(grupToUpsert).onDuplicateKeyUpdate({ set: { nome: sql`VALUES(nome)` } });
            const grupDb = grupToUpsert.length > 0 ? await silver_db.select({ id: gruposDespesa.id, codigo: gruposDespesa.codigo }).from(gruposDespesa).where(inArray(gruposDespesa.codigo, grupToUpsert.map(f => f.codigo))) : [];
            const grupIdMap = new Map(grupDb.map(f => [f.codigo, f.id]));

            // 11. Modalidades
            const modToUpsertMap = new Map<string, typeof modalidadesAplicacao.$inferInsert>();
            bronzeData.forEach(b => { if (b.codigoModalidadeAplicacao) modToUpsertMap.set(b.codigoModalidadeAplicacao, { codigo: b.codigoModalidadeAplicacao, nome: b.modalidadeAplicacao || 'NÃO INFORMADO' }); });
            const modToUpsert = Array.from(modToUpsertMap.values());
            if (modToUpsert.length > 0) await silver_db.insert(modalidadesAplicacao).values(modToUpsert).onDuplicateKeyUpdate({ set: { nome: sql`VALUES(nome)` } });
            const modDb = modToUpsert.length > 0 ? await silver_db.select({ id: modalidadesAplicacao.id, codigo: modalidadesAplicacao.codigo }).from(modalidadesAplicacao).where(inArray(modalidadesAplicacao.codigo, modToUpsert.map(f => f.codigo))) : [];
            const modIdMap = new Map(modDb.map(f => [f.codigo, f.id]));

            // 12. Elementos
            const elemToUpsertMap = new Map<string, typeof elementosDespesa.$inferInsert>();
            bronzeData.forEach(b => { if (b.codigoElementoDespesa) elemToUpsertMap.set(b.codigoElementoDespesa, { codigo: b.codigoElementoDespesa, nome: b.elementoDespesa || 'NÃO INFORMADO' }); });
            const elemToUpsert = Array.from(elemToUpsertMap.values());
            if (elemToUpsert.length > 0) await silver_db.insert(elementosDespesa).values(elemToUpsert).onDuplicateKeyUpdate({ set: { nome: sql`VALUES(nome)` } });
            const elemDb = elemToUpsert.length > 0 ? await silver_db.select({ id: elementosDespesa.id, codigo: elementosDespesa.codigo }).from(elementosDespesa).where(inArray(elementosDespesa.codigo, elemToUpsert.map(f => f.codigo))) : [];
            const elemIdMap = new Map(elemDb.map(f => [f.codigo, f.id]));

            // 13. Fontes
            const fonteToUpsertMap = new Map<string, typeof fontesRecursos.$inferInsert>();
            bronzeData.forEach(b => {
                if (b.codigoFonteRecurso) {
                    fonteToUpsertMap.set(b.codigoFonteRecurso, {
                        codigoFonte: b.codigoFonteRecurso,
                        nome: b.fonteRecurso || 'NÃO INFORMADO',
                        codigoGrupo: b.codigoGrupoFonteRecurso || null,
                        grupo: b.grupoFonteRecurso || null
                    });
                }
            });
            const fonteToUpsert = Array.from(fonteToUpsertMap.values());
            if (fonteToUpsert.length > 0) await silver_db.insert(fontesRecursos).values(fonteToUpsert).onDuplicateKeyUpdate({ set: { nome: sql`VALUES(nome)` } });
            const fonteDb = fonteToUpsert.length > 0 ? await silver_db.select({ id: fontesRecursos.id, codigoFonte: fontesRecursos.codigoFonte }).from(fontesRecursos).where(inArray(fontesRecursos.codigoFonte, fonteToUpsert.map(f => f.codigoFonte))) : [];
            const fonteIdMap = new Map(fonteDb.map(f => [f.codigoFonte, f.id]));

            // 14. Inserção na Entidade Principal (despesas_empenhos)
            const empenhosToInsert = bronzeData.map(b => {
                if (!b.codigoEmpenho) return null;

                const dataEmissaoDate = parseDate(b.dataEmissao);
                const exRef = b.exercicioReferencia || (dataEmissaoDate ? dataEmissaoDate.substring(0, 4) : '0000');

                let favPfId = null;
                let favPjId = null;
                const type = getDocType(b.codigoFavorecido);
                if (type === 'PF') favPfId = pfIdMap.get(formatCpf(b.codigoFavorecido)!) || null;
                else if (type === 'PJ') favPjId = pjIdMap.get(formatCnpj(b.codigoFavorecido)!) || null;

                return {
                    codigoEmpenho: b.codigoEmpenho,
                    dataEmissao: dataEmissaoDate || '1970-01-01',
                    unidadeGestoraId: b.codigoUnidadeGestora ? ugIdMap.get(b.codigoUnidadeGestora) || null : null,
                    gestaoId: b.codigoGestao ? gesIdMap.get(b.codigoGestao) || null : null,
                    favorecidoPfId: favPfId,
                    favorecidoPjId: favPjId,
                    funcaoId: b.codigoFuncao ? funIdMap.get(b.codigoFuncao) || null : null,
                    subfuncaoId: b.codigoSubfuncao ? sfunIdMap.get(b.codigoSubfuncao) || null : null,
                    acaoOrcamentariaId: b.codigoAcao ? acaoIdMap.get(b.codigoAcao) || null : null,
                    planoOrcamentarioId: b.codigoPlanoOrcamentario ? poIdMap.get(b.codigoPlanoOrcamentario) || null : null,
                    categoriaDespesaId: b.codigoCategoriaDespesa ? catIdMap.get(b.codigoCategoriaDespesa) || null : null,
                    grupoDespesaId: b.codigoGrupoDespesa ? grupIdMap.get(b.codigoGrupoDespesa) || null : null,
                    modalidadeAplicacaoId: b.codigoModalidadeAplicacao ? modIdMap.get(b.codigoModalidadeAplicacao) || null : null,
                    elementoDespesaId: b.codigoElementoDespesa ? elemIdMap.get(b.codigoElementoDespesa) || null : null,
                    fonteRecursoId: b.codigoFonteRecurso ? fonteIdMap.get(b.codigoFonteRecurso) || null : null,
                    tipoEmpenho: b.tipoEmpenho || null,
                    especieEmpenho: b.especieEmpenho || null,
                    processo: b.processo || null,
                    modalidadeLicitacao: b.modalidadeLicitacao || null,
                    observacao: b.observacao || null,
                    valorOriginalEmpenho: parseDecimal(b.valorOriginalEmpenho) || '0',
                    valorEmpenhoConvertido: parseDecimal(b.valorEmpenhoConvertido) || null,
                    exercicioReferencia: exRef,
                };
            }).filter(e => e && e.unidadeGestoraId) as typeof despesasEmpenhos.$inferInsert[];

            if (empenhosToInsert.length > 0) {
                const uniqueEmpMap = new Map<string, typeof empenhosToInsert[0]>();
                for (const e of empenhosToInsert) uniqueEmpMap.set(e.codigoEmpenho, e);
                const uniqueEmpenhos = Array.from(uniqueEmpMap.values());

                const existingEmp = await silver_db.select({ id: despesasEmpenhos.id, codigo: despesasEmpenhos.codigoEmpenho }).from(despesasEmpenhos).where(inArray(despesasEmpenhos.codigoEmpenho, uniqueEmpenhos.map(e => e.codigoEmpenho)));
                const existingSet = new Set(existingEmp.map(e => e.codigo));

                const toInsert = uniqueEmpenhos.filter(e => !existingSet.has(e.codigoEmpenho));
                if (toInsert.length > 0) {
                    log.info({ data: { count: toInsert.length } }, 'Inserindo despesas_empenhos');
                    for (let i = 0; i < toInsert.length; i += 1000) {
                        await silver_db.insert(despesasEmpenhos).values(toInsert.slice(i, i + 1000));
                    }
                }
            }

            offset += BATCH_SIZE;
        }
    } catch (error) {
        log.error({ data: error }, 'Erro fatal durante o processamento de Despesas - Empenhos');
        throw error;
    }
}

export async function processDespesasLiquidacao() {
    const log = logger.forMethod('processDespesasLiquidacao');
    const BATCH_SIZE = 1000;
    let offset = 0;
    let hasMore = true;

    log.info('Iniciando processamento de Despesas - Liquidações');

    try {
        while (hasMore) {
            const bronzeData = await bronze_db.select()
                .from(DespesasLiquidacao)
                .limit(BATCH_SIZE)
                .offset(offset);

            if (bronzeData.length === 0) {
                hasMore = false;
                break;
            }

            // Reutiliza logica simplificada para resolver UGs e Gestoes que devem ter vindo do Empenho ou extraídas agora
            const ugToUpsertMap = new Map();
            bronzeData.forEach(b => { if (b.codigoUnidadeGestora) ugToUpsertMap.set(b.codigoUnidadeGestora, { codigo: b.codigoUnidadeGestora, nome: b.unidadeGestora || 'NÃO INFORMADO' }); });
            if (ugToUpsertMap.size > 0) await silver_db.insert(unidadesGestoras).values(Array.from(ugToUpsertMap.values())).onDuplicateKeyUpdate({ set: { id: sql`id` } });
            const ugDb = ugToUpsertMap.size > 0 ? await silver_db.select({ id: unidadesGestoras.id, c: unidadesGestoras.codigo }).from(unidadesGestoras).where(inArray(unidadesGestoras.codigo, Array.from(ugToUpsertMap.keys()))) : [];
            const ugIdMap = new Map(ugDb.map(u => [u.c, u.id]));

            const gesToUpsertMap = new Map();
            bronzeData.forEach(b => { if (b.codigoGestao) gesToUpsertMap.set(b.codigoGestao, { codigo: b.codigoGestao, nome: b.gestao || 'NÃO INFORMADO' }); });
            if (gesToUpsertMap.size > 0) await silver_db.insert(gestoes).values(Array.from(gesToUpsertMap.values())).onDuplicateKeyUpdate({ set: { nome: sql`VALUES(nome)` } });
            const gesDb = gesToUpsertMap.size > 0 ? await silver_db.select({ id: gestoes.id, c: gestoes.codigo }).from(gestoes).where(inArray(gestoes.codigo, Array.from(gesToUpsertMap.keys()))) : [];
            const gesIdMap = new Map(gesDb.map(u => [u.c, u.id]));

            const pfMap = new Map(), pjMap = new Map();
            bronzeData.forEach(b => {
                const type = getDocType(b.codigoFavorecido);
                if (type === 'PF') pfMap.set(formatCpf(b.codigoFavorecido)!, { cpf: formatCpf(b.codigoFavorecido)!, nome: b.favorecido || 'NÃO INFORMADO' });
                else if (type === 'PJ') pjMap.set(formatCnpj(b.codigoFavorecido)!, { cnpj: formatCnpj(b.codigoFavorecido)!, razaoSocial: b.favorecido || 'NÃO INFORMADO' });
            });
            if (pjMap.size > 0) await silver_db.insert(pessoasJuridicas).values(Array.from(pjMap.values())).onDuplicateKeyUpdate({ set: { id: sql`id` } });
            if (pfMap.size > 0) {
                const exists = await silver_db.select({ cpf: pessoasFisicas.cpf }).from(pessoasFisicas).where(inArray(pessoasFisicas.cpf, Array.from(pfMap.keys())));
                const eSet = new Set(exists.map(e => e.cpf));
                const toI = Array.from(pfMap.values()).filter(p => !eSet.has(p.cpf));
                if (toI.length > 0) await silver_db.insert(pessoasFisicas).values(toI);
            }
            const pfDb = pfMap.size > 0 ? await silver_db.select({ id: pessoasFisicas.id, c: pessoasFisicas.cpf }).from(pessoasFisicas).where(inArray(pessoasFisicas.cpf, Array.from(pfMap.keys()))) : [];
            const pjDb = pjMap.size > 0 ? await silver_db.select({ id: pessoasJuridicas.id, c: pessoasJuridicas.cnpj }).from(pessoasJuridicas).where(inArray(pessoasJuridicas.cnpj, Array.from(pjMap.keys()))) : [];
            const pfIdMap = new Map(pfDb.map(p => [p.c, p.id]));
            const pjIdMap = new Map(pjDb.map(p => [p.c, p.id]));

            const liquidacoesToInsert = bronzeData.map(b => {
                if (!b.codigoLiquidacao) return null;
                const dDate = parseDate(b.dataEmissao);
                const exRef = b.exercicioReferencia || (dDate ? dDate.substring(0, 4) : '0000');
                const type = getDocType(b.codigoFavorecido);
                return {
                    codigoLiquidacao: b.codigoLiquidacao,
                    dataEmissao: dDate || '1970-01-01',
                    unidadeGestoraId: b.codigoUnidadeGestora ? ugIdMap.get(b.codigoUnidadeGestora) || null : null,
                    gestaoId: b.codigoGestao ? gesIdMap.get(b.codigoGestao) || null : null,
                    favorecidoPfId: type === 'PF' ? pfIdMap.get(formatCpf(b.codigoFavorecido)!) || null : null,
                    favorecidoPjId: type === 'PJ' ? pjIdMap.get(formatCnpj(b.codigoFavorecido)!) || null : null,
                    observacao: b.observacao || null,
                    exercicioReferencia: exRef
                };
            }).filter(e => e && e.unidadeGestoraId) as typeof despesasLiquidacoes.$inferInsert[];

            if (liquidacoesToInsert.length > 0) {
                const uniqueMap = new Map();
                for (const e of liquidacoesToInsert) uniqueMap.set(e.codigoLiquidacao, e);
                const uniqueItems = Array.from(uniqueMap.values());

                const existing = await silver_db.select({ codigo: despesasLiquidacoes.codigoLiquidacao }).from(despesasLiquidacoes).where(inArray(despesasLiquidacoes.codigoLiquidacao, uniqueItems.map(e => e.codigoLiquidacao)));
                const existingSet = new Set(existing.map(e => e.codigo));

                const toInsert = uniqueItems.filter(e => !existingSet.has(e.codigoLiquidacao));
                if (toInsert.length > 0) {
                    log.info({ data: { count: toInsert.length } }, 'Inserindo despesas_liquidacoes');
                    for (let i = 0; i < toInsert.length; i += 1000) {
                        await silver_db.insert(despesasLiquidacoes).values(toInsert.slice(i, i + 1000));
                    }
                }
            }
            offset += BATCH_SIZE;
        }
    } catch (error) {
        log.error({ data: error }, 'Erro fatal durante o processamento de Despesas - Liquidações');
        throw error;
    }
}

export async function processDespesasPagamento() {
    const log = logger.forMethod('processDespesasPagamento');
    const BATCH_SIZE = 1000;
    let offset = 0;
    let hasMore = true;

    log.info('Iniciando processamento de Despesas - Pagamentos');

    try {
        while (hasMore) {
            const bronzeData = await bronze_db.select()
                .from(DespesasPagamento)
                .limit(BATCH_SIZE)
                .offset(offset);

            if (bronzeData.length === 0) {
                hasMore = false;
                break;
            }

            const ugToUpsertMap = new Map();
            bronzeData.forEach(b => { if (b.codigoUnidadeGestora) ugToUpsertMap.set(b.codigoUnidadeGestora, { codigo: b.codigoUnidadeGestora, nome: b.unidadeGestora || 'NÃO INFORMADO' }); });
            if (ugToUpsertMap.size > 0) await silver_db.insert(unidadesGestoras).values(Array.from(ugToUpsertMap.values())).onDuplicateKeyUpdate({ set: { id: sql`id` } });
            const ugDb = ugToUpsertMap.size > 0 ? await silver_db.select({ id: unidadesGestoras.id, c: unidadesGestoras.codigo }).from(unidadesGestoras).where(inArray(unidadesGestoras.codigo, Array.from(ugToUpsertMap.keys()))) : [];
            const ugIdMap = new Map(ugDb.map(u => [u.c, u.id]));

            const gesToUpsertMap = new Map();
            bronzeData.forEach(b => { if (b.codigoGestao) gesToUpsertMap.set(b.codigoGestao, { codigo: b.codigoGestao, nome: b.gestao || 'NÃO INFORMADO' }); });
            if (gesToUpsertMap.size > 0) await silver_db.insert(gestoes).values(Array.from(gesToUpsertMap.values())).onDuplicateKeyUpdate({ set: { nome: sql`VALUES(nome)` } });
            const gesDb = gesToUpsertMap.size > 0 ? await silver_db.select({ id: gestoes.id, c: gestoes.codigo }).from(gestoes).where(inArray(gestoes.codigo, Array.from(gesToUpsertMap.keys()))) : [];
            const gesIdMap = new Map(gesDb.map(u => [u.c, u.id]));

            const pfMap = new Map(), pjMap = new Map();
            bronzeData.forEach(b => {
                const type = getDocType(b.codigoFavorecido);
                if (type === 'PF') pfMap.set(formatCpf(b.codigoFavorecido)!, { cpf: formatCpf(b.codigoFavorecido)!, nome: b.favorecido || 'NÃO INFORMADO' });
                else if (type === 'PJ') pjMap.set(formatCnpj(b.codigoFavorecido)!, { cnpj: formatCnpj(b.codigoFavorecido)!, razaoSocial: b.favorecido || 'NÃO INFORMADO' });
            });
            if (pjMap.size > 0) await silver_db.insert(pessoasJuridicas).values(Array.from(pjMap.values())).onDuplicateKeyUpdate({ set: { id: sql`id` } });
            if (pfMap.size > 0) {
                const exists = await silver_db.select({ cpf: pessoasFisicas.cpf }).from(pessoasFisicas).where(inArray(pessoasFisicas.cpf, Array.from(pfMap.keys())));
                const eSet = new Set(exists.map(e => e.cpf));
                const toI = Array.from(pfMap.values()).filter(p => !eSet.has(p.cpf));
                if (toI.length > 0) await silver_db.insert(pessoasFisicas).values(toI);
            }
            const pfDb = pfMap.size > 0 ? await silver_db.select({ id: pessoasFisicas.id, c: pessoasFisicas.cpf }).from(pessoasFisicas).where(inArray(pessoasFisicas.cpf, Array.from(pfMap.keys()))) : [];
            const pjDb = pjMap.size > 0 ? await silver_db.select({ id: pessoasJuridicas.id, c: pessoasJuridicas.cnpj }).from(pessoasJuridicas).where(inArray(pessoasJuridicas.cnpj, Array.from(pjMap.keys()))) : [];
            const pfIdMap = new Map(pfDb.map(p => [p.c, p.id]));
            const pjIdMap = new Map(pjDb.map(p => [p.c, p.id]));

            const pagamentosToInsert = bronzeData.map(b => {
                if (!b.codigoPagamento) return null;
                const dDate = parseDate(b.dataEmissao);
                const exRef = b.exercicioReferencia || (dDate ? dDate.substring(0, 4) : '0000');
                const type = getDocType(b.codigoFavorecido);
                return {
                    codigoPagamento: b.codigoPagamento,
                    dataEmissao: dDate || '1970-01-01',
                    unidadeGestoraId: b.codigoUnidadeGestora ? ugIdMap.get(b.codigoUnidadeGestora) || null : null,
                    gestaoId: b.codigoGestao ? gesIdMap.get(b.codigoGestao) || null : null,
                    favorecidoPfId: type === 'PF' ? pfIdMap.get(formatCpf(b.codigoFavorecido)!) || null : null,
                    favorecidoPjId: type === 'PJ' ? pjIdMap.get(formatCnpj(b.codigoFavorecido)!) || null : null,
                    tipoOb: b.tipoOb || null,
                    extraorcamentario: b.extraorcamentario?.toUpperCase() === 'SIM',
                    processo: b.processo || null,
                    observacao: b.observacao || null,
                    valorOriginalPagamento: parseDecimal(b.valorOriginalPagamento) || '0',
                    valorPagamentoConvertido: parseDecimal(b.valorPagamentoConvertido) || null,
                    exercicioReferencia: exRef
                };
            }).filter(e => e && e.unidadeGestoraId) as typeof despesasPagamentos.$inferInsert[];

            if (pagamentosToInsert.length > 0) {
                const uniqueMap = new Map();
                for (const e of pagamentosToInsert) uniqueMap.set(e.codigoPagamento, e);
                const uniqueItems = Array.from(uniqueMap.values());

                const existing = await silver_db.select({ codigo: despesasPagamentos.codigoPagamento }).from(despesasPagamentos).where(inArray(despesasPagamentos.codigoPagamento, uniqueItems.map(e => e.codigoPagamento)));
                const existingSet = new Set(existing.map(e => e.codigo));

                const toInsert = uniqueItems.filter(e => !existingSet.has(e.codigoPagamento));
                if (toInsert.length > 0) {
                    log.info({ data: { count: toInsert.length } }, 'Inserindo despesas_pagamentos');
                    for (let i = 0; i < toInsert.length; i += 1000) {
                        await silver_db.insert(despesasPagamentos).values(toInsert.slice(i, i + 1000));
                    }
                }
            }
            offset += BATCH_SIZE;
        }
    } catch (error) {
        log.error({ data: error }, 'Erro fatal durante o processamento de Despesas - Pagamentos');
        throw error;
    }
}

export async function processDespesasItemEmpenho() {
    const log = logger.forMethod('processDespesasItemEmpenho');
    const BATCH_SIZE = 1000;
    let offset = 0;
    let hasMore = true;

    log.info('Iniciando processamento de Despesas - Itens de Empenho');

    try {
        while (hasMore) {
            const bronzeData = await bronze_db.select()
                .from(DespesasItemEmpenho)
                .limit(BATCH_SIZE)
                .offset(offset);

            if (bronzeData.length === 0) {
                hasMore = false;
                break;
            }

            // Subelementos e Elementos
            const elemToUpsertMap = new Map();
            bronzeData.forEach(b => { if (b.codigoElementoDespesa) elemToUpsertMap.set(b.codigoElementoDespesa, { codigo: b.codigoElementoDespesa, nome: b.elementoDespesa || 'NÃO INFORMADO' }); });
            if (elemToUpsertMap.size > 0) await silver_db.insert(elementosDespesa).values(Array.from(elemToUpsertMap.values())).onDuplicateKeyUpdate({ set: { nome: sql`VALUES(nome)` } });
            const elemDb = elemToUpsertMap.size > 0 ? await silver_db.select({ id: elementosDespesa.id, c: elementosDespesa.codigo }).from(elementosDespesa).where(inArray(elementosDespesa.codigo, Array.from(elemToUpsertMap.keys()))) : [];
            const elemIdMap = new Map(elemDb.map(f => [f.c, f.id]));

            const subElemToUpsertMap = new Map();
            bronzeData.forEach(b => {
                if (b.codigoSubElementoDespesa) {
                    subElemToUpsertMap.set(b.codigoSubElementoDespesa, {
                        codigo: b.codigoSubElementoDespesa,
                        nome: b.subElementoDespesa || 'NÃO INFORMADO',
                        elementoDespesaId: b.codigoElementoDespesa ? elemIdMap.get(b.codigoElementoDespesa) || null : null
                    });
                }
            });
            if (subElemToUpsertMap.size > 0) {
                // Sub-elementos are unique by code
                await silver_db.insert(subelementosDespesa).values(Array.from(subElemToUpsertMap.values())).onDuplicateKeyUpdate({ set: { nome: sql`VALUES(nome)` } });
            }
            const subElemDb = subElemToUpsertMap.size > 0 ? await silver_db.select({ id: subelementosDespesa.id, c: subelementosDespesa.codigo }).from(subelementosDespesa).where(inArray(subelementosDespesa.codigo, Array.from(subElemToUpsertMap.keys()))) : [];
            const subElemIdMap = new Map(subElemDb.map(f => [f.c, f.id]));

            const empLote = [...new Set(bronzeData.map(b => b.codigoEmpenho).filter(Boolean) as string[])];
            const empDb = empLote.length > 0 ? await silver_db.select({ id: despesasEmpenhos.id, codigo: despesasEmpenhos.codigoEmpenho }).from(despesasEmpenhos).where(inArray(despesasEmpenhos.codigoEmpenho, empLote)) : [];
            const empIdMap = new Map(empDb.map(e => [e.codigo, e.id]));

            const itensToInsert = bronzeData.map(b => {
                const empenhoId = b.codigoEmpenho ? empIdMap.get(b.codigoEmpenho) : null;
                if (!empenhoId || !b.sequencial) return null;

                return {
                    empenhoId,
                    sequencial: parseInt(b.sequencial) || 0,
                    subelementoDespesaId: b.codigoSubElementoDespesa ? subElemIdMap.get(b.codigoSubElementoDespesa) || null : null,
                    descricao: b.descricao || 'NÃO INFORMADO',
                    quantidade: parseDecimal(b.quantidade) || '0',
                    valorUnitario: parseDecimal(b.valorUnitario) || '0',
                    valorTotal: parseDecimal(b.valorTotal) || '0',
                };
            }).filter(Boolean) as typeof despesasEmpenhoItens.$inferInsert[];

            if (itensToInsert.length > 0) {
                const empenhoIds = [...new Set(itensToInsert.map(i => i.empenhoId))];
                const existing = await silver_db.select({ empenhoId: despesasEmpenhoItens.empenhoId, sequencial: despesasEmpenhoItens.sequencial }).from(despesasEmpenhoItens).where(inArray(despesasEmpenhoItens.empenhoId, empenhoIds));
                const existingSet = new Set(existing.map(i => `${i.empenhoId}-${i.sequencial}`));

                const toInsertMap = new Map();
                for (const i of itensToInsert) {
                    const key = `${i.empenhoId}-${i.sequencial}`;
                    if (!existingSet.has(key)) toInsertMap.set(key, i);
                }

                const finalInsert = Array.from(toInsertMap.values());
                if (finalInsert.length > 0) {
                    log.info({ data: { count: finalInsert.length } }, 'Inserindo despesas_empenho_itens');
                    for (let i = 0; i < finalInsert.length; i += 1000) {
                        await silver_db.insert(despesasEmpenhoItens).values(finalInsert.slice(i, i + 1000));
                    }
                }
            }
            offset += BATCH_SIZE;
        }
    } catch (error) {
        log.error({ data: error }, 'Erro fatal durante o processamento de Itens de Empenho');
        throw error;
    }
}

export async function processDespesasLiquidacaoEmpenhosImpactados() {
    const log = logger.forMethod('processDespesasLiquidacaoEmpenhosImpactados');
    const BATCH_SIZE = 1000;
    let offset = 0;
    let hasMore = true;

    log.info('Iniciando processamento de Despesas - Liquidacao Empenhos Impactados');

    try {
        while (hasMore) {
            const bronzeData = await bronze_db.select()
                .from(DespesasLiquidacaoEmpenhosImpactados)
                .limit(BATCH_SIZE)
                .offset(offset);

            if (bronzeData.length === 0) {
                hasMore = false;
                break;
            }

            const liqLote = [...new Set(bronzeData.map(b => b.codigoLiquidacao).filter(Boolean) as string[])];
            const liqDb = liqLote.length > 0 ? await silver_db.select({ id: despesasLiquidacoes.id, codigo: despesasLiquidacoes.codigoLiquidacao }).from(despesasLiquidacoes).where(inArray(despesasLiquidacoes.codigoLiquidacao, liqLote)) : [];
            const liqIdMap = new Map(liqDb.map(l => [l.codigo, l.id]));

            const empLote = [...new Set(bronzeData.map(b => b.codigoEmpenho).filter(Boolean) as string[])];
            const empDb = empLote.length > 0 ? await silver_db.select({ id: despesasEmpenhos.id, codigo: despesasEmpenhos.codigoEmpenho }).from(despesasEmpenhos).where(inArray(despesasEmpenhos.codigoEmpenho, empLote)) : [];
            const empIdMap = new Map(empDb.map(e => [e.codigo, e.id]));

            const impactadosToInsert = bronzeData.map(b => {
                const liquidacaoId = b.codigoLiquidacao ? liqIdMap.get(b.codigoLiquidacao) : null;
                const empenhoId = b.codigoEmpenho ? empIdMap.get(b.codigoEmpenho) : null;

                if (!liquidacaoId || !empenhoId) return null;

                return {
                    liquidacaoId,
                    empenhoId,
                    naturezaDespesaCompleta: b.codigoNaturezaDespesaCompleta || null,
                    subitem: b.subitem || null,
                    valorLiquidado: parseDecimal(b.valorLiquidado) || '0',
                    valorRestosAPagarInscritos: parseDecimal(b.valorRestosAPagarInscritos) || null,
                    valorRestosAPagarCancelado: parseDecimal(b.valorRestosAPagarCancelado) || null,
                    valorRestosAPagarPagos: parseDecimal(b.valorRestosAPagarPagos) || null,
                };
            }).filter(Boolean) as typeof despesasLiquidacaoEmpenhos.$inferInsert[];

            if (impactadosToInsert.length > 0) {
                const liqIds = [...new Set(impactadosToInsert.map(i => i.liquidacaoId))];
                const existing = await silver_db.select({ liquidacaoId: despesasLiquidacaoEmpenhos.liquidacaoId, empenhoId: despesasLiquidacaoEmpenhos.empenhoId }).from(despesasLiquidacaoEmpenhos).where(inArray(despesasLiquidacaoEmpenhos.liquidacaoId, liqIds));
                const existingSet = new Set(existing.map(i => `${i.liquidacaoId}-${i.empenhoId}`));

                const toInsertMap = new Map();
                for (const i of impactadosToInsert) {
                    const key = `${i.liquidacaoId}-${i.empenhoId}`;
                    if (!existingSet.has(key)) toInsertMap.set(key, i);
                }

                const finalInsert = Array.from(toInsertMap.values());
                if (finalInsert.length > 0) {
                    log.info({ data: { count: finalInsert.length } }, 'Inserindo despesas_liquidacao_empenhos');
                    for (let i = 0; i < finalInsert.length; i += 1000) {
                        await silver_db.insert(despesasLiquidacaoEmpenhos).values(finalInsert.slice(i, i + 1000));
                    }
                }
            }
            offset += BATCH_SIZE;
        }
    } catch (error) {
        log.error({ data: error }, 'Erro fatal durante o processamento de Liquidação Empenhos Impactados');
        throw error;
    }
}

export async function processDespesasPagamentoEmpenhosImpactados() {
    const log = logger.forMethod('processDespesasPagamentoEmpenhosImpactados');
    const BATCH_SIZE = 1000;
    let offset = 0;
    let hasMore = true;

    log.info('Iniciando processamento de Despesas - Pagamento Empenhos Impactados');

    try {
        while (hasMore) {
            const bronzeData = await bronze_db.select()
                .from(DespesasPagamentoEmpenhosImpactados)
                .limit(BATCH_SIZE)
                .offset(offset);

            if (bronzeData.length === 0) {
                hasMore = false;
                break;
            }

            const pagLote = [...new Set(bronzeData.map(b => b.codigoPagamento).filter(Boolean) as string[])];
            const pagDb = pagLote.length > 0 ? await silver_db.select({ id: despesasPagamentos.id, codigo: despesasPagamentos.codigoPagamento }).from(despesasPagamentos).where(inArray(despesasPagamentos.codigoPagamento, pagLote)) : [];
            const pagIdMap = new Map(pagDb.map(p => [p.codigo, p.id]));

            const empLote = [...new Set(bronzeData.map(b => b.codigoEmpenho).filter(Boolean) as string[])];
            const empDb = empLote.length > 0 ? await silver_db.select({ id: despesasEmpenhos.id, codigo: despesasEmpenhos.codigoEmpenho }).from(despesasEmpenhos).where(inArray(despesasEmpenhos.codigoEmpenho, empLote)) : [];
            const empIdMap = new Map(empDb.map(e => [e.codigo, e.id]));

            const impactadosToInsert = bronzeData.map(b => {
                const pagamentoId = b.codigoPagamento ? pagIdMap.get(b.codigoPagamento) : null;
                const empenhoId = b.codigoEmpenho ? empIdMap.get(b.codigoEmpenho) : null;

                if (!pagamentoId || !empenhoId) return null;

                return {
                    pagamentoId,
                    empenhoId,
                    naturezaDespesaCompleta: b.codigoNaturezaDespesaCompleta || null,
                    subitem: b.subitem || null,
                    valorPago: parseDecimal(b.valorPago) || '0',
                    valorRestosAPagarInscritos: parseDecimal(b.valorRestosAPagarInscritos) || null,
                    valorRestosAPagarCancelado: parseDecimal(b.valorRestosAPagarCancelado) || null,
                    valorRestosAPagarPagos: parseDecimal(b.valorRestosAPagarPagos) || null,
                };
            }).filter(Boolean) as typeof despesasPagamentoEmpenhos.$inferInsert[];

            if (impactadosToInsert.length > 0) {
                const pagIds = [...new Set(impactadosToInsert.map(i => i.pagamentoId))];
                const existing = await silver_db.select({ pagamentoId: despesasPagamentoEmpenhos.pagamentoId, empenhoId: despesasPagamentoEmpenhos.empenhoId }).from(despesasPagamentoEmpenhos).where(inArray(despesasPagamentoEmpenhos.pagamentoId, pagIds));
                const existingSet = new Set(existing.map(i => `${i.pagamentoId}-${i.empenhoId}`));

                const toInsertMap = new Map();
                for (const i of impactadosToInsert) {
                    const key = `${i.pagamentoId}-${i.empenhoId}`;
                    if (!existingSet.has(key)) toInsertMap.set(key, i);
                }

                const finalInsert = Array.from(toInsertMap.values());
                if (finalInsert.length > 0) {
                    log.info({ data: { count: finalInsert.length } }, 'Inserindo despesas_pagamento_empenhos');
                    for (let i = 0; i < finalInsert.length; i += 1000) {
                        await silver_db.insert(despesasPagamentoEmpenhos).values(finalInsert.slice(i, i + 1000));
                    }
                }
            }
            offset += BATCH_SIZE;
        }
    } catch (error) {
        log.error({ data: error }, 'Erro fatal durante o processamento de Pagamento Empenhos Impactados');
        throw error;
    }
}

export async function processDespesasItemEmpenhoHistorico() {
    const log = logger.forMethod('processDespesasItemEmpenhoHistorico');
    const BATCH_SIZE = 1000;
    let offset = 0;
    let hasMore = true;

    log.info('Iniciando processamento de Despesas - Histórico de Itens de Empenho');

    try {
        while (hasMore) {
            const bronzeData = await bronze_db.select()
                .from(DespesasItemEmpenhoHistorico)
                .limit(BATCH_SIZE)
                .offset(offset);

            if (bronzeData.length === 0) {
                hasMore = false;
                break;
            }

            const empLote = [...new Set(bronzeData.map(b => b.codigoEmpenho).filter(Boolean) as string[])];
            const empDb = empLote.length > 0
                ? await silver_db.select({ id: despesasEmpenhos.id, codigo: despesasEmpenhos.codigoEmpenho })
                    .from(despesasEmpenhos)
                    .where(inArray(despesasEmpenhos.codigoEmpenho, empLote))
                : [];
            const empIdMap = new Map(empDb.map(e => [e.codigo, e.id]));

            const empenhoIds = Array.from(empIdMap.values());
            const itensDb = empenhoIds.length > 0
                ? await silver_db.select({ id: despesasEmpenhoItens.id, empenhoId: despesasEmpenhoItens.empenhoId, sequencial: despesasEmpenhoItens.sequencial })
                    .from(despesasEmpenhoItens)
                    .where(inArray(despesasEmpenhoItens.empenhoId, empenhoIds))
                : [];
            const itemIdMap = new Map(itensDb.map(i => [`${i.empenhoId}-${i.sequencial}`, i.id]));

            const historicoToInsert = bronzeData.map(b => {
                const empenhoId = b.codigoEmpenho ? empIdMap.get(b.codigoEmpenho) : null;
                if (!empenhoId || !b.sequencial) return null;

                const sequencial = parseInt(b.sequencial);
                const empenhoItemId = itemIdMap.get(`${empenhoId}-${sequencial}`);

                if (!empenhoItemId || !b.tipoOperacao || !b.dataOperacao) return null;

                return {
                    empenhoItemId,
                    tipoOperacao: b.tipoOperacao,
                    dataOperacao: parseDate(b.dataOperacao) || '1970-01-01',
                    quantidadeItem: parseDecimal(b.quantidadeItem) || '0',
                    valorUnitarioItem: parseDecimal(b.valorUnitarioItem) || '0',
                    valorTotalItem: parseDecimal(b.valorTotalItem) || '0',
                };
            }).filter(Boolean) as typeof despesasEmpenhoItemHistorico.$inferInsert[];

            if (historicoToInsert.length > 0) {
                // Idempotência: Deleta todos os históricos atrelados aos itens desta remessa
                const itemIds = [...new Set(historicoToInsert.map(h => h.empenhoItemId))];
                await silver_db.delete(despesasEmpenhoItemHistorico)
                    .where(inArray(despesasEmpenhoItemHistorico.empenhoItemId, itemIds));

                log.info({ data: { count: historicoToInsert.length } }, 'Inserindo despesas_empenho_item_historico');
                for (let i = 0; i < historicoToInsert.length; i += 1000) {
                    await silver_db.insert(despesasEmpenhoItemHistorico).values(historicoToInsert.slice(i, i + 1000));
                }
            }
            offset += BATCH_SIZE;
        }
    } catch (error) {
        log.error({ data: error }, 'Erro fatal durante o processamento de Histórico de Itens de Empenho');
        throw error;
    }
}

export async function processDespesasPagamentoFavorecidosFinais() {
    const log = logger.forMethod('processDespesasPagamentoFavorecidosFinais');
    const BATCH_SIZE = 1000;
    let offset = 0;
    let hasMore = true;

    log.info('Iniciando processamento de Despesas - Favorecidos Finais de Pagamento');

    try {
        while (hasMore) {
            const bronzeData = await bronze_db.select()
                .from(DespesasPagamentoFavorecidosFinais)
                .limit(BATCH_SIZE)
                .offset(offset);

            if (bronzeData.length === 0) {
                hasMore = false;
                break;
            }

            const pagLote = [...new Set(bronzeData.map(b => b.codigoPagamento).filter(Boolean) as string[])];
            const pagDb = pagLote.length > 0
                ? await silver_db.select({ id: despesasPagamentos.id, codigo: despesasPagamentos.codigoPagamento })
                    .from(despesasPagamentos)
                    .where(inArray(despesasPagamentos.codigoPagamento, pagLote))
                : [];
            const pagIdMap = new Map(pagDb.map(p => [p.codigo, p.id]));

            const pfMap = new Map<string, typeof pessoasFisicas.$inferInsert>();
            const pjMap = new Map<string, typeof pessoasJuridicas.$inferInsert>();

            bronzeData.forEach(b => {
                const docType = getDocType(b.codigoFavorecido);
                if (docType === 'PF') {
                    const cpf = formatCpf(b.codigoFavorecido)!;
                    pfMap.set(cpf, { cpf, nome: b.favorecido || 'NÃO INFORMADO' });
                } else if (docType === 'PJ') {
                    const cnpj = formatCnpj(b.codigoFavorecido)!;
                    pjMap.set(cnpj, { cnpj, razaoSocial: b.favorecido || 'NÃO INFORMADO' });
                }
            });

            if (pjMap.size > 0) {
                await silver_db.insert(pessoasJuridicas).values(Array.from(pjMap.values())).onDuplicateKeyUpdate({ set: { id: sql`id` } });
            }

            if (pfMap.size > 0) {
                const exists = await silver_db.select({ cpf: pessoasFisicas.cpf }).from(pessoasFisicas).where(inArray(pessoasFisicas.cpf, Array.from(pfMap.keys())));
                const eSet = new Set(exists.map(e => e.cpf));
                const toInsert = Array.from(pfMap.values()).filter(p => !eSet.has(p.cpf));
                if (toInsert.length > 0) await silver_db.insert(pessoasFisicas).values(toInsert);
            }

            const pfDb = pfMap.size > 0 ? await silver_db.select({ id: pessoasFisicas.id, cpf: pessoasFisicas.cpf }).from(pessoasFisicas).where(inArray(pessoasFisicas.cpf, Array.from(pfMap.keys()))) : [];
            const pjDb = pjMap.size > 0 ? await silver_db.select({ id: pessoasJuridicas.id, cnpj: pessoasJuridicas.cnpj }).from(pessoasJuridicas).where(inArray(pessoasJuridicas.cnpj, Array.from(pjMap.keys()))) : [];
            const pfIdMap = new Map(pfDb.map(p => [p.cpf, p.id]));
            const pjIdMap = new Map(pjDb.map(p => [p.cnpj, p.id]));

            const favFinaisToInsert = bronzeData.map(b => {
                const pagamentoId = b.codigoPagamento ? pagIdMap.get(b.codigoPagamento) : null;
                if (!pagamentoId || !b.codigoLista) return null;

                let favorecidoPfId = null;
                let favorecidoPjId = null;
                const docType = getDocType(b.codigoFavorecido);
                if (docType === 'PF') {
                    favorecidoPfId = pfIdMap.get(formatCpf(b.codigoFavorecido)!) || null;
                } else if (docType === 'PJ') {
                    favorecidoPjId = pjIdMap.get(formatCnpj(b.codigoFavorecido)!) || null;
                }

                return {
                    pagamentoId,
                    codigoLista: b.codigoLista,
                    favorecidoPfId,
                    favorecidoPjId,
                    valorPagamento: parseDecimal(b.valorPagamento) || '0',
                };
            }).filter(Boolean) as typeof despesasPagamentoFavorecidosFinais.$inferInsert[];

            if (favFinaisToInsert.length > 0) {
                const pagIds = [...new Set(favFinaisToInsert.map(f => f.pagamentoId))];
                await silver_db.delete(despesasPagamentoFavorecidosFinais).where(inArray(despesasPagamentoFavorecidosFinais.pagamentoId, pagIds));

                log.info({ data: { count: favFinaisToInsert.length } }, 'Inserindo despesas_pagamento_favorecidos_finais');
                for (let i = 0; i < favFinaisToInsert.length; i += 1000) {
                    await silver_db.insert(despesasPagamentoFavorecidosFinais).values(favFinaisToInsert.slice(i, i + 1000));
                }
            }
            offset += BATCH_SIZE;
        }
    } catch (error) {
        log.error({ data: error }, 'Erro fatal durante o processamento de Favorecidos Finais de Pagamento');
        throw error;
    }
}

export async function processDespesasPagamentoListaBancos() {
    const log = logger.forMethod('processDespesasPagamentoListaBancos');
    const BATCH_SIZE = 1000;
    let offset = 0;
    let hasMore = true;

    log.info('Iniciando processamento de Despesas - Lista de Bancos');

    try {
        while (hasMore) {
            const bronzeData = await bronze_db.select()
                .from(DespesasPagamentoListaBancos)
                .limit(BATCH_SIZE)
                .offset(offset);

            if (bronzeData.length === 0) {
                hasMore = false;
                break;
            }

            const bancoMap = new Map<string, typeof bancos.$inferInsert>();
            bronzeData.forEach(b => {
                if (b.codigoBanco) {
                    bancoMap.set(b.codigoBanco, { codigo: b.codigoBanco, nome: b.nomeBanco || 'NÃO INFORMADO' });
                }
            });

            const bancosToUpsert = Array.from(bancoMap.values());
            if (bancosToUpsert.length > 0) {
                await silver_db.insert(bancos).values(bancosToUpsert).onDuplicateKeyUpdate({ set: { nome: sql`VALUES(nome)` } });
            }

            const bancosDb = bancosToUpsert.length > 0
                ? await silver_db.select({ id: bancos.id, codigo: bancos.codigo }).from(bancos).where(inArray(bancos.codigo, bancosToUpsert.map(b => b.codigo)))
                : [];
            const bancoIdMap = new Map(bancosDb.map(b => [b.codigo, b.id]));

            const pagLote = [...new Set(bronzeData.map(b => b.codigoPagamento).filter(Boolean) as string[])];
            const pagDb = pagLote.length > 0
                ? await silver_db.select({ id: despesasPagamentos.id, codigo: despesasPagamentos.codigoPagamento })
                    .from(despesasPagamentos)
                    .where(inArray(despesasPagamentos.codigoPagamento, pagLote))
                : [];
            const pagIdMap = new Map(pagDb.map(p => [p.codigo, p.id]));

            const listaToInsert = bronzeData.map(b => {
                const pagamentoId = b.codigoPagamento ? pagIdMap.get(b.codigoPagamento) : null;
                const bancoId = b.codigoBanco ? bancoIdMap.get(b.codigoBanco) : null;

                if (!pagamentoId || !bancoId || !b.codigoLista) return null;

                return {
                    pagamentoId,
                    codigoLista: b.codigoLista,
                    bancoId,
                    agencia: b.agencia || 'N/A',
                    valor: parseDecimal(b.valor) || '0',
                };
            }).filter(Boolean) as typeof despesasPagamentoListaBancos.$inferInsert[];

            if (listaToInsert.length > 0) {
                const pagIds = [...new Set(listaToInsert.map(l => l.pagamentoId))];
                await silver_db.delete(despesasPagamentoListaBancos).where(inArray(despesasPagamentoListaBancos.pagamentoId, pagIds));

                log.info({ data: { count: listaToInsert.length } }, 'Inserindo despesas_pagamento_lista_bancos');
                for (let i = 0; i < listaToInsert.length; i += 1000) {
                    await silver_db.insert(despesasPagamentoListaBancos).values(listaToInsert.slice(i, i + 1000));
                }
            }
            offset += BATCH_SIZE;
        }
    } catch (error) {
        log.error({ data: error }, 'Erro fatal durante o processamento de Lista de Bancos');
        throw error;
    }
}

export async function processDespesasPagamentoListaFaturas() {
    const log = logger.forMethod('processDespesasPagamentoListaFaturas');
    const BATCH_SIZE = 1000;
    let offset = 0;
    let hasMore = true;

    log.info('Iniciando processamento de Despesas - Lista de Faturas');

    try {
        while (hasMore) {
            const bronzeData = await bronze_db.select()
                .from(DespesasPagamentoListaFaturas)
                .limit(BATCH_SIZE)
                .offset(offset);

            if (bronzeData.length === 0) {
                hasMore = false;
                break;
            }

            const pagLote = [...new Set(bronzeData.map(b => b.codigoPagamento).filter(Boolean) as string[])];
            const pagDb = pagLote.length > 0
                ? await silver_db.select({ id: despesasPagamentos.id, codigo: despesasPagamentos.codigoPagamento })
                    .from(despesasPagamentos)
                    .where(inArray(despesasPagamentos.codigoPagamento, pagLote))
                : [];
            const pagIdMap = new Map(pagDb.map(p => [p.codigo, p.id]));

            const faturasToInsert = bronzeData.map(b => {
                const pagamentoId = b.codigoPagamento ? pagIdMap.get(b.codigoPagamento) : null;
                if (!pagamentoId || !b.codigoLista || !b.codigoSequencia) return null;

                return {
                    pagamentoId,
                    codigoLista: b.codigoLista,
                    sequencialFatura: parseInt(b.codigoSequencia) || 0,
                    valorLancamento: parseDecimal(b.valorLancamento) || '0',
                    valorDesconto: parseDecimal(b.valorDesconto) || '0',
                    valorJuros: parseDecimal(b.valorJuros) || '0',
                    valorDeducao: parseDecimal(b.valorDeducao) || '0',
                    valorAcrescimo: parseDecimal(b.valorAcrescimo) || '0',
                };
            }).filter(Boolean) as typeof despesasPagamentoListaFaturas.$inferInsert[];

            if (faturasToInsert.length > 0) {
                const pagIds = [...new Set(faturasToInsert.map(f => f.pagamentoId))];
                await silver_db.delete(despesasPagamentoListaFaturas).where(inArray(despesasPagamentoListaFaturas.pagamentoId, pagIds));

                log.info({ data: { count: faturasToInsert.length } }, 'Inserindo despesas_pagamento_lista_faturas');
                for (let i = 0; i < faturasToInsert.length; i += 1000) {
                    await silver_db.insert(despesasPagamentoListaFaturas).values(faturasToInsert.slice(i, i + 1000));
                }
            }
            offset += BATCH_SIZE;
        }
    } catch (error) {
        log.error({ data: error }, 'Erro fatal durante o processamento de Lista de Faturas');
        throw error;
    }
}

export async function processDespesasPagamentoListaPrecatorios() {
    const log = logger.forMethod('processDespesasPagamentoListaPrecatorios');
    const BATCH_SIZE = 1000;
    let offset = 0;
    let hasMore = true;

    log.info('Iniciando processamento de Despesas - Lista de Precatórios');

    try {
        while (hasMore) {
            const bronzeData = await bronze_db.select()
                .from(DespesasPagamentoListaPrecatorios)
                .limit(BATCH_SIZE)
                .offset(offset);

            if (bronzeData.length === 0) {
                hasMore = false;
                break;
            }

            const pagLote = [...new Set(bronzeData.map(b => b.codigoPagamento).filter(Boolean) as string[])];
            const pagDb = pagLote.length > 0
                ? await silver_db.select({ id: despesasPagamentos.id, codigo: despesasPagamentos.codigoPagamento })
                    .from(despesasPagamentos)
                    .where(inArray(despesasPagamentos.codigoPagamento, pagLote))
                : [];
            const pagIdMap = new Map(pagDb.map(p => [p.codigo, p.id]));

            const precatoriosToInsert = bronzeData.map(b => {
                const pagamentoId = b.codigoPagamento ? pagIdMap.get(b.codigoPagamento) : null;
                if (!pagamentoId || !b.codigoLista || !b.numeroParcela) return null;

                return {
                    pagamentoId,
                    codigoLista: b.codigoLista,
                    numeroParcela: b.numeroParcela,
                    valorPrecatorio: parseDecimal(b.valorPrecatorio) || '0',
                };
            }).filter(Boolean) as typeof despesasPagamentoListaPrecatorios.$inferInsert[];

            if (precatoriosToInsert.length > 0) {
                const pagIds = [...new Set(precatoriosToInsert.map(p => p.pagamentoId))];
                await silver_db.delete(despesasPagamentoListaPrecatorios).where(inArray(despesasPagamentoListaPrecatorios.pagamentoId, pagIds));

                log.info({ data: { count: precatoriosToInsert.length } }, 'Inserindo despesas_pagamento_lista_precatorios');
                for (let i = 0; i < precatoriosToInsert.length; i += 1000) {
                    await silver_db.insert(despesasPagamentoListaPrecatorios).values(precatoriosToInsert.slice(i, i + 1000));
                }
            }
            offset += BATCH_SIZE;
        }
    } catch (error) {
        log.error({ data: error }, 'Erro fatal durante o processamento de Lista de Precatórios');
        throw error;
    }
}

export async function processDespesas() {
    const log = logger.forMethod('processDespesas');
    try {
        await processDespesasEmpenho();
        await processDespesasItemEmpenho();
        await processDespesasItemEmpenhoHistorico();
        await processDespesasLiquidacao();
        await processDespesasLiquidacaoEmpenhosImpactados();
        await processDespesasPagamento();
        await processDespesasPagamentoEmpenhosImpactados();
        await processDespesasPagamentoFavorecidosFinais();
        await processDespesasPagamentoListaBancos();
        await processDespesasPagamentoListaFaturas();
        await processDespesasPagamentoListaPrecatorios();
    } catch (error) {
        log.error({ data: error }, 'Erro fatal durante a orquestração do módulo de Despesas');
        throw error;
    }
}