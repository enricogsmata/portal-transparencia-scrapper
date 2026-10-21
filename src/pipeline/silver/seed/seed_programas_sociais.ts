import { sql } from 'drizzle-orm';
import { silver_db } from '../../../database/silver_connection.js';
import { programasSociais } from '../../../database/silver_schema.js';

const CATALOGO_PROGRAMAS_SOCIAIS = [
  { codigo: 'AUXILIO_BRASIL', nome: 'Programa Auxílio Brasil', descricao: 'Pagamentos do programa social Auxílio Brasil aos cidadãos' },
  { codigo: 'AUXILIO_EMERGENCIAL', nome: 'Auxílio Emergencial', descricao: 'Concessões e parcelas do Auxílio Emergencial e extensões' },
  { codigo: 'AUXILIO_RECONSTRUCAO', nome: 'Auxílio Reconstrução', descricao: 'Auxílio de reconstrução financeira para vítimas de calamidades' },
  { codigo: 'BOLSA_FAMILIA', nome: 'Programa Bolsa Família', descricao: 'Folha de pagamentos mensal do Programa Bolsa Família' },
  { codigo: 'BOLSA_FAMILIA_SAQUE', nome: 'Saques do Bolsa Família', descricao: 'Registros de saques efetivados de parcelas do Bolsa Família' },
  { codigo: 'NOVO_BOLSA_FAMILIA', nome: 'Novo Bolsa Família', descricao: 'Pagamentos mensais do Novo Bolsa Família unificado' },
  { codigo: 'GARANTIA_SAFRA', nome: 'Garantia-Safra', descricao: 'Benefício do Programa Garantia-Safra para agricultores familiares' },
  { codigo: 'GAS_DO_POVO', nome: 'Gás do Povo', descricao: 'Auxílio Gás dos Brasileiros (Gás do Povo) e controle de vales' },
  { codigo: 'PE_DE_MEIA', nome: 'Pé-de-Meia', descricao: 'Incentivo financeiro-educacional para estudantes do Ensino Médio' },
  { codigo: 'PETI', nome: 'PETI', descricao: 'Programa de Erradicação do Trabalho Infantil' },
  { codigo: 'BPC', nome: 'BPC/LOAS', descricao: 'Benefício de Prestação Continuada' },
  { codigo: 'SEGURO_DEFESO', nome: 'Seguro Defeso', descricao: 'Seguro-desemprego concedido a pescadores artesanais no período de defeso' }
];

export async function generateProgramasSociaisSeed() {
  await silver_db
    .insert(programasSociais)
    .values(CATALOGO_PROGRAMAS_SOCIAIS)
    .onDuplicateKeyUpdate({
      set: {
        nome: sql`VALUES(nome)`,
        descricao: sql`VALUES(descricao)`,
      },
    });
}