// src\pipeline\silver\shared\formatters.ts
/**
 * Normaliza e formata CNPJ desformatado da camada bronze (apenas números ou formatos parciais)
 * para o padrão completo da camada silver: XX.XXX.XXX/XXXX-XX
 */
export function formatCnpj(cnpj?: string | null): string | null {
  if (!cnpj) return null;
  const numbers = cnpj.replace(/\D/g, '').padStart(14, '0');
  return `${numbers.slice(0, 2)}.${numbers.slice(2, 5)}.${numbers.slice(5, 8)}/${numbers.slice(8, 12)}-${numbers.slice(12, 14)}`;
}

/**
 * Analisa e converte datas da camada bronze, tipicamente em DD/MM/YYYY
 * para o padrão de banco de dados relacional YYYY-MM-DD.
 */
export function parseDate(dateStr?: string | null): string | null {
  if (!dateStr) return null;
  const parts = dateStr.split('/');
  if (parts.length === 3) {
    return `${parts[2]}-${parts[1].padStart(2, '0')}-${parts[0].padStart(2, '0')}`;
  }
  return dateStr;
}

/**
 * Normaliza strings financeiras do formato brasileiro (1.234,56) 
 * para o formato aceito pelo SQL/Decimal (1234.56).
 */
export function parseDecimal(value?: string | null): string | null {
  if (!value) return null;
  const cleaned = value.replace(/\./g, '').replace(',', '.');
  return isNaN(Number(cleaned)) ? null : cleaned;
}

export function formatCep(cep?: string | null): string | null {
  if (!cep) return null;
  const cleaned = cep.replace(/\D/g, '').padStart(8, '0');
  return `${cleaned.slice(0, 5)}-${cleaned.slice(5, 8)}`;
}

export function formatCpf(cpf?: string | null): string | null {
  if (!cpf || cpf.trim() === '' || cpf.trim().toUpperCase() === 'SIGILOSO' || cpf.trim().toUpperCase() === 'NÃO APLICÁVEL') return null;
  const numbers = cpf.replace(/\D/g, '');
  return numbers.length > 0 ? numbers.padStart(11, '0') : null;
}

export function getDocumentType(doc?: string | null): 'PF' | 'PJ' | null {
  if (!doc || doc.trim().toUpperCase() === 'SIGILOSO' || doc.trim().toUpperCase() === 'NÃO APLICÁVEL') return null;
  const numbers = doc.replace(/\D/g, '');
  if (numbers.length === 11) return 'PF';
  if (numbers.length > 11) return 'PJ';
  return null;
}

export function formatAnoMes(anoMes?: string | null): string | null {
  if (!anoMes) return null;
  const cleaned = anoMes.replace(/\D/g, '');
  if (cleaned.length === 6) {
    return `${cleaned.substring(0, 4)}-${cleaned.substring(4, 6)}`;
  }
  return cleaned.substring(0, 7);
}