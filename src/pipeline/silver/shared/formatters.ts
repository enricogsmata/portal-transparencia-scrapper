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