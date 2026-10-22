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

function formatValidDate(year: number, month: number, day: number): string | null {
  if (year < 1900 || year > 2100 || month < 1 || month > 12 || day < 1 || day > 31) {
    return null;
  }
  const dt = new Date(Date.UTC(year, month - 1, day));
  if (dt.getUTCFullYear() !== year || dt.getUTCMonth() !== month - 1 || dt.getUTCDate() !== day) {
    return null;
  }
  return `${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
}

/**
 * Analisa e converte datas da camada bronze, tipicamente em DD/MM/YYYY
 * para o padrão de banco de dados relacional YYYY-MM-DD com validação estrita.
 */
export function parseDate(dateStr?: string | null): string | null {
  if (!dateStr) return null;
  const raw = dateStr.replace(/["']/g, '').trim().split(/[ T\r\n]/)[0];
  if (!raw) return null;

  const invertedMatch = raw.match(/^(\d{2})(\d{2})-(\d{2})-(\d{2})$/);
  if (invertedMatch) {
    const [, m, c, y, d] = invertedMatch;
    if (c === '19' || c === '20') {
      const res = formatValidDate(parseInt(`${c}${y}`, 10), parseInt(m, 10), parseInt(d, 10));
      if (res) return res;
    }
  }

  if (raw.includes('/') || raw.includes('-')) {
    const parts = raw.split(/[\/\-]/);
    if (parts.length === 3) {
      const [p0, p1, p2] = parts;
      if (p0.length === 4) {
        const y = parseInt(p0, 10);
        const m = parseInt(p1, 10);
        const d = parseInt(p2, 10);
        const standard = formatValidDate(y, m, d);
        if (standard) return standard;
        const swapped = formatValidDate(y, d, m);
        if (swapped) return swapped;
      } else if (p2.length === 4) {
        const y = parseInt(p2, 10);
        const d = parseInt(p0, 10);
        const m = parseInt(p1, 10);
        const br = formatValidDate(y, m, d);
        if (br) return br;
        const us = formatValidDate(y, d, m);
        if (us) return us;
      }
    } else if (parts.length === 2 && parts[1].length === 4) {
      const m = parseInt(parts[0], 10);
      const y = parseInt(parts[1], 10);
      return formatValidDate(y, m, 1);
    }
  }

  const digits = raw.replace(/\D/g, '');
  if (digits.length === 8) {
    const dBr = parseInt(digits.slice(0, 2), 10);
    const mBr = parseInt(digits.slice(2, 4), 10);
    const yBr = parseInt(digits.slice(4, 8), 10);
    const brRes = formatValidDate(yBr, mBr, dBr);
    if (brRes) return brRes;

    const yIso = parseInt(digits.slice(0, 4), 10);
    const mIso = parseInt(digits.slice(4, 6), 10);
    const dIso = parseInt(digits.slice(6, 8), 10);
    const isoRes = formatValidDate(yIso, mIso, dIso);
    if (isoRes) return isoRes;
  }

  return null;
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
  const trimmed = anoMes.trim();
  if (!trimmed) return null;

  if (trimmed.includes('/') || trimmed.includes('-')) {
    const parts = trimmed.split(/[\/\-]/);
    if (parts.length === 2) {
      if (parts[1].length === 4) {
        return `${parts[1]}-${parts[0].padStart(2, '0')}`;
      }
      if (parts[0].length === 4) {
        return `${parts[0]}-${parts[1].padStart(2, '0')}`;
      }
    }
  }

  const cleaned = trimmed.replace(/\D/g, '');
  if (cleaned.length === 6) {
    if (cleaned.startsWith('19') || cleaned.startsWith('20')) {
      return `${cleaned.substring(0, 4)}-${cleaned.substring(4, 6)}`;
    }
    if (cleaned.substring(2).startsWith('19') || cleaned.substring(2).startsWith('20')) {
      return `${cleaned.substring(2, 6)}-${cleaned.substring(0, 2)}`;
    }
  }

  return cleaned.length >= 6 ? `${cleaned.substring(0, 4)}-${cleaned.substring(4, 6)}` : null;
}

export function formatCpfByNis(nis?: string | null): string {
  // Garantia Safra na API frequentemente fornece apenas NIS. 
  // Na ausência do CPF (que é NOT NULL), usamos o NIS formatado ou um fallback genérico seguro
  // como CPF "00000000000" para permitir inserção sem quebrar a constraint,
  // mas o ideal é que a extração mapeie corretamente. 
  // Aqui geramos um placeholder baseado no NIS se o CPF real não existir.
  if (!nis) return '00000000000';
  const numbers = nis.replace(/\D/g, '');
  return numbers.padStart(11, '0').slice(0, 11);
}