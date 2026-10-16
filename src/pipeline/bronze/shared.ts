import path from "node:path";

export const ZIP_FILES_DIRECTORY = path.resolve(process.cwd(), 'src/files/');

export interface ReferenceMetadata {
    exercicioReferencia: string | null;
    mesReferencia: string | null;
    diaReferencia: string | null;
    dadosAdicionais: string | null;
}

export function extractMetadataFromFileName(fileName: string): ReferenceMetadata {
    const baseName = path.basename(fileName, path.extname(fileName));

    const matchYYYYMMDDExtra = baseName.match(/^(\d{4})(\d{2})(\d{2})_(.+)$/);
    if (matchYYYYMMDDExtra) {
        return {
            exercicioReferencia: matchYYYYMMDDExtra[1],
            mesReferencia: matchYYYYMMDDExtra[2],
            diaReferencia: matchYYYYMMDDExtra[3],
            dadosAdicionais: matchYYYYMMDDExtra[4],
        };
    }

    const matchYYYYMMDD = baseName.match(/^(\d{4})(\d{2})(\d{2})$/);
    if (matchYYYYMMDD) {
        return {
            exercicioReferencia: matchYYYYMMDD[1],
            mesReferencia: matchYYYYMMDD[2],
            diaReferencia: matchYYYYMMDD[3],
            dadosAdicionais: null,
        };
    }

    const matchYYYYMMExtra = baseName.match(/^(\d{4})(\d{2})_(.+)$/);
    if (matchYYYYMMExtra) {
        return {
            exercicioReferencia: matchYYYYMMExtra[1],
            mesReferencia: matchYYYYMMExtra[2],
            diaReferencia: null,
            dadosAdicionais: matchYYYYMMExtra[3],
        };
    }

    const matchYYYYMM = baseName.match(/^(\d{4})(\d{2})$/);
    if (matchYYYYMM) {
        return {
            exercicioReferencia: matchYYYYMM[1],
            mesReferencia: matchYYYYMM[2],
            diaReferencia: null,
            dadosAdicionais: null,
        };
    }

    const matchYYYY = baseName.match(/^(\d{4})$/);
    if (matchYYYY) {
        return {
            exercicioReferencia: matchYYYY[1],
            mesReferencia: null,
            diaReferencia: null,
            dadosAdicionais: null,
        };
    }

    return {
        exercicioReferencia: null,
        mesReferencia: null,
        diaReferencia: null,
        dadosAdicionais: baseName,
    };
}

export const normalize = (str: string): string =>
    str ? str
        .replace(/ç/gi, "c")             // Converte Ç / ç para C / c antes de remover acentos
        .replace(/\$/g, "S")             // Converte $ para S
        .normalize("NFD")                // Decompõe vogais acentuadas (ex: Í -> I + ◌́)
        .replace(/[\u0300-\u036f]/g, "") // Remove os acentos decompostos
        .replace(/[^A-Z0-9\s]/gi, "")   // Remove caracteres especiais restantes
        .replace(/\s+/g, " ")           // Unifica múltiplos espaços
        .trim()
        .toUpperCase()
        : "";