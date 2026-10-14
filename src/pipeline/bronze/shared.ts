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