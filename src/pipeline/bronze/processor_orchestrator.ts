import { runAuxilioBrasilIngestion } from "./processors/auxilio-brasil.js";

export async function runProcessors() {
    await runAuxilioBrasilIngestion();

    return;
}