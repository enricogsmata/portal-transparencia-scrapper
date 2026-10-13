import path from "node:path";
import { Writable } from "node:stream";
import { fileURLToPath } from "node:url";
import pino from "pino";
import { db } from "../database/connection.js";
import { Logs } from "../database/schema.js";

type Log = typeof Logs.$inferInsert;

export function new_logger(import_meta_url: string) {
    const module_name = path.basename(fileURLToPath(import_meta_url));
    const logger = base_logger.child({ module: module_name });
    return {
        logger: logger,
        forMethod(method_name: string) {
            return logger.child({ context: method_name });
        }
    }
}

const db_stream = new Writable({
    async write(chunk, enconding, callback) {
        try {
            const logObj = JSON.parse(chunk.toString());

            if (logObj.level) {
                await db.insert(Logs).values({
                    created_at: new Date(logObj.time) ?? new Date(),
                    category: logObj.level ?? 'Indefinido',
                    module: logObj.module ?? 'Indefinido',
                    context: logObj.context ?? 'Indefinido',
                    data: logObj.data ?? 'Indefinido',
                    message: logObj.msg ?? 'Indefinido',
                }).execute();
            }

            callback();
        } catch (error: any) {
            console.error(`Falha ao inserir log no banco de dados\nData Length: ${chunk.toString().data.length ?? 0}\nErro: ${JSON.stringify(error, null, 4)}`);
            callback(error as Error);
        }
    }
})

const pino_stream = pino.transport({
    target: 'pino-pretty',
    options: {
        colorize: true,
        translateTime: 'SYS:yyyy-mm-dd HH:MM:ss',
        ignore: 'pid,hostname',
    },
})

const base_logger = pino({
    level: process.env.LOG_LEVEL || 'info'
},
    pino.multistream([
        { stream: db_stream },
        { stream: pino_stream },
    ])
);