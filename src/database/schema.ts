import { timestamp, varchar, mysqlTable, serial, text } from "drizzle-orm/mysql-core";

export const Logs = mysqlTable('logs', {
    id: serial('id'),
    category: varchar('category', { length: 255 }).default(''),
    module: varchar('module', { length: 255 }).default(''),
    context: varchar('context', { length: 255 }).default(''),
    message: varchar('message', { length: 255 }).default(''),
    data: text('data').default(''),
    created_at: timestamp('created_at').notNull(),
})