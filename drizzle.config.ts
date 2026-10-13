import 'dotenv/config';
import { defineConfig } from 'drizzle-kit';

export default defineConfig({
    schema: 'src/database/schema.ts',
    dialect: 'mysql',
    out: 'src/database/out/',
    dbCredentials: { url: process.env.DATABASE_URL || ''},
});