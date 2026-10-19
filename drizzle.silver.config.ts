import 'dotenv/config';
import { defineConfig } from 'drizzle-kit';

export default defineConfig({
    schema: 'src/database/silver_schema.ts',
    dialect: 'mysql',
    out: 'src/database/out/silver/',
    dbCredentials: { url: process.env.SILVER_DATABASE_URL || ''},
});