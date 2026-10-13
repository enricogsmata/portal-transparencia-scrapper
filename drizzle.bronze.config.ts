import 'dotenv/config';
import { defineConfig } from 'drizzle-kit';

export default defineConfig({
    schema: 'src/database/bronze_schema.ts',
    dialect: 'mysql',
    out: 'src/database/out/bronze/',
    dbCredentials: { url: process.env.BRONZE_DATABASE_URL || ''},
});