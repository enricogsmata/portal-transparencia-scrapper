import 'dotenv/config';
import { drizzle } from 'drizzle-orm/mysql2';
import mysql from 'mysql2/promise';
import * as silverSchema from './silver_schema.js';

const connection_string = process.env.SILVER_DATABASE_URL || process.env.BRONZE_DATABASE_URL || '';
const pool = mysql.createPool({
  uri: connection_string,
  waitForConnections: true,
  connectionLimit: 10,
  maxIdle: 10,
  idleTimeout: 60000,
  queueLimit: 0,
  enableKeepAlive: true,
  keepAliveInitialDelay: 0,
});

export const silver_db = drizzle(pool, { schema: silverSchema, mode: 'default' });
export const silver_pool = pool;
