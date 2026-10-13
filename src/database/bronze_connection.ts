import 'dotenv/config';
import { drizzle } from 'drizzle-orm/mysql2';
import mysql from 'mysql2/promise'

const connection_string = process.env.BRONZE_DATABASE_URL || '';
const pool = mysql.createPool(connection_string);

export const bronze_db = drizzle(pool);