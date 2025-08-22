// /apps/api/importacoes/status/route.ts
import { NextResponse } from 'next/server';
import { Pool } from 'pg';

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

export async function GET() {
  const { rows } = await pool.query(
    `SELECT import_id, distribuidora_nome, ano, camada, status,
            linhas_processadas, data_inicio, data_fim, erro, observacoes
     FROM import_status
     ORDER BY COALESCE(data_inicio, now()) DESC
     LIMIT 200;`
  );
  return NextResponse.json(rows);
}
