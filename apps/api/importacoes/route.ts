// /apps/api/importacoes/route.ts (Next.js app router) OU no seu Express
import { NextRequest, NextResponse } from 'next/server';
import { Pool } from 'pg';
import { randomUUID } from 'crypto';

const pool = new Pool({ connectionString: process.env.DATABASE_URL });

export async function POST(req: NextRequest) {
  const { distribuidoras, anos } = await req.json();

  if (!Array.isArray(distribuidoras) || !distribuidoras.length ||
      !Array.isArray(anos) || !anos.length) {
    return NextResponse.json({ error: 'Seleção inválida' }, { status: 400 });
  }

  const client = await pool.connect();
  try {
    // 1) busca URLs
    const { rows } = await client.query(
      `
      SELECT id, distribuidora, ano, url, title
      FROM dataset_url_normalized
      WHERE distribuidora = ANY($1) AND ano = ANY($2)
      ORDER BY distribuidora, ano;
      `,
      [distribuidoras, anos]
    );

    if (!rows.length) return NextResponse.json({ enfileirados: 0 });

    // 2) cria linhas em import_status (status = queued)
    const values: any[] = [];
    const placeholders: string[] = [];
    rows.forEach((r: any, i: number) => {
      // um import_id por linha facilita o tracking no grid
      const importId = randomUUID();
      // camada: você pode deixar null agora; quando processar e encontrar UCMT/UCBT etc, atualize
      values.push(
        importId,             // import_id
        null,                 // distribuidora_id (se não tiver, mantém null)
        r.ano,                // ano
        null,                 // camada
        'queued',             // status
        0,                    // linhas_processadas
        null,                 // data_inicio
        null,                 // data_fim
        null,                 // observacoes
        null,                 // erro
        r.distribuidora,      // distribuidora_nome
        r.id                  // << vamos guardar o id da linha do catálogo como observação (opcional)
      );
      const base = i * 12;
      placeholders.push(`($${base+1},$${base+2},$${base+3},$${base+4},$${base+5},$${base+6},$${base+7},$${base+8},$${base+9},$${base+10},$${base+11})`);
    });

    await client.query(
      `
      INSERT INTO import_status
        (import_id, distribuidora_id, ano, camada, status, linhas_processadas,
         data_inicio, data_fim, observacoes, erro, distribuidora_nome)
      VALUES ${placeholders.join(',')};
      `,
      values
    );

    return NextResponse.json({ enfileirados: rows.length });
  } finally {
    client.release();
  }
}
