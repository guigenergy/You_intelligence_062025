// /apps/worker/importWorker.ts
import { Pool } from 'pg';
import * as fs from 'node:fs';
import * as path from 'node:path';
import fetch from 'node-fetch';
import * as unzipper from 'unzipper';
import { pipeline } from 'stream/promises';
import { Readable } from 'stream';

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const BASE_DIR = process.env.DOWNLOAD_DIR ?? path.resolve(process.cwd(), 'data', 'downloads');
const BATCH = Number(process.env.WORKER_BATCH ?? 2);

async function getUrl(distribuidora_nome: string, ano: number) {
  const { rows } = await pool.query(
    `SELECT url
       FROM dataset_url_normalized
      WHERE distribuidora = $1 AND ano = $2
      LIMIT 1;`,
    [distribuidora_nome, ano]
  );
  return rows[0]?.url ?? null;
}

async function loop() {
  while (true) {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');

      const { rows: jobs } = await client.query(
        `
        SELECT import_id, distribuidora_nome, ano
          FROM import_status
         WHERE status = 'queued'
         ORDER BY ano, distribuidora_nome
         FOR UPDATE SKIP LOCKED
         LIMIT $1;
        `,
        [BATCH]
      );

      if (!jobs.length) {
        await client.query('COMMIT');
        await new Promise(r => setTimeout(r, 3000));
        continue;
      }

      // marca como running + data_inicio
      for (const j of jobs) {
        await client.query(
          `UPDATE import_status
              SET status='running', data_inicio = now(), erro = NULL
            WHERE import_id = $1;`,
          [j.import_id]
        );
      }
      await client.query('COMMIT'); // libera o lock

      // processa cada job fora da transação
      for (const j of jobs) {
        const url = await getUrl(j.distribuidora_nome, j.ano);
        if (!url) {
          await pool.query(
            `UPDATE import_status SET status='error', erro=$2, data_fim=now()
              WHERE import_id=$1;`,
            [j.import_id, 'URL não encontrada na view']
          );
          continue;
        }

        const destDir = path.join(BASE_DIR, j.distribuidora_nome, String(j.ano));
        await fs.promises.mkdir(destDir, { recursive: true });

        const zipPath = path.join(destDir, `dataset_${j.ano}.zip`);
        try {
          // baixa
          const res = await fetch(url);
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          if (!res.body) throw new Error('Resposta sem corpo');
          await new Promise<void>(async (resolve, reject) => {
            const ws = fs.createWriteStream(zipPath);
            const nodeReadable =
            typeof (res.body as any).pipe === 'function'
            ? (res.body as unknown as NodeJS.ReadableStream)   
            : Readable.fromWeb(res.body as any);                     
            await pipeline(nodeReadable, ws);

          });

          // unzip
          const unzipDest = path.join(destDir, 'unzipped');
          await fs.promises.mkdir(unzipDest, { recursive: true });
          await fs.createReadStream(zipPath).pipe(unzipper.Extract({ path: unzipDest })).promise();

          // (opcional) inferir a camada olhando nomes dos arquivos extraídos
          let camada: string | null = null;
          const files = await fs.promises.readdir(unzipDest);
          const hit = files.find(f => /UCMT|UCBT|UCAT|PONNOT/i.test(f));
          if (hit) {
            const m = hit.match(/(UCMT|UCBT|UCAT|PONNOT)/i);
            camada = m ? m[1].toUpperCase() : null;
          }

          await pool.query(
            `UPDATE import_status
               SET status='done',
                   camada = COALESCE($2, camada),
                   data_fim = now(),
                   observacoes = $3
             WHERE import_id = $1`,
            [j.import_id, camada, `Arquivos em: ${unzipDest}`]
          );
        } catch (e:any) {
          await pool.query(
            `UPDATE import_status
               SET status='error', erro=$2, data_fim=now()
             WHERE import_id=$1`,
            [j.import_id, e.message ?? String(e)]
          );
        }
      }
    } catch (e) {
      await pool.query('ROLLBACK').catch(()=>{});
    } finally {
      client.release();
    }
  }
}

loop().catch(console.error);
