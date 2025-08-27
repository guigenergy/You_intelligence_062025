export type ImportStatusItem = {
  distribuidora: string;
  ano: number;
  camada: string | null;
  status: 'pendente' | 'baixando' | 'extraindo' | 'importando' | 'concluido' | 'erro';
  data_execucao: string; // ISO
};

const API = (process.env.NEXT_PUBLIC_API_URL || 'http://127.0.0.1:8000/v1').replace(/\/$/, '');

export async function apiImportar(distribuidoras: string[], anos: number[], camada?: string) {
  const body: any = { distribuidoras, anos };
  if (camada) body.camada = camada;     // opcional: só envia se você quiser forçar a camada
  const r = await fetch(`${API}/importar`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!r.ok) {
    const txt = await r.text().catch(() => '');
    throw new Error(`Falha ao enfileirar (${r.status}): ${txt}`);
  }
  return (await r.json()) as { enfileirados: number; aviso?: string };
}

export async function apiImportStatus(limit = 200) {
  const r = await fetch(`${API}/import-status?limit=${limit}`, { cache: 'no-store' });
  if (!r.ok) throw new Error(`Falha ao carregar status (${r.status})`);
  return (await r.json()) as ImportStatusItem[];
}
