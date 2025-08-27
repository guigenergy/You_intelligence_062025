'use client';
import useSWR from 'swr';

type Camada = 'UCAT' | 'UCMT' | 'UCBT' | string;
type StatusUI = 'concluido' | 'erro' | 'importando' | 'baixando' | 'extraindo' | 'pendente' | string;

type ImportStatus = {
  id: string;
  distribuidora: string;
  ano: number;
  camada: Camada;
  status: StatusUI;
  data_execucao: string;     // ISO no backend
  observacoes?: any;         // backend já entrega parseado por _parse_obs
  erro?: string | null;
};

const fetcher = async (url: string) => {
  const res = await fetch(url);
  if (!res.ok) {
    const data = await res.json().catch(() => ({}));
    throw new Error(data.detail || `HTTP ${res.status}`);
  }
  return res.json();
};

export default function TabelaStatusImportacoes() {
  const API = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000';
  const { data = [], isLoading, error, mutate } = useSWR(`${API}/v1/import-status?limit=200`, fetcher, { refreshInterval: 5000 });
  
  const rows: ImportStatus[] = data ?? [];
  const statusStyle: Record<string, string> = {
    concluido: 'bg-green-500/20 text-green-400 border border-green-500/30',
    erro: 'bg-red-500/20 text-red-400 border border-red-500/30',
    importando: 'bg-yellow-500/20 text-yellow-400 border border-yellow-500/30',
    baixando: 'bg-blue-500/20 text-blue-400 border border-blue-500/30',
    extraindo: 'bg-purple-500/20 text-purple-400 border border-purple-500/30',
    pendente: 'bg-gray-500/20 text-gray-400 border border-gray-500/30',
    default: 'bg-zinc-600/20 text-zinc-300 border border-zinc-600/30',
  };

  const statusIcons: Record<string, string> = {
    concluido: '✓',
    erro: '✗',
    importando: '↻',
    baixando: '↓',
    extraindo: '⇲',
    pendente: '…',
    default: '•',
  };

  const camadaColors: Record<string, string> = {
    UCAT: 'text-blue-400',
    UCMT: 'text-purple-400',
    UCBT: 'text-green-400',
    default: 'text-zinc-300',
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-xl font-semibold text-white flex items-center gap-2">
          <span className="bg-blue-500/20 p-2 rounded-lg">📦</span>
          <span>Status de Importação</span>
        </h2>
        <div className="flex items-center gap-3">
          <span className="text-xs text-gray-400">Atualizando a cada 5s</span>
          <button
            onClick={() => mutate()}
            className="text-xs px-2 py-1 rounded-md bg-zinc-800 border border-zinc-700 text-gray-300 hover:bg-zinc-700 transition"
            title="Atualizar agora"
          >
            Atualizar
          </button>
        </div>
      </div>

      {error && (
        <div className="p-3 rounded-md bg-red-500/10 border border-red-500/30 text-red-300 text-sm">
          Falha ao carregar status: {error.message}
        </div>
      )}

      {isLoading ? (
        <div className="flex justify-center items-center h-32">
          <div className="animate-spin rounded-full h-8 w-8 border-t-2 border-b-2 border-blue-500" />
        </div>
      ) : (
        <div className="border border-zinc-800 rounded-xl overflow-hidden shadow-lg">
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="bg-zinc-800/80 backdrop-blur-sm">
                <tr className="text-gray-300">
                  <th className="p-3 text-left font-medium">Distribuidora</th>
                  <th className="p-3 text-left font-medium">Ano</th>
                  <th className="p-3 text-left font-medium">Camada</th>
                  <th className="p-3 text-left font-medium">Status</th>
                  <th className="p-3 text-left font-medium">Última Execução</th>
                  <th className="p-3 text-left font-medium">Observações / Erro</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-zinc-800">
                {rows.length > 0 ? (
                  rows.map((row) => {
                    const st = row.status?.toLowerCase?.() || 'default';
                    const style = statusStyle[st] ?? statusStyle.default;
                    const icon = statusIcons[st] ?? statusIcons.default;
                    const camadaClass = camadaColors[row.camada] ?? camadaColors.default;

                    return (
                      <tr key={row.id} className="hover:bg-zinc-800/50 transition-colors">
                        <td className="p-3 text-gray-200">{row.distribuidora}</td>
                        <td className="p-3 text-gray-300">{row.ano}</td>
                        <td className={`p-3 font-medium ${camadaClass}`}>{row.camada}</td>
                        <td className="p-3">
                          <div className={`inline-flex items-center gap-2 px-3 py-1 rounded-full text-xs font-medium ${style}`}>
                            <span>{icon}</span>
                            <span className="capitalize">{row.status}</span>
                          </div>
                        </td>
                        <td className="p-3 text-gray-400 text-xs">
                          {new Date(row.data_execucao).toLocaleString()}
                        </td>
                        <td className="p-3 text-gray-300 text-xs max-w-[24rem]">
                          {row.erro ? (
                            <span className="text-red-300">
                              {row.erro.length > 180 ? row.erro.slice(0, 180) + '…' : row.erro}
                            </span>
                          ) : row.observacoes ? (
                            <code className="bg-zinc-800/80 border border-zinc-700 px-2 py-1 rounded">
                              {typeof row.observacoes === 'string'
                                ? (row.observacoes.length > 180 ? row.observacoes.slice(0, 180) + '…' : row.observacoes)
                                : JSON.stringify(row.observacoes).slice(0, 180) + '…'}
                            </code>
                          ) : (
                            <span className="text-gray-500">—</span>
                          )}
                        </td>
                      </tr>
                    );
                  })
                ) : (
                  <tr>
                    <td colSpan={6} className="p-4 text-center text-gray-400">
                      Nenhum dado de importação disponível
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}
