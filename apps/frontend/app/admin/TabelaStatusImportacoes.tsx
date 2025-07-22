'use client';
import useSWR from 'swr';

const fetcher = (url: string) => fetch(url).then((res) => res.json());

export default function TabelaStatusImportacoes() {
  type ImportStatus = {
    distribuidora: string;
    ano: number;
    camada: 'UCAT' | 'UCMT' | 'UCBT';
    status: 'concluido' | 'erro' | 'importando' | 'baixando' | 'extraindo' | 'pendente';
    data_execucao: string;
  };

  const { data = [], isLoading } = useSWR('/v1/import-status', fetcher, { refreshInterval: 5000 });

  const statusStyle = {
    concluido: 'bg-green-500/20 text-green-400 border border-green-500/30',
    erro: 'bg-red-500/20 text-red-400 border border-red-500/30',
    importando: 'bg-yellow-500/20 text-yellow-400 border border-yellow-500/30',
    baixando: 'bg-blue-500/20 text-blue-400 border border-blue-500/30',
    extraindo: 'bg-purple-500/20 text-purple-400 border border-purple-500/30',
    pendente: 'bg-gray-500/20 text-gray-400 border border-gray-500/30',
  } as const;

  const statusIcons = {
    concluido: '✓',
    erro: '✗',
    importando: '↻',
    baixando: '↓',
    extraindo: '⇲',
    pendente: '…',
  } as const;

  const camadaColors = {
    UCAT: 'text-blue-400',
    UCMT: 'text-purple-400',
    UCBT: 'text-green-400',
  } as const;

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-xl font-semibold text-white flex items-center gap-2">
          <span className="bg-blue-500/20 p-2 rounded-lg">
            📦
          </span>
          <span>Status de Importação</span>
        </h2>
        <span className="text-xs text-gray-400">
          Atualizando a cada 5 segundos
        </span>
      </div>

      {isLoading ? (
        <div className="flex justify-center items-center h-32">
          <div className="animate-spin rounded-full h-8 w-8 border-t-2 border-b-2 border-blue-500"></div>
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
                </tr>
              </thead>
              <tbody className="divide-y divide-zinc-800">
                {data.length > 0 ? (
                  data.map((row: ImportStatus) => (
                    <tr 
                      key={`${row.distribuidora}-${row.ano}-${row.camada}`} 
                      className="hover:bg-zinc-800/50 transition-colors"
                    >
                      <td className="p-3 text-gray-200">{row.distribuidora}</td>
                      <td className="p-3 text-gray-300">{row.ano}</td>
                      <td className={`p-3 font-medium ${camadaColors[row.camada]}`}>
                        {row.camada}
                      </td>
                      <td className="p-3">
                        <div className={`inline-flex items-center gap-2 px-3 py-1 rounded-full text-xs font-medium ${statusStyle[row.status]}`}>
                          <span>{statusIcons[row.status]}</span>
                          <span className="capitalize">{row.status}</span>
                        </div>
                      </td>
                      <td className="p-3 text-gray-400 text-xs">
                        {new Date(row.data_execucao).toLocaleString()}
                      </td>
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td colSpan={5} className="p-4 text-center text-gray-400">
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