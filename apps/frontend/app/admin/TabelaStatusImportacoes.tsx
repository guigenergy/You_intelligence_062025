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
    concluido: 'bg-green-700 text-white rounded-full px-3 py-1 text-xs font-medium',
    erro: 'bg-red-700 text-white rounded-full px-3 py-1 text-xs font-medium',
    importando: 'bg-yellow-600 text-white rounded-full px-3 py-1 text-xs font-medium',
    baixando: 'bg-blue-700 text-white rounded-full px-3 py-1 text-xs font-medium',
    extraindo: 'bg-purple-700 text-white rounded-full px-3 py-1 text-xs font-medium',
    pendente: 'bg-gray-600 text-white rounded-full px-3 py-1 text-xs font-medium',
  } as const;

  return (
    <div>
      <h2 className="font-bold text-xl text-white mb-4">📦 Status de Importação</h2>
      {isLoading ? (
        <p className="text-gray-300">Carregando...</p>
      ) : (
        <table className="w-full text-sm text-left border border-zinc-700 rounded-lg overflow-hidden shadow bg-zinc-900 text-white">
          <thead className="bg-zinc-800 text-gray-200">
            <tr>
              <th className="p-2 border border-zinc-700">Distribuidora</th>
              <th className="p-2 border border-zinc-700">Ano</th>
              <th className="p-2 border border-zinc-700">Camada</th>
              <th className="p-2 border border-zinc-700">Status</th>
              <th className="p-2 border border-zinc-700">Data Execução</th>
            </tr>
          </thead>
          <tbody>
            {data.map((row: ImportStatus) => (
              <tr key={`${row.distribuidora}-${row.ano}-${row.camada}`} className="hover:bg-zinc-800 transition">
                <td className="p-2 border border-zinc-700">{row.distribuidora}</td>
                <td className="p-2 border border-zinc-700">{row.ano}</td>
                <td className="p-2 border border-zinc-700">{row.camada}</td>
                <td className="p-2 border border-zinc-700">
                  <span className={statusStyle[row.status] || ''}>
                    {row.status}
                  </span>
                </td>
                <td className="p-2 border border-zinc-700">{row.data_execucao}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
