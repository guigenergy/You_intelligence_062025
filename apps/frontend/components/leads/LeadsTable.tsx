'use client';

import { useState } from 'react';
import { useRouter } from 'next/navigation';
import { useLeads } from '@/services/leads'; // certifique-se que o caminho está correto
import type { Lead } from '@/app/types/lead';
import type { LeadsTableProps } from '@/app/types/props';

export default function LeadsTable({
  selectedId,
  estado,
  distribuidora,
  segmento,
  busca,
  order,
}: LeadsTableProps) {
  const [page, setPage] = useState(1);
  const [inputPage, setInputPage] = useState('1'); 
  const router = useRouter();
  const itemsPerPage = 100;
  const { leads, total, isLoading } = useLeads(page, {
    estado,
    distribuidora,
    segmento,
    busca,
    order,
  });
  if (isLoading) {
    return <p className="text-sm text-gray-400">Carregando leads...</p>;
  }

  if (!leads.length) {
    return <p className="text-sm text-gray-400">Nenhum lead encontrado.</p>;
  }

  return (
    <div className="overflow-x-auto rounded-xl border border-zinc-700 bg-zinc-900 shadow-lg">
      <table className="min-w-full text-sm text-zinc-200">
        <thead className="bg-zinc-800 text-left text-zinc-400">
          <tr>
            <th className="px-4 py-2 text-xs uppercase tracking-wider w-[160px]">ID</th>
            <th className="px-4 py-2 text-xs uppercase tracking-wider">Nome</th>
            <th className="px-4 py-2 text-xs uppercase tracking-wider">Dic</th>
            <th className="px-4 py-2 text-xs uppercase tracking-wider">Fic</th>
            <th className="px-4 py-2 text-xs uppercase tracking-wider whitespace-nowrap">CNAE</th>
            <th className="px-4 py-2 text-xs uppercase tracking-wider">Estado</th>
            <th className="px-4 py-2 text-xs uppercase tracking-wider">Distribuidora</th>
            <th className="px-4 py-2 text-xs uppercase tracking-wider">Segmento</th>
          </tr>
        </thead>
        <tbody>
          {leads.map((l: Lead) => (
            <tr
              key={l.id}
              id={`lead-row-${l.id}`}
              onClick={() => router.push(`/mapa?id=${l.id}`)}
              className={`cursor-pointer border-b border-zinc-700 hover:bg-zinc-800 transition-all ${
                selectedId === l.id ? 'bg-lime-900/40 ring-1 ring-lime-400' : ''
              }`}
            >
              <td className="px-4 py-2 max-w-[140px] truncate text-xs text-white-300" title={l.id}>
                {l.id.slice(0, 10)}...
              </td>
              <td className="px-4 py-2">{l.nome ?? '—'}</td>
              <td className="px-4 py-2">{l.dicMed ?? '—'}</td>
              <td className="px-4 py-2">{l.ficMed ?? '—'}</td>
              <td className="px-4 py-2 capitalize whitespace-nowrap">{l.cnae ?? '—'}</td>
              <td className="px-4 py-2 uppercase">{l.estado ?? '—'}</td>
              <td className="px-4 py-2 uppercase">{l.distribuidora ?? '—'}</td>
              <td className="px-4 py-2">{l.segmento ?? '—'}</td>
            </tr>
          ))}
        </tbody>
      </table>

      {/* Paginação */}
<div className="flex flex-col sm:flex-row sm:justify-between sm:items-center gap-4 p-4">
  <button
    onClick={() => {
      const next = Math.max(page - 1, 1);
      setPage(next);
      setInputPage(next.toString());
    }}
    disabled={page === 1}
    className="px-4 py-2 rounded bg-zinc-700 text-white hover:bg-zinc-600 disabled:opacity-50"
  >
    Anterior
  </button>

  <div className="flex items-center gap-2 text-sm text-zinc-400">
    Página
    <input
      type="number"
      min={1}
      max={Math.ceil(total / itemsPerPage)}
      value={inputPage}
      onChange={(e) => setInputPage(e.target.value)}
      onKeyDown={(e) => {
        if (e.key === 'Enter') {
          const num = parseInt(inputPage);
          if (!isNaN(num) && num >= 1 && num <= Math.ceil(total / itemsPerPage)) {
            setPage(num);
          }
        }
      }}
      className="w-20 text-center bg-zinc-800 border border-zinc-600 text-white rounded px-2 py-1 focus:outline-none focus:ring-2 focus:ring-lime-500"
    />
    de {Math.ceil(total / itemsPerPage)}
  </div>

  <button
    onClick={() => {
      const next = page + 1;
      setPage(next);
      setInputPage(next.toString());
    }}
    disabled={page * itemsPerPage >= total}
    className="px-4 py-2 rounded bg-zinc-700 text-white hover:bg-zinc-600 disabled:opacity-50"
  >
    Próxima
  </button>
</div>
</div>
  );
}
