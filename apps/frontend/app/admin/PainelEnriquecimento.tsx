'use client';
import useSWR from 'swr';
import { useState } from 'react';

const fetcher = (url: string) => fetch(url).then((res) => res.json());

export default function PainelEnriquecimento() {
  const { data, mutate } = useSWR('/v1/leads/status-count', fetcher);
  const [loading, setLoading] = useState(false);

  const handleEnriquecer = async () => {
    setLoading(true);
    await fetch('/v1/enriquecer', { method: 'POST' });
    mutate();
    setLoading(false);
    alert('Enriquecimento iniciado!');
  };

  return (
    <div className="bg-zinc-900 border border-zinc-700 text-white rounded-xl shadow p-6">
        <h2 className="text-xl font-bold text-white mb-4 flex items-center gap-2">
            📈 Enriquecimento de Leads
        </h2>
        
        <ul className="mb-4 space-y-1 text-gray-300">
            <li>
            Leads RAW: <strong className="text-white">{data?.raw || 0}</strong>
            </li>
            <li>
            Parcialmente Enriquecidos: <strong className="text-white">{data?.parcialmente_enriquecido || 0}</strong>
            </li>
            <li>
            Enriquecidos: <strong className="text-white">{data?.enriquecido || 0}</strong>
            </li>
            <li>
            Falhas: <strong className="text-white">{data?.falha_enriquecer || 0}</strong>
            </li>
        </ul>

        <button
            className="bg-emerald-600 hover:bg-emerald-700 text-white font-semibold rounded-lg px-5 py-2 shadow transition duration-200 disabled:opacity-50 disabled:cursor-not-allowed"
            onClick={handleEnriquecer}
            disabled={loading}
        >
            {loading ? '⏳ Executando...' : '⚙️ Iniciar Enriquecimento'}
        </button>
    </div>

  );
}
