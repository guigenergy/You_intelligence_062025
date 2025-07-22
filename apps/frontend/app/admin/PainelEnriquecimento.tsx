'use client';
import useSWR from 'swr';
import { useState } from 'react';

const fetcher = (url: string) => fetch(url).then((res) => res.json());

export default function PainelEnriquecimento() {
  const { data, mutate } = useSWR('/v1/leads/status-count', fetcher);
  const [loading, setLoading] = useState(false);

  const handleEnriquecer = async () => {
    setLoading(true);
    try {
      await fetch('/v1/enriquecer', { method: 'POST' });
      mutate();
      alert('Enriquecimento iniciado com sucesso!');
    } catch (error) {
      alert('Erro ao iniciar enriquecimento');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="bg-zinc-900 border border-zinc-700 text-white rounded-lg shadow-lg p-6 max-w-md w-full">
      <div className="flex items-center justify-between mb-6">
        <h2 className="text-xl font-semibold text-white">
          Enriquecimento de Leads
        </h2>
        <div className="h-2 w-2 bg-emerald-500 rounded-full animate-pulse"></div>
      </div>
      
      <div className="space-y-4 mb-6">
        <div className="flex justify-between items-center pb-2 border-b border-zinc-700">
          <span className="text-zinc-400">Leads RAW</span>
          <span className="font-medium text-white">{data?.raw || 0}</span>
        </div>
        <div className="flex justify-between items-center pb-2 border-b border-zinc-700">
          <span className="text-zinc-400">Parcialmente Enriquecidos</span>
          <span className="font-medium text-amber-400">{data?.parcialmente_enriquecido || 0}</span>
        </div>
        <div className="flex justify-between items-center pb-2 border-b border-zinc-700">
          <span className="text-zinc-400">Completamente Enriquecidos</span>
          <span className="font-medium text-emerald-400">{data?.enriquecido || 0}</span>
        </div>
        <div className="flex justify-between items-center">
          <span className="text-zinc-400">Falhas</span>
          <span className="font-medium text-rose-400">{data?.falha_enriquecer || 0}</span>
        </div>
      </div>

      <button
        className={`
          w-full bg-emerald-600 hover:bg-emerald-500 text-white 
          font-medium rounded-md py-2.5 px-4 shadow-md transition-all
          duration-200 disabled:opacity-70 disabled:cursor-not-allowed
          focus:outline-none focus:ring-2 focus:ring-emerald-500 focus:ring-offset-2 focus:ring-offset-zinc-900
          ${loading ? 'flex items-center justify-center gap-2' : ''}
        `}
        onClick={handleEnriquecer}
        disabled={loading}
      >
        {loading ? (
          <>
            <svg className="animate-spin h-4 w-4 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
              <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
              <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
            </svg>
            Processando...
          </>
        ) : 'Iniciar Enriquecimento'}
      </button>
    </div>
  );
}