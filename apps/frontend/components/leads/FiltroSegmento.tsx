// components/leads/FiltroSegmento.tsx
'use client';

import { useFilters } from '@/store/filters';
import { useLeads } from '@/services/leads';
import { CNAE_SEGMENTOS } from '@/utils/cnae';

export default function FiltroSegmento() {
  const { segmento, setSegmento } = useFilters();
  const { leads = [], isLoading, error } = useLeads();

  const cnaesUnicos = [...new Set(leads.map((l) => l.cnae).filter(Boolean))];

  return (
    <label className="text-sm text-white flex items-center gap-2">
      Segmento:
      <select
        value={segmento}
        onChange={(e) => setSegmento(e.target.value)}
        className="text-xs text-white bg-zinc-800 border border-zinc-600 px-3 py-1.5 rounded-md shadow-sm hover:border-zinc-500 focus:outline-none focus:ring-1 focus:ring-lime-500 transition"
      >
        <option value="">Todos</option>
      {cnaesUnicos
        .filter((c): c is string => typeof c === 'string') 
        .map((cnae) => (
        <option key={cnae} value={cnae}>
        {CNAE_SEGMENTOS[cnae] || `CNAE ${cnae}`}
        </option>
        ))}
      </select>
    </label>
  );
}
