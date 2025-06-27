'use client';

import { useFilters } from '@/store/filters';
import { useLeads } from '@/services/leads';
import { DISTRIBUIDORAS_MAP } from '@/utils/distribuidoras';

type Props = {
  distribuidoras: string[];
  value: string;
  onChange: (value: string) => void;
};

export default function FiltroDistribuidora({ distribuidoras, value, onChange }: Props) {
  return (
    <label className="flex items-center gap-2 text-white text-sm">
      Distribuidora:
      <select
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="bg-zinc-800 text-xs text-white border border-zinc-600 px-2 py-1 rounded"
      >
        <option value="">Todas</option>
        {distribuidoras.map((d) => (
          <option key={d} value={d}>
            {d}
          </option>
        ))}
      </select>
    </label>
  );
}

