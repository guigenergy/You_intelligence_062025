'use client';
import { useState } from 'react';

const ANOS = [2019, 2020, 2021, 2022, 2023, 2024, 2025];

export default function SelectAnos({ onChange }: { onChange?: (anos: number[]) => void }) {
  const [selecionados, setSelecionados] = useState<number[]>([]);

  const toggleAno = (ano: number) => {
    const novaLista = selecionados.includes(ano)
      ? selecionados.filter((a) => a !== ano)
      : [...selecionados, ano];
    setSelecionados(novaLista);
    onChange?.(novaLista);
  };

  return (
    <div>
      <label className="block font-medium text-white mb-2">Anos</label>
      <div className="bg-zinc-900 border border-zinc-700 rounded-lg p-4 w-48 space-y-2">
        {ANOS.map((ano) => (
          <div key={ano} className="flex items-center gap-2 text-white">
            <input
              type="checkbox"
              checked={selecionados.includes(ano)}
              onChange={() => toggleAno(ano)}
              className="accent-blue-500 w-4 h-4"
            />
            <span>{ano}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
