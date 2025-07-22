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

  const toggleAll = (selectAll: boolean) => {
    const novaLista = selectAll ? ANOS : [];
    setSelecionados(novaLista);
    onChange?.(novaLista);
  };

  return (
    <div className="space-y-3">
      <div className="flex justify-between items-center">
        <label className="block font-medium text-gray-200">Selecionar anos</label>
        <button
          onClick={() => toggleAll(selecionados.length !== ANOS.length)}
          className="text-xs text-blue-400 hover:text-blue-300 transition-colors"
        >
          {selecionados.length === ANOS.length ? 'Desmarcar todos' : 'Marcar todos'}
        </button>
      </div>
      
      <div className="bg-zinc-800/50 border border-zinc-700 rounded-lg p-3 space-y-3 backdrop-blur-sm">
        {ANOS.map((ano) => (
          <label
            key={ano}
            className={`flex items-center gap-3 p-2 rounded-md cursor-pointer transition-colors ${
              selecionados.includes(ano)
                ? 'bg-blue-900/30 hover:bg-blue-900/40'
                : 'hover:bg-zinc-700/50'
            }`}
          >
            <div className="relative flex items-center">
              <input
                type="checkbox"
                checked={selecionados.includes(ano)}
                onChange={() => toggleAno(ano)}
                className="opacity-0 absolute w-4 h-4"
              />
              <div className={`w-4 h-4 flex items-center justify-center border rounded transition-colors ${
                selecionados.includes(ano)
                  ? 'bg-blue-500 border-blue-500'
                  : 'border-zinc-500 hover:border-zinc-400'
              }`}>
                {selecionados.includes(ano) && (
                  <svg className="w-3 h-3 text-white" viewBox="0 0 20 20" fill="currentColor">
                    <path fillRule="evenodd" d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z" clipRule="evenodd" />
                  </svg>
                )}
              </div>
            </div>
            <span className="text-gray-200 select-none">{ano}</span>
          </label>
        ))}
      </div>

      {selecionados.length > 0 && (
        <div className="text-sm text-gray-400">
          Selecionados: {selecionados.sort().join(', ')}
        </div>
      )}
    </div>
  );
}