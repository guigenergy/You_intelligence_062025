'use client';
import { useState } from 'react';

const DISTRIBUIDORAS = [
  'ENEL DISTRIBUIÇÃO RIO',
  'CPFL PAULISTA',
  'LIGHT',
  'ELETROPAULO',
];

export default function SelectDistribuidoras({ onChange }: { onChange?: (value: string[]) => void }) {
  const [selecionadas, setSelecionadas] = useState<string[]>([]);

  const toggleSelecionada = (nome: string) => {
    const novaLista = selecionadas.includes(nome)
      ? selecionadas.filter((d) => d !== nome)
      : [...selecionadas, nome];
    setSelecionadas(novaLista);
    onChange?.(novaLista);
  };

  return (
    <div>
      <label className="block text-sm font-semibold text-white mb-2">Distribuidoras</label>
      <div className="bg-zinc-900 border border-zinc-700 rounded-lg shadow-sm p-4 space-y-2 w-64">
        {DISTRIBUIDORAS.map((d) => (
          <div key={d} className="flex items-center gap-2 text-white">
            <input
              type="checkbox"
              checked={selecionadas.includes(d)}
              onChange={() => toggleSelecionada(d)}
              className="accent-blue-500 w-4 h-4"
            />
            <span>{d}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
