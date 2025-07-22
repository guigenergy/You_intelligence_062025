'use client';
import { useState } from 'react';

const DISTRIBUIDORAS = [
  'ENEL DISTRIBUIÇÃO RIO',
  'CPFL PAULISTA',
  'LIGHT',
  'ELETROPAULO',
];

export default function SelectDistribuidoras({ 
  onChange 
}: { 
  onChange?: (value: string[]) => void 
}) {
  const [selecionadas, setSelecionadas] = useState<string[]>([]);

  const toggleSelecionada = (nome: string) => {
    const novaLista = selecionadas.includes(nome)
      ? selecionadas.filter((d) => d !== nome)
      : [...selecionadas, nome];
    setSelecionadas(novaLista);
    onChange?.(novaLista);
  };

  const toggleAll = (selectAll: boolean) => {
    const novaLista = selectAll ? DISTRIBUIDORAS : [];
    setSelecionadas(novaLista);
    onChange?.(novaLista);
  };

  return (
    <div className="space-y-3">
      <div className="flex justify-between items-center">
        <label className="block text-sm font-medium text-gray-200">
          Distribuidoras
        </label>
        <button
          onClick={() => toggleAll(selecionadas.length !== DISTRIBUIDORAS.length)}
          className="text-xs text-blue-400 hover:text-blue-300 transition-colors"
        >
          {selecionadas.length === DISTRIBUIDORAS.length ? 'Desmarcar todas' : 'Marcar todas'}
        </button>
      </div>
      
      <div className="bg-zinc-800/50 border border-zinc-700 rounded-lg p-3 space-y-2 backdrop-blur-sm max-h-60 overflow-y-auto">
        {DISTRIBUIDORAS.map((distribuidora) => (
          <label
            key={distribuidora}
            className={`flex items-center gap-3 p-2 rounded-md cursor-pointer transition-colors ${
              selecionadas.includes(distribuidora)
                ? 'bg-blue-900/30 hover:bg-blue-900/40'
                : 'hover:bg-zinc-700/50'
            }`}
          >
            <div className="relative flex items-center">
              <input
                type="checkbox"
                checked={selecionadas.includes(distribuidora)}
                onChange={() => toggleSelecionada(distribuidora)}
                className="opacity-0 absolute w-4 h-4"
              />
              <div className={`w-4 h-4 flex items-center justify-center border rounded transition-colors ${
                selecionadas.includes(distribuidora)
                  ? 'bg-blue-500 border-blue-500'
                  : 'border-zinc-500 hover:border-zinc-400'
              }`}>
                {selecionadas.includes(distribuidora) && (
                  <svg className="w-3 h-3 text-white" viewBox="0 0 20 20" fill="currentColor">
                    <path fillRule="evenodd" d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z" clipRule="evenodd" />
                  </svg>
                )}
              </div>
            </div>
            <span className="text-gray-200 text-sm select-none">
              {distribuidora.split(' ').map((word, i) => 
                word === 'DISTRIBUIÇÃO' ? (
                  <span key={i} className="text-blue-300">{word} </span>
                ) : (
                  <span key={i}>{word} </span>
                )
              )}
            </span>
          </label>
        ))}
      </div>

      {selecionadas.length > 0 && (
        <div className="text-xs text-gray-400 truncate">
          Selecionadas: {selecionadas.sort().join(', ')}
        </div>
      )}
    </div>
  );
}