'use client';
import { useState } from 'react';

export default function ButtonImportar({ distribuidoras, anos }: { distribuidoras: string[], anos: number[] }) {
  const [loading, setLoading] = useState(false);

  const handleImportar = async () => {
    setLoading(true);
    try {
      const res = await fetch('/v1/importar', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ distribuidoras, anos }),
      });
      const data = await res.json();
      console.log('Importado:', data);
      alert('Importação iniciada com sucesso!');
    } catch (err) {
      console.error(err);
      alert('Erro ao importar');
    } finally {
      setLoading(false);
    }
  };

  return (
    <button
      onClick={handleImportar}
      disabled={loading}
      className={`
        bg-blue-600 hover:bg-blue-700 text-white font-medium
        rounded-md px-6 py-2.5 shadow-md transition-all duration-200
        disabled:opacity-70 disabled:cursor-not-allowed
        focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2
        text-sm sm:text-base
        ${loading ? 'pl-8 pr-6' : ''}
      `}
    >
      {loading ? (
        <span className="relative">
          <span className="absolute -left-4 top-1/2 -translate-y-1/2 w-3 h-3 border-2 border-white border-t-transparent rounded-full animate-spin"></span>
          Importando...
        </span>
      ) : (
        'Importar Selecionados'
      )}
    </button>
  );
}