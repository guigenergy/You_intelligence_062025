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
        className="bg-blue-600 hover:bg-blue-700 text-white font-semibold rounded-lg px-5 py-2 shadow transition duration-200 disabled:opacity-50 disabled:cursor-not-allowed"
    >
    {loading ? '⏳ Importando...' : '▶️ Importar Selecionados'}
    </button>
  );
}
