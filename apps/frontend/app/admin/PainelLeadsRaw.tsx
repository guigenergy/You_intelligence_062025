"use client";

import { useEffect, useState } from "react";
import { getLeadsRaw, enrichGeo, enrichCNPJ } from "@/services/admin";

export default function PainelLeadsRaw() {
  const [leads, setLeads] = useState<any[]>([]);
  const [selecionados, setSelecionados] = useState<string[]>([]);
  const [loading, setLoading] = useState({
    geo: false,
    cnpj: false,
    table: true
  });

  useEffect(() => {
    getLeadsRaw()
      .then(setLeads)
      .finally(() => setLoading(prev => ({...prev, table: false})));
  }, []);

  const handleEnriquecerGeo = async () => {
    if (selecionados.length === 0) return alert("Selecione pelo menos 1 lead");
    setLoading(prev => ({...prev, geo: true}));
    try {
      await enrichGeo(selecionados);
      alert("Enriquecimento com Google iniciado com sucesso!");
    } catch (error) {
      alert("Erro ao iniciar enriquecimento com Google");
    } finally {
      setLoading(prev => ({...prev, geo: false}));
    }
  };

  const handleEnriquecerCNPJ = async () => {
    if (selecionados.length === 0) return alert("Selecione pelo menos 1 lead");
    setLoading(prev => ({...prev, cnpj: true}));
    try {
      await enrichCNPJ(selecionados);
      alert("Enriquecimento com CNPJ iniciado com sucesso!");
    } catch (error) {
      alert("Erro ao iniciar enriquecimento com CNPJ");
    } finally {
      setLoading(prev => ({...prev, cnpj: false}));
    }
  };

  const toggleSelectAll = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.checked) {
      setSelecionados(leads.map(lead => lead.id));
    } else {
      setSelecionados([]);
    }
  };

  return (
    <div className="bg-white shadow-lg rounded-xl overflow-hidden">
      <div className="p-6 border-b">
        <h2 className="text-xl font-semibold text-gray-800">Leads com status RAW</h2>
        <p className="text-sm text-gray-500 mt-1">
          {selecionados.length > 0 
            ? `${selecionados.length} lead(s) selecionado(s)` 
            : "Selecione leads para enriquecimento"}
        </p>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead className="bg-gray-50 text-gray-600">
            <tr>
              <th className="p-3 text-left w-10">
                <input
                  type="checkbox"
                  onChange={toggleSelectAll}
                  checked={selecionados.length === leads.length && leads.length > 0}
                />
              </th>
              <th className="p-3 text-left">ID</th>
              <th className="p-3 text-left">Bairro</th>
              <th className="p-3 text-left">Distribuidora</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-200">
            {loading.table ? (
              <tr>
                <td colSpan={4} className="p-4 text-center text-gray-500">
                  Carregando leads...
                </td>
              </tr>
            ) : leads.length === 0 ? (
              <tr>
                <td colSpan={4} className="p-4 text-center text-gray-500">
                  Nenhum lead encontrado
                </td>
              </tr>
            ) : (
              leads.map((lead: any) => (
                <tr 
                  key={lead.id} 
                  className={`hover:bg-gray-50 ${selecionados.includes(lead.id) ? 'bg-blue-50' : ''}`}
                >
                  <td className="p-3">
                    <input
                      type="checkbox"
                      checked={selecionados.includes(lead.id)}
                      onChange={(e) => {
                        const checked = e.target.checked;
                        setSelecionados(prev =>
                          checked ? [...prev, lead.id] : prev.filter(id => id !== lead.id)
                        );
                      }}
                      className="rounded text-blue-600 focus:ring-blue-500"
                    />
                  </td>
                  <td className="p-3 font-medium text-gray-800">{lead.id}</td>
                  <td className="p-3 text-gray-700">{lead.bairro || '-'}</td>
                  <td className="p-3 text-gray-700">{lead.distribuidora || '-'}</td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      <div className="p-4 bg-gray-50 border-t flex justify-between items-center">
        <span className="text-sm text-gray-600">
          Total: {leads.length} lead(s)
        </span>
        <div className="flex gap-3">
          <button
            onClick={handleEnriquecerGeo}
            disabled={loading.geo || selecionados.length === 0}
            className={`
              flex items-center gap-2 px-4 py-2 rounded-md text-white
              ${loading.geo ? 'bg-blue-500' : 'bg-blue-600 hover:bg-blue-700'}
              transition-colors disabled:opacity-70 disabled:cursor-not-allowed
              focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2
            `}
          >
            {loading.geo && (
              <svg className="animate-spin h-4 w-4 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
              </svg>
            )}
            Enriquecer com Google
          </button>
          <button
            onClick={handleEnriquecerCNPJ}
            disabled={loading.cnpj || selecionados.length === 0}
            className={`
              flex items-center gap-2 px-4 py-2 rounded-md text-white
              ${loading.cnpj ? 'bg-green-500' : 'bg-green-600 hover:bg-green-700'}
              transition-colors disabled:opacity-70 disabled:cursor-not-allowed
              focus:outline-none focus:ring-2 focus:ring-green-500 focus:ring-offset-2
            `}
          >
            {loading.cnpj && (
              <svg className="animate-spin h-4 w-4 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
              </svg>
            )}
            Enriquecer com CNPJ
          </button>
        </div>
      </div>
    </div>
  );
}