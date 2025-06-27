'use client';

import { useState, ChangeEvent } from 'react';
import LeadsTable from '@/components/leads/LeadsTable';
import FiltroDistribuidora from '@/components/leads/FiltroDistribuidora';
import FiltroSegmento from '@/components/leads/FiltroSegmento';
import { useFilters } from '@/store/filters';
import { useSort } from '@/store/sort';
import { CNAE_SEGMENTOS } from '@/utils/cnae';
import { DISTRIBUIDORAS_MAP } from '@/utils/distribuidoras';
import { stripDiacritics } from '@/utils/stripDiacritics';
import { Download, FileDown } from 'lucide-react';
import * as XLSX from 'xlsx';
import type { Lead } from '@/app/types/lead';
import { useLeads } from '@/services/leads';


function exportarParaExcel(leads: Lead[], nome = 'leads.xlsx') {
  const worksheet = XLSX.utils.json_to_sheet(leads);
  const workbook = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(workbook, worksheet, 'Leads');
  XLSX.writeFile(workbook, nome);
}

export default function LeadsPage() {
  const [mostrarFiltros, setMostrarFiltros] = useState(false);
  const [buscaInput, setBuscaInput] = useState('');

  const { estado, distribuidora, segmento, busca, clearFilters, setEstado, setBusca, setDistribuidora  } = useFilters();
  const { leads } = useLeads(1, {}, 10000);

  const { order, setOrder } = useSort();
  const distribuidorasUnicas = Array.from(
  new Set(leads.map((l) => l.distribuidora).filter(Boolean))
  ).sort();
  return (
    <section className="space-y-6 p-6">
      <h1 className="text-2xl font-bold text-white">Leads</h1>

      <button
        onClick={() => setMostrarFiltros((v) => !v)}
        className="flex items-center gap-2 bg-green-600 hover:bg-green-500 text-white px-3 py-1 rounded"
      >
        {mostrarFiltros ? 'Ocultar Filtros' : 'Mostrar Filtros'}
      </button>

      {mostrarFiltros && (
        <div className="flex flex-wrap items-end gap-4 p-4 bg-zinc-900 border border-zinc-700 rounded-xl">
          <label className="flex items-center gap-2 text-white text-sm">
            Estado:
            <select
              value={estado}
              onChange={(e: ChangeEvent<HTMLSelectElement>) => setEstado(e.target.value)}
              className="bg-zinc-800 text-xs text-white border border-zinc-600 px-2 py-1 rounded"
            >
              <option value="">Todos</option>
              {/* idealmente você pode gerar os estados dinamicamente com base na API */}
              <option value="SP">SP</option>
              <option value="RJ">RJ</option>
            </select>
          </label>

          <label className="flex items-center gap-2 text-white text-sm">
            Ordenar por:
            <select
              value={order}
              onChange={(e: ChangeEvent<HTMLSelectElement>) => setOrder(e.target.value as any)}
              className="bg-zinc-800 text-xs text-white border border-zinc-600 px-2 py-1 rounded"
            >
              <option value="none">–</option>
              <option value="dic-asc">DIC ↑</option>
              <option value="dic-desc">DIC ↓</option>
              <option value="fic-asc">FIC ↑</option>
              <option value="fic-desc">FIC ↓</option>
            </select>
          </label>

          <input
            type="text"
            placeholder="Buscar..."
            value={buscaInput}
            onChange={(e) => setBuscaInput(e.target.value)}
            className="bg-zinc-800 text-xs text-white border border-zinc-600 px-2 py-1 rounded flex-1"
          />

          <button
            onClick={() => setBusca(buscaInput)}
            className="bg-green-600 hover:bg-green-500 text-white px-3 py-1 rounded text-xs"
          >
            Buscar
          </button>

          <FiltroDistribuidora
            distribuidoras={distribuidorasUnicas}
            value={distribuidora}
            onChange={setDistribuidora}
          />
          <FiltroSegmento />

          <button
            onClick={clearFilters}
            className="bg-red-600 hover:bg-red-500 text-white px-3 py-1 rounded text-xs"
          >
            Limpar filtros
          </button>
        </div>
      )}

      {/* botões de exportação futuramente devem usar leads server-side com filtros */}
      <div className="flex justify-end gap-2">
        <button
          onClick={() => alert('Exportação de filtrados ainda não implementada com server-side')}
          className="flex items-center gap-1 bg-blue-500 hover:bg-blue-400 text-white px-3 py-1 rounded text-xs"
        >
          <FileDown size={14} /> Exportar Filtrados
        </button>
        <button
          onClick={() => alert('Exportação de todos ainda não implementada com server-side')}
          className="flex items-center gap-1 bg-green-600 hover:bg-green-500 text-white px-3 py-1 rounded text-xs"
        >
          <Download size={14} /> Exportar Todos
        </button>
      </div>

      <LeadsTable
        estado={estado}
        distribuidora={distribuidora}
        segmento={segmento}
        busca={busca}
        order={order}
      />
    </section>
  );
}
