'use client'

import MapView from '@/components/mapa/MapView'
import { useLeads } from '@/services/leads'
import { useSearchParams } from 'next/navigation'
import { useFilters } from '@/store/filters'
import { useSort } from '@/store/sort'
import { DISTRIBUIDORAS_MAP } from '@/utils/distribuidoras'
import FiltroSegmento from '@/components/leads/FiltroSegmento'


export default function MapaPage() {
  const {
    estado,
    setEstado,
    distribuidora,
    setDistribuidora,
    segmento,
    clearFilters,
  } = useFilters()

  
  const { order } = useSort()
  const { leads = [] } = useLeads()

  // ✅ ID agora como string
  const params = useSearchParams()
  const selectedId = params?.get('id') ?? null

  const rows = (() => {
  let arr = leads

  if (estado) {
    arr = arr.filter((l) => l.estado === estado)
  }

  if (distribuidora) {
    arr = arr.filter((l) => String(l.codigoDistribuidora) === distribuidora)
  }

    if (segmento)
    arr = arr.filter((l) => l.cnae === segmento)

  switch (order) {
    case 'dic-asc':
      arr.sort((a, b) => (a.dicMed ?? 0) - (b.dicMed ?? 0))
      break
    case 'dic-desc':
      arr.sort((a, b) => (b.dicMed ?? 0) - (a.dicMed ?? 0))
      break
    case 'fic-asc':
      arr.sort((a, b) => (a.ficMed ?? 0) - (b.ficMed ?? 0))
      break
    case 'fic-desc':
      arr.sort((a, b) => (b.ficMed ?? 0) - (a.ficMed ?? 0))
      break
  }

  return arr
})()


  return (
    <div className="flex flex-col h-screen">
      {/* Filtros */}
      <div className="px-6 py-4 bg-zinc-900 border-b border-zinc-700">
        <div className="flex flex-wrap items-end gap-4">
          {/* Estado */}
          <label className="text-sm text-white flex items-center gap-2">
            Estado:
            <select
              value={estado}
              onChange={(e) => setEstado(e.target.value)}
              className="text-xs text-white bg-zinc-800 border border-zinc-600 px-3 py-1.5 rounded-md shadow-sm hover:border-zinc-500 focus:outline-none focus:ring-1 focus:ring-lime-500 transition"
            >
              <option value="">Todos</option>
              {[...new Set(leads.map((l) => l.estado))].map((uf) => (
                <option key={uf} value={uf}>
                  {uf}
                </option>
              ))}
            </select>
          </label>

          {/* Distribuidora */}
          <label className="text-sm text-white flex items-center gap-2">
            Distribuidora:
            <select
              value={distribuidora}
              onChange={(e) => setDistribuidora(e.target.value)}
              className="text-xs text-white bg-zinc-800 border border-zinc-600 px-3 py-1.5 rounded-md shadow-sm hover:border-zinc-500 focus:outline-none focus:ring-1 focus:ring-lime-500 transition"
            >
              <option value="">Todas</option>
              {[...new Set(leads.map((l) => l.codigoDistribuidora))]
                .filter((cod) => cod in DISTRIBUIDORAS_MAP)
                .map((cod) => (
                  <option key={cod} value={cod}>
                    {DISTRIBUIDORAS_MAP[cod] || `Distribuidora ${cod}`}
                  </option>
              ))}
            </select>
          </label>

          {/* Segmento CNAE */}
          <FiltroSegmento />

          {/* Limpar */}
          <button
            onClick={clearFilters}
            className="flex items-center gap-1 bg-red-600 hover:bg-red-500 text-white text-xs font-medium px-2.5 py-1.5 rounded-md shadow-sm transition"
          >
            Limpar filtros
          </button>
        </div>
      </div>

      {/* Mapa */}
      <div className="flex-1">
        <MapView leads={rows} selectedId={selectedId} />
      </div>
    </div>
  )
}
