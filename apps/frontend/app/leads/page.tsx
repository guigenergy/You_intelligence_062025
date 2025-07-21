'use client'

import { useMemo, useState, ChangeEvent } from 'react'
import LeadsTable from '@/components/leads/LeadsTable'
import FiltroDistribuidora from '@/components/leads/FiltroDistribuidora'
import FiltroSegmento from '@/components/leads/FiltroSegmento'
import { useFilters } from '@/store/filters'
import { useSort } from '@/store/sort'
import { useLeads } from '@/services/leads'
import { CNAE_SEGMENTOS } from '@/utils/cnae'
import { DISTRIBUIDORAS_MAP } from '@/utils/distribuidoras'
import { stripDiacritics } from '@/utils/stripDiacritics'
import { FileDown, Download } from 'lucide-react'
import * as XLSX from 'xlsx'
import type { Lead } from '@/app/types/lead'

const ITEMS_POR_PAGINA = 10

function exportarParaExcel(leadsParaExportar: Lead[], nomeArquivo = 'leads.xlsx') {
  const worksheet = XLSX.utils.json_to_sheet(leadsParaExportar)
  const workbook = XLSX.utils.book_new()
  XLSX.utils.book_append_sheet(workbook, worksheet, 'Leads')
  XLSX.writeFile(workbook, nomeArquivo)
}

export default function LeadsPage() {
  const [pagina, setPagina] = useState(1)
  const [buscaInput, setBuscaInput] = useState('')
  const { estado, distribuidora, segmento, tipo, clearFilters, setEstado, setBusca, busca, setTipo } = useFilters()
  const { order, setOrder } = useSort()
  const { leads, isLoading, error } = useLeads()

  console.log('[🚨] Exemplo de lead:', leads[0])
  console.log('[⚙️] Filtros aplicados:', { estado, tipo, distribuidora, segmento, busca })

  const estados = useMemo<string[]>(() => {
    return Array.from(new Set(leads.map((l) => l.estado).filter(Boolean))).sort()
  }, [leads])

const leadsFiltrados = useMemo<Lead[]>(() => {
  let arr = [...leads]

  const filtroEstado = estado.trim().toLowerCase()
  const filtroDistribuidora = distribuidora.trim()
  const filtroSegmento = segmento.trim()
  const filtroTipo = tipo.trim()

  if (filtroEstado)
    arr = arr.filter((l) => l.estado?.toLowerCase() === filtroEstado)

  if (filtroDistribuidora)
  arr = arr.filter((l) =>
    l.distribuidora === filtroDistribuidora
  )

  if (filtroSegmento)
    arr = arr.filter((l) => l.cnae === filtroSegmento)

  if (filtroTipo)
  arr = arr.filter((l) => l.tipo === filtroTipo)

  if (busca) {
    const term = stripDiacritics(busca.toLowerCase())

    arr = arr.filter((l) => {
      const estado = stripDiacritics(l.estado?.toLowerCase() ?? '')
      const cnae = stripDiacritics(l.cnae?.toLowerCase() ?? '')
      const distribuidora = stripDiacritics(DISTRIBUIDORAS_MAP[l.distribuidora]?.toLowerCase() ?? '')
      const segmentoNome = stripDiacritics(CNAE_SEGMENTOS[l.cnae]?.toLowerCase() ?? '')
      const segmentoLead = stripDiacritics(l.segmento?.toLowerCase() ?? '')

      return (
        estado.includes(term) ||
        cnae.includes(term) ||
        distribuidora.includes(term) ||
        segmentoNome.includes(term) ||
        segmentoLead.includes(term)
      )
    })
  }

  switch (order) {
    case 'dic-asc':
      arr.sort((a, b) => (a.dicMed ?? 0) - (b.dicMed ?? 0)); break
    case 'dic-desc':
      arr.sort((a, b) => (b.dicMed ?? 0) - (a.dicMed ?? 0)); break
    case 'fic-asc':
      arr.sort((a, b) => (a.ficMed ?? 0) - (b.ficMed ?? 0)); break
    case 'fic-desc':
      arr.sort((a, b) => (b.ficMed ?? 0) - (a.ficMed ?? 0)); break
  }

  return arr
}, [leads, estado, distribuidora, segmento, order, busca, tipo])


  const totalPaginas = Math.ceil(leadsFiltrados.length / ITEMS_POR_PAGINA)
  const inicio = (pagina - 1) * ITEMS_POR_PAGINA
  const fim = inicio + ITEMS_POR_PAGINA
  const leadsPagina = leadsFiltrados.slice(inicio, fim)

  const irParaPagina = (p: number) => {
    if (p >= 1 && p <= totalPaginas) setPagina(p)
  }

  if (isLoading) return <p className="p-6 text-white">Carregando…</p>
  if (error) return <p className="p-6 text-red-500">Erro: {error.message}</p>

return (
  <section className="space-y-6 p-6">
    <h1 className="text-2xl font-bold text-white">
      Leads ({leadsFiltrados.length})
    </h1>

    {/* Filtros */}
    <div className="flex flex-wrap items-end gap-4 p-4 bg-zinc-900 border border-zinc-700 rounded-xl">
      <label className="flex items-center gap-2 text-white text-sm">
        Estado:
        <select
          value={estado}
          onChange={(e) => setEstado(e.target.value)}
          className="bg-zinc-800 text-xs text-white border border-zinc-600 px-2 py-1 rounded"
        >
          <option value="">Todos</option>
          {estados.map((uf) => (
          <option key={uf} value={uf}>{uf.toUpperCase()}</option>
          ))}
        </select>
      </label>

      <label className="flex items-center gap-2 text-white text-sm">
        Ordenar por:
        <select
          value={order}
          onChange={(e) => setOrder(e.target.value as any)}
          className="bg-zinc-800 text-xs text-white border border-zinc-600 px-2 py-1 rounded"
        >
          <option value="none">–</option>
          <option value="dic-asc">DIC ↑</option>
          <option value="dic-desc">DIC ↓</option>
          <option value="fic-asc">FIC ↑</option>
          <option value="fic-desc">FIC ↓</option>
        </select>
      </label>
      <label className="flex items-center gap-2 text-white text-sm">
        tipo:
        <select
          value={tipo}
          onChange={(e) => setTipo(e.target.value)}
          className="bg-zinc-800 text-xs text-white border border-zinc-600 px-2 py-1 rounded"
        >
          <option value="">Todos</option>
          <option value="ucmt">UCMT</option>
          <option value="ucbt">UCBT</option>
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

      <FiltroDistribuidora />
      <FiltroSegmento />

      <button
        onClick={() => {
          clearFilters()
          setBuscaInput('')
          setPagina(1)
        }}
        className="bg-red-600 hover:bg-red-500 text-white px-3 py-1 rounded text-xs"
      >
        Limpar filtros
      </button>
    </div>

    {/* Botões de exportação */}
    <div className="flex justify-end gap-2">
      <button
        onClick={() => exportarParaExcel(leadsPagina, 'leads-filtrados.xlsx')}
        className="flex items-center gap-1 bg-blue-500 hover:bg-blue-400 text-white px-3 py-1 rounded text-xs"
      >
        <FileDown size={14} /> Exportar Página
      </button>
      <button
        onClick={() => exportarParaExcel(leadsFiltrados, 'leads-filtrados-completos.xlsx')}
        className="flex items-center gap-1 bg-green-600 hover:bg-green-500 text-white px-3 py-1 rounded text-xs"
      >
        <Download size={14} /> Exportar Todos
      </button>
    </div>

    {/* Tabela */}
    <LeadsTable rows={leadsPagina} />

    {/* Paginação */}
    <div className="flex items-center justify-center gap-4 text-white mt-4">
      <button
        onClick={() => irParaPagina(pagina - 1)}
        disabled={pagina === 1}
        className="px-3 py-1 bg-zinc-800 rounded disabled:opacity-50"
      >
        Anterior
      </button>

      <span className="text-sm">
        Página <strong>{pagina}</strong> de <strong>{totalPaginas}</strong>
      </span>

      <button
        onClick={() => irParaPagina(pagina + 1)}
        disabled={pagina === totalPaginas}
        className="px-3 py-1 bg-zinc-800 rounded disabled:opacity-50"
      >
        Próxima
      </button>

      <input
        type="number"
        min={1}
        max={totalPaginas}
        value={pagina}
        onChange={(e) => irParaPagina(Number(e.target.value))}
        className="w-16 text-center bg-zinc-800 border border-zinc-600 rounded text-sm text-white px-1 py-0.5"
      />
    </div>
  </section>
)

}
