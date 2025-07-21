// components/leads/LeadsTable.tsx
'use client'

import { useRouter } from 'next/navigation'
import type { Lead } from '@/app/types/lead'
import { useEffect } from 'react'
import { useSearchParams } from 'next/navigation'
import { clsx } from 'clsx'
import { DISTRIBUIDORAS_MAP } from '@/utils/distribuidoras'
import { ORIGENS_MAP } from '@/utils/origens'
type Props = {
  rows: Lead[]
}

export default function LeadsTable({ rows }: Props) {
  const router = useRouter()
  const params = useSearchParams()
  const selectedId = params?.get('id') ?? null
    useEffect(() => {
    if (selectedId) {
      const el = document.getElementById(`lead-${selectedId}`)
      if (el) {
        el.scrollIntoView({ behavior: 'smooth', block: 'center' })
      }
    }
  }, [selectedId])
 
  
  if (!rows.length) {
    return <p className="text-sm text-gray-400">Nenhum lead encontrado.</p>
  }

  return (
    <div className="overflow-x-auto rounded-xl border border-zinc-700 bg-zinc-900 shadow-lg">
      <table className="min-w-full text-sm text-zinc-200">
        <thead className="bg-zinc-800 text-left text-zinc-400">
          <tr>
            <th className="px-4 py-2 text-xs uppercase tracking-wider">Dic</th>
            <th className="px-4 py-2 text-xs uppercase tracking-wider">Fic</th>
            <th className="px-4 py-2 text-xs uppercase tracking-wider ">CNAE</th>
            <th className="px-4 py-2 text-xs uppercase tracking-wider">Bairro</th>
            <th className="px-4 py-2 text-xs uppercase tracking-wider">CEP</th>
            <th className="px-4 py-2 text-xs uppercase tracking-wider">Estado</th>
            <th className="px-4 py-2 text-xs uppercase tracking-wider">Distribuidora</th>
            <th className="px-4 py-2 text-xs uppercase tracking-wider">Segmento</th>
            <th className="px-4 py-2 text-xs uppercase tracking-wider">Descrição</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((l, index) => (
            <tr
              key={`${l.id}-${index}`}
              id={`lead-${l.id}`}
              onClick={() => router.push(`/mapa?id=${l.id}`)}
              className={clsx(
                "cursor-pointer border-b border-zinc-700 transition-all",
                l.id === selectedId ? 'bg-lime-100 text-black font-semibold' : 'hover:bg-zinc-800'
              )}
            >
              <td className="px-4 py-2 max-w-[140px] truncate text-xs" title={l.id}>
                {l.id ? `${l.id.slice(0, 10)}...` : '—'}
              </td>
              <td className="px-4 py-2">{l.dicMed?.toFixed(2) ?? '—'}</td>
              <td className="px-4 py-2">{l.ficMed?.toFixed(2) ?? '—'}</td>
              <td className="px-4 py-2 whitespace-nowrap">{l.cnae ?? '—'}</td>
              <td className="px-4 py-2 whitespace-nowrap">{l.bairro ?? '—'}</td>
              <td className="px-4 py-2 whitespace-nowrap">{l.cep ?? '—'}</td>
              <td className="px-4 py-2">{l.estado ?? '—'}</td>
              <td className="px-4 py-2">{DISTRIBUIDORAS_MAP[l.distribuidora] ?? l.distribuidora ?? '—'}</td>
              <td className="px-4 py-2">{l.segmento ?? '—'}</td>
              <td className="px-4 py-2" title={l.descricao}>{l.descricao ?? '—'}</td>
              </tr>
          ))}
        </tbody>
      </table>
      
    </div>
    
  )
}
