'use client'
import { useEffect, useState } from 'react'


type GDBItem = {
  prefixo: string
  ano: number
  distribuidora: string
  camada: string
  status: string
}

export default function AdminPage() {
  const [gdbs, setGdbs] = useState<GDBItem[]>([])

  useEffect(() => {
    fetch(`${process.env.NEXT_PUBLIC_API_BASE}/admin/gdbs`)
      .then(res => res.json())
      .then(setGdbs)
  }, [])

  async function importar(gdb: any) {
    const res = await fetch(`${process.env.NEXT_PUBLIC_API_BASE}/admin/importar`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        camada: gdb.camada,
        prefixo: gdb.prefixo,
        ano: gdb.ano,
        distribuidora: gdb.distribuidora
      })
    })
    const result = await res.json()
    alert(result.status || result.erro)
  }

  return (
    <div className="p-6 space-y-6">
      <h1 className="text-2xl font-bold">Admin de Importações (.gdb)</h1>
      <table className="w-full text-sm">
        <thead>
          <tr className="bg-gray-800 text-white">
            <th className="p-2">Prefixo</th>
            <th className="p-2">Ano</th>
            <th className="p-2">Distribuidora</th>
            <th className="p-2">Camada</th>
            <th className="p-2">Status</th>
            <th className="p-2">Ação</th>
          </tr>
        </thead>
        <tbody>
          {gdbs.map((gdb, i) => (
            <tr key={i} className="border-b">
              <td className="p-2">{gdb.prefixo}</td>
              <td className="p-2">{gdb.ano}</td>
              <td className="p-2">{gdb.distribuidora}</td>
              <td className="p-2">{gdb.camada}</td>
              <td className="p-2">{gdb.status}</td>
              <td className="p-2">
                <button
                  onClick={() => importar(gdb)}
                  className="bg-green-600 text-white px-2 py-1 rounded"
                >
                  Importar
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
