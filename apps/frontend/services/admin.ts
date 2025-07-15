export async function getStatusGeral() {
  const res = await fetch(`${process.env.NEXT_PUBLIC_API_BASE}/admin/status`)
  return res.json()
}

export async function acionarImportacao(tipo: string, ano: number) {
  const res = await fetch(`${process.env.NEXT_PUBLIC_API_BASE}/admin/importar`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ tipo, ano }),
  })
  return res.json()
}
