import useSWR from 'swr'
import type { Lead } from '@/app/types/lead'
import { CNAE_SEGMENTOS } from '@/utils/cnae'

const base = process.env.NEXT_PUBLIC_API_BASE ?? ''

// Função para buscar os leads formatando os campos
const fetcherTop300 = async (): Promise<Lead[]> => {
  const res = await fetch(`${base}/leads-geo`)
  if (!res.ok) throw new Error('Erro ao carregar os top 300 leads')

  const raw = await res.json()

  return raw.map((item: any) => {

    return {
      id: item.COD_ID,
      dicMed: item.dic_med,
      ficMed: item.fic_med,
      cnae: item.CNAE,
      bairro: item.BRR,
      cep: item.CEP,
      distribuidora: String(item.DIST),
      codigoDistribuidora: String(item.DIST),
      segmento: CNAE_SEGMENTOS[item.CNAE] ?? 'Outro',
      descricao: item.DESCR,
      tipo: item.tipo,
      estado: item.estado,
      latitude: item.latitude,
      longitude: item.longitude,
    }
  })
}
export function useLeads() {
  const { data, error } = useSWR<Lead[]>('/leads-geo', fetcherTop300, {
    revalidateOnFocus: false,
    onErrorRetry: () => {},
  })

  const leads = data ?? []
  const isLoading = !data && !error

  return {
    leads,
    total: leads.length,
    isLoading,
    error,
  }
}
