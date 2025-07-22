import useSWR from 'swr'
import type { Lead } from '@/app/types/lead'
import { CNAE_SEGMENTOS } from '@/utils/cnae'

const base = process.env.NEXT_PUBLIC_API_BASE ?? ''

// Fetcher SP
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

      ENE_01: item.ENE_01, ENE_02: item.ENE_02, ENE_03: item.ENE_03, ENE_04: item.ENE_04,
      ENE_05: item.ENE_05, ENE_06: item.ENE_06, ENE_07: item.ENE_07, ENE_08: item.ENE_08,
      ENE_09: item.ENE_09, ENE_10: item.ENE_10, ENE_11: item.ENE_11, ENE_12: item.ENE_12,
      DEM_01: item.DEM_01, DEM_02: item.DEM_02, DEM_03: item.DEM_03, DEM_04: item.DEM_04,
      DEM_05: item.DEM_05, DEM_06: item.DEM_06, DEM_07: item.DEM_07, DEM_08: item.DEM_08,
      DEM_09: item.DEM_09, DEM_10: item.DEM_10, DEM_11: item.DEM_11, DEM_12: item.DEM_12,
      DIC_01: item.DIC_01, DIC_02: item.DIC_02, DIC_03: item.DIC_03, DIC_04: item.DIC_04,
      DIC_05: item.DIC_05, DIC_06: item.DIC_06, DIC_07: item.DIC_07, DIC_08: item.DIC_08,
      DIC_09: item.DIC_09, DIC_10: item.DIC_10, DIC_11: item.DIC_11, DIC_12: item.DIC_12,
      FIC_01: item.FIC_01, FIC_02: item.FIC_02, FIC_03: item.FIC_03, FIC_04: item.FIC_04,
      FIC_05: item.FIC_05, FIC_06: item.FIC_06, FIC_07: item.FIC_07, FIC_08: item.FIC_08,
      FIC_09: item.FIC_09, FIC_10: item.FIC_10, FIC_11: item.FIC_11, FIC_12: item.FIC_12,
    }
  })
}

export function useLeads() {
  const { data, error } = useSWR<Lead[]>('/leads-geo', fetcherTop300, {
    revalidateOnFocus: false,
    onErrorRetry: () => {},
  })

  const leads = data ?? []
  const isLoading = !data

  console.log('[📊] Leads finais no front:', leads.length)

  return {
    leads,
    total: leads.length,
    isLoading,
    error,
  }
}

