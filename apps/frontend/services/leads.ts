import useSWR from 'swr'
import type { Lead } from '@/app/types/lead'
import { CNAE_SEGMENTOS } from '@/utils/cnae'

const base = process.env.NEXT_PUBLIC_API_BASE ?? ''

// Função para buscar os leads formatando os campos
const fetcherLeads = async (): Promise<Lead[]> => {
  const res = await fetch(`${base}/v1/leads`)
  if (!res.ok) throw new Error('Erro ao carregar os leads')

  const raw = await res.json()

  return raw.map((item: any) => ({
    id: item.cod_id, // campo em minúsculo
    dicMed: Number(item.media_dic),  // 👈 converte corretamente
    ficMed: Number(item.media_fic),
    cnae: item.cnae,
    bairro: item.bairro,
    cep: item.cep,
    distribuidora: item.distribuidora ?? 'Desconhecida',
    codigoDistribuidora: item.codigo_distribuidora ?? item.distribuidora,
    segmento: CNAE_SEGMENTOS[item.cnae] ?? 'Outro',
    descricao: item.descricao,
    tipo: item.tipo_sistema_desc ?? item.classe_desc ?? 'N/A',
    estado: item.municipio_uf ?? item.estado ?? 'UF',
    origem: item.origem ?? 'Desconhecida',
    latitude: item.latitude_final ?? item.latitude,
    longitude: item.longitude_final ?? item.longitude,
  }))
}

export function useLeads() {
  const { data, error } = useSWR<Lead[]>('/leads', fetcherLeads, {
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
