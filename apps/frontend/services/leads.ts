import useSWR from 'swr'
import type { Lead, LeadList } from '@/app/types/lead'

const base = process.env.NEXT_PUBLIC_API_BASE ?? ''

const fetcher = (url: string) =>
  fetch(`${base}${url}`).then((r) => {
    if (!r.ok) throw new Error('Erro ao carregar leads da API')
    return r.json() as Promise<LeadList>
  })

export function useLeads() {
  const { data, error, isValidating } = useSWR<LeadList>('/v1/leads', fetcher, {
    revalidateOnFocus: false,
    onErrorRetry: () => {},
  })

  return {
    leads: data?.items ?? [],
    total: data?.total ?? 0,
    isLoading: isValidating,
    error,
  }
}
