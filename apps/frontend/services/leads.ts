import useSWR from 'swr'
import type { Lead, LeadList } from '@/app/types/lead'
import type { LeadsTableProps } from '@/app/types/props';
const base = process.env.NEXT_PUBLIC_API_BASE ?? ''

const fetcher = (url: string) =>
  fetch(`${base}${url}`).then((r) => {
    if (!r.ok) throw new Error('Erro ao carregar leads da API')
    return r.json() as Promise<LeadList>
  })

export function useLeads(
  page = 1,
  filters: Partial<LeadsTableProps> = {},
  limit = 100
) {
  const skip = (page - 1) * limit;

  const query = new URLSearchParams({
    skip: skip.toString(),
    limit: limit.toString(),
    ...(filters.estado && { estado: filters.estado }),
    ...(filters.distribuidora && { distribuidora: filters.distribuidora }),
    ...(filters.segmento && { segmento: filters.segmento }),
    ...(filters.busca && { busca: filters.busca }),
    ...(filters.order && { order: filters.order }),
  });

  const url = `/v1/leads?${query.toString()}`;

  const { data, error, isValidating } = useSWR<LeadList>(url, fetcher, {
    revalidateOnFocus: false,
    onErrorRetry: () => {},
  });

  return {
    leads: data?.items ?? [],
    total: data?.total ?? 0,
    isLoading: isValidating,
    error,
  };
}

