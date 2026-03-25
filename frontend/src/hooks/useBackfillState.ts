import useSWR from 'swr'
import type { BackfillState } from '../types/backfill'

const fetcher = (url: string) => fetch(url).then(r => r.json())

export function useBackfillState(refreshInterval = 4000) {
  const { data, error, isLoading, mutate } = useSWR<BackfillState>(
    '/api/v1/backfill/status',
    fetcher,
    { refreshInterval },
  )
  return { state: data, error, isLoading, refresh: mutate }
}

export async function triggerRun(source: string) {
  const res = await fetch(`/api/v1/backfill/run/${source}`, { method: 'POST' })
  if (!res.ok) throw new Error(await res.text())
  return res.json()
}

export async function resetState() {
  const res = await fetch('/api/v1/backfill/reset', { method: 'POST' })
  if (!res.ok) throw new Error(await res.text())
  return res.json()
}
