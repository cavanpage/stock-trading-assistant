export type TaskStatus = 'pending' | 'done' | 'failed' | 'skipped'

export interface BackfillTask {
  source: string
  symbol: string
  timeframe: string
  status: TaskStatus
  bars_ingested: number
  completed_at: string | null
  error: string | null
  date_range: [string, string] | null
}

export interface RateLimit {
  calls_per_day: number | null
  calls_made_today: number
  window_start: string
}

export interface SourceState {
  name: string
  rate_limit: RateLimit
  tasks: BackfillTask[]
}

export interface BackfillState {
  updated_at: string | null
  initialized: boolean
  sources: Record<string, SourceState>
}

// Derived helpers
export function sourceDoneCount(src: SourceState) {
  return src.tasks.filter(t => t.status === 'done').length
}

export function sourcePendingCount(src: SourceState) {
  return src.tasks.filter(t => t.status === 'pending').length
}

export function sourceFailedCount(src: SourceState) {
  return src.tasks.filter(t => t.status === 'failed').length
}

export function sourceTotalBars(src: SourceState) {
  return src.tasks.reduce((sum, t) => sum + t.bars_ingested, 0)
}

export function sourceCompletionPct(src: SourceState) {
  const runnable = src.tasks.filter(t => t.status !== 'skipped')
  if (!runnable.length) return 0
  return runnable.filter(t => t.status === 'done').length / runnable.length
}

export function callsRemaining(rl: RateLimit): number | null {
  if (rl.calls_per_day === null) return null
  const windowStart = new Date(rl.window_start)
  const midnight = new Date()
  midnight.setUTCHours(0, 0, 0, 0)
  const callsToday = windowStart < midnight ? 0 : rl.calls_made_today
  return Math.max(0, rl.calls_per_day - callsToday)
}

export function resetIn(rl: RateLimit): string | null {
  if (rl.calls_per_day === null) return null
  const now = new Date()
  const midnight = new Date()
  midnight.setUTCDate(midnight.getUTCDate() + 1)
  midnight.setUTCHours(0, 0, 0, 0)
  const delta = midnight.getTime() - now.getTime()
  const h = Math.floor(delta / 3_600_000)
  const m = Math.floor((delta % 3_600_000) / 60_000)
  const s = Math.floor((delta % 60_000) / 1_000)
  return `${h}h ${m}m ${s}s`
}
