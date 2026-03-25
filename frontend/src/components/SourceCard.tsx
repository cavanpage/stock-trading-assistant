import { useState } from 'react'
import type { SourceState } from '../types/backfill'
import {
  sourceCompletionPct, sourceDoneCount, sourcePendingCount,
  sourceFailedCount, sourceTotalBars, callsRemaining,
} from '../types/backfill'
import ProgressBar from './ProgressBar'
import RateLimitBadge from './RateLimitBadge'
import TaskTable from './TaskTable'
import { triggerRun } from '../hooks/useBackfillState'

interface Props {
  sourceKey: string
  source: SourceState
  onRunComplete: () => void
}

const sourceIcons: Record<string, string> = {
  polygon:       '🔷',
  alpha_vantage: '📊',
  coingecko:     '🪙',
  yahoo:         '🟣',
}

export default function SourceCard({ sourceKey, source, onRunComplete }: Props) {
  const [expanded, setExpanded]   = useState(false)
  const [running, setRunning]     = useState(false)
  const [runError, setRunError]   = useState<string | null>(null)

  const pct      = sourceCompletionPct(source)
  const done     = sourceDoneCount(source)
  const pending  = sourcePendingCount(source)
  const failed   = sourceFailedCount(source)
  const bars     = sourceTotalBars(source)
  const total    = source.tasks.filter(t => t.status !== 'skipped').length
  const remaining = callsRemaining(source.rate_limit)
  const canRun   = pending > 0 && (remaining === null || remaining > 0)
  const icon     = sourceIcons[sourceKey] ?? '📁'

  const barColor = pct === 1 ? 'green' : failed > 0 ? 'yellow' : 'blue'

  async function handleRun() {
    setRunning(true)
    setRunError(null)
    try {
      await triggerRun(sourceKey)
      setTimeout(onRunComplete, 1500)
    } catch (e) {
      setRunError(e instanceof Error ? e.message : 'Unknown error')
    } finally {
      setRunning(false)
    }
  }

  return (
    <div className="card flex flex-col gap-4">
      {/* Header */}
      <div className="flex items-start justify-between">
        <div className="flex items-center gap-2.5">
          <span className="text-xl">{icon}</span>
          <div>
            <h3 className="font-semibold text-white leading-tight">{source.name}</h3>
            <p className="text-xs text-gray-500 mt-0.5">
              {done}/{total} tasks · {bars.toLocaleString()} bars
            </p>
          </div>
        </div>
        <span className="text-2xl font-bold tabular-nums text-white">
          {(pct * 100).toFixed(0)}%
        </span>
      </div>

      {/* Progress bar */}
      <ProgressBar pct={pct} color={barColor} height="h-2" />

      {/* Status chips */}
      <div className="flex flex-wrap gap-2">
        {done > 0 && (
          <span className="badge bg-emerald-900/40 text-emerald-400 border border-emerald-800/60">
            ✓ {done} done
          </span>
        )}
        {pending > 0 && (
          <span className="badge bg-yellow-900/40 text-yellow-400 border border-yellow-800/60">
            ○ {pending} pending
          </span>
        )}
        {failed > 0 && (
          <span className="badge bg-red-900/40 text-red-400 border border-red-800/60">
            ✗ {failed} failed
          </span>
        )}
      </div>

      {/* Rate limit */}
      <RateLimitBadge rl={source.rate_limit} />

      {/* Actions */}
      <div className="flex items-center gap-2 pt-1">
        <button
          onClick={handleRun}
          disabled={!canRun || running}
          className="flex-1 py-1.5 px-3 rounded-lg text-sm font-medium transition-colors
            disabled:opacity-40 disabled:cursor-not-allowed
            enabled:bg-blue-600 enabled:hover:bg-blue-500 enabled:text-white
            disabled:bg-gray-800 disabled:text-gray-500"
        >
          {running ? (
            <span className="flex items-center justify-center gap-2">
              <span className="w-3 h-3 border-2 border-white/30 border-t-white rounded-full animate-spin" />
              Running…
            </span>
          ) : canRun ? `Run ${pending} task${pending !== 1 ? 's' : ''}` : pct === 1 ? 'Complete' : 'Rate limited'}
        </button>

        <button
          onClick={() => setExpanded(e => !e)}
          className="py-1.5 px-3 rounded-lg text-sm text-gray-400 hover:text-white hover:bg-gray-800 transition-colors"
        >
          {expanded ? 'Hide' : 'Details'}
        </button>
      </div>

      {runError && (
        <p className="text-xs text-red-400 bg-red-900/20 rounded px-2 py-1">{runError}</p>
      )}

      {/* Expandable task list */}
      {expanded && (
        <TaskTable tasks={source.tasks} />
      )}
    </div>
  )
}
