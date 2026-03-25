import type { BackfillState } from '../types/backfill'
import { sourcePendingCount, callsRemaining, resetIn } from '../types/backfill'

interface Props {
  state: BackfillState
}

export default function NextActions({ state }: Props) {
  const items = Object.entries(state.sources).map(([key, src]) => {
    const pending   = sourcePendingCount(src)
    const remaining = callsRemaining(src.rate_limit)
    const exhausted = remaining !== null && remaining === 0
    const reset     = resetIn(src.rate_limit)

    if (!pending) return { key, src, type: 'complete' as const, pending, remaining, reset }
    if (exhausted) return { key, src, type: 'blocked' as const, pending, remaining, reset }
    return { key, src, type: 'ready' as const, pending, remaining, reset }
  })

  const ready   = items.filter(i => i.type === 'ready')
  const blocked = items.filter(i => i.type === 'blocked')
  const done    = items.filter(i => i.type === 'complete')

  return (
    <div className="card">
      <h2 className="text-base font-semibold text-white mb-4">Next Actions</h2>
      <div className="space-y-2.5">

        {ready.map(item => (
          <div key={item.key} className="flex items-start gap-3 p-3 rounded-lg bg-blue-950/40 border border-blue-900/50">
            <span className="text-blue-400 mt-0.5">→</span>
            <div>
              <p className="text-sm text-white">
                <span className="font-medium">{item.src.name}</span>
                {' — '}
                <span className="text-blue-300">{item.pending} task{item.pending !== 1 ? 's' : ''} ready to run</span>
                {item.remaining !== null && (
                  <span className="text-gray-500"> · {item.remaining} API calls available</span>
                )}
              </p>
              <code className="text-xs text-gray-500 mt-0.5 block">
                python scripts/backfill_manager.py --run --source {item.key}
              </code>
            </div>
          </div>
        ))}

        {blocked.map(item => (
          <div key={item.key} className="flex items-start gap-3 p-3 rounded-lg bg-gray-800/60 border border-gray-700/50">
            <span className="text-yellow-500 mt-0.5">⏳</span>
            <div>
              <p className="text-sm text-white">
                <span className="font-medium">{item.src.name}</span>
                {' — '}
                <span className="text-yellow-400">{item.pending} tasks deferred</span>
                <span className="text-gray-500">, API budget exhausted</span>
              </p>
              {item.reset && (
                <p className="text-xs text-gray-500 mt-0.5">Budget resets in <span className="text-yellow-400">{item.reset}</span></p>
              )}
            </div>
          </div>
        ))}

        {done.map(item => (
          <div key={item.key} className="flex items-start gap-3 p-3 rounded-lg bg-gray-800/30 border border-gray-800/40">
            <span className="text-emerald-500 mt-0.5">✓</span>
            <p className="text-sm text-gray-400">
              <span className="font-medium text-gray-300">{item.src.name}</span> — all tasks complete
            </p>
          </div>
        ))}

        {!ready.length && !blocked.length && (
          <div className="text-center py-6">
            <p className="text-4xl mb-2">🎉</p>
            <p className="text-emerald-400 font-medium">All sources complete!</p>
            <p className="text-gray-500 text-sm mt-1">
              Run <code className="text-gray-400">python scripts/build_training_dataset.py</code> next
            </p>
          </div>
        )}
      </div>
    </div>
  )
}
