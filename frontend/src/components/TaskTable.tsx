import type { BackfillTask } from '../types/backfill'

interface Props {
  tasks: BackfillTask[]
  limit?: number
}

const statusConfig = {
  done:    { dot: 'bg-emerald-500', text: 'text-emerald-400', label: 'done' },
  pending: { dot: 'bg-yellow-400 animate-pulse', text: 'text-yellow-400', label: 'pending' },
  failed:  { dot: 'bg-red-500', text: 'text-red-400', label: 'failed' },
  skipped: { dot: 'bg-gray-600', text: 'text-gray-500', label: 'skipped' },
}

export default function TaskTable({ tasks, limit }: Props) {
  const visible = limit ? tasks.slice(0, limit) : tasks
  const hidden  = tasks.length - visible.length

  return (
    <div className="mt-3">
      <div className="grid grid-cols-[1fr_auto_auto_auto] gap-x-4 gap-y-0 text-xs">
        {/* Header */}
        <span className="text-gray-600 pb-1.5 border-b border-gray-800">Symbol / Timeframe</span>
        <span className="text-gray-600 pb-1.5 border-b border-gray-800 text-right">Bars</span>
        <span className="text-gray-600 pb-1.5 border-b border-gray-800">Date range</span>
        <span className="text-gray-600 pb-1.5 border-b border-gray-800">Status</span>

        {visible.map(task => {
          const cfg = statusConfig[task.status]
          return (
            <>
              <div key={`sym-${task.source}-${task.symbol}-${task.timeframe}`} className="flex items-center gap-2 py-1.5 border-b border-gray-800/60">
                <span className="font-mono text-gray-200">{task.symbol}</span>
                <span className="text-gray-600">{task.timeframe}</span>
              </div>

              <div key={`bars-${task.source}-${task.symbol}-${task.timeframe}`} className="py-1.5 border-b border-gray-800/60 text-right tabular-nums text-gray-300">
                {task.bars_ingested > 0 ? task.bars_ingested.toLocaleString() : '—'}
              </div>

              <div key={`range-${task.source}-${task.symbol}-${task.timeframe}`} className="py-1.5 border-b border-gray-800/60 text-gray-500 tabular-nums">
                {task.date_range
                  ? `${task.date_range[0]} → ${task.date_range[1]}`
                  : task.status === 'done' ? '—' : ''}
              </div>

              <div key={`status-${task.source}-${task.symbol}-${task.timeframe}`} className="py-1.5 border-b border-gray-800/60">
                <div className="flex items-center gap-1.5">
                  <span className={`w-1.5 h-1.5 rounded-full flex-shrink-0 ${cfg.dot}`} />
                  <span className={cfg.text}>{cfg.label}</span>
                  {task.error && (
                    <span className="text-red-400 truncate max-w-[120px]" title={task.error}>
                      · {task.error}
                    </span>
                  )}
                </div>
              </div>
            </>
          )
        })}
      </div>

      {hidden > 0 && (
        <p className="text-xs text-gray-600 mt-2 text-center">+ {hidden} more tasks</p>
      )}
    </div>
  )
}
