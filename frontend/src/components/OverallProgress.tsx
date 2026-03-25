import type { BackfillState } from '../types/backfill'
import ProgressBar from './ProgressBar'

interface Props {
  state: BackfillState
  lastUpdated: string | null
}

export default function OverallProgress({ state, lastUpdated }: Props) {
  const allTasks = Object.values(state.sources).flatMap(s => s.tasks).filter(t => t.status !== 'skipped')
  const done    = allTasks.filter(t => t.status === 'done').length
  const failed  = allTasks.filter(t => t.status === 'failed').length
  const pending = allTasks.filter(t => t.status === 'pending').length
  const total   = allTasks.length
  const pct     = total ? done / total : 0
  const totalBars = allTasks.reduce((s, t) => s + t.bars_ingested, 0)

  return (
    <div className="card">
      <div className="flex items-start justify-between mb-4">
        <div>
          <h2 className="text-lg font-semibold text-white">Overall Progress</h2>
          {lastUpdated && (
            <p className="text-xs text-gray-500 mt-0.5">
              Last updated {new Date(lastUpdated).toLocaleTimeString()}
            </p>
          )}
        </div>
        <span className="text-3xl font-bold text-white tabular-nums">
          {(pct * 100).toFixed(0)}%
        </span>
      </div>

      <ProgressBar pct={pct} color="blue" height="h-3" />

      <div className="grid grid-cols-4 gap-3 mt-5">
        <Stat label="Tasks Done"    value={`${done} / ${total}`}     color="text-emerald-400" />
        <Stat label="Pending"       value={pending.toString()}         color="text-yellow-400" />
        <Stat label="Failed"        value={failed.toString()}          color={failed > 0 ? 'text-red-400' : 'text-gray-500'} />
        <Stat label="Bars Ingested" value={totalBars.toLocaleString()} color="text-blue-400" />
      </div>
    </div>
  )
}

function Stat({ label, value, color }: { label: string; value: string; color: string }) {
  return (
    <div className="bg-gray-800/60 rounded-lg px-3 py-2.5">
      <p className="text-xs text-gray-500 mb-1">{label}</p>
      <p className={`text-lg font-semibold tabular-nums ${color}`}>{value}</p>
    </div>
  )
}
