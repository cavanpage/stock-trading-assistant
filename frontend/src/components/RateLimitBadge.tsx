import { useEffect, useState } from 'react'
import { type RateLimit, callsRemaining, resetIn } from '../types/backfill'

interface Props {
  rl: RateLimit
}

export default function RateLimitBadge({ rl }: Props) {
  const [countdown, setCountdown] = useState(resetIn(rl))

  useEffect(() => {
    if (rl.calls_per_day === null) return
    const id = setInterval(() => setCountdown(resetIn(rl)), 1000)
    return () => clearInterval(id)
  }, [rl])

  if (rl.calls_per_day === null) {
    return (
      <span className="badge bg-emerald-900/50 text-emerald-400 border border-emerald-800">
        ∞ Unlimited
      </span>
    )
  }

  const remaining = callsRemaining(rl)
  const used = rl.calls_made_today
  const limit = rl.calls_per_day
  const pct = used / limit
  const exhausted = remaining === 0

  return (
    <div className="flex flex-col gap-1.5">
      <div className="flex items-center gap-2">
        <span className={`badge ${exhausted ? 'bg-red-900/50 text-red-400 border border-red-800' : 'bg-yellow-900/40 text-yellow-400 border border-yellow-800'}`}>
          {exhausted ? '🔴 Exhausted' : `🟡 ${remaining} calls left today`}
        </span>
        <span className="text-xs text-gray-500">{used}/{limit} used</span>
      </div>

      {/* call budget bar */}
      <div className="w-full h-1.5 bg-gray-800 rounded-full overflow-hidden">
        <div
          className={`h-full rounded-full transition-all duration-500 ${pct > 0.8 ? 'bg-red-500' : pct > 0.5 ? 'bg-yellow-400' : 'bg-emerald-500'}`}
          style={{ width: `${(pct * 100).toFixed(1)}%` }}
        />
      </div>

      {exhausted && countdown && (
        <p className="text-xs text-gray-500">Resets in <span className="text-yellow-400 tabular-nums">{countdown}</span></p>
      )}
    </div>
  )
}
