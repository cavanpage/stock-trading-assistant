interface Props {
  pct: number          // 0–1
  color?: 'blue' | 'green' | 'yellow' | 'red'
  height?: string
  animated?: boolean
}

const colorMap = {
  blue:   'bg-blue-500',
  green:  'bg-emerald-500',
  yellow: 'bg-yellow-400',
  red:    'bg-red-500',
}

export default function ProgressBar({ pct, color = 'blue', height = 'h-2', animated = false }: Props) {
  const pctClamped = Math.min(1, Math.max(0, pct))
  const bar = colorMap[color]

  return (
    <div className={`w-full ${height} bg-gray-800 rounded-full overflow-hidden`}>
      <div
        className={`${height} ${bar} rounded-full transition-all duration-700 ease-out ${animated ? 'animate-pulse' : ''}`}
        style={{ width: `${(pctClamped * 100).toFixed(1)}%` }}
      />
    </div>
  )
}
