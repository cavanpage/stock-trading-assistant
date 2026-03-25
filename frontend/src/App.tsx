import { useState } from 'react'
import { useBackfillState, resetState } from './hooks/useBackfillState'
import OverallProgress from './components/OverallProgress'
import SourceCard from './components/SourceCard'
import NextActions from './components/NextActions'

const SOURCE_ORDER = ['polygon', 'alpha_vantage', 'coingecko', 'yahoo']

export default function App() {
  const [autoRefresh, setAutoRefresh]     = useState(true)
  const [confirmReset, setConfirmReset]   = useState(false)
  const { state, error, isLoading, refresh } = useBackfillState(autoRefresh ? 4000 : 0)

  async function handleReset() {
    await resetState()
    setConfirmReset(false)
    refresh()
  }

  return (
    <div className="min-h-screen bg-gray-950">
      {/* Top bar */}
      <header className="border-b border-gray-800 bg-gray-900/80 backdrop-blur sticky top-0 z-10">
        <div className="max-w-7xl mx-auto px-6 py-3 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <span className="text-xl">📈</span>
            <div>
              <h1 className="text-sm font-semibold text-white leading-tight">Trading Assistant</h1>
              <p className="text-xs text-gray-500">Backfill Manager</p>
            </div>
          </div>

          <div className="flex items-center gap-3">
            {/* Live indicator */}
            <div className="flex items-center gap-1.5 text-xs text-gray-500">
              <span className={`w-1.5 h-1.5 rounded-full ${autoRefresh ? 'bg-emerald-500 animate-pulse' : 'bg-gray-600'}`} />
              {autoRefresh ? 'Live' : 'Paused'}
            </div>

            <button
              onClick={() => setAutoRefresh(v => !v)}
              className="text-xs px-2.5 py-1 rounded border border-gray-700 text-gray-400 hover:text-white hover:border-gray-600 transition-colors"
            >
              {autoRefresh ? 'Pause' : 'Resume'}
            </button>

            <button
              onClick={refresh}
              className="text-xs px-2.5 py-1 rounded border border-gray-700 text-gray-400 hover:text-white hover:border-gray-600 transition-colors"
            >
              Refresh
            </button>

            {!confirmReset ? (
              <button
                onClick={() => setConfirmReset(true)}
                className="text-xs px-2.5 py-1 rounded border border-gray-700 text-red-500 hover:text-red-400 hover:border-red-800 transition-colors"
              >
                Reset
              </button>
            ) : (
              <div className="flex items-center gap-1.5">
                <span className="text-xs text-red-400">Confirm?</span>
                <button onClick={handleReset} className="text-xs px-2 py-1 rounded bg-red-600 hover:bg-red-500 text-white transition-colors">Yes</button>
                <button onClick={() => setConfirmReset(false)} className="text-xs px-2 py-1 rounded border border-gray-700 text-gray-400 hover:text-white transition-colors">No</button>
              </div>
            )}
          </div>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-6 py-8 space-y-6">

        {/* Loading state */}
        {isLoading && !state && (
          <div className="flex items-center justify-center py-20">
            <div className="flex items-center gap-3 text-gray-400">
              <span className="w-5 h-5 border-2 border-gray-600 border-t-blue-500 rounded-full animate-spin" />
              <span>Connecting to API…</span>
            </div>
          </div>
        )}

        {/* Error state */}
        {error && (
          <div className="card border-red-900/50 bg-red-950/20">
            <p className="text-red-400 text-sm">
              ⚠ Could not reach the API. Make sure the backend is running on{' '}
              <code className="text-red-300">localhost:8000</code>
            </p>
            <code className="text-xs text-gray-500 mt-2 block">uvicorn backend.main:app --reload</code>
          </div>
        )}

        {/* Empty / not initialized */}
        {state && !state.updated_at && (
          <div className="card text-center py-10">
            <p className="text-4xl mb-3">🗄</p>
            <p className="text-white font-medium mb-1">No backfill state found</p>
            <p className="text-gray-500 text-sm mb-4">Run the manager to initialize tracking</p>
            <code className="text-xs bg-gray-800 rounded px-3 py-1.5 text-gray-400">
              python scripts/backfill_manager.py --run
            </code>
          </div>
        )}

        {/* Main content */}
        {state?.updated_at && (
          <>
            {/* Overall progress */}
            <OverallProgress state={state} lastUpdated={state.updated_at} />

            {/* Source cards grid */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
              {SOURCE_ORDER.filter(k => k in state.sources).map(key => (
                <SourceCard
                  key={key}
                  sourceKey={key}
                  source={state.sources[key]}
                  onRunComplete={refresh}
                />
              ))}
            </div>

            {/* Next actions */}
            <NextActions state={state} />

            {/* Build dataset CTA */}
            {Object.values(state.sources).every(s =>
              s.tasks.filter(t => t.status !== 'skipped').every(t => t.status === 'done')
            ) && (
              <div className="card border-emerald-900/50 bg-emerald-950/20 text-center py-6">
                <p className="text-emerald-400 font-semibold mb-1">All sources complete — ready to build training data</p>
                <code className="text-xs text-gray-400">python scripts/build_training_dataset.py</code>
              </div>
            )}
          </>
        )}
      </main>
    </div>
  )
}
