#!/usr/bin/env python3
"""
Backfill manager with progress tracking and rate limit awareness.

Tracks every (source, symbol, timeframe) task individually. State is
persisted to data/backfill_state.json so you can kill and resume at any time.

Usage:
    python scripts/backfill_manager.py              # show status dashboard
    python scripts/backfill_manager.py --run        # execute next available batch
    python scripts/backfill_manager.py --run --source polygon
    python scripts/backfill_manager.py --run --source alpha_vantage
    python scripts/backfill_manager.py --run --source coingecko
    python scripts/backfill_manager.py --run --source yahoo
    python scripts/backfill_manager.py --reset      # clear all state and start fresh
"""
import asyncio
import json
import os
import sys
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum
from pathlib import Path
from typing import Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from rich.console import Console
from rich.table import Table
from rich.progress import (
    Progress, BarColumn, TextColumn, TaskProgressColumn,
    TimeRemainingColumn, SpinnerColumn,
)
from rich.panel import Panel
from rich.layout import Layout
from rich.text import Text
from rich import box
from rich.rule import Rule

console = Console()

STATE_FILE = Path("data/backfill_state.json")


# ── Data model ────────────────────────────────────────────────────────────────

class TaskStatus(str, Enum):
    PENDING  = "pending"
    DONE     = "done"
    FAILED   = "failed"
    SKIPPED  = "skipped"   # e.g. crypto symbol from a stock-only source


@dataclass
class BackfillTask:
    source: str
    symbol: str
    timeframe: str
    status: TaskStatus = TaskStatus.PENDING
    bars_ingested: int = 0
    completed_at: Optional[str] = None
    error: Optional[str] = None
    date_range: Optional[list] = None   # [from, to] as ISO strings


@dataclass
class RateLimit:
    calls_per_day: Optional[int]        # None = unlimited
    calls_made_today: int = 0
    window_start: str = field(          # ISO timestamp when today's window started
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    @property
    def resets_at(self) -> Optional[datetime]:
        """When the daily call budget resets (midnight UTC)."""
        if self.calls_per_day is None:
            return None
        today = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        return today + timedelta(days=1)

    @property
    def calls_remaining(self) -> Optional[int]:
        if self.calls_per_day is None:
            return None
        # Reset counter if we've crossed midnight
        window_start_dt = datetime.fromisoformat(self.window_start)
        midnight = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        if window_start_dt < midnight:
            self.calls_made_today = 0
            self.window_start = midnight.isoformat()
        return max(0, self.calls_per_day - self.calls_made_today)

    @property
    def is_exhausted(self) -> bool:
        remaining = self.calls_remaining
        return remaining is not None and remaining <= 0

    def time_until_reset(self) -> Optional[str]:
        reset = self.resets_at
        if reset is None:
            return None
        now = datetime.now(timezone.utc)
        delta = reset - now
        hours, rem = divmod(int(delta.total_seconds()), 3600)
        minutes = rem // 60
        return f"{hours}h {minutes}m"


@dataclass
class SourceState:
    name: str
    rate_limit: RateLimit
    tasks: list[BackfillTask] = field(default_factory=list)

    @property
    def done_count(self) -> int:
        return sum(1 for t in self.tasks if t.status == TaskStatus.DONE)

    @property
    def pending_count(self) -> int:
        return sum(1 for t in self.tasks if t.status == TaskStatus.PENDING)

    @property
    def failed_count(self) -> int:
        return sum(1 for t in self.tasks if t.status == TaskStatus.FAILED)

    @property
    def total_bars(self) -> int:
        return sum(t.bars_ingested for t in self.tasks)

    @property
    def completion_pct(self) -> float:
        runnable = [t for t in self.tasks if t.status != TaskStatus.SKIPPED]
        if not runnable:
            return 0.0
        return sum(1 for t in runnable if t.status == TaskStatus.DONE) / len(runnable)


# ── Source definitions ─────────────────────────────────────────────────────────

STOCK_SYMBOLS = ["AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "GOOGL", "META", "SPY", "QQQ", "GLD"]
CRYPTO_SYMBOLS = ["BTC-USD", "ETH-USD", "SOL-USD", "BNB-USD", "XRP-USD"]
ALL_SYMBOLS = STOCK_SYMBOLS + CRYPTO_SYMBOLS

MACRO_SYMBOLS = [
    "MACRO_FED_FUNDS_RATE", "MACRO_CPI", "MACRO_INFLATION",
    "MACRO_UNEMPLOYMENT", "MACRO_REAL_GDP",
]

COINGECKO_COIN_MAP = {
    "BTC-USD": "bitcoin", "ETH-USD": "ethereum", "SOL-USD": "solana",
    "BNB-USD": "binancecoin", "XRP-USD": "ripple",
}


def build_initial_state() -> dict[str, SourceState]:
    return {
        "polygon": SourceState(
            name="Polygon.io",
            rate_limit=RateLimit(calls_per_day=None),  # unlimited on bar data
            tasks=[
                BackfillTask("polygon", sym, "daily")
                for sym in STOCK_SYMBOLS
            ] + [
                BackfillTask("polygon", sym, "5min")
                for sym in STOCK_SYMBOLS
            ],
        ),
        "alpha_vantage": SourceState(
            name="Alpha Vantage",
            rate_limit=RateLimit(calls_per_day=25),
            tasks=[
                BackfillTask("alpha_vantage", sym, "daily_20yr")
                for sym in STOCK_SYMBOLS
            ] + [
                BackfillTask("alpha_vantage", macro, "monthly")
                for macro in MACRO_SYMBOLS
            ],
        ),
        "coingecko": SourceState(
            name="CoinGecko",
            rate_limit=RateLimit(calls_per_day=None),
            tasks=[
                BackfillTask("coingecko", sym, "daily_max")
                for sym in CRYPTO_SYMBOLS
            ],
        ),
        "yahoo": SourceState(
            name="Yahoo Finance",
            rate_limit=RateLimit(calls_per_day=None),
            tasks=[
                BackfillTask("yahoo", sym, "10yr_daily")
                for sym in ALL_SYMBOLS
            ],
        ),
    }


# ── State persistence ──────────────────────────────────────────────────────────

def save_state(sources: dict[str, SourceState]):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "sources": {
            key: {
                "name": s.name,
                "rate_limit": asdict(s.rate_limit),
                "tasks": [asdict(t) for t in s.tasks],
            }
            for key, s in sources.items()
        },
    }
    STATE_FILE.write_text(json.dumps(data, indent=2))


def load_state() -> dict[str, SourceState]:
    if not STATE_FILE.exists():
        return build_initial_state()

    raw = json.loads(STATE_FILE.read_text())
    sources = {}
    for key, src_data in raw["sources"].items():
        rl_data = src_data["rate_limit"]
        rate_limit = RateLimit(
            calls_per_day=rl_data["calls_per_day"],
            calls_made_today=rl_data["calls_made_today"],
            window_start=rl_data["window_start"],
        )
        tasks = [
            BackfillTask(
                source=t["source"],
                symbol=t["symbol"],
                timeframe=t["timeframe"],
                status=TaskStatus(t["status"]),
                bars_ingested=t["bars_ingested"],
                completed_at=t.get("completed_at"),
                error=t.get("error"),
                date_range=t.get("date_range"),
            )
            for t in src_data["tasks"]
        ]
        sources[key] = SourceState(
            name=src_data["name"],
            rate_limit=rate_limit,
            tasks=tasks,
        )

    # Add any new tasks from current definition that aren't in saved state
    fresh = build_initial_state()
    for key, fresh_src in fresh.items():
        if key not in sources:
            sources[key] = fresh_src
        else:
            existing_keys = {(t.source, t.symbol, t.timeframe) for t in sources[key].tasks}
            for task in fresh_src.tasks:
                if (task.source, task.symbol, task.timeframe) not in existing_keys:
                    sources[key].tasks.append(task)

    return sources


# ── Dashboard display ──────────────────────────────────────────────────────────

def render_dashboard(sources: dict[str, SourceState]):
    console.print()
    console.print(Rule("[bold cyan]Backfill Status Dashboard[/bold cyan]"))
    console.print()

    # Overall summary
    all_tasks = [t for s in sources.values() for t in s.tasks if t.status != TaskStatus.SKIPPED]
    total = len(all_tasks)
    done = sum(1 for t in all_tasks if t.status == TaskStatus.DONE)
    failed = sum(1 for t in all_tasks if t.status == TaskStatus.FAILED)
    pending = sum(1 for t in all_tasks if t.status == TaskStatus.PENDING)
    total_bars = sum(t.bars_ingested for t in all_tasks)
    overall_pct = done / total if total else 0

    # Overall progress bar
    bar_width = 40
    filled = int(bar_width * overall_pct)
    bar = f"[green]{'█' * filled}[/green][dim]{'░' * (bar_width - filled)}[/dim]"

    console.print(Panel(
        f"  Overall Progress: {bar}  [bold]{overall_pct:.0%}[/bold]  "
        f"({done}/{total} tasks)\n"
        f"  [green]Done:[/green] {done}  "
        f"[yellow]Pending:[/yellow] {pending}  "
        f"[red]Failed:[/red] {failed}  "
        f"[cyan]Total bars ingested:[/cyan] {total_bars:,}",
        title="[bold]All Sources[/bold]",
        border_style="cyan",
    ))
    console.print()

    # Per-source tables
    for key, src in sources.items():
        _render_source(src)


def _render_source(src: SourceState):
    pct = src.completion_pct
    bar_width = 30
    filled = int(bar_width * pct)
    bar = f"[green]{'█' * filled}[/green][dim]{'░' * (bar_width - filled)}[/dim]"

    # Rate limit status
    rl = src.rate_limit
    if rl.calls_per_day is None:
        budget_str = "[green]Unlimited[/green]"
    elif rl.is_exhausted:
        budget_str = f"[red]Exhausted — resets in {rl.time_until_reset()}[/red]"
    else:
        remaining = rl.calls_remaining
        budget_str = (
            f"[yellow]{rl.calls_made_today}/{rl.calls_per_day} calls used today[/yellow]  "
            f"[green]{remaining} remaining[/green]  "
            f"(resets in {rl.time_until_reset()})"
        )

    header = (
        f"  {bar}  [bold]{pct:.0%}[/bold]  "
        f"({src.done_count}/{len([t for t in src.tasks if t.status != TaskStatus.SKIPPED])} tasks)  "
        f"|  {src.total_bars:,} bars\n"
        f"  API budget: {budget_str}"
    )

    # Task table
    table = Table(box=box.SIMPLE, show_header=True, header_style="bold dim")
    table.add_column("Symbol",     style="cyan",  width=14)
    table.add_column("Timeframe",  style="dim",   width=14)
    table.add_column("Status",                    width=12)
    table.add_column("Bars",       justify="right", width=10)
    table.add_column("Date Range", style="dim",   width=28)
    table.add_column("Note",       style="dim",   width=30)

    for task in src.tasks:
        status_str = {
            TaskStatus.DONE:    "[green]✓ done[/green]",
            TaskStatus.PENDING: "[yellow]○ pending[/yellow]",
            TaskStatus.FAILED:  "[red]✗ failed[/red]",
            TaskStatus.SKIPPED: "[dim]– skipped[/dim]",
        }[task.status]

        date_range = ""
        if task.date_range:
            date_range = f"{task.date_range[0]} → {task.date_range[1]}"

        note = task.error[:28] if task.error else ""

        table.add_row(
            task.symbol,
            task.timeframe,
            status_str,
            f"{task.bars_ingested:,}" if task.bars_ingested else "—",
            date_range,
            note,
        )

    console.print(Panel(
        header + "\n",
        title=f"[bold]{src.name}[/bold]",
        border_style="blue",
    ))
    console.print(table)
    console.print()


def render_next_actions(sources: dict[str, SourceState]):
    """Print what you can and can't run right now."""
    console.print(Rule("[bold]What to run next[/bold]"))
    console.print()

    for key, src in sources.items():
        pending = [t for t in src.tasks if t.status == TaskStatus.PENDING]
        if not pending:
            console.print(f"  [green]✓[/green] [bold]{src.name}[/bold] — all tasks complete")
            continue

        if src.rate_limit.is_exhausted:
            console.print(
                f"  [red]✗[/red] [bold]{src.name}[/bold] — "
                f"{len(pending)} tasks pending but [red]API budget exhausted[/red]. "
                f"Resets in {src.rate_limit.time_until_reset()}"
            )
        else:
            if src.rate_limit.calls_per_day:
                can_run = src.rate_limit.calls_remaining
                console.print(
                    f"  [yellow]→[/yellow] [bold]{src.name}[/bold] — "
                    f"{len(pending)} tasks pending, "
                    f"can run [green]{can_run}[/green] now:  "
                    f"[dim]python scripts/backfill_manager.py --run --source {key}[/dim]"
                )
            else:
                console.print(
                    f"  [yellow]→[/yellow] [bold]{src.name}[/bold] — "
                    f"{len(pending)} tasks pending, no rate limit:  "
                    f"[dim]python scripts/backfill_manager.py --run --source {key}[/dim]"
                )

    console.print()


# ── Task execution ─────────────────────────────────────────────────────────────

async def run_source(key: str, sources: dict[str, SourceState], dry_run: bool = False):
    src = sources[key]
    pending = [t for t in src.tasks if t.status == TaskStatus.PENDING]

    if not pending:
        console.print(f"[green]{src.name}: nothing to do[/green]")
        return

    if src.rate_limit.is_exhausted:
        console.print(
            f"[red]{src.name}: API budget exhausted. "
            f"Resets in {src.rate_limit.time_until_reset()}[/red]"
        )
        return

    # Cap batch size to remaining API budget
    budget = src.rate_limit.calls_remaining
    batch = pending if budget is None else pending[:budget]

    console.print(f"\n[bold]{src.name}[/bold] — running {len(batch)} tasks "
                  f"({'all pending' if len(batch) == len(pending) else f'{len(pending) - len(batch)} deferred to next window'})")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=30),
        TaskProgressColumn(),
        TextColumn("[cyan]{task.fields[bars]} bars"),
        TimeRemainingColumn(),
        console=console,
        transient=False,
    ) as progress:
        overall = progress.add_task(
            f"[bold]{src.name}[/bold]",
            total=len(batch),
            bars=f"{src.total_bars:,}",
        )

        for task in batch:
            task_id = progress.add_task(
                f"  [dim]{task.symbol}[/dim] [{task.timeframe}]",
                total=1,
                bars="—",
            )

            if dry_run:
                await asyncio.sleep(0.1)
                task.status = TaskStatus.DONE
                task.bars_ingested = 0
                progress.update(task_id, completed=1, bars="dry-run")
                progress.advance(overall)
                save_state(sources)
                continue

            try:
                bars = await _execute_task(task, src)
                task.status = TaskStatus.DONE
                task.bars_ingested = bars
                task.completed_at = datetime.now(timezone.utc).isoformat()
                src.rate_limit.calls_made_today += 1
                progress.update(
                    task_id,
                    completed=1,
                    description=f"  [green]✓[/green] [dim]{task.symbol}[/dim] [{task.timeframe}]",
                    bars=f"{bars:,}",
                )
            except Exception as e:
                task.status = TaskStatus.FAILED
                task.error = str(e)[:80]
                progress.update(
                    task_id,
                    completed=1,
                    description=f"  [red]✗[/red] [dim]{task.symbol}[/dim] [{task.timeframe}]",
                    bars="error",
                )
                console.print(f"    [red]Error:[/red] {e}")

            progress.update(overall, bars=f"{src.total_bars:,}")
            progress.advance(overall)
            save_state(sources)   # persist after every task

    # Summary for this source
    done_in_batch = sum(1 for t in batch if t.status == TaskStatus.DONE)
    bars_in_batch = sum(t.bars_ingested for t in batch)
    console.print(
        f"\n[bold]{src.name}[/bold]: {done_in_batch}/{len(batch)} tasks done, "
        f"{bars_in_batch:,} bars ingested"
    )

    still_pending = [t for t in src.tasks if t.status == TaskStatus.PENDING]
    if still_pending and src.rate_limit.calls_per_day:
        console.print(
            f"[yellow]{len(still_pending)} tasks deferred — "
            f"run again after {src.rate_limit.time_until_reset()} when budget resets[/yellow]"
        )


async def _execute_task(task: BackfillTask, src: SourceState) -> int:
    """Dispatch to the right ingester for this task."""
    from datetime import date

    if task.source == "polygon":
        from backend.ingestion.polygon import PolygonIngester
        timespan, multiplier = ("day", 1) if task.timeframe == "daily" else ("minute", 5)
        ingester = PolygonIngester(
            symbols=[task.symbol],
            timespan=timespan,
            multiplier=multiplier,
            from_date=(date.today().replace(year=date.today().year - 5)).isoformat()
                      if timespan == "day"
                      else (date.today() - __import__('datetime').timedelta(days=730)).isoformat(),
        )
        count = await ingester.ingest()
        task.date_range = [ingester.from_date, ingester.to_date]
        return count

    elif task.source == "alpha_vantage":
        from backend.ingestion.alpha_vantage import AlphaVantageIngester
        ingester = AlphaVantageIngester(symbols=[task.symbol])
        if task.symbol.startswith("MACRO_"):
            results = await ingester.ingest_macro_indicators()
            return results.get(task.symbol.lower().replace("macro_", "macro_"), 0)
        else:
            return await ingester.ingest()

    elif task.source == "coingecko":
        from backend.ingestion.coingecko import CoinGeckoIngester
        coin_id = COINGECKO_COIN_MAP.get(task.symbol)
        if not coin_id:
            task.status = TaskStatus.SKIPPED
            return 0
        ingester = CoinGeckoIngester(
            coins={coin_id: task.symbol},
            days="max",
        )
        return await ingester.ingest()

    elif task.source == "yahoo":
        from backend.ingestion.yahoo_finance import YahooFinanceIngester
        ingester = YahooFinanceIngester(
            symbols=[task.symbol],
            period="10y",
            interval="1d",
        )
        return await ingester.ingest()

    raise ValueError(f"Unknown source: {task.source}")


# ── CLI entry point ────────────────────────────────────────────────────────────

async def main():
    import argparse
    parser = argparse.ArgumentParser(description="Backfill manager")
    parser.add_argument("--run",      action="store_true", help="Execute pending tasks")
    parser.add_argument("--source",   type=str, help="Limit to one source (polygon|alpha_vantage|coingecko|yahoo)")
    parser.add_argument("--reset",    action="store_true", help="Clear all state and start fresh")
    parser.add_argument("--dry-run",  action="store_true", dest="dry_run", help="Step through tasks without calling APIs")
    args = parser.parse_args()

    if args.reset:
        if STATE_FILE.exists():
            STATE_FILE.unlink()
        console.print("[yellow]State cleared.[/yellow]")

    sources = load_state()

    if args.run:
        targets = [args.source] if args.source else list(sources.keys())
        for key in targets:
            if key not in sources:
                console.print(f"[red]Unknown source: {key}[/red]")
                continue
            await run_source(key, sources, dry_run=args.dry_run)
        console.print()

    # Always show dashboard after any action
    render_dashboard(sources)
    render_next_actions(sources)


if __name__ == "__main__":
    asyncio.run(main())
