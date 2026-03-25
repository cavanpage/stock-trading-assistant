#!/usr/bin/env python3
"""
Build labeled training datasets for XGBoost and LSTM models.

This script:
  1. Pulls historical OHLCV from the database (all sources)
  2. Computes technical indicator features
  3. Attaches macro context features (Fed funds rate, VIX, CPI)
  4. Generates forward-looking labels (what happened N bars later)
  5. Splits into train/validation/test sets using TimeSeriesSplit
     — NEVER random splits, always chronological
  6. Saves to parquet files in data/training/

Run after backfilling historical data:
    python scripts/backfill_historical.py
    python scripts/build_training_dataset.py

Output files:
    data/training/{symbol}_{timeframe}_train.parquet
    data/training/{symbol}_{timeframe}_val.parquet
    data/training/{symbol}_{timeframe}_test.parquet
    data/training/dataset_stats.json
"""
import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from backend.db.session import AsyncSessionLocal
from backend.processing.feature_engineer import compute_indicators
from sqlalchemy import select, and_
from backend.models.ohlcv import OHLCV

OUTPUT_DIR = Path("data/training")

# ── Label parameters ────────────────────────────────────────────────────────
# Adjust these to tune what "BUY" and "SELL" mean for your strategy
FORWARD_BARS = 5          # predict return 5 bars forward
BUY_THRESHOLD = 0.02      # +2% gain → label 2 (BUY)
SELL_THRESHOLD = -0.02    # -2% loss → label 0 (SELL)
# label 1 = HOLD (between thresholds)

# ── Train/val/test split ratios ─────────────────────────────────────────────
TRAIN_RATIO = 0.70
VAL_RATIO   = 0.15
# test = remaining 15% — always the most recent data

SYMBOLS = [
    "AAPL", "MSFT", "NVDA", "TSLA", "AMZN",
    "GOOGL", "META", "SPY", "QQQ",
    "BTC-USD", "ETH-USD", "SOL-USD",
]

SOURCES_PREFERENCE = [
    # Ordered by data quality preference — use first available per symbol/date
    "polygon_1day",
    "alpha_vantage_daily",
    "yahoo_finance",
    "binance",
    "coingecko_daily",
]


async def load_ohlcv(symbol: str) -> pd.DataFrame:
    """Load all OHLCV records for a symbol, preferring higher-quality sources."""
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(OHLCV)
            .where(OHLCV.symbol == symbol)
            .order_by(OHLCV.timestamp.asc())
        )
        rows = result.scalars().all()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame([{
        "timestamp": r.timestamp,
        "open":   r.open,
        "high":   r.high,
        "low":    r.low,
        "close":  r.close,
        "volume": r.volume or 0.0,
        "source": r.source,
    } for r in rows])

    # Deduplicate: where multiple sources cover the same date, keep highest-quality
    df["source_rank"] = df["source"].apply(_source_rank)
    df = (
        df.sort_values(["timestamp", "source_rank"])
          .drop_duplicates(subset="timestamp", keep="first")
          .drop(columns=["source", "source_rank"])
          .set_index("timestamp")
          .sort_index()
    )

    # Drop rows with null close
    df = df.dropna(subset=["close"])
    return df


def _source_rank(source: str) -> int:
    for i, preferred in enumerate(SOURCES_PREFERENCE):
        if preferred in source:
            return i
    return len(SOURCES_PREFERENCE)


async def load_macro_features() -> pd.DataFrame:
    """
    Load macro indicators as features. Resampled to daily, forward-filled.
    These are symbol-agnostic context features added to every row.
    """
    macro_symbols = [
        "MACRO_FED_FUNDS_RATE",
        "MACRO_CPI",
        "MACRO_INFLATION",
        "MACRO_UNEMPLOYMENT",
        "MACRO_REAL_GDP",
    ]

    frames = {}
    async with AsyncSessionLocal() as session:
        for sym in macro_symbols:
            result = await session.execute(
                select(OHLCV)
                .where(OHLCV.symbol == sym)
                .order_by(OHLCV.timestamp.asc())
            )
            rows = result.scalars().all()
            if rows:
                s = pd.Series(
                    {r.timestamp: r.close for r in rows},
                    name=sym.lower().replace("macro_", "macro_"),
                )
                frames[sym.lower()] = s

    if not frames:
        return pd.DataFrame()

    macro_df = pd.DataFrame(frames)
    macro_df.index = pd.to_datetime(macro_df.index, utc=True)
    macro_df = macro_df.resample("D").last().ffill()
    return macro_df


def add_labels(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add forward-looking labels. This is the supervised learning target.

    forward_return = close[t + FORWARD_BARS] / close[t] - 1

    label:
      2 = BUY  (forward_return > BUY_THRESHOLD)
      0 = SELL (forward_return < SELL_THRESHOLD)
      1 = HOLD (everything else)

    The threshold choice directly shapes your model:
      - Tight (±0.5%): many signals, more noise, harder to learn
      - Wide (±5%):   fewer signals, cleaner labels, misses smaller moves
      ±2% daily is a reasonable starting point for daily bars.
    """
    df = df.copy()
    df["forward_return"] = df["close"].shift(-FORWARD_BARS) / df["close"] - 1

    df["label"] = 1  # HOLD default
    df.loc[df["forward_return"] > BUY_THRESHOLD,  "label"] = 2   # BUY
    df.loc[df["forward_return"] < SELL_THRESHOLD, "label"] = 0   # SELL

    # Drop the last FORWARD_BARS rows — they have no valid label
    df = df.iloc[:-FORWARD_BARS]

    return df


def add_return_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add lagged return features — the model needs to see recent price history
    not just snapshot indicators.
    """
    df = df.copy()
    for lag in [1, 2, 3, 5, 10, 20]:
        df[f"return_{lag}d"] = df["close"].pct_change(lag)

    # Realised volatility (20-day rolling std of returns)
    df["realized_vol_20d"] = df["close"].pct_change().rolling(20).std()

    # Volume z-score vs 20-day mean (how unusual is today's volume?)
    vol_mean = df["volume"].rolling(20).mean()
    vol_std  = df["volume"].rolling(20).std().replace(0, 1)
    df["volume_zscore"] = (df["volume"] - vol_mean) / vol_std

    return df


def time_series_split(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Chronological train/val/test split.

    NEVER use random splits for time-series financial data.
    Random splits leak future information into training — your model will
    look great in validation but fail live because it trained on future bars.

        |──── train (70%) ────|── val (15%) ──|── test (15%) ──|
                                                                ↑ most recent
    """
    n = len(df)
    train_end = int(n * TRAIN_RATIO)
    val_end   = int(n * (TRAIN_RATIO + VAL_RATIO))

    train = df.iloc[:train_end]
    val   = df.iloc[train_end:val_end]
    test  = df.iloc[val_end:]

    return train, val, test


def build_symbol_dataset(symbol: str, df: pd.DataFrame, macro_df: pd.DataFrame) -> dict:
    """
    Full pipeline for one symbol:
      raw OHLCV → indicators → labels → macro join → split → parquet
    """
    print(f"  Building dataset for {symbol}: {len(df)} bars "
          f"({df.index[0].date()} → {df.index[-1].date()})")

    # Technical indicators
    df = compute_indicators(df)

    # Lagged returns and volume features
    df = add_return_features(df)

    # Attach macro features if available
    if not macro_df.empty:
        df = df.join(macro_df, how="left").ffill()

    # Forward-looking labels
    df = add_labels(df)

    # Drop any remaining NaN rows from indicator warmup period
    df = df.dropna()

    if len(df) < 100:
        print(f"    ⚠ Skipping {symbol} — only {len(df)} usable rows after feature computation")
        return {}

    # Log class balance — important to catch before training
    label_counts = df["label"].value_counts().to_dict()
    total = len(df)
    print(f"    Labels: SELL={label_counts.get(0,0)/total:.1%} "
          f"HOLD={label_counts.get(1,0)/total:.1%} "
          f"BUY={label_counts.get(2,0)/total:.1%}")

    # Check for class imbalance — warn if any class < 10%
    for cls, name in [(0, "SELL"), (2, "BUY")]:
        if label_counts.get(cls, 0) / total < 0.10:
            print(f"    ⚠ {name} class underrepresented ({label_counts.get(cls,0)/total:.1%}) "
                  f"— consider adjusting BUY_THRESHOLD/SELL_THRESHOLD")

    train, val, test = time_series_split(df)

    # Save
    symbol_slug = symbol.replace("-", "_")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    train.to_parquet(OUTPUT_DIR / f"{symbol_slug}_daily_train.parquet")
    val.to_parquet(OUTPUT_DIR   / f"{symbol_slug}_daily_val.parquet")
    test.to_parquet(OUTPUT_DIR  / f"{symbol_slug}_daily_test.parquet")

    return {
        "symbol": symbol,
        "total_bars": total,
        "train_bars": len(train),
        "val_bars":   len(val),
        "test_bars":  len(test),
        "date_range": [str(df.index[0].date()), str(df.index[-1].date())],
        "label_balance": {
            "sell": round(label_counts.get(0, 0) / total, 3),
            "hold": round(label_counts.get(1, 0) / total, 3),
            "buy":  round(label_counts.get(2, 0) / total, 3),
        },
        "feature_count": len([c for c in df.columns if c not in ("label", "forward_return")]),
    }


async def main():
    print("=" * 60)
    print("Building training datasets")
    print(f"  Forward bars:  {FORWARD_BARS}")
    print(f"  Buy threshold: +{BUY_THRESHOLD:.0%}")
    print(f"  Sell threshold:{SELL_THRESHOLD:.0%}")
    print(f"  Split: {TRAIN_RATIO:.0%} train / {VAL_RATIO:.0%} val / "
          f"{1-TRAIN_RATIO-VAL_RATIO:.0%} test")
    print("=" * 60)

    print("\nLoading macro features...")
    macro_df = await load_macro_features()
    if macro_df.empty:
        print("  No macro data found — run Alpha Vantage backfill first (optional)")
    else:
        print(f"  Loaded {len(macro_df.columns)} macro indicators: {list(macro_df.columns)}")

    stats = []
    for symbol in SYMBOLS:
        print(f"\n{symbol}")
        df = await load_ohlcv(symbol)
        if df.empty:
            print(f"  No data found — run backfill scripts first")
            continue

        result = build_symbol_dataset(symbol, df, macro_df)
        if result:
            stats.append(result)

    # Save dataset summary
    summary = {
        "built_at": datetime.now(timezone.utc).isoformat(),
        "parameters": {
            "forward_bars": FORWARD_BARS,
            "buy_threshold": BUY_THRESHOLD,
            "sell_threshold": SELL_THRESHOLD,
            "train_ratio": TRAIN_RATIO,
            "val_ratio": VAL_RATIO,
        },
        "datasets": stats,
        "total_train_bars": sum(s["train_bars"] for s in stats),
    }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_DIR / "dataset_stats.json", "w") as f:
        json.dump(summary, f, indent=2)

    print(f"\n{'='*60}")
    print(f"Done. {len(stats)} datasets saved to {OUTPUT_DIR}/")
    print(f"Total training bars: {summary['total_train_bars']:,}")
    print(f"\nNext step: python scripts/train_xgboost.py")


if __name__ == "__main__":
    asyncio.run(main())
