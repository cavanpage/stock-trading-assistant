import logging

import pandas as pd
import pandas_ta as ta

logger = logging.getLogger(__name__)


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute common technical indicators for an OHLCV DataFrame.

    Input DataFrame must have columns: open, high, low, close, volume
    with a datetime index sorted ascending.
    Returns the same DataFrame with additional indicator columns.
    """
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]

    # Trend
    df.ta.ema(length=9, append=True)
    df.ta.ema(length=21, append=True)
    df.ta.ema(length=50, append=True)
    df.ta.macd(fast=12, slow=26, signal=9, append=True)

    # Momentum
    df.ta.rsi(length=14, append=True)
    df.ta.stoch(append=True)

    # Volatility
    df.ta.bbands(length=20, std=2, append=True)
    df.ta.atr(length=14, append=True)

    # Volume
    df.ta.obv(append=True)
    df.ta.vwap(append=True)

    return df.dropna()
