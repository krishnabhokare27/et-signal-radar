"""
ET Signal Radar — Agent B: Technical Scanner
Scans NSE universe for chart patterns using TA-Lib / pandas-ta.
Returns pattern type + score 0-25.
"""

import pandas as pd
import numpy as np
import logging
from typing import Optional
from scipy.signal import argrelextrema

logger = logging.getLogger(__name__)


def detect_patterns(symbol: str, df: pd.DataFrame) -> Optional[dict]:
    """
    Main entry point. Runs all pattern detectors on OHLCV data.
    Returns highest-scoring pattern or None if no signal found.
    """
    if df is None or len(df) < 50:
        return None

    try:
        df = df.copy()
        df = _add_indicators(df)

        detectors = [
            _detect_breakout,
            _detect_rsi_divergence,
            _detect_macd_crossover,
            _detect_volume_surge,
        ]

        best = None
        for detector in detectors:
            result = detector(symbol, df)
            if result and (best is None or result["score"] > best["score"]):
                best = result

        return best

    except Exception as e:
        logger.error(f"Pattern detection error for {symbol}: {e}")
        return None


def _add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Add all technical indicators to dataframe."""
    close = df["close"]
    high = df["high"]
    low = df["low"]
    volume = df["volume"]

    # RSI
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    rs = gain / loss
    df["rsi"] = 100 - (100 / (1 + rs))

    # MACD
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    df["macd"] = ema12 - ema26
    df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean()
    df["macd_hist"] = df["macd"] - df["macd_signal"]

    # EMAs
    df["ema20"] = close.ewm(span=20, adjust=False).mean()
    df["ema50"] = close.ewm(span=50, adjust=False).mean()
    df["ema200"] = close.ewm(span=200, adjust=False).mean()

    # Volume metrics
    df["vol_ma20"] = volume.rolling(20).mean()
    df["vol_ratio"] = volume / df["vol_ma20"]

    # 52-week high/low
    df["high_52w"] = high.rolling(252).max()
    df["low_52w"] = low.rolling(252).min()

    # ATR for volatility
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs()
    ], axis=1).max(axis=1)
    df["atr"] = tr.rolling(14).mean()

    return df


def _calculate_sr_zones(df: pd.DataFrame, n: int = 20) -> dict:
    """Calculate support and resistance zones from price action."""
    close = df["close"].values
    high = df["high"].values
    low = df["low"].values

    # Find local highs and lows
    local_highs = argrelextrema(high, np.greater, order=5)[0]
    local_lows = argrelextrema(low, np.less, order=5)[0]

    current_price = close[-1]

    # Resistance: nearest local high above current price
    resistance_levels = [high[i] for i in local_highs if high[i] > current_price]
    support_levels = [low[i] for i in local_lows if low[i] < current_price]

    resistance = sorted(resistance_levels)[:2] if resistance_levels else [current_price * 1.05, current_price * 1.10]
    support = sorted(support_levels, reverse=True)[:2] if support_levels else [current_price * 0.95, current_price * 0.90]

    return {
        "resistance_zone": [round(resistance[0], 2), round(resistance[-1], 2)],
        "support_zone": [round(support[0], 2), round(support[-1], 2)],
    }


def _detect_breakout(symbol: str, df: pd.DataFrame) -> Optional[dict]:
    """
    52-week high breakout with volume confirmation.
    Score: 10 base + 5 volume + 5 trend alignment + 5 momentum
    """
    latest = df.iloc[-1]
    prev = df.iloc[-2]

    close = latest["close"]
    high_52w = latest["high_52w"]

    # Breakout condition: within 1% of 52-week high + volume surge
    near_52w_high = close >= high_52w * 0.99
    volume_surge = latest["vol_ratio"] >= 1.5
    above_ema50 = close > latest["ema50"]
    rsi_ok = 50 <= latest["rsi"] <= 75  # Not overbought

    if not (near_52w_high and volume_surge):
        return None

    score = 10
    if volume_surge: score += 5
    if above_ema50: score += 5
    if rsi_ok: score += 5

    zones = _calculate_sr_zones(df)

    return {
        "symbol": symbol,
        "pattern_type": "52-Week High Breakout",
        "score": score,
        "volume_ratio": round(latest["vol_ratio"], 2),
        "rsi": round(latest["rsi"], 1),
        "current_price": round(close, 2),
        "price_change_pct": round(((close - prev["close"]) / prev["close"]) * 100, 2),
        **zones,
    }


def _detect_rsi_divergence(symbol: str, df: pd.DataFrame) -> Optional[dict]:
    """
    Bullish RSI divergence: price lower low but RSI higher low.
    Most reliable reversal signal.
    """
    recent = df.tail(30)
    close = recent["close"].values
    rsi = recent["rsi"].values

    if len(close) < 20:
        return None

    # Find recent lows
    price_lows = argrelextrema(close, np.less, order=3)[0]
    rsi_lows = argrelextrema(rsi, np.less, order=3)[0]

    if len(price_lows) < 2 or len(rsi_lows) < 2:
        return None

    # Check: last price low < previous price low (lower low)
    # AND last RSI low > previous RSI low (higher low)
    p1, p2 = price_lows[-2], price_lows[-1]
    r1, r2 = rsi_lows[-2], rsi_lows[-1]

    price_lower_low = close[p2] < close[p1]
    rsi_higher_low = rsi[r2] > rsi[r1]

    if not (price_lower_low and rsi_higher_low):
        return None

    # Additional confirmation: RSI should be oversold territory
    oversold = rsi[-1] < 45
    vol_ok = df.iloc[-1]["vol_ratio"] > 0.8

    score = 12
    if oversold: score += 8
    if vol_ok: score += 5

    zones = _calculate_sr_zones(df)
    latest = df.iloc[-1]

    return {
        "symbol": symbol,
        "pattern_type": "Bullish RSI Divergence",
        "score": score,
        "volume_ratio": round(latest["vol_ratio"], 2),
        "rsi": round(latest["rsi"], 1),
        "current_price": round(latest["close"], 2),
        "price_change_pct": round(((latest["close"] - df.iloc[-2]["close"]) / df.iloc[-2]["close"]) * 100, 2),
        **zones,
    }


def _detect_macd_crossover(symbol: str, df: pd.DataFrame) -> Optional[dict]:
    """
    MACD bullish crossover with volume confirmation.
    """
    latest = df.iloc[-1]
    prev = df.iloc[-2]

    # MACD just crossed above signal line
    macd_crossed = (prev["macd"] < prev["macd_signal"]) and (latest["macd"] > latest["macd_signal"])
    # MACD histogram turning positive
    hist_positive = latest["macd_hist"] > 0
    # Price above EMA20
    above_ema20 = latest["close"] > latest["ema20"]
    # Volume confirmation
    volume_ok = latest["vol_ratio"] >= 1.2

    if not macd_crossed:
        return None

    score = 10
    if hist_positive: score += 5
    if above_ema20: score += 5
    if volume_ok: score += 5

    zones = _calculate_sr_zones(df)

    return {
        "symbol": symbol,
        "pattern_type": "MACD Bullish Crossover",
        "score": score,
        "volume_ratio": round(latest["vol_ratio"], 2),
        "rsi": round(latest["rsi"], 1),
        "current_price": round(latest["close"], 2),
        "price_change_pct": round(((latest["close"] - prev["close"]) / prev["close"]) * 100, 2),
        **zones,
    }


def _detect_volume_surge(symbol: str, df: pd.DataFrame) -> Optional[dict]:
    """
    Unusual volume surge with price breakout above EMA50.
    """
    latest = df.iloc[-1]
    prev = df.iloc[-2]

    massive_volume = latest["vol_ratio"] >= 2.5
    price_up = latest["close"] > prev["close"]
    above_ema50 = latest["close"] > latest["ema50"]
    rsi_range = 40 < latest["rsi"] < 70

    if not (massive_volume and price_up and above_ema50):
        return None

    score = 10
    if latest["vol_ratio"] >= 3.0: score += 7
    elif latest["vol_ratio"] >= 2.5: score += 5
    if rsi_range: score += 5
    if latest["close"] > latest["ema200"]: score += 3

    zones = _calculate_sr_zones(df)

    return {
        "symbol": symbol,
        "pattern_type": "Volume Surge Breakout",
        "score": min(25, score),
        "volume_ratio": round(latest["vol_ratio"], 2),
        "rsi": round(latest["rsi"], 1),
        "current_price": round(latest["close"], 2),
        "price_change_pct": round(((latest["close"] - prev["close"]) / prev["close"]) * 100, 2),
        **zones,
    }
