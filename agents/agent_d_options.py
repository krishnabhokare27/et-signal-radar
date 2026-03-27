"""
ET Signal Radar — Agent D: Options Intelligence
Scores options chain signals: PCR shift, OI spike, IV contraction.
Returns score 0-30.
"""

import logging
from typing import Optional
from data.feeds import fetch_options_chain, parse_options_metrics

logger = logging.getLogger(__name__)


def analyze_options(symbol: str) -> dict:
    """
    Main entry. Fetches and scores options chain for a symbol.
    Returns {options_score, pcr, pcr_signal, oi_spike, detail}
    Gracefully degrades if NSE API is unavailable.
    """
    try:
        chain_data = fetch_options_chain(symbol)
        if not chain_data:
            return _default_options()

        metrics = parse_options_metrics(chain_data)
        score = _score_options(metrics)

        return {
            "options_score": score,
            "pcr": metrics["pcr"],
            "pcr_signal": metrics["pcr_signal"],
            "total_call_oi": metrics["total_call_oi"],
            "total_put_oi": metrics["total_put_oi"],
            "oi_spike_score": metrics["oi_spike_score"],
            "detail": _build_detail(metrics),
        }

    except Exception as e:
        logger.error(f"Options analysis error for {symbol}: {e}")
        return _default_options()


def _score_options(metrics: dict) -> int:
    """
    Score options signal 0-30:
    - PCR signal: 0-15 pts
    - OI build-up: 0-10 pts
    - Volume activity: 0-5 pts
    """
    score = 0
    pcr = metrics.get("pcr", 1.0)
    pcr_signal = metrics.get("pcr_signal", "neutral")
    oi_spike = metrics.get("oi_spike_score", 0)

    # PCR scoring
    if pcr_signal == "bullish":
        if pcr < 0.5:
            score += 15  # Very bullish
        elif pcr < 0.7:
            score += 10
        else:
            score += 5
    elif pcr_signal == "neutral":
        score += 3

    # OI spike
    score += min(10, oi_spike)

    # Bonus for very high OI activity
    total_oi = metrics.get("total_call_oi", 0) + metrics.get("total_put_oi", 0)
    if total_oi > 10_000_000:
        score += 5

    return min(30, score)


def _build_detail(metrics: dict) -> str:
    """Build human-readable options summary."""
    pcr = metrics.get("pcr", 1.0)
    signal = metrics.get("pcr_signal", "neutral")
    call_oi = metrics.get("total_call_oi", 0)
    put_oi = metrics.get("total_put_oi", 0)

    oi_str = f"Call OI: {call_oi:,} | Put OI: {put_oi:,}"
    return f"PCR: {pcr} ({signal.capitalize()}) | {oi_str}"


def _default_options() -> dict:
    """Returns neutral defaults when options data unavailable."""
    return {
        "options_score": 0,
        "pcr": 1.0,
        "pcr_signal": "unavailable",
        "total_call_oi": 0,
        "total_put_oi": 0,
        "oi_spike_score": 0,
        "detail": "Options data unavailable — NSE API timeout",
    }
