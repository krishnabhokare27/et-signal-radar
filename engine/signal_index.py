"""
ET Signal Radar — Signal Strength Index Engine + Orchestrator
Combines all 4 agent scores into SSI (0-100).
Fires alert only when SSI >= threshold (default 65).
"""

import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)
ALERT_THRESHOLD = int(os.getenv("ALERT_THRESHOLD", "65"))


def calculate_ssi(
    catalyst_score: int,      # Agent A: 0-30
    pattern_score: int,       # Agent B: 0-25 (split: quality 15 + type 10)
    backtest_score: int,      # Agent C: 0-25
    options_score: int,       # Agent D: 0-20
    insider_score: int,       # Agent A insider: 0-10
) -> dict:
    """
    Calculate Signal Strength Index.
    Component max: A(30) + B_quality(15) + C(25) + D(20) + insider(10) = 100
    """
    # Normalize pattern score to 15-point quality component
    pattern_quality = min(15, int(pattern_score * 15 / 25))

    # Normalize backtest score (already 0-25 from win_rate)
    bt_score = min(25, backtest_score)

    # Cap options at 20
    opt_score = min(20, options_score)

    # Cap insider at 10
    ins_score = min(10, insider_score)

    # Cap catalyst at 30
    cat_score = min(30, catalyst_score)

    total = cat_score + pattern_quality + bt_score + opt_score + ins_score

    return {
        "ssi": min(100, total),
        "breakdown": {
            "catalyst": cat_score,
            "pattern_quality": pattern_quality,
            "backtest": bt_score,
            "options": opt_score,
            "insider": ins_score,
        },
        "fires_alert": total >= ALERT_THRESHOLD,
    }


def backtest_to_score(backtest: dict) -> int:
    """Convert backtest stats to 0-25 score."""
    win_rate = backtest.get("win_rate", 0.5)
    n = backtest.get("n_occurrences", 0)
    avg_gain = backtest.get("avg_gain_pct", 0)
    avg_loss = backtest.get("avg_loss_pct", 0)

    # Base score from win rate
    score = int(win_rate * 15)

    # Bonus for sample size (more occurrences = more reliable)
    if n >= 20:
        score += 7
    elif n >= 10:
        score += 5
    elif n >= 5:
        score += 3

    # Bonus for positive expectancy
    expectancy = (win_rate * avg_gain) + ((1 - win_rate) * avg_loss)
    if expectancy > 5:
        score += 3

    return min(25, score)


def run_full_pipeline(symbol: str, df, announcement: Optional[dict] = None, bulk_deal: Optional[dict] = None) -> Optional[dict]:
    """
    Run complete 4-agent pipeline for a single symbol.
    Returns full alert dict or None if SSI < threshold.
    """
    import sys, os
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

    from agents.agent_b_technical import detect_patterns
    from agents.agent_c_backtester import run_backtest
    from agents.agent_a_fundamental import parse_filing, parse_bulk_deal, CATALYST_SCORES
    from agents.agent_d_options import analyze_options
    from compliance.formatter import format_alert

    try:
        # Agent B — always runs (no API dependency)
        pattern = detect_patterns(symbol, df)
        if pattern is None:
            return None  # No technical signal — don't waste API calls

        # Run remaining agents in parallel
        results = {}
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {
                executor.submit(run_backtest, symbol, pattern["pattern_type"], df): "backtest",
                executor.submit(analyze_options, symbol): "options",
            }
            if announcement:
                futures[executor.submit(parse_filing, announcement)] = "fundamental"

            for future in as_completed(futures):
                key = futures[future]
                try:
                    results[key] = future.result()
                except Exception as e:
                    logger.error(f"Agent {key} failed for {symbol}: {e}")
                    results[key] = None

        # Parse bulk deal if provided
        insider_data = {"insider_score": 0, "insider_detail": "No insider data"}
        if bulk_deal:
            insider_data = parse_bulk_deal(bulk_deal)

        backtest = results.get("backtest") or {"win_rate": 0.5, "bull_win_rate": 0.55, "bear_win_rate": 0.4, "avg_gain_pct": 5.0, "avg_loss_pct": -3.0, "n_occurrences": 5}
        options = results.get("options") or {"options_score": 0, "pcr": 1.0, "pcr_signal": "neutral"}
        fundamental = results.get("fundamental") or {"catalyst_type": "Unknown", "catalyst_score": 5, "summary": "No filing detected", "sentiment": "neutral"}

        # Calculate SSI
        bt_score = backtest_to_score(backtest)
        ssi_result = calculate_ssi(
            catalyst_score=fundamental.get("catalyst_score", 5),
            pattern_score=pattern["score"],
            backtest_score=bt_score,
            options_score=options.get("options_score", 0),
            insider_score=insider_data.get("insider_score", 0),
        )

        if not ssi_result["fires_alert"]:
            logger.debug(f"{symbol} SSI={ssi_result['ssi']} — below threshold {ALERT_THRESHOLD}")
            return None

        # Build full alert
        raw_alert = {
            "symbol": symbol,
            "pattern_type": pattern["pattern_type"],
            "pattern_score": pattern["score"],
            "volume_ratio": pattern.get("volume_ratio", 1.0),
            "rsi": pattern.get("rsi", 50),
            "current_price": pattern.get("current_price", 0),
            "price_change_pct": pattern.get("price_change_pct", 0),
            "resistance_zone": pattern.get("resistance_zone", [0, 0]),
            "support_zone": pattern.get("support_zone", [0, 0]),
            "catalyst_type": fundamental.get("catalyst_type", "Unknown"),
            "catalyst_summary": fundamental.get("summary", ""),
            "catalyst_score": fundamental.get("catalyst_score", 5),
            "catalyst_sentiment": fundamental.get("sentiment", "neutral"),
            "backtest": backtest,
            "options": options,
            "insider_score": insider_data.get("insider_score", 0),
            "insider_detail": insider_data.get("insider_detail", ""),
            "ssi": ssi_result["ssi"],
            "ssi_breakdown": ssi_result["breakdown"],
            "timestamp": datetime.now().isoformat(),
        }

        return format_alert(raw_alert)

    except Exception as e:
        logger.error(f"Pipeline error for {symbol}: {e}")
        return None
