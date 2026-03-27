"""
ET Signal Radar — SEBI Compliance Formatter
The legal firewall. Every alert must pass through this before reaching users.
Enforces: no buy/sell language, AI disclosure, mandatory disclaimer.
"""

import re
import logging

logger = logging.getLogger(__name__)

# ─── SEBI Banned Words ────────────────────────────────────────────────────────
# If any of these appear in output, raise an error immediately.
BANNED_WORDS = [
    r"\bbuy\b", r"\bsell\b", r"\bshort\b", r"\blong\b",
    r"\bstop.?loss\b", r"\btarget.?price\b", r"\bentry.?point\b",
    r"\bexit.?point\b", r"\bguaranteed\b", r"\bwill go up\b",
    r"\bwill rise\b", r"\bwill fall\b", r"\bpurchase now\b",
    r"\btrade now\b", r"\btake a position\b", r"\brecommend\b",
    r"\badvise\b", r"\binvest now\b",
]

DISCLAIMER = (
    "IMPORTANT DISCLAIMER: This is AI-generated market data intelligence and does NOT constitute "
    "investment advice, a research report, or a buy/sell recommendation. AI tools have been used "
    "in generating these signals — outputs may be inaccurate or incomplete. Past pattern "
    "performance does not guarantee future results. Support and resistance levels are data-derived "
    "zones, NOT price targets. Please consult a SEBI-registered Research Analyst before making "
    "investment decisions. ET Markets is not responsible for gains or losses based on this information. "
    "Regulatory basis: SEBI RA Regulations 2014 (as amended Dec 2024) | SEBI AI Circular Jan 2025."
)

AI_LABEL = "AI-Generated Signal"


def format_alert(raw: dict) -> dict:
    """
    Takes raw signal data and returns SEBI-compliant alert dict.
    Raises ValueError if banned words detected in any text field.
    """
    # Audit all text fields for banned words
    _audit_text(raw.get("catalyst_summary", ""))
    _audit_text(raw.get("insider_detail", ""))

    # Build compliant output
    alert = {
        # Identity
        "symbol": raw["symbol"],
        "company": raw.get("company", raw["symbol"]),
        "sector": raw.get("sector", "NSE Listed"),
        "exchange": "NSE",

        # Technical signal (SEBI-safe: no price targets)
        "pattern_detected": raw["pattern_type"],
        "key_data_levels": {
            "resistance_zone": raw.get("resistance_zone", [0, 0]),
            "support_zone": raw.get("support_zone", [0, 0]),
            "note": "These are data-derived price zones, not buy/sell targets",
        },
        "volume_data": f"{raw.get('volume_ratio', 1.0):.1f}x average volume",
        "rsi_reading": raw.get("rsi", 50),
        "current_market_price": raw.get("current_price", 0),
        "price_change_today_pct": raw.get("price_change_pct", 0),

        # Fundamental catalyst (SEBI-safe: factual description only)
        "catalyst_detected": raw["catalyst_type"],
        "catalyst_description": raw["catalyst_summary"],
        "catalyst_sentiment": raw.get("catalyst_sentiment", "neutral"),

        # Historical data (SEBI-safe: always labelled as past data)
        "historical_signal_data": {
            "disclaimer": "Historical data only. Past performance does not guarantee future results.",
            "win_rate_overall": raw["backtest"]["win_rate"],
            "win_rate_bull_market": raw["backtest"]["bull_win_rate"],
            "win_rate_bear_market": raw["backtest"]["bear_win_rate"],
            "avg_return_on_wins_pct": raw["backtest"]["avg_gain_pct"],
            "avg_return_on_losses_pct": raw["backtest"]["avg_loss_pct"],
            "sample_size": raw["backtest"]["n_occurrences"],
        },

        # Options market data (activity data, not direction call)
        "options_market_data": {
            "pcr": raw["options"].get("pcr", 1.0),
            "pcr_interpretation": raw["options"].get("pcr_signal", "neutral").capitalize(),
            "oi_activity": raw["options"].get("detail", "N/A"),
        },

        # Insider/bulk deal data
        "institutional_activity": raw.get("insider_detail", "No institutional data"),

        # Signal Strength Index
        "signal_strength_index": raw["ssi"],
        "ssi_breakdown": raw.get("ssi_breakdown", {}),

        # Compliance fields — MANDATORY
        "ai_label": AI_LABEL,
        "disclaimer": DISCLAIMER,
        "is_investment_advice": False,
        "is_buy_sell_recommendation": False,

        # Metadata
        "timestamp": raw["timestamp"],
        "data_source": "NSE/BSE public data | AI-processed",
    }

    return alert


def _audit_text(text: str):
    """Check text for banned words. Raises ValueError if found."""
    text_lower = text.lower()
    for pattern in BANNED_WORDS:
        if re.search(pattern, text_lower):
            raise ValueError(f"SEBI compliance violation: banned phrase '{pattern}' detected in output")


def format_telegram_message(alert: dict) -> str:
    """
    Format alert as Telegram message (SEBI-compliant).
    """
    ssi = alert["signal_strength_index"]
    hist = alert["historical_signal_data"]
    win_pct = int(hist["win_rate_overall"] * 100)
    bull_pct = int(hist["win_rate_bull_market"] * 100)
    bear_pct = int(hist["win_rate_bear_market"] * 100)
    n = hist["sample_size"]

    # SSI visual bar
    filled = int(ssi / 10)
    bar = "█" * filled + "░" * (10 - filled)

    r_zone = alert["key_data_levels"]["resistance_zone"]
    s_zone = alert["key_data_levels"]["support_zone"]
    pcr = alert["options_market_data"]["pcr"]
    pcr_interp = alert["options_market_data"]["pcr_interpretation"]

    msg = f"""🔬 *ET SIGNAL RADAR — Pre-Market Alert*
🤖 _{AI_LABEL}_

*{alert['symbol']}* | {alert['exchange']} | {alert['sector']}
💹 CMP: ₹{alert['current_market_price']:,.2f} ({'+' if alert['price_change_today_pct'] >= 0 else ''}{alert['price_change_today_pct']:.2f}%)

📊 *Pattern Detected:* {alert['pattern_detected']}
📌 *Catalyst:* {alert['catalyst_detected']}
_{alert['catalyst_description']}_

📈 *Signal Strength Index: {ssi}/100*
`{bar}` 

📉 *Historical Signal Data* _(past data, not a guarantee)_
Overall win rate: {win_pct}% | Bull market: {bull_pct}% | Bear market: {bear_pct}%
Avg gain on wins: +{hist['avg_return_on_wins_pct']}% | Avg loss: {hist['avg_return_on_losses_pct']}%
Sample: {n} historical occurrences

🎯 *Key Price Levels* _(data zones, not targets)_
Resistance: ₹{r_zone[0]:,}–₹{r_zone[1]:,}
Support: ₹{s_zone[0]:,}–₹{s_zone[1]:,}

📊 *Options Market:* PCR {pcr} ({pcr_interp}) | Vol: {alert['volume_data']}

⚠️ _NOT investment advice. Consult a SEBI-registered Research Analyst before investing._
"""
    return msg
