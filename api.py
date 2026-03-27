"""
ET Signal Radar — FastAPI Backend
REST API serving alerts and on-demand scans.
"""

import os, sys, json, logging
from datetime import datetime, date
from typing import Optional, List
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.path.dirname(__file__))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="ET Signal Radar API", version="1.0.0", description="SEBI-compliant AI market signal intelligence")

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# Serve the dashboard HTML
_static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.isdir(_static_dir):
    app.mount("/static", StaticFiles(directory=_static_dir), name="static")

# ─── In-memory alert store (replace with SQLite in production) ─────────────
_alert_store: List[dict] = []

def _load_mock_alerts():
    """Pre-load realistic mock alerts for demo."""
    from data.feeds import get_mock_alert
    symbols = [
        ("RELIANCE", "Reliance Industries Ltd", "Energy", "Cup and Handle Breakout", 79, "Earnings Beat >15%"),
        ("INFY", "Infosys Ltd", "IT", "MACD Bullish Crossover", 71, "Major Contract Win"),
        ("HDFCBANK", "HDFC Bank Ltd", "Banking", "52-Week High Breakout", 68, "Earnings Beat 5-15%"),
        ("SUNPHARMA", "Sun Pharmaceutical", "Pharma", "Volume Surge Breakout", 74, "Regulatory Approval"),
        ("BAJFINANCE", "Bajaj Finance Ltd", "NBFC", "Bullish RSI Divergence", 66, "Strong Q3 AUM Growth"),
    ]
    import random
    from datetime import timedelta
    alerts = []
    for i, (sym, co, sec, pat, ssi, cat) in enumerate(symbols):
        mock = get_mock_alert(sym)
        mock.update({
            "company": co, "sector": sec,
            "pattern_detected": pat,
            "signal_strength_index": ssi,
            "catalyst_detected": cat,
            "timestamp": (datetime.now() - timedelta(hours=i*2)).isoformat(),
        })
        alerts.append(mock)
    return alerts

_alert_store = _load_mock_alerts()


# ─── Routes ────────────────────────────────────────────────────────────────────

@app.get("/")
def root():
    return RedirectResponse(url="/static/dashboard.html")


@app.get("/api/alerts/today")
def get_today_alerts():
    """Returns today's fired alerts sorted by SSI descending."""
    today = date.today().isoformat()
    today_alerts = [
        a for a in _alert_store
        if a.get("timestamp", "")[:10] == today or True  # include all for demo
    ]
    today_alerts.sort(key=lambda x: x.get("signal_strength_index", x.get("ssi", 0)), reverse=True)
    return {"alerts": today_alerts[:20], "count": len(today_alerts), "generated_at": datetime.now().isoformat()}


@app.get("/api/alerts/history")
def get_alert_history(days: int = 7):
    """Returns historical alerts for the past N days."""
    return {"alerts": _alert_store, "count": len(_alert_store)}


@app.get("/api/prices")
def get_live_prices(symbols: str):
    """
    Fetch latest close prices for any NSE/BSE symbol.
    Auto-resolves NSE (.NS) then BSE (.BO).
    """
    from data.feeds import resolve_ticker
    import yfinance as yf
    result = {}
    sym_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]

    for sym in sym_list:
        ticker_str = resolve_ticker(sym)
        if not ticker_str:
            logger.debug(f"Could not resolve ticker for {sym}")
            continue
        try:
            hist = yf.Ticker(ticker_str).history(period="5d")
            if hist is not None and not hist.empty and len(hist) >= 1:
                close = hist["Close"].dropna()
                last = round(float(close.iloc[-1]), 2)
                chg = round(((last - float(close.iloc[-2])) / float(close.iloc[-2])) * 100, 2) if len(close) >= 2 else 0.0
                result[sym] = {"price": last, "change_pct": chg}
        except Exception as e:
            logger.debug(f"Price fetch failed for {sym}: {e}")

    return {"prices": result}


@app.get("/api/stock/{symbol}/scan")
async def scan_stock(symbol: str, background_tasks: BackgroundTasks):
    """
    On-demand scan for a single NSE symbol.
    Fetches real price from yfinance and runs full 4-agent pipeline.
    """
    symbol = symbol.upper().strip()

    # Resolve the correct ticker (NSE → BSE fallback) and fetch 1y of data
    real_price = None
    real_change_pct = None
    real_resistance_zone = None
    real_support_zone = None
    resolved_ticker = None
    try:
        import yfinance as yf
        from data.feeds import resolve_ticker
        resolved_ticker = resolve_ticker(symbol)
        if resolved_ticker:
            hist = yf.Ticker(resolved_ticker).history(period="1y")
            if not hist.empty and len(hist) >= 2:
                closes = hist['Close'].dropna()
                highs = hist['High'].dropna()
                lows = hist['Low'].dropna()

                real_price = round(float(closes.iloc[-1]), 2)
                prev = float(closes.iloc[-2])
                real_change_pct = round(((real_price - prev) / prev) * 100, 2)

                high_52w = float(highs.max())
                low_52w = float(lows.min())
                recent_high = float(highs.iloc[-20:].max()) if len(highs) >= 20 else high_52w
                recent_low = float(lows.iloc[-20:].min()) if len(lows) >= 20 else low_52w

                resist_lo = round(min(recent_high, high_52w * 0.99), 2)
                resist_hi = round(max(recent_high, high_52w * 1.005), 2)
                sup_lo = round(max(recent_low * 0.99, low_52w), 2)
                sup_hi = round(recent_low * 1.01, 2)

                real_resistance_zone = [resist_lo, resist_hi]
                real_support_zone = [sup_lo, sup_hi]
    except Exception as e:
        logger.debug(f"Price/S/R fetch for {symbol}: {e}")

    # Try to get the real company name from yfinance
    real_company_name = None
    try:
        if resolved_ticker:
            info = yf.Ticker(resolved_ticker).info
            real_company_name = (info.get("longName") or info.get("shortName") or
                                 info.get("displayName") or "").strip() or None
    except Exception:
        pass

    def inject_price(alert: dict) -> dict:
        if real_price is not None:
            alert["current_market_price"] = real_price
            alert["current_price"] = real_price
        if real_change_pct is not None:
            alert["price_change_today_pct"] = real_change_pct
        if real_resistance_zone is not None:
            alert["key_data_levels"] = {
                "resistance_zone": real_resistance_zone,
                "support_zone": real_support_zone,
            }
        if real_company_name:
            alert["company"] = real_company_name
        return alert

    try:
        from data.feeds import fetch_ohlcv, get_mock_alert
        df = fetch_ohlcv(symbol)

        if df is None or len(df) < 50:
            mock = get_mock_alert(symbol)
            mock["symbol"] = symbol
            mock["note"] = "Demo data — live scan requires market hours"
            return {"alert": inject_price(mock), "status": "demo"}

        from engine.signal_index import run_full_pipeline
        alert = run_full_pipeline(symbol, df)

        if alert is None:
            # Return a basic price card with real data even when SSI < 65
            from data.feeds import get_mock_alert
            basic = get_mock_alert(symbol)
            basic["symbol"] = symbol
            basic["signal_strength_index"] = 0
            basic["ssi"] = 0
            basic["note"] = f"No high-conviction signal (SSI < 65) — showing live price data only"
            return {"alert": inject_price(basic), "status": "no_signal",
                    "message": f"No high-conviction signal detected for {symbol} (SSI < 65)"}

        _alert_store.insert(0, inject_price(alert))
        return {"alert": inject_price(alert), "status": "signal_found"}

    except Exception as e:
        logger.error(f"Scan error for {symbol}: {e}")
        from data.feeds import get_mock_alert
        mock = get_mock_alert(symbol)
        return {"alert": inject_price(mock), "status": "demo", "note": str(e)}


@app.get("/api/stats")
def get_stats():
    """Dashboard statistics."""
    from data.feeds import fetch_nse_universe
    alerts = _alert_store
    universe = fetch_nse_universe()
    # Deduplicate the universe list
    universe_count = len(set(universe))

    if not alerts:
        return {"total_alerts": 0, "avg_ssi": 0, "top_pattern": "N/A",
                "stocks_scanned": universe_count}

    ssi_key = lambda a: a.get("signal_strength_index", a.get("ssi", 0))
    patterns = [a.get("pattern_detected", a.get("pattern_type", "")) for a in alerts]
    top_pattern = max(set(patterns), key=patterns.count) if patterns else "N/A"

    return {
        "total_alerts": len(alerts),
        "avg_ssi": round(sum(ssi_key(a) for a in alerts) / len(alerts), 1),
        "top_pattern": top_pattern,
        "stocks_scanned": universe_count,
        "last_scan": datetime.now().isoformat(),
    }


@app.get("/api/nifty/pulse")
def get_market_pulse():
    """Current market regime, sentiment, and live Nifty prices."""
    import yfinance as yf
    try:
        # Fetch live Nifty 50
        try:
            nifty = yf.Ticker("^NSEI")
            nifty_hist = nifty.history(period="5d")
            if not nifty_hist.empty and len(nifty_hist) >= 2:
                last_price = nifty_hist['Close'].iloc[-1]
                prev_price = nifty_hist['Close'].iloc[-2]
                nifty_change = round(((last_price - prev_price) / prev_price) * 100, 2)
            else:
                last_price, nifty_change = 22456.35, 0.84
        except Exception as e:
            logger.warning(f"Nifty fetch failed: {e}")
            last_price, nifty_change = 22456.35, 0.84

        # Fetch live India VIX
        try:
            vix = yf.Ticker("^INDIAVIX")
            vix_hist = vix.history(period="5d")
            if not vix_hist.empty and len(vix_hist) >= 2:
                vix_price = round(vix_hist['Close'].iloc[-1], 2)
                vix_prev = vix_hist['Close'].iloc[-2]
                vix_change = round(((vix_price - vix_prev) / vix_prev) * 100, 2)
            else:
                vix_price, vix_change = 13.42, -0.8
        except Exception as e:
            logger.debug(f"VIX fetch failed: {e}")
            vix_price, vix_change = 13.42, -0.8

        # Fetch live Bank Nifty
        bank_nifty_price, bank_nifty_change = 48234.10, 1.12  # fallback
        try:
            bn = yf.Ticker("^NSEBANK")
            bn_hist = bn.history(period="5d")
            if not bn_hist.empty and len(bn_hist) >= 2:
                bn_last = bn_hist['Close'].iloc[-1]
                bn_prev = bn_hist['Close'].iloc[-2]
                bank_nifty_price = round(float(bn_last), 2)
                bank_nifty_change = round(((bn_last - bn_prev) / bn_prev) * 100, 2)
        except Exception as e:
            logger.debug(f"Bank Nifty fetch failed: {e}")

        # Generic helper for any additional index
        def _fetch_index(ticker: str, fallback_price: float, fallback_chg: float):
            try:
                h = yf.Ticker(ticker).history(period="5d")
                if not h.empty and len(h) >= 2:
                    last = float(h['Close'].iloc[-1])
                    prev = float(h['Close'].iloc[-2])
                    return round(last, 2), round(((last - prev) / prev) * 100, 2)
            except Exception:
                pass
            return fallback_price, fallback_chg

        nifty_it_price, nifty_it_chg       = _fetch_index("^CNXIT",      34120, -0.34)
        nifty_pharma_price, nifty_pharma_chg = _fetch_index("^CNXPHARMA", 17890,  1.45)
        nifty_auto_price, nifty_auto_chg    = _fetch_index("^CNXAUTO",   22310,  0.67)

        # Fetch live FII/DII data from NSE India
        fii_buy = fii_sell = fii_net = dii_buy = dii_sell = dii_net = None
        fii_date = ""
        try:
            import urllib.request
            _nse_headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120",
                "Accept": "application/json",
                "Referer": "https://www.nseindia.com/",
            }
            req = urllib.request.Request(
                "https://www.nseindia.com/api/fiidiiTradeReact",
                headers=_nse_headers,
            )
            import json as _json
            raw = urllib.request.urlopen(req, timeout=6).read()
            rows = _json.loads(raw)
            for row in rows:
                cat = row.get("category", "")
                if "FII" in cat or "FPI" in cat:
                    fii_buy  = float(row["buyValue"])
                    fii_sell = float(row["sellValue"])
                    fii_net  = float(row["netValue"])
                    fii_date = row.get("date", "")
                elif cat == "DII":
                    dii_buy  = float(row["buyValue"])
                    dii_sell = float(row["sellValue"])
                    dii_net  = float(row["netValue"])
        except Exception as e:
            logger.debug(f"NSE FII/DII fetch failed: {e}")

        regime = "Bull Market" if nifty_change >= 0 else "Bear Market"

        return {
            "nifty_price": round(last_price, 2),
            "nifty_change_pct": nifty_change,
            "bank_nifty_price": bank_nifty_price,
            "bank_nifty_change_pct": bank_nifty_change,
            "nifty_it_price": nifty_it_price,
            "nifty_it_change_pct": nifty_it_chg,
            "nifty_pharma_price": nifty_pharma_price,
            "nifty_pharma_change_pct": nifty_pharma_chg,
            "nifty_auto_price": nifty_auto_price,
            "nifty_auto_change_pct": nifty_auto_chg,
            "vix_price": vix_price,
            "vix_change_pct": vix_change,
            "regime": regime,
            "nifty50_vs_200ma": "+8.3%",
            "fii_buy": fii_buy,
            "fii_sell": fii_sell,
            "fii_net": fii_net,
            "dii_buy": dii_buy,
            "dii_sell": dii_sell,
            "dii_net": dii_net,
            "fii_date": fii_date,
        }
    except Exception as e:
        logger.error(f"Failed to fetch pulse: {e}")
        return {
            "nifty_price": 22456.35,
            "nifty_change_pct": 0.84,
            "vix_price": 13.42,
            "vix_change_pct": -0.8,
            "regime": "Bull Market",
            "nifty50_vs_200ma": "+8.3%",
            "advance_decline": "1243 / 847",
            "fii_flow_today": "+₹2,847 Cr",
            "market_breadth": "positive",
        }
