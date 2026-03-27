"""
ET Signal Radar — Data Feeds Layer
All NSE/BSE data fetchers. NSE requires session-based cookie auth.
"""

import time
import requests
import yfinance as yf
import pandas as pd
import json
import logging
from datetime import datetime, timedelta
from typing import Optional
import random

logger = logging.getLogger(__name__)

# ─── NSE Session Manager ─────────────────────────────────────────────────────
# NSE blocks direct API calls. Must hit homepage first to get cookies.

class NSESession:
    BASE_URL = "https://www.nseindia.com"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www.nseindia.com/",
        "Connection": "keep-alive",
    }

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
        self._initialized = False

    def init(self):
        """Hit NSE homepage to acquire cookies before API calls."""
        try:
            self.session.get(self.BASE_URL, timeout=10)
            time.sleep(1)
            self.session.get(f"{self.BASE_URL}/market-data/live-equity-market", timeout=10)
            time.sleep(0.5)
            self._initialized = True
            logger.info("NSE session initialized")
        except Exception as e:
            logger.error(f"NSE session init failed: {e}")

    def get(self, endpoint: str, params: dict = None) -> Optional[dict]:
        if not self._initialized:
            self.init()
        try:
            url = f"{self.BASE_URL}/api/{endpoint}"
            resp = self.session.get(url, params=params, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"NSE API error [{endpoint}]: {e}")
            return None

# Global session instance
_nse = NSESession()


# ─── Fundamental Feeds ────────────────────────────────────────────────────────

def fetch_bse_announcements(days_back: int = 1) -> list[dict]:
    """
    Fetch recent corporate announcements from BSE.
    Returns list of {symbol, company, category, headline, date, pdf_url}
    """
    try:
        url = "https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w"
        params = {
            "strCat": "-1",
            "strPrevDate": (datetime.now() - timedelta(days=days_back)).strftime("%Y%m%d"),
            "strScrip": "",
            "strSearch": "P",
            "strToDate": datetime.now().strftime("%Y%m%d"),
            "strType": "C",
        }
        headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.bseindia.com"}
        resp = requests.get(url, params=params, headers=headers, timeout=15)
        data = resp.json()

        announcements = []
        for item in data.get("Table", []):
            announcements.append({
                "symbol": item.get("SCRIP_CD", ""),
                "company": item.get("SLONGNAME", ""),
                "category": item.get("CATEGORYNAME", ""),
                "headline": item.get("HEADLINE", ""),
                "date": item.get("NEWS_DT", ""),
                "pdf_url": f"https://www.bseindia.com/xml-data/corpfiling/AttachLive/{item.get('ATTACHMENTNAME', '')}",
            })
        logger.info(f"Fetched {len(announcements)} BSE announcements")
        return announcements
    except Exception as e:
        logger.error(f"BSE announcements fetch failed: {e}")
        return []


def fetch_bulk_deals() -> list[dict]:
    """Fetch bulk/block deals from NSE."""
    data = _nse.get("block-deal")
    if not data:
        return []
    deals = []
    for item in data.get("data", []):
        deals.append({
            "symbol": item.get("symbol", ""),
            "client": item.get("clientName", ""),
            "buy_sell": item.get("buySell", ""),
            "quantity": item.get("quantity", 0),
            "price": item.get("tradePrice", 0),
            "date": item.get("tradeDate", ""),
        })
    return deals


def fetch_insider_trades() -> list[dict]:
    """Fetch insider/promoter trades (PIT disclosures) from NSE."""
    data = _nse.get("corporates-pit", params={"index": "equities"})
    if not data:
        return []
    trades = []
    for item in data.get("data", []):
        trades.append({
            "symbol": item.get("symbol", ""),
            "name": item.get("name", ""),
            "category": item.get("personCategory", ""),
            "type": item.get("secType", ""),
            "quantity": item.get("noOfSecurities", 0),
            "value": item.get("valueSold", 0) or item.get("valueAcquired", 0),
            "date": item.get("date", ""),
        })
    return trades


# ─── Technical / Price Feeds ──────────────────────────────────────────────────

def fetch_nse_universe() -> list[str]:
    """
    Returns list of all NSE equity symbols.
    Falls back to a curated Nifty500 list if live fetch fails.
    """
    try:
        data = _nse.get("equity-stockIndices", params={"index": "NIFTY 500"})
        if data and "data" in data:
            symbols = [item["symbol"] for item in data["data"] if item.get("symbol")]
            logger.info(f"Fetched {len(symbols)} NSE symbols")
            return symbols
    except Exception as e:
        logger.error(f"NSE universe fetch failed: {e}")

    # Fallback — comprehensive Nifty 500 constituent list
    return [
        # Nifty 50
        "RELIANCE","TCS","HDFCBANK","INFY","ICICIBANK","HINDUNILVR","SBIN","BHARTIARTL",
        "ITC","KOTAKBANK","LT","AXISBANK","BAJFINANCE","MARUTI","TITAN","SUNPHARMA",
        "ULTRACEMCO","WIPRO","HCLTECH","NESTLEIND","POWERGRID","TECHM","BAJAJFINSV",
        "NTPC","ONGC","TATASTEEL","M&M","JSWSTEEL","ADANIENT","ADANIPORTS","COALINDIA",
        "DIVISLAB","DRREDDY","CIPLA","APOLLOHOSP","HDFCLIFE","SBILIFE","INDUSINDBK",
        "ASIANPAINT","BRITANNIA",
        # Nifty Next 50
        "PIDILITIND","SIEMENS","ABB","GODREJCP","MARICO","DABUR","BERGEPAINT",
        "HAVELLS","VOLTAS","AMBUJACEM","ACC","GRASIM","SHREECEM","INDIGO","TATAPOWER",
        "TATACONSUM","DMART","NYKAA","ZOMATO","PAYTM","LTI","MPHASIS","LTTS",
        "PERSISTENT","COFORGE","HAPPSTMNDS","OFSS","KPITTECH","HEXAWARE",
        # Banking & Finance
        "BANKBARODA","PNB","CANBK","IDFCFIRSTB","FEDERALBNK","RBLBANK","BANDHANBNK",
        "KARURVYSYA","DCBBANK","EQUITASBNK","UJJIVANSFB","AUBANK","SURYODAY",
        "CHOLAFIN","BAJAJHLDNG","MOTILALOFS","ANGELONE","5PAISA","IIFLWAM",
        "MANAPPURAM","MUTHOOTFIN","SHRIRAMFIN","LICHSGFIN","PNBHOUSING","CANFINHOME",
        "HOMEFIRST","AAVAS","APTUS","CREDITACC","SPANDANA",
        # IT & Technology
        "INFY","TCS","WIPRO","HCLTECH","TECHM","LTI","MPHASIS","LTTS","PERSISTENT",
        "COFORGE","HAPPSTMNDS","OFSS","KPITTECH","MASTEK","TATAELXSI","CYIENT",
        "NIIT","INTELLECT","TANLA","RATEGAIN","NEWGEN","ROUTE","ZENSAR","BIRLASOFT",
        # Auto & Auto-Ancillary
        "MARUTI","M&M","TATAMOTORS","BAJAJ-AUTO","HEROMOTOCO","EICHERMOT","TVS-SRICHAK",
        "TVSMOTOR","MAHINDCIE","MOTHERSON","BOSCHLTD","BHARATFORG","EXIDEIND","AMARAJABAT",
        "ENDURANCE","MINDAIND","SUNDRMFAST","TIINDIA","GABRIEL","SUPRAJIT",
        # Pharma & Healthcare
        "SUNPHARMA","DRREDDY","CIPLA","DIVISLAB","APOLLOHOSP","AUROPHARMA","BIOCON",
        "TORNTPHARM","ALKEM","LUPIN","ABBOTINDIA","PFIZER","SANOFI","GLAXO","NATCOPHARMA",
        "GRANULES","STRIDES","GLAND","LAURUS","DIVI","IPCALAB","JBCHEPHARM","SEQUENT",
        # FMCG
        "HINDUNILVR","ITC","NESTLEIND","BRITANNIA","MARICO","DABUR","GODREJCP","EMAMILTD",
        "COLPAL","PGHH","GILLETTE","BATAINDIA","VBLLTD","RADICO","UNITDSPR","MCDOWELL-N",
        "UBL","VBL","JUBLFT","CCL",
        # Cement & Building Materials
        "ULTRACEMCO","SHREECEM","AMBUJACEM","ACC","RAMCOCEM","HEIDELBERG","JKCEMENT",
        "DALMIACEMENTB","BIRLACORPN","PRISMJOINTS","NUVOCO","STAR","JKPAPER",
        # Metals & Mining
        "TATASTEEL","JSWSTEEL","HINDALCO","VEDL","NMDC","SAIL","NATIONALUM","WELCORP",
        "JINDALSTEL","APL","RATNAMANI","TUBEINVEST","KALYANKJIL","THANGAMAYIL",
        # Oil, Gas & Power
        "RELIANCE","ONGC","BPCL","IOC","HINDPETRO","GAIL","MGL","IGL","GUJARATGAS",
        "PETRONET","TATAPOWER","ADANIGREEN","ADANITRANS","TORNTPOWER","CESC","NHPC",
        "SJVN","RECLTD","PFC","IRFC","POWERGRID","NTPC",
        # Capital Goods & Engineering
        "LT","SIEMENS","ABB","BHEL","HAL","BEL","BHEL","THERMAX","CUMMINSIND",
        "TIMKEN","SKF","SCHAEFFLER","ELGIEQUIP","GRINDWELL","CARBORUNDUM","AIAENG",
        "SWANENERGY","KIRLOSKAR","KENNAMETAL","KAYNES","ELIN","DDBL",
        # Real Estate & Infrastructure
        "DLF","GODREJPROP","SOBHA","OBEROIRLTY","PRESTIGE","PHOENIXLTD","BRIGADE",
        "MAHLIFE","KOLTEPATIL","PURAVANKARA","SUNTECK","RUSTOMJEE",
        # Telecom & Media
        "BHARTIARTL","IDEA","TATACOMM","HFCL","TEJAS","RAILTEL","INDIAMART",
        "JUSTDIAL","MAKEMYTRIP","POLICYBZR","PAYTM","ZOMATO","NYKAA","DELHIVERY",
        # Retail & Consumption
        "DMART","TRENT","SHOPERSTOP","RELAXO","BATAINDI","VMART","MANYAVAR",
        "SAPPHIRE","CAMPUS","METROBRAND","RAJESHEXPO",
        # Chemicals & Specialty
        "PIIND","SRF","DEEPAKNITR","NAVINFLUOR","AARTI","VINYLCHEM","SUDARSCHEM",
        "CLEAN","TATACHEM","GNFC","GSFC","CHAMBALFERT","COROMANDEL","RALLIS",
        "BALAMINES","FINEORG","ALKYLAMINE","NOCIL",
    ]


# Ticker suffix priority: NSE → BSE
_INDEX_TICKERS = {"NIFTY": "^NSEI", "SENSEX": "^BSESN", "BANKNIFTY": "^NSEBANK"}


# In-memory cache so resolve_ticker doesn't re-probe the same symbol twice
_ticker_cache: dict = {}


def resolve_ticker(symbol: str) -> Optional[str]:
    """
    Resolve an NSE/BSE symbol to a Yahoo Finance ticker.
    Tries: special map → .NS (NSE) → .BO (BSE)
    Results are cached in memory for the lifetime of the process.
    """
    sym = symbol.upper().strip()
    if sym in _ticker_cache:
        return _ticker_cache[sym]
    if sym in _INDEX_TICKERS:
        _ticker_cache[sym] = _INDEX_TICKERS[sym]
        return _INDEX_TICKERS[sym]
    for suffix in (".NS", ".BO"):
        ticker = f"{sym}{suffix}"
        try:
            h = yf.Ticker(ticker).history(period="5d")
            if not h.empty:
                _ticker_cache[sym] = ticker
                return ticker
        except Exception:
            pass
    _ticker_cache[sym] = None  # cache negative result too
    return None


def fetch_ohlcv(symbol: str, period: str = "2y") -> Optional[pd.DataFrame]:
    """
    Fetch OHLCV data for any NSE/BSE symbol.
    Auto-resolves the correct Yahoo Finance ticker.
    """
    try:
        ticker = resolve_ticker(symbol)
        if ticker is None:
            logger.debug(f"No valid ticker found for {symbol}")
            return None
        df = yf.download(ticker, period=period, interval="1d",
                         progress=False, auto_adjust=True)
        if df.empty:
            return None
        df.columns = [c.lower() if isinstance(c, str) else c[0].lower() for c in df.columns]
        df = df.dropna()
        return df
    except Exception as e:
        logger.debug(f"OHLCV fetch failed for {symbol}: {e}")
        return None


def fetch_ohlcv_batch(symbols: list[str], period: str = "2y") -> dict[str, pd.DataFrame]:
    """Batch download OHLCV with rate limiting."""
    results = {}
    for i, symbol in enumerate(symbols):
        df = fetch_ohlcv(symbol, period)
        if df is not None and len(df) > 50:
            results[symbol] = df
        if i % 10 == 9:
            time.sleep(1)  # Rate limit: pause every 10 requests
    logger.info(f"OHLCV batch: {len(results)}/{len(symbols)} successful")
    return results


# ─── Options Chain Feed ───────────────────────────────────────────────────────

def fetch_options_chain(symbol: str) -> Optional[dict]:
    """
    Fetch live options chain for a symbol from NSE public API.
    Returns raw chain data with OI, IV, PCR.
    """
    data = _nse.get("option-chain-equities", params={"symbol": symbol})
    return data


def parse_options_metrics(chain_data: dict) -> dict:
    """
    Extract PCR, unusual OI, and IV from raw options chain.
    Returns {pcr, oi_spike_score, iv_signal, total_call_oi, total_put_oi}
    """
    try:
        records = chain_data.get("records", {}).get("data", [])
        total_call_oi = sum(r.get("CE", {}).get("openInterest", 0) for r in records if r.get("CE"))
        total_put_oi = sum(r.get("PE", {}).get("openInterest", 0) for r in records if r.get("PE"))

        pcr = round(total_put_oi / total_call_oi, 2) if total_call_oi > 0 else 1.0

        # PCR signal: < 0.7 = bullish (more calls), > 1.3 = bearish
        pcr_signal = "bullish" if pcr < 0.7 else ("bearish" if pcr > 1.3 else "neutral")

        return {
            "pcr": pcr,
            "pcr_signal": pcr_signal,
            "total_call_oi": total_call_oi,
            "total_put_oi": total_put_oi,
            "oi_spike_score": min(20, int((total_call_oi + total_put_oi) / 1_000_000)),
        }
    except Exception as e:
        logger.error(f"Options parse error: {e}")
        return {"pcr": 1.0, "pcr_signal": "neutral", "total_call_oi": 0, "total_put_oi": 0, "oi_spike_score": 0}


# ─── Mock Data (for demo / when APIs are rate-limited) ───────────────────────

def get_mock_alert(symbol: str = "RELIANCE") -> dict:
    """Returns a realistic mock alert for demo purposes."""
    return {
        "symbol": symbol,
        "company": "Reliance Industries Ltd",
        "sector": "Energy",
        "pattern_type": "Cup and Handle Breakout",
        "pattern_score": 22,
        "catalyst_type": "Earnings Beat",
        "catalyst_summary": f"{symbol} Q3 results beat consensus estimate by 18%. Revenue growth at 14% YoY.",
        "catalyst_score": 28,
        "backtest": {
            "win_rate": 0.68,
            "bull_win_rate": 0.74,
            "bear_win_rate": 0.45,
            "avg_gain_pct": 11.2,
            "avg_loss_pct": -4.8,
            "n_occurrences": 17,
        },
        "options": {
            "pcr": 0.62,
            "pcr_signal": "bullish",
            "oi_spike_score": 18,
        },
        "insider_score": 8,
        "ssi": 79,
        "resistance_zone": [2950, 3020],
        "support_zone": [2780, 2830],
        "current_price": 2912.45,
        "price_change_pct": 2.34,
        "volume_ratio": 2.1,
        "timestamp": datetime.now().isoformat(),
        "ai_label": "AI-Generated Signal",
        "compliant": True,
    }
