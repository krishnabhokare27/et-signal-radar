# TradeVantage AI (ET Signal Radar) 🚀

An institutional-grade AI-powered stock screening dashboard that bridges the gap between retail trading and quantum-level analysis. Built for the ET AI Hackathon 2026.

TradeVantage AI scans the NSE universe (500+ stocks) in real-time, executing high-conviction pattern recognition to deliver actionable trading signals without violating SEBI regulations.

![TradeVantage AI Dashboard](dashboard_preview.png)

## 📌 Features

*   **Real-time Market Pulse:** Live tracking of Nifty 50, Bank Nifty, VIX, and major sectors using live `yfinance` data.
*   **Institutional Order Flow (FII/DII):** Displays live net buying/selling figures from institutional investors to gauge market sentiment via NSE public APIs.
*   **Signal Strength Index (SSI):** A proprietary 0-100 scoring system measuring the confluence of pattern quality, option chain data, catalyst activity, and insider trading.
*   **SEBI-Compliant Observation Zones:** Data-derived Entry, Target/Exit, and Stop zones based on quantitative backtesting (not financial advice).
*   **Directional AI Signals:** Identifies both Bullish (Upside) and Bearish (Downside) opportunities with calculated Risk/Reward ratios.
*   **Historical Backtesting Data:** Transparent win-rate statistics separated by bull and bear market conditions.

## 🛠️ Technology Stack

*   **Backend:** Python 3.12, FastAPI, Uvicorn
*   **Data Feeds:** `yfinance`, NSE Public APIs
*   **Quantitative Analysis:** `pandas`, `pandas-ta`
*   **Frontend:** Vanilla HTML/CSS/JS (Zero-build pipeline for max speed)
*   **UX/UI:** Glassmorphism design, dynamic CSS variables, real-time DOM updates

## 🚀 Getting Started

### Prerequisites
*   Python 3.12+
*   pip (Python package manager)

### Installation & Run

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/your-username/TradeVantage-AI.git
    cd TradeVantage-AI/et-signal-radar
    ```

2.  **Install dependencies:**
    ```bash
    pip install fastapi uvicorn yfinance pandas
    pip install pandas-ta --no-deps
    ```

3.  **Run the Backend API:**
    ```bash
    python -m uvicorn api:app --reload --port 8000
    ```

4.  **View the Dashboard:**
    Open your browser and navigate to `http://127.0.0.1:8000`

## 📁 Architecture Overview

*   `api.py`: FastAPI server, routing, and real-time endpoints (`/api/nifty/pulse`, `/api/stats`, `/api/stock/{symbol}/scan`).
*   `engine/`: Core logic for signal generation (`signal_index.py` handles the quantitative pipeline calculating SSI).
*   `data/`: Data ingestion layer (`feeds.py` connects to `yfinance` and NSE APIs, managing the top 500 Nifty stocks).
*   `static/`: Frontend assets (`dashboard.html` features the dynamic UI, Observation Zones, and Glassmorphism styling).

## ⚠️ Disclaimer
**AI-GENERATED SIGNAL · NOT INVESTMENT ADVICE**
This project provides AI-generated market data intelligence for educational and hackathon demonstration purposes. It does not constitute a buy/sell recommendation. Always consult a SEBI-registered Research Analyst before investing. Past pattern performance does not guarantee future results.
