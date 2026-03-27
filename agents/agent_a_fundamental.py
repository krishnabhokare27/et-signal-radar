"""
ET Signal Radar — Agent A: Fundamental Parser
Uses Groq (free LLaMA-3) to parse BSE filings and extract catalysts.
Returns catalyst type + magnitude score 0-30.
"""

import os
import json
import logging
import pdfplumber
import requests
from io import BytesIO
from typing import Optional

logger = logging.getLogger(__name__)

CATALYST_SCORES = {
    "Earnings Beat >15%": 30,
    "Earnings Beat 5-15%": 22,
    "Buyback Announcement": 25,
    "Promoter/Insider Buying": 20,
    "Major Contract Win": 22,
    "Capacity Expansion": 18,
    "Debt Reduction": 16,
    "Dividend Announcement": 14,
    "Management Change": 12,
    "Regulatory Approval": 20,
    "Merger/Acquisition": 18,
    "Routine Filing": 4,
    "Unknown": 5,
}

PROMPT_TEMPLATE = """You are a financial analyst parsing an Indian stock exchange filing.
Extract the following from the text and return ONLY valid JSON, no other text:

{{
  "catalyst_type": "<one of: Earnings Beat >15%, Earnings Beat 5-15%, Buyback Announcement, Promoter/Insider Buying, Major Contract Win, Capacity Expansion, Debt Reduction, Dividend Announcement, Management Change, Regulatory Approval, Merger/Acquisition, Routine Filing, Unknown>",
  "sentiment": "<bullish|bearish|neutral>",
  "magnitude": "<high|medium|low>",
  "summary": "<one sentence plain English, max 15 words, no jargon>",
  "key_number": "<most important number mentioned, e.g. '18% revenue growth' or 'N/A'>"
}}

Filing text (truncated to 800 chars):
{filing_text}"""


def parse_filing(filing: dict) -> dict:
    """
    Main entry. Takes a BSE announcement dict, fetches PDF, extracts catalyst.
    Returns {catalyst_type, sentiment, catalyst_score, summary, key_number}
    """
    # Try to extract text from PDF
    filing_text = _extract_pdf_text(filing.get("pdf_url", ""))

    if not filing_text:
        # Fall back to headline parsing
        filing_text = filing.get("headline", "") + " " + filing.get("category", "")

    if not filing_text.strip():
        return _default_result()

    # Parse with LLM
    result = _call_groq(filing_text)

    # Add score based on catalyst type
    result["catalyst_score"] = CATALYST_SCORES.get(result.get("catalyst_type", "Unknown"), 5)
    return result


def parse_bulk_deal(deal: dict) -> dict:
    """
    Parse bulk/block deal for insider buying signal.
    Returns score 0-10.
    """
    buy_sell = deal.get("buy_sell", "").upper()
    client = deal.get("client", "").lower()
    quantity = deal.get("quantity", 0)

    # Promoter/insider buying is most bullish
    is_promoter = any(kw in client for kw in ["promoter", "director", "founder", "management", "ltd"])
    is_institutional = any(kw in client for kw in ["fund", "lic", "mutual", "insurance", "fii", "dii"])

    if buy_sell == "BUY":
        if is_promoter:
            score = 10
        elif is_institutional:
            score = 7
        else:
            score = 5
        sentiment = "bullish"
    else:
        score = 0
        sentiment = "bearish"

    return {
        "insider_score": score,
        "insider_sentiment": sentiment,
        "insider_detail": f"{'Promoter' if is_promoter else 'Institutional' if is_institutional else 'Client'} {'buying' if buy_sell == 'BUY' else 'selling'} {quantity:,} shares",
    }


def _extract_pdf_text(pdf_url: str, max_chars: int = 1500) -> str:
    """Download BSE PDF and extract text."""
    if not pdf_url or "AttachLive" not in pdf_url:
        return ""
    try:
        headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.bseindia.com"}
        resp = requests.get(pdf_url, headers=headers, timeout=15)
        resp.raise_for_status()

        with pdfplumber.open(BytesIO(resp.content)) as pdf:
            text = ""
            for page in pdf.pages[:3]:  # First 3 pages only
                page_text = page.extract_text() or ""
                text += page_text + "\n"
                if len(text) >= max_chars:
                    break
        return text[:max_chars].strip()
    except Exception as e:
        logger.debug(f"PDF extraction failed: {e}")
        return ""


def _call_groq(filing_text: str) -> dict:
    """Call Groq API (free LLaMA-3) to parse filing."""
    api_key = os.getenv("GROQ_API_KEY", "")
    if not api_key:
        logger.warning("GROQ_API_KEY not set — using keyword-based fallback")
        return _keyword_fallback(filing_text)

    try:
        import httpx
        prompt = PROMPT_TEMPLATE.format(filing_text=filing_text[:800])
        response = httpx.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={
                "model": "llama-3.1-8b-instant",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 300,
                "temperature": 0.1,
            },
            timeout=15,
        )
        content = response.json()["choices"][0]["message"]["content"].strip()

        # Parse JSON response
        if "```" in content:
            content = content.split("```")[1].replace("json", "").strip()
        result = json.loads(content)
        return result

    except Exception as e:
        logger.error(f"Groq API error: {e}")
        return _keyword_fallback(filing_text)


def _keyword_fallback(text: str) -> dict:
    """Rule-based fallback when LLM is unavailable."""
    text_lower = text.lower()

    if any(kw in text_lower for kw in ["earnings", "profit", "revenue", "results", "quarter"]):
        if any(kw in text_lower for kw in ["beat", "exceed", "higher than", "above estimate"]):
            return {"catalyst_type": "Earnings Beat 5-15%", "sentiment": "bullish",
                    "magnitude": "high", "summary": "Results beat expectations", "key_number": "N/A"}
        return {"catalyst_type": "Routine Filing", "sentiment": "neutral",
                "magnitude": "low", "summary": "Quarterly results filed", "key_number": "N/A"}

    if any(kw in text_lower for kw in ["buyback", "buy back", "repurchase"]):
        return {"catalyst_type": "Buyback Announcement", "sentiment": "bullish",
                "magnitude": "high", "summary": "Buyback announced", "key_number": "N/A"}

    if any(kw in text_lower for kw in ["contract", "order", "agreement", "win"]):
        return {"catalyst_type": "Major Contract Win", "sentiment": "bullish",
                "magnitude": "medium", "summary": "Major order or contract secured", "key_number": "N/A"}

    if any(kw in text_lower for kw in ["dividend"]):
        return {"catalyst_type": "Dividend Announcement", "sentiment": "bullish",
                "magnitude": "low", "summary": "Dividend declared", "key_number": "N/A"}

    return {"catalyst_type": "Routine Filing", "sentiment": "neutral",
            "magnitude": "low", "summary": "Corporate filing submitted", "key_number": "N/A"}


def _default_result() -> dict:
    return {
        "catalyst_type": "Unknown",
        "sentiment": "neutral",
        "magnitude": "low",
        "summary": "Filing received — content not parseable",
        "key_number": "N/A",
        "catalyst_score": 5,
    }
