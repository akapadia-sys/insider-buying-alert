"""
Daily Insider Open-Market Purchase Alert Agent
================================================
Scrapes OpenInsider.com for recent sizeable open-market purchases
by company insiders and emails a daily digest via Gmail.

OpenInsider aggregates SEC Form 4 data into clean HTML tables,
which is far more reliable than parsing raw EDGAR XML.

Setup:
  1. pip install requests beautifulsoup4
  2. Set environment variables (see CONFIG section)
  3. Run daily via cron or GitHub Actions
"""

import os
import sys
import time
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta, timezone
from typing import Optional
import requests
from bs4 import BeautifulSoup

# ──────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────
GMAIL_ADDRESS    = os.environ.get("GMAIL_ADDRESS", "you@gmail.com")
GMAIL_APP_PASS   = os.environ.get("GMAIL_APP_PASSWORD", "")
RECIPIENT_EMAIL  = os.environ.get("RECIPIENT_EMAIL", GMAIL_ADDRESS)
MIN_PURCHASE_USD = float(os.environ.get("MIN_PURCHASE_USD", "250000"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
})


# ──────────────────────────────────────────────
# 1. FETCH INSIDER PURCHASES FROM OPENINSIDER
# ──────────────────────────────────────────────
def fetch_insider_purchases(lookback_days: int = 3) -> list[dict]:
    """
    Scrapes OpenInsider.com for recent open-market purchases.
    Uses their screener with filters for:
      - Transaction type: P (Purchase)
      - Min transaction value
      - Recent filing date
    """
    now = datetime.now(timezone.utc)
    start_date = (now - timedelta(days=lookback_days)).strftime("%m/%d/%Y")
    end_date = now.strftime("%m/%d/%Y")

    # OpenInsider screener URL for open-market purchases
    # cnt=500 gets up to 500 results
    url = (
        f"http://openinsider.com/screener?"
        f"s=&o=&pl=&ph=&st=1&td=0&tdr=&fdlyl=&fdlyh="
        f"&dtefrom={start_date}&dteto={end_date}"
        f"&xp=1&vl={int(MIN_PURCHASE_USD)}&vh="
        f"&ocl=&och=&session=1&sic1=-1&sicl=100&sich=9999"
        f"&grp=0&nfl=&nfh=&nil=&nih=&nol=&noh=&v2l=&v2h="
        f"&oc2l=&oc2h=&sortcol=0&cnt=500&page=1"
    )

    log.info(f"Fetching purchases from OpenInsider (last {lookback_days} days, >= ${MIN_PURCHASE_USD:,.0f})...")
    log.info(f"URL: {url}")

    try:
        resp = SESSION.get(url, timeout=30)
        if resp.status_code != 200:
            log.error(f"OpenInsider returned HTTP {resp.status_code}")
            return []
    except Exception as e:
        log.error(f"Failed to fetch OpenInsider: {e}")
        return []

    return parse_openinsider_table(resp.text)


def parse_openinsider_table(html: str) -> list[dict]:
    """
    Parses the OpenInsider screener results table.
    Columns (typical order):
      X, Filing Date, Trade Date, Ticker, Insider Name, Title,
      Trade Type, Price, Qty, Owned, Delta Own, Value, ...
    """
    soup = BeautifulSoup(html, "html.parser")

    # Find the main results table — it's the one with class "tinytable"
    table = soup.find("table", class_="tinytable")
    if not table:
        # Try finding any table with insider data
        tables = soup.find_all("table")
        for t in tables:
            if t.find("a", href=lambda h: h and "/screener?" in h if h else False):
                continue
            headers = [th.get_text(strip=True).lower() for th in t.find_all("th")]
            if any("ticker" in h for h in headers):
                table = t
                break

    if not table:
        log.error("Could not find results table on OpenInsider page")
        log.info(f"Page length: {len(html)} chars")
        # Log first 500 chars for debugging
        log.info(f"Page preview: {html[:500]}")
        return []

    # Parse header row to determine column indices
    header_row = table.find("tr")
    if not header_row:
        log.error("No header row found in table")
        return []

    headers = []
    for th in header_row.find_all(["th", "td"]):
        headers.append(th.get_text(strip=True).lower())

    log.info(f"Table headers: {headers}")

    # Map column names to indices (OpenInsider uses varying header names)
    col_map = {}
    for i, h in enumerate(headers):
        h_clean = h.replace("\xa0", " ").strip().lower()
        if "filing" in h_clean and "date" in h_clean:
            col_map["filing_date"] = i
        elif "trade" in h_clean and "date" in h_clean:
            col_map["trade_date"] = i
        elif "ticker" in h_clean:
            col_map["ticker"] = i
        elif "insider" in h_clean and "name" in h_clean:
            col_map["insider_name"] = i
        elif "title" in h_clean:
            col_map["title"] = i
        elif "price" in h_clean:
            col_map["price"] = i
        elif h_clean in ("qty", "shares"):
            col_map["qty"] = i
        elif "value" in h_clean:
            col_map["value"] = i
        elif "company" in h_clean:
            col_map["company"] = i

    log.info(f"Column mapping: {col_map}")

    # Parse data rows
    purchases = []
    rows = table.find_all("tr")[1:]  # skip header

    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 5:
            continue

        try:
            # Extract ticker — often in a link
            ticker_idx = col_map.get("ticker", 3)
            ticker_cell = cells[ticker_idx] if ticker_idx < len(cells) else None
            if ticker_cell:
                link = ticker_cell.find("a")
                ticker = link.get_text(strip=True) if link else ticker_cell.get_text(strip=True)
            else:
                continue

            # Extract other fields
            insider_name = _cell_text(cells, col_map.get("insider_name", 4))
            title = _cell_text(cells, col_map.get("title", 5))
            trade_date = _cell_text(cells, col_map.get("trade_date", 2))
            filing_date = _cell_text(cells, col_map.get("filing_date", 1))

            # Price and value — strip $, commas, +, etc.
            price_str = _cell_text(cells, col_map.get("price", 7))
            value_str = _cell_text(cells, col_map.get("value", 11))
            qty_str = _cell_text(cells, col_map.get("qty", 8))

            price = _parse_number(price_str)
            value = _parse_number(value_str)
            qty = _parse_number(qty_str)

            # If we have qty and price but not value, calculate it
            if value == 0 and qty > 0 and price > 0:
                value = qty * price

            if value < MIN_PURCHASE_USD:
                continue

            # Get company name if available
            company = _cell_text(cells, col_map.get("company", -1))

            purchases.append({
                "ticker": ticker.upper().strip(),
                "issuer_name": company or ticker,
                "owner_name": insider_name,
                "role": title,
                "trade_date": trade_date,
                "filing_date": filing_date,
                "total_shares": qty,
                "avg_price": price,
                "total_invested": value,
                "sec_url": f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&company={ticker}&CIK=&type=4&dateb=&owner=include&count=10&search_text=&action=getcompany",
            })

        except Exception as e:
            log.debug(f"Error parsing row: {e}")
            continue

    log.info(f"Parsed {len(purchases)} qualifying purchases from OpenInsider")
    return purchases


def _cell_text(cells: list, idx: int) -> str:
    """Safely get text from a table cell by index."""
    if idx < 0 or idx >= len(cells):
        return ""
    return cells[idx].get_text(strip=True)


def _parse_number(s: str) -> float:
    """Parse a number string, removing $, commas, +, and other formatting."""
    if not s:
        return 0
    cleaned = s.replace("$", "").replace(",", "").replace("+", "").replace(" ", "").strip()
    try:
        return abs(float(cleaned))
    except ValueError:
        return 0


# ──────────────────────────────────────────────
# 2. BUILD + SEND EMAIL DIGEST
# ──────────────────────────────────────────────
def build_email_html(trades: list[dict], date_str: str) -> str:
    """Build a clean HTML email digest."""
    if not trades:
        return f"""
        <html><body style="font-family: Arial, sans-serif; color: #333;">
        <h2 style="color: #1a5276;">Insider Buying Digest &mdash; {date_str}</h2>
        <p>No sizeable open-market insider purchases were filed recently
        (min threshold: ${MIN_PURCHASE_USD:,.0f}).</p>
        </body></html>
        """

    trades.sort(key=lambda t: t["total_invested"], reverse=True)

    rows = ""
    for t in trades:
        ticker_link = f"https://finance.yahoo.com/quote/{t['ticker']}"
        rows += f"""
        <tr style="border-bottom: 1px solid #eee;">
            <td style="padding: 10px;">
                <strong><a href="{ticker_link}"
                    style="color: #2980b9; text-decoration: none;">{t['ticker']}</a></strong><br>
                <span style="font-size: 12px; color: #777;">{t['issuer_name']}</span></td>
            <td style="padding: 10px;">{t['owner_name']}<br>
                <span style="font-size: 12px; color: #777;">{t['role']}</span></td>
            <td style="padding: 10px; text-align: right;">{t['total_shares']:,.0f}</td>
            <td style="padding: 10px; text-align: right;">${t['avg_price']:,.2f}</td>
            <td style="padding: 10px; text-align: right; font-weight: bold; color: #27ae60;">
                ${t['total_invested']:,.0f}</td>
            <td style="padding: 10px; text-align: center; font-size: 12px; color: #777;">
                {t.get('trade_date', '')}</td>
        </tr>
        """

    total_all = sum(t["total_invested"] for t in trades)

    return f"""
    <html><body style="font-family: Arial, sans-serif; color: #333; max-width: 800px; margin: auto;">
    <h2 style="color: #1a5276; border-bottom: 3px solid #2980b9; padding-bottom: 8px;">
        Insider Buying Digest &mdash; {date_str}</h2>
    <p style="color: #555;">
        <strong>{len(trades)}</strong> sizeable open-market purchase(s) totaling
        <strong style="color: #27ae60;">${total_all:,.0f}</strong>
        &nbsp;(threshold: &ge;${MIN_PURCHASE_USD:,.0f})
    </p>
    <table style="width: 100%; border-collapse: collapse; font-size: 14px;">
        <thead>
            <tr style="background: #f8f9fa; border-bottom: 2px solid #dee2e6;">
                <th style="padding: 10px; text-align: left;">Ticker</th>
                <th style="padding: 10px; text-align: left;">Insider</th>
                <th style="padding: 10px; text-align: right;">Shares</th>
                <th style="padding: 10px; text-align: right;">Price</th>
                <th style="padding: 10px; text-align: right;">Total Value</th>
                <th style="padding: 10px; text-align: center;">Trade Date</th>
            </tr>
        </thead>
        <tbody>{rows}</tbody>
    </table>
    <p style="font-size: 11px; color: #aaa; margin-top: 20px;">
        Source: SEC Form 4 filings via OpenInsider.com. Open-market purchases only.
        This is not investment advice.</p>
    </body></html>
    """


def send_email(html_body: str, date_str: str):
    """Send the digest email via Gmail SMTP."""
    if not GMAIL_APP_PASS:
        log.error("GMAIL_APP_PASSWORD not set. Cannot send email.")
        sys.exit(1)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"Insider Buying Alert — {date_str}"
    msg["From"] = GMAIL_ADDRESS
    msg["To"] = RECIPIENT_EMAIL
    msg.attach(MIMEText(html_body, "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_ADDRESS, GMAIL_APP_PASS)
            server.sendmail(GMAIL_ADDRESS, RECIPIENT_EMAIL, msg.as_string())
        log.info(f"Email sent to {RECIPIENT_EMAIL}")
    except Exception as e:
        log.error(f"Failed to send email: {e}")
        raise


# ──────────────────────────────────────────────
# 3. MAIN
# ──────────────────────────────────────────────
def main():
    log.info("=== Insider Buying Alert Agent ===")
    log.info(f"Min purchase threshold: ${MIN_PURCHASE_USD:,.0f}")

    purchases = fetch_insider_purchases(lookback_days=3)

    # Deduplicate by (ticker, owner_name)
    seen = set()
    unique = []
    for t in purchases:
        key = (t["ticker"], t["owner_name"])
        if key not in seen:
            seen.add(key)
            unique.append(t)
    purchases = unique

    log.info(f"Total: {len(purchases)} unique insider purchases >= ${MIN_PURCHASE_USD:,.0f}")

    date_str = datetime.now(timezone.utc).strftime("%B %d, %Y")
    html = build_email_html(purchases, date_str)
    send_email(html, date_str)
    log.info("Done!")


if __name__ == "__main__":
    main()
