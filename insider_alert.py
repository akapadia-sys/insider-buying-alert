"""
Daily Insider Open-Market Purchase Alert Agent
================================================
Scrapes OpenInsider.com's latest insider purchases page,
filters for sizeable open-market buys, and emails a daily digest.

Setup:
  1. pip install requests beautifulsoup4
  2. Set environment variables (see CONFIG section)
  3. Run daily via cron or GitHub Actions
"""

import os
import sys
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta, timezone
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
# 1. FETCH + PARSE OPENINSIDER DATA
# ──────────────────────────────────────────────
def fetch_insider_purchases(lookback_days: int = 3) -> list[dict]:
    """
    Fetches the OpenInsider "latest insider purchases" page.
    This is a known, stable URL that shows the most recent
    open-market purchases filed with the SEC.
    
    URL: http://openinsider.com/latest-insider-purchases
    """
    # This page shows the ~100 most recent insider purchases
    # No finicky screener parameters needed
    urls_to_try = [
        "http://openinsider.com/latest-insider-purchases",
        "http://openinsider.com/screener?s=&o=&pl=&ph=&st=1&td=7&tdr=&fdlyl=&fdlyh=&dtefrom=&dteto=&xp=1&vl=&vh=&ocl=&och=&session=1&sic1=-1&sicl=100&sich=9999&grp=0&nfl=&nfh=&nil=&nih=&nol=&noh=&v2l=&v2h=&oc2l=&oc2h=&sortcol=0&cnt=500&page=1",
    ]

    for url in urls_to_try:
        log.info(f"Fetching: {url}")
        try:
            resp = SESSION.get(url, timeout=30)
            if resp.status_code != 200:
                log.warning(f"HTTP {resp.status_code} from {url}")
                continue
        except Exception as e:
            log.warning(f"Request failed for {url}: {e}")
            continue

        purchases = parse_openinsider_table(resp.text)
        if purchases:
            return purchases
        log.warning(f"No purchases parsed from {url}, trying next...")

    log.error("All sources failed")
    return []


def parse_openinsider_table(html: str) -> list[dict]:
    """
    Parses OpenInsider HTML for the insider transactions table.
    
    Known columns (with non-breaking spaces in headers):
      X, Filing Date, Trade Date, Ticker, Company Name, Insider Name,
      Title, Trade Type, Price, Qty, Owned, ΔOwn, Value, 1d, 1w, 1m, 6m
    """
    soup = BeautifulSoup(html, "html.parser")

    # Find the main results table — class "tinytable"
    table = soup.find("table", class_="tinytable")

    if not table:
        # Fallback: find any table with "Ticker" in a header
        for t in soup.find_all("table"):
            header_text = " ".join(th.get_text() for th in t.find_all("th"))
            if "Ticker" in header_text or "ticker" in header_text.lower():
                table = t
                break

    if not table:
        log.error("Could not find data table on page")
        # Log some page content for debugging
        text = soup.get_text()[:1000]
        log.info(f"Page text preview: {text}")
        return []

    # Get all rows
    rows = table.find_all("tr")
    if len(rows) < 2:
        log.error("Table has fewer than 2 rows")
        return []

    # Parse headers
    header_cells = rows[0].find_all(["th", "td"])
    headers = [c.get_text(strip=True).lower().replace("\xa0", " ") for c in header_cells]
    log.info(f"Headers ({len(headers)}): {headers}")

    # Build column index map
    col = {}
    for i, h in enumerate(headers):
        if "filing" in h and "date" in h:
            col["filing_date"] = i
        elif "trade" in h and "date" in h:
            col["trade_date"] = i
        elif h == "ticker":
            col["ticker"] = i
        elif "company" in h:
            col["company"] = i
        elif "insider" in h:
            col["insider"] = i
        elif h == "title":
            col["title"] = i
        elif "trade" in h and "type" in h:
            col["trade_type"] = i
        elif h == "price":
            col["price"] = i
        elif h in ("qty", "shares"):
            col["qty"] = i
        elif h == "value":
            col["value"] = i

    log.info(f"Column map: {col}")

    # Verify we have the essential columns
    required = ["ticker", "insider", "price", "value"]
    missing = [r for r in required if r not in col]
    if missing:
        log.error(f"Missing required columns: {missing}")
        log.info(f"Available: {list(col.keys())}")
        return []

    # Parse data rows
    purchases = []
    skipped_value = 0
    skipped_type = 0

    for row in rows[1:]:
        cells = row.find_all("td")
        if len(cells) < len(headers) - 2:  # allow some slack
            continue

        try:
            ticker = _cell_text(cells, col["ticker"]).upper().strip()
            if not ticker:
                continue

            # Check trade type if available — we only want "P - Purchase"
            if "trade_type" in col:
                trade_type = _cell_text(cells, col["trade_type"]).upper().strip()
                if trade_type and "P" not in trade_type:
                    skipped_type += 1
                    continue

            insider = _cell_text(cells, col["insider"])
            title = _cell_text(cells, col.get("title", -1))
            company = _cell_text(cells, col.get("company", -1))
            filing_date = _cell_text(cells, col.get("filing_date", -1))
            trade_date = _cell_text(cells, col.get("trade_date", -1))
            price = _parse_number(_cell_text(cells, col["price"]))
            qty = _parse_number(_cell_text(cells, col.get("qty", -1)))
            value = _parse_number(_cell_text(cells, col["value"]))

            # Calculate value if missing
            if value == 0 and qty > 0 and price > 0:
                value = qty * price

            # Filter by minimum value
            if value < MIN_PURCHASE_USD:
                skipped_value += 1
                continue

            purchases.append({
                "ticker": ticker,
                "issuer_name": company or ticker,
                "owner_name": insider,
                "role": title,
                "trade_date": trade_date,
                "filing_date": filing_date,
                "total_shares": qty,
                "avg_price": price,
                "total_invested": value,
            })

        except Exception as e:
            log.debug(f"Error parsing row: {e}")
            continue

    log.info(f"Parsed {len(purchases)} purchases >= ${MIN_PURCHASE_USD:,.0f} "
             f"(skipped {skipped_value} below threshold, {skipped_type} non-purchase)")
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
# 2. EMAIL
# ──────────────────────────────────────────────
def build_email_html(trades: list[dict], date_str: str) -> str:
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
                {t.get('filing_date', '')}</td>
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
                <th style="padding: 10px; text-align: center;">Filed</th>
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
    if not GMAIL_APP_PASS:
        log.error("GMAIL_APP_PASSWORD not set.")
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

    # Filter: only keep trades filed in the last 3 days
    cutoff = datetime.now(timezone.utc) - timedelta(days=3)
    filtered = []
    for t in purchases:
        fd = t.get("filing_date", "")
        try:
            filed_dt = datetime.strptime(fd, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            if filed_dt >= cutoff:
                filtered.append(t)
            else:
                log.info(f"  Skipping (filed {fd}): {t['ticker']} | {t['owner_name']} | ${t['total_invested']:,.0f}")
        except ValueError:
            # Can't parse date — log it and include anyway
            log.warning(f"  Unparseable filing date '{fd}' for {t['ticker']} | {t['owner_name']} — including")
            filtered.append(t)
    
    log.info(f"After date filter: {len(filtered)} of {len(purchases)} purchases are from the last 3 days")
    purchases = filtered

    # Deduplicate
    seen = set()
    unique = []
    for t in purchases:
        key = (t["ticker"], t["owner_name"])
        if key not in seen:
            seen.add(key)
            unique.append(t)
    purchases = unique

    log.info(f"Final: {len(purchases)} unique purchases >= ${MIN_PURCHASE_USD:,.0f}")

    # Log each one for debugging
    for t in purchases:
        log.info(f"  -> {t['ticker']} | {t['owner_name']} | {t['role']} | "
                 f"${t['total_invested']:,.0f} | filed {t.get('filing_date','?')}")

    date_str = datetime.now(timezone.utc).strftime("%B %d, %Y")
    html = build_email_html(purchases, date_str)
    send_email(html, date_str)
    log.info("Done!")


if __name__ == "__main__":
    main()
