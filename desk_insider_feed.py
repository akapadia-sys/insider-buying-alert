"""
Desk insider feed
=================
For each ticker in DESK_TICKERS, pulls every Form 4 transaction filed in the
last LOOKBACK_DAYS days from OpenInsider (purchases, sales, tax withholding,
grants, option exercises, gifts...) and emails one plain-text digest to the
research desk inbox. Sends a digest every run, even when nothing was filed, so
the desk can tell the feed ran.

Includes the per-share transaction price as filed (allowed in the desk's internal notes).

Env:
  GMAIL_ADDRESS, GMAIL_APP_PASSWORD  - same secrets as insider_alert.py
  DESK_RECIPIENT                     - desk inbox
  DESK_TICKERS                       - space-separated tickers
  LOOKBACK_DAYS                      - filing-date window (default 3)
"""

import os
import sys
import time
import smtplib
import logging
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText

import requests
from bs4 import BeautifulSoup

GMAIL_ADDRESS = os.environ.get("GMAIL_ADDRESS", "")
GMAIL_APP_PASS = os.environ.get("GMAIL_APP_PASSWORD", "")
DESK_RECIPIENT = os.environ.get("DESK_RECIPIENT", "")
DESK_TICKERS = os.environ.get("DESK_TICKERS", "").split()
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "3"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
})

# OpenInsider screener: one ticker, filing date within N days, every transaction type.
SCREENER = (
    "http://openinsider.com/screener?s={ticker}&o=&pl=&ph=&ll=&lh=&fd={days}&fdr=&td=0&tdr="
    "&fdlyl=&fdlyh=&daysago=&xp=1&xs=1&xa=1&xd=1&xg=1&xf=1&xm=1&xx=1&xc=1&xw=1"
    "&vl=&vh=&ocl=&och=&sic1=-1&sicl=100&sich=9999&grp=0&nfl=&nfh=&nil=&nih=&nol=&noh="
    "&v2l=&v2h=&oc2l=&oc2h=&sortcol=0&cnt=100&page=1"
)

CODE_LABELS = {
    "P": "open-market purchase",
    "S": "open-market sale",
    "F": "tax withholding",
    "A": "grant/award",
    "D": "disposition to issuer",
    "G": "gift",
    "M": "option exercise",
    "X": "option exercise",
    "C": "conversion",
    "W": "will/inheritance",
}


def cell(cells, idx):
    if idx is None or idx < 0 or idx >= len(cells):
        return ""
    return cells[idx].get_text(" ", strip=True)


def fetch_ticker(ticker: str) -> tuple[list[dict], str]:
    """Returns (rows, status). status is 'ok' or an error string."""
    url = SCREENER.format(ticker=ticker, days=LOOKBACK_DAYS)
    try:
        resp = SESSION.get(url, timeout=30)
    except Exception as e:
        return [], f"request failed: {e}"
    if resp.status_code != 200:
        return [], f"HTTP {resp.status_code}"

    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table", class_="tinytable")
    if not table:
        # OpenInsider shows no table when there are no matches.
        return [], "ok"

    head = table.find("thead") or table
    headers = [th.get_text(strip=True).lower().replace("\xa0", " ") for th in head.find_all("th")]
    col = {}
    for i, h in enumerate(headers):
        if "filing" in h and "date" in h:
            col["filed"] = i
        elif "trade" in h and "date" in h:
            col["traded"] = i
        elif h == "ticker":
            col["ticker"] = i
        elif "insider" in h:
            col["insider"] = i
        elif h == "title":
            col["title"] = i
        elif "trade" in h and "type" in h:
            col["type"] = i
        elif h == "price":
            col["price"] = i
        elif h == "qty":
            col["qty"] = i
        elif h == "owned":
            col["owned"] = i
        elif h == "value":
            col["value"] = i
    if "insider" not in col or "type" not in col:
        return [], f"unexpected table layout: {headers}"

    body = table.find("tbody") or table
    rows = []
    for tr in body.find_all("tr"):
        cells = tr.find_all("td")
        if len(cells) < len(headers) - 2:
            continue
        rows.append({
            "filed": cell(cells, col.get("filed")),
            "traded": cell(cells, col.get("traded")),
            "insider": cell(cells, col.get("insider")),
            "title": cell(cells, col.get("title")),
            "type": cell(cells, col.get("type")),
            "price": cell(cells, col.get("price")),
            "qty": cell(cells, col.get("qty")),
            "owned": cell(cells, col.get("owned")),
            "value": cell(cells, col.get("value")),
        })
    return rows, "ok"


def label(trade_type: str) -> str:
    code = trade_type.strip()[:1].upper()
    lab = CODE_LABELS.get(code, "other")
    if "+OE" in trade_type.upper():
        lab += " (with option exercise)"
    return f"{code} – {lab}" if code else "unknown"


def build_body(results: dict, date_str: str) -> tuple[str, int]:
    total = sum(len(r) for r, _ in results.values())
    lines = [
        f"Desk insider feed – {date_str}",
        f"Form 4 filings in the last {LOOKBACK_DAYS} days for {len(results)} tickers. "
        f"Source: SEC Form 4 via OpenInsider.",
        f"Total filings: {total}",
        "",
    ]
    for ticker, (rows, status) in results.items():
        if status != "ok":
            lines.append(f"{ticker}: NOT CHECKED ({status})")
            continue
        if not rows:
            continue
        lines.append(f"{ticker}:")
        for r in rows:
            lines.append(
                f"  - filed {r['filed']} | traded {r['traded']} | {r['insider']} ({r['title']}) | "
                f"{label(r['type'])} | shares {r['qty']} at {r['price']} | value {r['value']} | owned after {r['owned']}"
            )
        lines.append("")
    quiet = [t for t, (r, s) in results.items() if s == "ok" and not r]
    if quiet:
        lines.append("No filings: " + ", ".join(quiet))
    return "\n".join(lines), total


def send(subject: str, body: str):
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = GMAIL_ADDRESS
    msg["To"] = DESK_RECIPIENT
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(GMAIL_ADDRESS, GMAIL_APP_PASS)
        server.sendmail(GMAIL_ADDRESS, DESK_RECIPIENT, msg.as_string())
    log.info(f"Sent to {DESK_RECIPIENT}")


def main():
    if not (GMAIL_ADDRESS and GMAIL_APP_PASS and DESK_RECIPIENT and DESK_TICKERS):
        log.error("Missing GMAIL_ADDRESS, GMAIL_APP_PASSWORD, DESK_RECIPIENT or DESK_TICKERS")
        sys.exit(1)
    results = {}
    for t in DESK_TICKERS:
        results[t] = fetch_ticker(t)
        log.info(f"{t}: {results[t][1]}, {len(results[t][0])} rows")
        time.sleep(1.5)  # be polite to OpenInsider
    now_et = datetime.now(timezone(timedelta(hours=-5)))
    date_str = now_et.strftime("%Y-%m-%d")
    body, total = build_body(results, date_str)
    failed = sum(1 for _, s in results.values() if s != "ok")
    subject = f"Desk insider feed {date_str} – {total} filings" + (f", {failed} not checked" if failed else "")
    send(subject, body)


if __name__ == "__main__":
    main()
