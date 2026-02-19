"""
Daily Insider Open-Market Purchase Alert Agent
================================================
Pulls SEC EDGAR Form 4 filings, parses for open-market purchases
by insiders, and emails a daily digest via Gmail.

Data flow:
  1. Use EDGAR company submissions API to get recent Form 4 accessions
  2. Fetch each filing's index page to find the XML document
  3. Parse the XML for transaction code "P" (open-market purchase)
  4. Filter by dollar threshold, build HTML email, send via Gmail

Setup:
  1. pip install requests
  2. Set environment variables (see CONFIG section)
  3. Run daily via cron or GitHub Actions
"""

import os
import sys
import re
import json
import time
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta, timezone
from xml.etree import ElementTree as ET
from typing import Optional
import requests

# ──────────────────────────────────────────────
# CONFIG — set via environment variables
# ──────────────────────────────────────────────
GMAIL_ADDRESS    = os.environ.get("GMAIL_ADDRESS", "you@gmail.com")
GMAIL_APP_PASS   = os.environ.get("GMAIL_APP_PASSWORD", "")
RECIPIENT_EMAIL  = os.environ.get("RECIPIENT_EMAIL", GMAIL_ADDRESS)
MIN_PURCHASE_USD = float(os.environ.get("MIN_PURCHASE_USD", "250000"))

# IMPORTANT: SEC requires a real name + email in the User-Agent.
# Format: "CompanyOrName AdminEmail"   e.g. "MyFirm john@example.com"
EDGAR_USER_AGENT = os.environ.get("EDGAR_USER_AGENT", "InsiderAlert admin@example.com")

REQUEST_DELAY = 0.12  # SEC rate limit: max 10 req/sec

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": EDGAR_USER_AGENT,
    "Accept-Encoding": "gzip, deflate",
    "Accept": "application/json, text/html, application/xml, */*",
})


# ──────────────────────────────────────────────
# 1. FETCH RECENT FORM 4 FILINGS
#    Using EDGAR full-text search (EFTS) API
#    Docs: https://efts.sec.gov/LATEST/
# ──────────────────────────────────────────────
def fetch_form4_filings(lookback_days: int = 3) -> list[dict]:
    """
    Uses EDGAR's EFTS full-text search API to find recent Form 4 filings.
    Endpoint: https://efts.sec.gov/LATEST/search-index
    Returns list of dicts with accession_no, cik, entity_name, file_date.
    """
    now = datetime.now(timezone.utc)
    start_date = (now - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    end_date = now.strftime("%Y-%m-%d")

    # The documented EFTS endpoint
    url = "https://efts.sec.gov/LATEST/search-index"
    params = {
        "q": '"4"',
        "forms": "4",
        "dateRange": "custom",
        "startdt": start_date,
        "enddt": end_date,
    }

    log.info(f"Trying EFTS search: {url}")
    try:
        resp = SESSION.get(url, params=params, timeout=30)
        time.sleep(REQUEST_DELAY)
        log.info(f"EFTS response: HTTP {resp.status_code}")
        if resp.status_code == 200:
            data = resp.json()
            hits = data.get("hits", {}).get("hits", [])
            log.info(f"EFTS returned {len(hits)} hits")
            return hits
    except Exception as e:
        log.warning(f"EFTS search failed: {e}")

    return []


def fetch_form4_via_full_index(lookback_days: int = 3) -> list[dict]:
    """
    Fallback: Download EDGAR daily full-index master files to find Form 4s.
    These are plain text files with one line per filing.
    URL format: https://www.sec.gov/Archives/edgar/full-index/YYYY/QTRN/master.idx
    """
    now = datetime.now(timezone.utc)
    filings = []

    for day_offset in range(lookback_days + 1):
        d = now - timedelta(days=day_offset)
        quarter = (d.month - 1) // 3 + 1

        # Try the daily master file first
        # Format varies: master.YYYYMMDD.idx or just in the full master.idx
        # The full master.idx for the quarter contains ALL filings
        # For daily, EDGAR provides files at:
        #   /Archives/edgar/daily-index/YYYY/QTRN/master.YYYYMMDD.idx
        daily_url = (
            f"https://www.sec.gov/Archives/edgar/daily-index/"
            f"{d.year}/QTR{quarter}/master.{d.strftime('%Y%m%d')}.idx"
        )

        log.info(f"Fetching daily index: {daily_url}")
        try:
            resp = SESSION.get(daily_url, timeout=30)
            time.sleep(REQUEST_DELAY)
            if resp.status_code == 200:
                for line in resp.text.splitlines():
                    # Format: CIK|Company Name|Form Type|Date Filed|Filename
                    parts = line.split("|")
                    if len(parts) == 5 and parts[2].strip() in ("4", "4/A"):
                        filings.append({
                            "cik": parts[0].strip(),
                            "company_name": parts[1].strip(),
                            "form_type": parts[2].strip(),
                            "date_filed": parts[3].strip(),
                            "filename": parts[4].strip(),
                        })
                log.info(f"  Found {len([p for p in filings if p['date_filed'] == d.strftime('%Y-%m-%d')])} Form 4s for {d.strftime('%Y-%m-%d')}")
            else:
                log.info(f"  No daily index for {d.strftime('%Y-%m-%d')} (HTTP {resp.status_code})")
        except Exception as e:
            log.warning(f"  Error fetching daily index: {e}")

    log.info(f"Full-index method found {len(filings)} total Form 4 filings")
    return filings


# ──────────────────────────────────────────────
# 2. RESOLVE XML URLS FROM FILING METADATA
# ──────────────────────────────────────────────
def resolve_xml_url_from_index_entry(filing: dict) -> Optional[str]:
    """
    Given a filing from the daily index, construct the filing index page URL
    and find the XML document.

    The filename field looks like: edgar/data/789019/0001062993-26-003456.txt
    We need: https://www.sec.gov/Archives/edgar/data/789019/000106299326003456/0001062993-26-003456-index.htm
    """
    filename = filing.get("filename", "")
    if not filename:
        return None

    # Build the full URL to the txt filing
    base = "https://www.sec.gov/Archives/"

    # Extract accession number from filename
    # e.g., edgar/data/789019/0001062993-26-003456.txt
    parts = filename.split("/")
    if len(parts) < 4:
        return None

    cik = parts[2]
    txt_name = parts[3]  # 0001062993-26-003456.txt
    accession_dashed = txt_name.replace(".txt", "")
    accession_nodash = accession_dashed.replace("-", "")

    # Filing index page URL
    idx_url = f"{base}edgar/data/{cik}/{accession_nodash}/{accession_dashed}-index.htm"

    return _find_xml_on_index_page(idx_url, f"{base}edgar/data/{cik}/{accession_nodash}")


def _find_xml_on_index_page(idx_url: str, folder_url: str) -> Optional[str]:
    """Fetch a filing index page and extract the primary XML document URL."""
    try:
        resp = SESSION.get(idx_url, timeout=30)
        time.sleep(REQUEST_DELAY)

        if resp.status_code != 200:
            log.debug(f"Index page HTTP {resp.status_code}: {idx_url}")
            return None

        # Look for XML file links in the index page
        xml_links = re.findall(r'href="([^"]+\.xml)"', resp.text, re.IGNORECASE)

        if not xml_links:
            # Sometimes the XML is referenced differently
            # Try looking for any .xml reference in the page
            xml_links = re.findall(r'([^\s"<>]+\.xml)', resp.text, re.IGNORECASE)

        for link in xml_links:
            basename = link.rsplit("/", 1)[-1] if "/" in link else link
            # Skip R_ files (XBRL rendering files) and FilingSummary
            if basename.lower().startswith("r_"):
                continue
            if "filingsummary" in basename.lower():
                continue
            if link.startswith("http"):
                return link
            if link.startswith("/"):
                return f"https://www.sec.gov{link}"
            return f"{folder_url}/{link}"

    except Exception as e:
        log.debug(f"Error fetching index page: {e}")
    return None


# ──────────────────────────────────────────────
# 3. PARSE FORM 4 XML FOR PURCHASE DETAILS
# ──────────────────────────────────────────────
def parse_form4_xml(xml_url: str) -> Optional[dict]:
    """
    Downloads and parses a Form 4 XML filing.
    Returns purchase info if it contains open-market purchases
    above the minimum threshold. Returns None otherwise.
    """
    try:
        resp = SESSION.get(xml_url, timeout=30)
        time.sleep(REQUEST_DELAY)
        if resp.status_code != 200:
            return None

        content = resp.content
        # Strip XML namespaces that can break ElementTree
        try:
            root = ET.fromstring(content)
        except ET.ParseError:
            clean = re.sub(rb'\sxmlns[^"]*"[^"]*"', b'', content)
            try:
                root = ET.fromstring(clean)
            except ET.ParseError:
                return None
    except Exception as e:
        log.debug(f"Failed to fetch/parse {xml_url}: {e}")
        return None

    # --- Issuer (company) info ---
    issuer_name = _text(root, ".//issuerName") or _text(root, ".//issuer/issuerName")
    issuer_ticker = _text(root, ".//issuerTradingSymbol") or _text(root, ".//issuer/issuerTradingSymbol")
    issuer_cik = _text(root, ".//issuerCik") or _text(root, ".//issuer/issuerCik")

    if not issuer_name:
        return None

    # --- Reporting owner (insider) info ---
    owner_name = _text(root, ".//rptOwnerName") or _text(root, ".//reportingOwner//rptOwnerName")

    rel = root.find(".//reportingOwnerRelationship")
    is_director = _text(rel, "isDirector") in ("1", "true") if rel is not None else False
    is_officer = _text(rel, "isOfficer") in ("1", "true") if rel is not None else False
    is_ten_pct = _text(rel, "isTenPercentOwner") in ("1", "true") if rel is not None else False
    officer_title = _text(rel, "officerTitle") if rel is not None else ""

    roles = []
    if is_officer and officer_title:
        roles.append(officer_title)
    elif is_officer:
        roles.append("Officer")
    if is_director:
        roles.append("Director")
    if is_ten_pct:
        roles.append("10%+ Owner")
    role_str = ", ".join(roles) if roles else "Insider"

    # --- Non-derivative transactions: look for code "P" (purchase) ---
    purchases = []
    for txn in root.findall(".//nonDerivativeTransaction"):
        code = _text(txn, ".//transactionCode") or _text(txn, ".//transactionCoding/transactionCode")
        if code != "P":
            continue

        acq_disp = (
            _text(txn, ".//transactionAcquiredDisposedCode/value")
            or _text(txn, ".//transactionAcquiredDisposedCode")
        )
        if acq_disp and acq_disp != "A":
            continue

        shares_str = _text(txn, ".//transactionShares/value") or _text(txn, ".//transactionShares")
        price_str = _text(txn, ".//transactionPricePerShare/value") or _text(txn, ".//transactionPricePerShare")
        date_str = _text(txn, ".//transactionDate/value") or _text(txn, ".//transactionDate")

        try:
            shares = float(shares_str) if shares_str else 0
            price = float(price_str) if price_str else 0
        except ValueError:
            continue

        total_value = shares * price

        if total_value >= MIN_PURCHASE_USD:
            purchases.append({
                "date": date_str,
                "shares": shares,
                "price": price,
                "total_value": total_value,
            })

    if not purchases:
        return None

    total_invested = sum(p["total_value"] for p in purchases)
    total_shares = sum(p["shares"] for p in purchases)
    avg_price = total_invested / total_shares if total_shares else 0

    return {
        "issuer_name": issuer_name,
        "ticker": issuer_ticker.upper() if issuer_ticker else "N/A",
        "issuer_cik": issuer_cik,
        "owner_name": owner_name,
        "role": role_str,
        "purchases": purchases,
        "total_invested": total_invested,
        "total_shares": total_shares,
        "avg_price": avg_price,
        "filing_url": xml_url,
    }


def _text(el, path: str) -> str:
    """Safely extract text from an XML element."""
    if el is None:
        return ""
    child = el.find(path)
    return (child.text or "").strip() if child is not None else ""


# ──────────────────────────────────────────────
# 4. BUILD + SEND EMAIL DIGEST
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
        </tr>
        """

    total_all = sum(t["total_invested"] for t in trades)

    return f"""
    <html><body style="font-family: Arial, sans-serif; color: #333; max-width: 750px; margin: auto;">
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
                <th style="padding: 10px; text-align: right;">Avg Price</th>
                <th style="padding: 10px; text-align: right;">Total Value</th>
            </tr>
        </thead>
        <tbody>{rows}</tbody>
    </table>
    <p style="font-size: 11px; color: #aaa; margin-top: 20px;">
        Source: SEC EDGAR Form 4 filings. Open-market purchases only (code &ldquo;P&rdquo;).
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
# 5. MAIN PIPELINE
# ──────────────────────────────────────────────
def main():
    log.info("=== Insider Buying Alert Agent ===")
    log.info(f"Min purchase threshold: ${MIN_PURCHASE_USD:,.0f}")
    log.info(f"User-Agent: {EDGAR_USER_AGENT}")

    # ── Step 1: Get Form 4 filing list from daily index files ──
    # This is the most reliable method — plain text files, no API auth needed
    filings = fetch_form4_via_full_index(lookback_days=3)

    if not filings:
        log.warning("Daily index returned no filings. Trying EFTS search as fallback...")
        efts_hits = fetch_form4_filings(lookback_days=3)
        # Convert EFTS hits to a common format for processing
        for hit in efts_hits:
            src = hit.get("_source", {})
            acc = src.get("accession_no", "")
            cik = str(src.get("cik", "")).lstrip("0")
            if acc and cik:
                acc_nodash = acc.replace("-", "")
                filings.append({
                    "cik": cik,
                    "company_name": src.get("entity_name", ""),
                    "form_type": "4",
                    "date_filed": src.get("file_date", ""),
                    "filename": f"edgar/data/{cik}/{acc}.txt",
                })

    if not filings:
        log.info("No Form 4 filings found from any source. Sending empty digest.")
        date_str = datetime.now(timezone.utc).strftime("%B %d, %Y")
        send_email(build_email_html([], date_str), date_str)
        return

    # ── Step 2: Resolve XML URLs from filing index pages ──
    log.info(f"Resolving XML URLs for {len(filings)} Form 4 filings...")
    xml_urls = []
    for i, f in enumerate(filings):
        xml_url = resolve_xml_url_from_index_entry(f)
        if xml_url:
            xml_urls.append(xml_url)
        else:
            log.debug(f"No XML found for {f.get('company_name', '?')} ({f.get('filename', '?')})")

        if (i + 1) % 100 == 0:
            log.info(f"  Resolved {i+1}/{len(filings)} filings → {len(xml_urls)} XMLs so far")

    log.info(f"Resolved {len(xml_urls)} XML URLs from {len(filings)} filings")

    if not xml_urls:
        log.warning("Could not resolve any XML URLs. Sending empty digest.")
        date_str = datetime.now(timezone.utc).strftime("%B %d, %Y")
        send_email(build_email_html([], date_str), date_str)
        return

    # ── Step 3: Parse each XML for qualifying purchases ──
    log.info(f"Parsing {len(xml_urls)} Form 4 XMLs for purchases >= ${MIN_PURCHASE_USD:,.0f}...")
    insider_purchases = []
    for i, url in enumerate(xml_urls):
        result = parse_form4_xml(url)
        if result:
            insider_purchases.append(result)
            log.info(f"  ✓ PURCHASE: {result['ticker']} | {result['owner_name']} | "
                     f"{result['role']} | ${result['total_invested']:,.0f}")

        if (i + 1) % 100 == 0:
            log.info(f"  Parsed {i+1}/{len(xml_urls)}, "
                     f"found {len(insider_purchases)} qualifying purchases...")

    # ── Step 4: Deduplicate and send ──
    seen = set()
    unique = []
    for t in insider_purchases:
        key = (t["ticker"], t["owner_name"])
        if key not in seen:
            seen.add(key)
            unique.append(t)
    insider_purchases = unique

    log.info(f"Total: {len(insider_purchases)} unique insider purchases >= ${MIN_PURCHASE_USD:,.0f}")

    date_str = datetime.now(timezone.utc).strftime("%B %d, %Y")
    html = build_email_html(insider_purchases, date_str)
    send_email(html, date_str)
    log.info("Done!")


if __name__ == "__main__":
    main()
