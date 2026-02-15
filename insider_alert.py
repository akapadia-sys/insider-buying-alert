"""
Daily Insider Open-Market Purchase Alert Agent
================================================
Pulls SEC EDGAR Form 4 filings, filters for sizeable open-market purchases
by insiders (officers, directors, 10% owners), and emails a daily digest.

Setup:
  1. pip install requests lxml
  2. Set environment variables (see CONFIG section)
  3. Run daily via cron or GitHub Actions (see bottom of file)
"""

import os
import sys
import json
import time
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from xml.etree import ElementTree as ET
from typing import Optional
import requests

# ──────────────────────────────────────────────
# CONFIG — set via environment variables
# ──────────────────────────────────────────────
GMAIL_ADDRESS    = os.environ.get("GMAIL_ADDRESS", "you@gmail.com")
GMAIL_APP_PASS   = os.environ.get("GMAIL_APP_PASSWORD", "")       # Gmail App Password (NOT your login password)
RECIPIENT_EMAIL  = os.environ.get("RECIPIENT_EMAIL", GMAIL_ADDRESS)
MIN_PURCHASE_USD = float(os.environ.get("MIN_PURCHASE_USD", "250000"))  # Minimum $ value to include
EDGAR_USER_AGENT = os.environ.get("EDGAR_USER_AGENT", "InsiderBot/1.0 (you@gmail.com)")  # SEC requires this

# SEC EDGAR rate limit: max 10 requests/sec — we'll be conservative
REQUEST_DELAY = 0.15  # seconds between requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


# ──────────────────────────────────────────────
# 1. FETCH RECENT FORM 4 FILINGS FROM EDGAR
# ──────────────────────────────────────────────
def fetch_recent_form4_index(lookback_days: int = 1) -> list[dict]:
    """
    Uses EDGAR full-text search to find Form 4 filings from recent days.
    Returns a list of dicts with accession numbers and filing metadata.
    """
    filings = []
    headers = {"User-Agent": EDGAR_USER_AGENT, "Accept-Encoding": "gzip, deflate"}

    # EDGAR EFTS (full-text search) endpoint for recent Form 4s
    base_url = "https://efts.sec.gov/LATEST/search-index"
    
    # Use the EDGAR full-text search API
    search_url = "https://efts.sec.gov/LATEST/search-index"
    
    # Alternative: use the EDGAR XBRL companion API for structured Form 4 data
    # We'll use the RSS feed approach which is simpler and reliable
    
    target_date = datetime.utcnow() - timedelta(days=lookback_days)
    date_str = target_date.strftime("%Y-%m-%d")
    
    # Approach: pull from EDGAR full-text search for Form 4
    url = (
        f"https://efts.sec.gov/LATEST/search-index?"
        f"q=%224%22&dateRange=custom&startdt={date_str}"
        f"&enddt={datetime.utcnow().strftime('%Y-%m-%d')}"
        f"&forms=4"
    )
    
    # More reliable: use the EDGAR company filings recent feed
    # Pull the daily index files
    today = datetime.utcnow()
    for day_offset in range(lookback_days + 1):
        d = today - timedelta(days=day_offset)
        quarter = (d.month - 1) // 3 + 1
        idx_url = (
            f"https://www.sec.gov/Archives/edgar/full-index/"
            f"{d.year}/QTR{quarter}/company.idx"
        )
        # For efficiency, use the daily index instead
        # EDGAR publishes daily index at a known path
        daily_url = (
            f"https://www.sec.gov/Archives/edgar/daily-index/"
            f"{d.year}/QTR{quarter}/company.{d.strftime('%Y%m%d')}.idx"
        )
        
        log.info(f"Fetching daily index: {daily_url}")
        try:
            resp = requests.get(daily_url, headers=headers, timeout=30)
            time.sleep(REQUEST_DELAY)
            if resp.status_code != 200:
                log.warning(f"No daily index for {d.strftime('%Y-%m-%d')} (HTTP {resp.status_code})")
                continue
            
            for line in resp.text.splitlines():
                # Format: Company Name | Form Type | CIK | Date Filed | Filename
                parts = [p.strip() for p in line.split("|")] if "|" in line else line.split()
                if len(parts) >= 5 and parts[1].strip() in ("4", "4/A"):
                    filings.append({
                        "company_name": parts[0].strip(),
                        "form_type": parts[1].strip(),
                        "cik": parts[2].strip(),
                        "date_filed": parts[3].strip(),
                        "filename": parts[4].strip(),
                    })
        except Exception as e:
            log.error(f"Error fetching index for {d.strftime('%Y-%m-%d')}: {e}")
    
    log.info(f"Found {len(filings)} Form 4 filings in the last {lookback_days} day(s)")
    return filings


# ──────────────────────────────────────────────
# 2. PARSE A FORM 4 XML FOR PURCHASE DETAILS
# ──────────────────────────────────────────────
def parse_form4_xml(xml_url: str) -> Optional[dict]:
    """
    Downloads and parses a Form 4 XML filing.
    Returns a dict with insider + transaction info if it contains
    open-market purchases above the minimum threshold.
    """
    headers = {"User-Agent": EDGAR_USER_AGENT}
    try:
        resp = requests.get(xml_url, headers=headers, timeout=30)
        time.sleep(REQUEST_DELAY)
        if resp.status_code != 200:
            return None
        
        root = ET.fromstring(resp.content)
    except Exception as e:
        log.debug(f"Failed to parse {xml_url}: {e}")
        return None

    # --- Extract issuer (company) info ---
    issuer = root.find(".//issuer")
    if issuer is None:
        return None
    issuer_name = _text(issuer, "issuerName")
    issuer_ticker = _text(issuer, "issuerTradingSymbol")
    issuer_cik = _text(issuer, "issuerCik")

    # --- Extract reporting owner (insider) info ---
    owner = root.find(".//reportingOwner")
    if owner is None:
        return None
    
    owner_name = _text(owner, ".//rptOwnerName")
    
    # Get relationship
    rel = owner.find(".//reportingOwnerRelationship")
    is_director = _text(rel, "isDirector") == "1" if rel is not None else False
    is_officer = _text(rel, "isOfficer") == "1" if rel is not None else False
    is_ten_pct = _text(rel, "isTenPercentOwner") == "1" if rel is not None else False
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

    # --- Extract non-derivative transactions ---
    purchases = []
    for txn in root.findall(".//nonDerivativeTransaction"):
        # transactionCode: P = Purchase, S = Sale, A = Grant, etc.
        code = _text(txn, ".//transactionCoding/transactionCode")
        if code != "P":
            continue
        
        # Check acquisition/disposition
        acq_disp = _text(txn, ".//transactionAmounts/transactionAcquiredDisposedCode/value")
        if acq_disp != "A":  # A = Acquired
            continue
        
        shares_str = _text(txn, ".//transactionAmounts/transactionShares/value")
        price_str = _text(txn, ".//transactionAmounts/transactionPricePerShare/value")
        date_str = _text(txn, ".//transactionDate/value")
        
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

    return {
        "issuer_name": issuer_name,
        "ticker": issuer_ticker.upper() if issuer_ticker else "N/A",
        "issuer_cik": issuer_cik,
        "owner_name": owner_name,
        "role": role_str,
        "purchases": purchases,
        "total_invested": total_invested,
        "total_shares": total_shares,
        "filing_url": xml_url,
    }


def _text(el, path: str) -> str:
    """Safely extract text from an XML element."""
    if el is None:
        return ""
    child = el.find(path)
    return (child.text or "").strip() if child is not None else ""


# ──────────────────────────────────────────────
# 3. RESOLVE FORM 4 XML URL FROM INDEX ENTRY
# ──────────────────────────────────────────────
def get_form4_xml_url(filing: dict) -> Optional[str]:
    """
    Given a filing index entry, find the actual XML document URL
    by fetching the filing's index page.
    """
    headers = {"User-Agent": EDGAR_USER_AGENT}
    base = "https://www.sec.gov/Archives/"
    index_url = base + filing["filename"]

    # The filename in the daily index points to the .txt filing.
    # We need the -index.htm page to find the XML.
    # Convert: edgar/data/CIK/ACCESSION.txt -> index page
    idx_page = index_url.replace(".txt", "-index.htm")
    
    try:
        resp = requests.get(idx_page, headers=headers, timeout=30)
        time.sleep(REQUEST_DELAY)
        if resp.status_code != 200:
            # Try fetching the .txt and look for XML reference
            resp2 = requests.get(index_url, headers=headers, timeout=30)
            time.sleep(REQUEST_DELAY)
            if resp2.status_code != 200:
                return None
            # Look for .xml file reference in the SGML wrapper
            for line in resp2.text.splitlines():
                if ".xml" in line.lower() and "primary_doc" not in line.lower():
                    # Try to extract filename
                    if "<FILENAME>" in line:
                        xml_name = line.split("<FILENAME>")[1].strip()
                        folder = index_url.rsplit("/", 1)[0]
                        return f"{folder}/{xml_name}"
            return None

        # Parse the index page for the XML document link
        text = resp.text
        # Find links ending in .xml (the structured Form 4 data)
        import re
        xml_links = re.findall(r'href="([^"]+\.xml)"', text, re.IGNORECASE)
        
        if not xml_links:
            return None
        
        # Pick the primary XML (usually the first non-R_ file)
        for link in xml_links:
            if not link.startswith("R_") and "primary_doc" not in link:
                if link.startswith("http"):
                    return link
                folder = idx_page.rsplit("/", 1)[0]
                return f"{folder}/{link}"
        
        return None
    except Exception as e:
        log.error(f"Error resolving XML URL: {e}")
        return None


# ──────────────────────────────────────────────
# 4. BUILD + SEND EMAIL DIGEST
# ──────────────────────────────────────────────
def build_email_html(trades: list[dict], date_str: str) -> str:
    """Build a clean HTML email from a list of parsed insider purchases."""
    if not trades:
        return f"""
        <html><body style="font-family: Arial, sans-serif; color: #333;">
        <h2 style="color: #1a5276;">Insider Buying Digest — {date_str}</h2>
        <p>No sizeable open-market insider purchases were filed today
        (min threshold: ${MIN_PURCHASE_USD:,.0f}).</p>
        </body></html>
        """

    # Sort by total invested, descending
    trades.sort(key=lambda t: t["total_invested"], reverse=True)

    rows = ""
    for t in trades:
        sec_link = t["filing_url"]
        rows += f"""
        <tr style="border-bottom: 1px solid #eee;">
            <td style="padding: 10px;"><strong><a href="https://finance.yahoo.com/quote/{t['ticker']}" 
                style="color: #2980b9; text-decoration: none;">{t['ticker']}</a></strong><br>
                <span style="font-size: 12px; color: #777;">{t['issuer_name']}</span></td>
            <td style="padding: 10px;">{t['owner_name']}<br>
                <span style="font-size: 12px; color: #777;">{t['role']}</span></td>
            <td style="padding: 10px; text-align: right;">{t['total_shares']:,.0f}</td>
            <td style="padding: 10px; text-align: right; font-weight: bold; color: #27ae60;">
                ${t['total_invested']:,.0f}</td>
            <td style="padding: 10px; text-align: center;">
                <a href="{sec_link}" style="color: #2980b9; font-size: 12px;">SEC Filing</a></td>
        </tr>
        """

    total_all = sum(t["total_invested"] for t in trades)

    return f"""
    <html><body style="font-family: Arial, sans-serif; color: #333; max-width: 700px; margin: auto;">
    <h2 style="color: #1a5276; border-bottom: 3px solid #2980b9; padding-bottom: 8px;">
        Insider Buying Digest — {date_str}</h2>
    <p style="color: #555;">
        <strong>{len(trades)}</strong> sizeable open-market purchases totaling 
        <strong style="color: #27ae60;">${total_all:,.0f}</strong>
        (threshold: ${MIN_PURCHASE_USD:,.0f}+)
    </p>
    <table style="width: 100%; border-collapse: collapse; font-size: 14px;">
        <thead>
            <tr style="background: #f8f9fa; border-bottom: 2px solid #dee2e6;">
                <th style="padding: 10px; text-align: left;">Ticker</th>
                <th style="padding: 10px; text-align: left;">Insider</th>
                <th style="padding: 10px; text-align: right;">Shares</th>
                <th style="padding: 10px; text-align: right;">Value</th>
                <th style="padding: 10px; text-align: center;">Filing</th>
            </tr>
        </thead>
        <tbody>{rows}</tbody>
    </table>
    <p style="font-size: 11px; color: #aaa; margin-top: 20px;">
        Source: SEC EDGAR Form 4 filings. Open-market purchases only (code "P").
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

    # Fetch yesterday's + today's filings (filings can appear with a lag)
    filings = fetch_recent_form4_index(lookback_days=1)

    if not filings:
        log.info("No Form 4 filings found in index. Sending empty digest.")
        date_str = datetime.utcnow().strftime("%B %d, %Y")
        send_email(build_email_html([], date_str), date_str)
        return

    log.info(f"Processing {len(filings)} Form 4 filings...")
    
    insider_purchases = []
    processed = 0
    
    for f in filings:
        xml_url = get_form4_xml_url(f)
        if not xml_url:
            continue
        
        result = parse_form4_xml(xml_url)
        if result:
            insider_purchases.append(result)
        
        processed += 1
        if processed % 50 == 0:
            log.info(f"  Processed {processed}/{len(filings)} filings, "
                     f"found {len(insider_purchases)} purchases so far...")

    log.info(f"Found {len(insider_purchases)} insider purchases >= ${MIN_PURCHASE_USD:,.0f}")

    # Deduplicate by (ticker, owner_name)
    seen = set()
    unique = []
    for t in insider_purchases:
        key = (t["ticker"], t["owner_name"])
        if key not in seen:
            seen.add(key)
            unique.append(t)
    insider_purchases = unique

    date_str = datetime.utcnow().strftime("%B %d, %Y")
    html = build_email_html(insider_purchases, date_str)
    send_email(html, date_str)
    
    log.info("Done!")


if __name__ == "__main__":
    main()
