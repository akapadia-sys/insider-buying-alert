"""
Daily Insider Open-Market Purchase Alert Agent
================================================
Pulls SEC EDGAR Form 4 filings via the EFTS search API, parses the XML
for open-market purchases by insiders, and emails a daily digest.

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
from datetime import datetime, timedelta
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
EDGAR_USER_AGENT = os.environ.get("EDGAR_USER_AGENT", "InsiderBot/1.0 (you@gmail.com)")

REQUEST_DELAY = 0.12  # SEC rate limit: max 10 req/sec

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": EDGAR_USER_AGENT, "Accept-Encoding": "gzip, deflate"})


# ──────────────────────────────────────────────
# 1. FETCH RECENT FORM 4 FILINGS VIA EFTS API
# ──────────────────────────────────────────────
def fetch_form4_filings(lookback_days: int = 3) -> list[dict]:
    """
    Uses EDGAR's EFTS (full-text search) API to find recent Form 4 filings.
    Returns a list of dicts with accession numbers and filing URLs.
    """
    start_date = (datetime.utcnow() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    end_date = datetime.utcnow().strftime("%Y-%m-%d")

    filings = []
    start = 0
    page_size = 100  # EFTS max per request

    while True:
        url = (
            f"https://efts.sec.gov/LATEST/search-index?"
            f"q=%224%22&forms=4&dateRange=custom"
            f"&startdt={start_date}&enddt={end_date}"
            f"&from={start}&size={page_size}"
        )

        log.info(f"Fetching EFTS results (offset {start})...")
        try:
            resp = SESSION.get(url, timeout=30)
            time.sleep(REQUEST_DELAY)
        except Exception as e:
            log.error(f"EFTS request failed: {e}")
            break

        if resp.status_code != 200:
            log.warning(f"EFTS returned HTTP {resp.status_code}, trying alternative approach")
            break

        try:
            data = resp.json()
        except Exception:
            log.warning("EFTS returned non-JSON response, trying alternative approach")
            break

        hits = data.get("hits", {}).get("hits", [])
        if not hits:
            break

        for hit in hits:
            src = hit.get("_source", {})
            acc = src.get("file_num", "") or hit.get("_id", "")
            filings.append({
                "accession": src.get("accession_no", ""),
                "entity_name": src.get("entity_name", ""),
                "file_date": src.get("file_date", ""),
                "source": src,
                "hit": hit,
            })

        total = data.get("hits", {}).get("total", {})
        total_val = total.get("value", 0) if isinstance(total, dict) else total
        start += page_size
        if start >= total_val:
            break

    log.info(f"EFTS returned {len(filings)} Form 4 filings")
    return filings


def fetch_form4_filings_rss(lookback_days: int = 3) -> list[dict]:
    """
    Fallback: Uses EDGAR's RSS feed for recent Form 4 filings.
    """
    url = (
        "https://www.sec.gov/cgi-bin/browse-edgar?"
        "action=getcurrent&type=4&dateb=&owner=include&count=100"
        "&search_text=&start=0&output=atom"
    )
    filings = []
    log.info("Fetching Form 4 filings via RSS feed...")

    try:
        resp = SESSION.get(url, timeout=30)
        time.sleep(REQUEST_DELAY)
        if resp.status_code != 200:
            log.error(f"RSS feed returned HTTP {resp.status_code}")
            return filings

        ns = {
            "atom": "http://www.w3.org/2005/Atom",
        }
        root = ET.fromstring(resp.content)
        cutoff = datetime.utcnow() - timedelta(days=lookback_days)

        for entry in root.findall(".//atom:entry", ns):
            title = entry.findtext("atom:title", "", ns)
            if "4" not in title:
                continue
            link_el = entry.find("atom:link", ns)
            link = link_el.get("href", "") if link_el is not None else ""
            updated = entry.findtext("atom:updated", "", ns)

            filings.append({
                "title": title,
                "link": link,
                "updated": updated,
            })

        log.info(f"RSS returned {len(filings)} Form 4 entries")
    except Exception as e:
        log.error(f"RSS fetch failed: {e}")

    return filings


# ──────────────────────────────────────────────
# 2. GET FILING INDEX PAGE & FIND XML URL
# ──────────────────────────────────────────────
def get_filing_documents(accession: str, cik: str = "") -> Optional[str]:
    """
    Given an accession number, fetch the filing index page and
    find the primary XML document URL.
    """
    # Clean accession: remove dashes for URL path
    acc_raw = accession.replace("-", "")
    acc_dashed = accession

    # Try to find the filing index
    # Format: https://www.sec.gov/Archives/edgar/data/{CIK}/{ACC_NO_DASHES}/{ACC_DASHED}-index.htm
    # Without CIK, we can use the accession-based URL
    index_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_raw}/{acc_dashed}-index.htm" if cik else None

    # Alternative: use the EDGAR filing viewer
    viewer_url = f"https://www.sec.gov/cgi-bin/viewer?action=view&cik={cik}&type=4&dateb=&owner=include&count=1&search_text=&accession={acc_dashed}" if cik else None

    # Most reliable: directly construct the filing page URL
    if cik:
        idx_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_raw}/{acc_dashed}-index.htm"
        return _find_xml_on_index_page(idx_url)

    return None


def _find_xml_on_index_page(idx_url: str) -> Optional[str]:
    """Fetch a filing index page and extract the XML document URL."""
    try:
        resp = SESSION.get(idx_url, timeout=30)
        time.sleep(REQUEST_DELAY)
        if resp.status_code != 200:
            log.debug(f"Index page HTTP {resp.status_code}: {idx_url}")
            return None

        folder = idx_url.rsplit("/", 1)[0]
        xml_links = re.findall(r'href="([^"]+\.xml)"', resp.text, re.IGNORECASE)

        for link in xml_links:
            basename = link.rsplit("/", 1)[-1] if "/" in link else link
            if basename.lower().startswith("r_"):
                continue
            if link.startswith("http"):
                return link
            return f"{folder}/{link}"

    except Exception as e:
        log.debug(f"Error fetching index page {idx_url}: {e}")
    return None


# ──────────────────────────────────────────────
# 3. ALTERNATIVE: SCRAPE RECENT FILINGS DIRECTLY
# ──────────────────────────────────────────────
def fetch_form4_via_fulltext_search(lookback_days: int = 3) -> list[dict]:
    """
    Uses EDGAR full-text search (EFTS) REST API — the most reliable method.
    Returns filing metadata with direct links to documents.
    """
    start_date = (datetime.utcnow() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    end_date = datetime.utcnow().strftime("%Y-%m-%d")

    url = (
        f"https://efts.sec.gov/LATEST/search-index?"
        f"q=%224%22&forms=4&dateRange=custom"
        f"&startdt={start_date}&enddt={end_date}"
    )
    # Actually, let's use the better-documented endpoint:
    url = (
        f"https://efts.sec.gov/LATEST/search-index?"
        f"forms=4&dateRange=custom"
        f"&startdt={start_date}&enddt={end_date}"
    )

    try:
        resp = SESSION.get(url, timeout=30)
        time.sleep(REQUEST_DELAY)
        if resp.status_code == 200:
            return resp.json().get("hits", {}).get("hits", [])
    except Exception as e:
        log.error(f"Full-text search failed: {e}")
    return []


def fetch_form4_robust(lookback_days: int = 3) -> list[str]:
    """
    Most robust approach: use EDGAR's company search to get recent Form 4
    filing index pages, then extract XML URLs from each.
    
    Returns a list of XML document URLs ready to parse.
    """
    xml_urls = []
    start_date = (datetime.utcnow() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    end_date = datetime.utcnow().strftime("%Y-%m-%d")

    # Step 1: Use EDGAR full-text search to get filing accessions
    log.info(f"Searching EDGAR for Form 4 filings from {start_date} to {end_date}...")
    
    base_url = "https://efts.sec.gov/LATEST/search-index"
    params = {
        "forms": "4",
        "dateRange": "custom",
        "startdt": start_date,
        "enddt": end_date,
    }

    offset = 0
    page_size = 50
    total_found = 0

    while True:
        params["from"] = offset
        params["size"] = page_size

        try:
            resp = SESSION.get(base_url, params=params, timeout=30)
            time.sleep(REQUEST_DELAY)
        except Exception as e:
            log.error(f"Search request failed: {e}")
            break

        if resp.status_code != 200:
            log.warning(f"Search returned HTTP {resp.status_code}")
            # Fall back to RSS approach
            break

        try:
            data = resp.json()
        except Exception:
            log.warning("Non-JSON search response")
            break

        hits = data.get("hits", {}).get("hits", [])
        if not hits:
            break

        total_raw = data.get("hits", {}).get("total", 0)
        total_count = total_raw.get("value", 0) if isinstance(total_raw, dict) else total_raw

        for hit in hits:
            src = hit.get("_source", {})
            acc = src.get("accession_no", "")
            cik = str(src.get("cik", "")).lstrip("0")
            
            if acc and cik:
                acc_clean = acc.replace("-", "")
                idx_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_clean}/{acc}-index.htm"
                xml_url = _find_xml_on_index_page(idx_url)
                if xml_url:
                    xml_urls.append(xml_url)
                    total_found += 1
                else:
                    log.warning(f"No XML found for accession {acc} (CIK {cik})")

            if total_found % 25 == 0 and total_found > 0:
                log.info(f"  Resolved {total_found} XML URLs so far...")

        offset += page_size
        if offset >= total_count or offset >= 500:
            # Cap at 500 to avoid excessive requests
            break

    log.info(f"Resolved {len(xml_urls)} Form 4 XML URLs total")
    return xml_urls


# ──────────────────────────────────────────────
# 4. PARSE FORM 4 XML FOR PURCHASE DETAILS
# ──────────────────────────────────────────────
def parse_form4_xml(xml_url: str) -> Optional[dict]:
    """
    Downloads and parses a Form 4 XML filing.
    Returns a dict with insider + transaction info if it contains
    open-market purchases above the minimum threshold.
    """
    try:
        resp = SESSION.get(xml_url, timeout=30)
        time.sleep(REQUEST_DELAY)
        if resp.status_code != 200:
            return None

        # Some Form 4 XMLs have namespace declarations — strip them
        content = resp.content
        try:
            root = ET.fromstring(content)
        except ET.ParseError:
            # Try stripping XML namespace
            clean = re.sub(rb'\sxmlns[^"]*"[^"]*"', b'', content)
            root = ET.fromstring(clean)
    except Exception as e:
        log.debug(f"Failed to parse {xml_url}: {e}")
        return None

    # --- Extract issuer (company) info ---
    issuer_name = _text(root, ".//issuerName") or _text(root, ".//issuer/issuerName")
    issuer_ticker = _text(root, ".//issuerTradingSymbol") or _text(root, ".//issuer/issuerTradingSymbol")
    issuer_cik = _text(root, ".//issuerCik") or _text(root, ".//issuer/issuerCik")

    if not issuer_name:
        return None

    # --- Extract reporting owner (insider) info ---
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

    # --- Extract non-derivative transactions (P = open market purchase) ---
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
# 5. BUILD + SEND EMAIL DIGEST
# ──────────────────────────────────────────────
def build_email_html(trades: list[dict], date_str: str) -> str:
    """Build a clean HTML email from a list of parsed insider purchases."""
    if not trades:
        return f"""
        <html><body style="font-family: Arial, sans-serif; color: #333;">
        <h2 style="color: #1a5276;">Insider Buying Digest &mdash; {date_str}</h2>
        <p>No sizeable open-market insider purchases were filed in the last 3 days
        (min threshold: ${MIN_PURCHASE_USD:,.0f}).</p>
        </body></html>
        """

    trades.sort(key=lambda t: t["total_invested"], reverse=True)

    rows = ""
    for t in trades:
        ticker_link = f"https://finance.yahoo.com/quote/{t['ticker']}"
        # Build SEC filing link (go to human-readable page)
        sec_link = t["filing_url"].replace(".xml", "-index.htm") if t["filing_url"] else "#"

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
            <td style="padding: 10px; text-align: center;">
                <a href="{sec_link}" style="color: #2980b9; font-size: 12px;">Filing</a></td>
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
                <th style="padding: 10px; text-align: center;">Source</th>
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
# 6. MAIN PIPELINE
# ──────────────────────────────────────────────
def main():
    log.info("=== Insider Buying Alert Agent ===")
    log.info(f"Min purchase threshold: ${MIN_PURCHASE_USD:,.0f}")

    # Step 1: Get all Form 4 XML URLs from the last 3 days
    xml_urls = fetch_form4_robust(lookback_days=3)

    if not xml_urls:
        log.warning("Primary search returned no results. Trying RSS fallback...")
        rss_filings = fetch_form4_filings_rss(lookback_days=3)
        for f in rss_filings:
            link = f.get("link", "")
            if link:
                xml_url = _find_xml_on_index_page(link)
                if xml_url:
                    xml_urls.append(xml_url)

    if not xml_urls:
        log.info("No Form 4 XML documents found. Sending empty digest.")
        date_str = datetime.utcnow().strftime("%B %d, %Y")
        send_email(build_email_html([], date_str), date_str)
        return

    log.info(f"Processing {len(xml_urls)} Form 4 XML documents...")

    insider_purchases = []
    for i, url in enumerate(xml_urls):
        result = parse_form4_xml(url)
        if result:
            insider_purchases.append(result)
            log.info(f"  PURCHASE: {result['ticker']} — {result['owner_name']} "
                     f"— ${result['total_invested']:,.0f}")

        if (i + 1) % 50 == 0:
            log.info(f"  Processed {i+1}/{len(xml_urls)}, "
                     f"found {len(insider_purchases)} purchases...")

    # Deduplicate by (ticker, owner_name)
    seen = set()
    unique = []
    for t in insider_purchases:
        key = (t["ticker"], t["owner_name"])
        if key not in seen:
            seen.add(key)
            unique.append(t)
    insider_purchases = unique

    log.info(f"Found {len(insider_purchases)} unique insider purchases >= ${MIN_PURCHASE_USD:,.0f}")

    date_str = datetime.utcnow().strftime("%B %d, %Y")
    html = build_email_html(insider_purchases, date_str)
    send_email(html, date_str)

    log.info("Done!")


if __name__ == "__main__":
    main()
