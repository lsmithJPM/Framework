"""
UK Procurement Opportunity Trawler
===================================
Searches Contracts Finder and Find a Tender for acoustics,
noise, vibration, and air quality related opportunities.

Usage:
    python trawler.py                    # search last 7 days, print to console
    python trawler.py --days 1           # search last 24 hours
    python trawler.py --email            # send email digest
    python trawler.py --days 1 --email   # daily run (use with cron/GitHub Actions)
    python trawler.py --output results.csv  # save to CSV

Setup:
    pip install requests python-dotenv
    Copy .env.example to .env and fill in your email settings (only needed for --email)
"""

import requests
import json
import csv
import argparse
import smtplib
import os
import sys
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dataclasses import dataclass, field
from typing import Optional

# ─────────────────────────────────────────────
# CONFIGURATION — edit these to suit your needs
# ─────────────────────────────────────────────

# Keywords to search for. The script looks for these in titles and descriptions.
# Any opportunity matching at least one keyword will be included.
KEYWORDS = [
    "acoustics", "acoustic",
    "noise", "noise assessment", "noise survey", "noise impact",
    "vibration", "NVH",
    "air quality", "air pollution", "dust assessment",
    "environmental noise", "noise vibration",
    "BS4142", "CRTN", "PPG24",
    "sound insulation", "noise mapping",
    "construction noise", "construction vibration",
]

# CPV codes relevant to environmental / acoustics consultancy
# These are used to filter Find a Tender results
CPV_CODES = [
    "71313000",  # Environmental engineering consultancy
    "90711000",  # Environmental impact assessment
    "71312000",  # Structural engineering consultancy
    "71314000",  # Energy and related services
    "90720000",  # Environmental protection
    "71318000",  # Engineering advisory and consultancy services
    "90711500",  # Environmental monitoring
]

# ─────────────────────────────────────────────
# DATA MODEL
# ─────────────────────────────────────────────

@dataclass
class Opportunity:
    title: str
    buyer: str
    source: str          # "Contracts Finder" or "Find a Tender"
    published: str
    deadline: str
    value: str
    description: str
    url: str
    matched_keywords: list = field(default_factory=list)

    def to_dict(self):
        return {
            "Title": self.title,
            "Buyer": self.buyer,
            "Source": self.source,
            "Published": self.published,
            "Deadline": self.deadline,
            "Estimated Value": self.value,
            "Matched Keywords": ", ".join(self.matched_keywords),
            "URL": self.url,
            "Description": self.description[:300] + "..." if len(self.description) > 300 else self.description,
        }


# ─────────────────────────────────────────────
# KEYWORD MATCHING
# ─────────────────────────────────────────────

def find_matching_keywords(text: str) -> list:
    """Return list of keywords found in the given text (case-insensitive)."""
    text_lower = text.lower()
    return [kw for kw in KEYWORDS if kw.lower() in text_lower]


def is_relevant(opportunity_text: str) -> list:
    """Returns matched keywords if relevant, else empty list."""
    return find_matching_keywords(opportunity_text)


# ─────────────────────────────────────────────
# CONTRACTS FINDER API
# ─────────────────────────────────────────────

def fetch_contracts_finder(days_back: int) -> list[Opportunity]:
    """
    Query the Contracts Finder OCDS API for recent notices.
    Docs: https://www.contractsfinder.service.gov.uk/apidocumentation
    No API key required for search.
    """
    print(f"\n[Contracts Finder] Searching last {days_back} days...")

    published_from = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%dT%H:%M:%SZ")

    base_url = "https://www.contractsfinder.service.gov.uk/Published/Notices/OCDS/Search"
    params = {
        "publishedFrom": published_from,
        "size": 100,
        "page": 1,
    }

    opportunities = []
    seen_ids = set()

    while True:
        try:
            response = requests.get(base_url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            print(f"  [!] Contracts Finder API error: {e}")
            break

        releases = data.get("releases", [])
        if not releases:
            break

        for release in releases:
            ocid = release.get("ocid", "")
            if ocid in seen_ids:
                continue
            seen_ids.add(ocid)

            tender = release.get("tender", {})
            title = tender.get("title", "No title")
            description = tender.get("description", "")
            buyer_name = ""
            parties = release.get("parties", [])
            for party in parties:
                if "buyer" in party.get("roles", []):
                    buyer_name = party.get("name", "")
                    break

            # Check relevance
            search_text = f"{title} {description}"
            matched = is_relevant(search_text)
            if not matched:
                continue

            # Extract dates
            published_date = release.get("date", "")[:10] if release.get("date") else "Unknown"
            deadline = tender.get("tenderPeriod", {}).get("endDate", "")
            deadline = deadline[:10] if deadline else "Unknown"

            # Extract value
            value_obj = tender.get("value", {})
            value = "Unknown"
            if value_obj.get("amount"):
                currency = value_obj.get("currency", "GBP")
                value = f"{currency} {value_obj['amount']:,.0f}"

            # Build URL
            notice_id = release.get("id", "")
            url = f"https://www.contractsfinder.service.gov.uk/Notice/{notice_id}" if notice_id else "https://www.contractsfinder.service.gov.uk"

            opportunities.append(Opportunity(
                title=title,
                buyer=buyer_name or "Unknown",
                source="Contracts Finder",
                published=published_date,
                deadline=deadline,
                value=value,
                description=description,
                url=url,
                matched_keywords=matched,
            ))

        # Pagination
        total = data.get("total", 0)
        fetched_so_far = params["page"] * params["size"]
        if fetched_so_far >= total or fetched_so_far >= 500:  # cap at 500 to be reasonable
            break
        params["page"] += 1

    print(f"  → Found {len(opportunities)} relevant results")
    return opportunities


# ─────────────────────────────────────────────
# FIND A TENDER (FTS) API
# ─────────────────────────────────────────────

def fetch_find_a_tender(days_back: int) -> list[Opportunity]:
    """
    Query the Find a Tender OCDS API.
    Docs: https://www.find-tender.service.gov.uk/Developer/Documentation
    Uses the public OCDS search endpoint - no API key required.
    """
    print(f"\n[Find a Tender] Searching last {days_back} days...")

    published_from = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%d")
    published_to = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Correct public search endpoint
    base_url = "https://www.find-tender.service.gov.uk/api/1.0/ocdsReleasePackages"
    params = {
        "released[from]": published_from,
        "released[to]": published_to,
        "stages[]": "tender",
        "page": 1,
    }

    opportunities = []
    seen_ids = set()

    while True:
        try:
            response = requests.get(base_url, params=params, timeout=30,
                                    headers={"Accept": "application/json"})
            if response.status_code in (400, 404):
                print(f"  [!] Find a Tender API error: {response.status_code} — {response.text[:200]}")
                break
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            print(f"  [!] Find a Tender API error: {e}")
            break

        releases = data.get("releases", [])
        if not releases:
            break

        for release in releases:
            ocid = release.get("ocid", "")
            if ocid in seen_ids:
                continue
            seen_ids.add(ocid)

            tender = release.get("tender", {})
            title = tender.get("title", "No title")
            description = tender.get("description", "")

            # Also check CPV codes
            cpv_match = False
            for item in tender.get("items", []):
                for classification in item.get("additionalClassifications", []) + [item.get("classification", {})]:
                    if classification.get("id", "") in CPV_CODES:
                        cpv_match = True
                        break

            search_text = f"{title} {description}"
            matched = is_relevant(search_text)

            if not matched and not cpv_match:
                continue

            if cpv_match and not matched:
                matched = ["[CPV code match]"]

            # Buyer
            buyer_name = ""
            parties = release.get("parties", [])
            for party in parties:
                if "buyer" in party.get("roles", []):
                    buyer_name = party.get("name", "")
                    break

            published_date = release.get("date", "")[:10] if release.get("date") else "Unknown"
            deadline = tender.get("tenderPeriod", {}).get("endDate", "")
            deadline = deadline[:10] if deadline else "Unknown"

            value_obj = tender.get("value", {})
            value = "Unknown"
            if value_obj and value_obj.get("amount"):
                currency = value_obj.get("currency", "GBP")
                value = f"{currency} {value_obj['amount']:,.0f}"

            notice_id = release.get("id", "")
            url = f"https://www.find-tender.service.gov.uk/Notice/{notice_id}" if notice_id else "https://www.find-tender.service.gov.uk"

            opportunities.append(Opportunity(
                title=title,
                buyer=buyer_name or "Unknown",
                source="Find a Tender",
                published=published_date,
                deadline=deadline,
                value=value,
                description=description,
                url=url,
                matched_keywords=matched,
            ))

        # Pagination — FTS uses page-based pagination
        links = data.get("links", {})
        if not links.get("next"):
            break
        params["page"] += 1
        if params["page"] > 10:  # safety cap
            break

    print(f"  → Found {len(opportunities)} relevant results")
    return opportunities


# ─────────────────────────────────────────────
# OUTPUT FUNCTIONS
# ─────────────────────────────────────────────

def print_to_console(opportunities: list[Opportunity]):
    if not opportunities:
        print("\n  No relevant opportunities found.")
        return

    print(f"\n{'='*70}")
    print(f"  FOUND {len(opportunities)} RELEVANT OPPORTUNITIES")
    print(f"{'='*70}")

    # Sort by published date descending
    opportunities.sort(key=lambda x: x.published, reverse=True)

    for i, opp in enumerate(opportunities, 1):
        print(f"\n[{i}] {opp.title}")
        print(f"    Buyer:     {opp.buyer}")
        print(f"    Source:    {opp.source}")
        print(f"    Published: {opp.published}")
        print(f"    Deadline:  {opp.deadline}")
        print(f"    Value:     {opp.value}")
        print(f"    Keywords:  {', '.join(opp.matched_keywords)}")
        print(f"    URL:       {opp.url}")
        if opp.description:
            snippet = opp.description[:200].replace("\n", " ")
            print(f"    Desc:      {snippet}...")
        print(f"    {'-'*60}")


def save_to_csv(opportunities: list[Opportunity], filepath: str):
    if not opportunities:
        print("No results to save.")
        return

    fieldnames = ["Title", "Buyer", "Source", "Published", "Deadline",
                  "Estimated Value", "Matched Keywords", "URL", "Description"]

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for opp in opportunities:
            writer.writerow(opp.to_dict())

    print(f"\nResults saved to: {filepath}")


def build_html_email(opportunities: list[Opportunity], days_back: int) -> str:
    """Build a clean HTML email digest."""
    today = datetime.now().strftime("%d %B %Y")
    count = len(opportunities)

    rows = ""
    for opp in sorted(opportunities, key=lambda x: x.published, reverse=True):
        kw_tags = "".join(
            f'<span style="background:#e8f4f8;color:#1a6b8a;padding:2px 7px;border-radius:10px;'
            f'font-size:11px;margin-right:4px;">{kw}</span>'
            for kw in opp.matched_keywords
        )
        snippet = opp.description[:250].replace("\n", " ") + "..." if len(opp.description) > 250 else opp.description
        rows += f"""
        <tr style="border-bottom:1px solid #eee;">
          <td style="padding:14px 10px;vertical-align:top;">
            <a href="{opp.url}" style="font-weight:bold;color:#1a6b8a;text-decoration:none;font-size:14px;">{opp.title}</a><br>
            <span style="color:#666;font-size:12px;">{opp.buyer} &nbsp;|&nbsp; {opp.source}</span><br>
            <div style="margin:6px 0;">{kw_tags}</div>
            <p style="color:#444;font-size:12px;margin:4px 0 0;">{snippet}</p>
          </td>
          <td style="padding:14px 10px;vertical-align:top;white-space:nowrap;font-size:12px;color:#555;min-width:110px;">
            <b>Published:</b> {opp.published}<br>
            <b>Deadline:</b> {opp.deadline}<br>
            <b>Value:</b> {opp.value}
          </td>
        </tr>"""

    if not rows:
        rows = '<tr><td colspan="2" style="padding:20px;color:#888;text-align:center;">No matching opportunities found this period.</td></tr>'

    return f"""
    <html><body style="font-family:Arial,sans-serif;max-width:900px;margin:0 auto;color:#333;">
    <div style="background:#1a6b8a;color:white;padding:20px 24px;border-radius:6px 6px 0 0;">
      <h2 style="margin:0;">🔊 Acoustics & Air Quality Procurement Digest</h2>
      <p style="margin:4px 0 0;opacity:0.85;">{today} &nbsp;|&nbsp; {count} opportunities in the last {days_back} day{'s' if days_back != 1 else ''}</p>
    </div>
    <table style="width:100%;border-collapse:collapse;background:white;border:1px solid #ddd;border-top:none;">
      {rows}
    </table>
    <p style="font-size:11px;color:#aaa;margin-top:12px;text-align:center;">
      Sources: Contracts Finder &amp; Find a Tender &nbsp;|&nbsp; Auto-generated by opportunity trawler
    </p>
    </body></html>
    """


def send_email_digest(opportunities: list[Opportunity], days_back: int):
    """Send HTML email digest via SMTP. Reads credentials from .env / environment."""
    smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER", "")
    smtp_pass = os.getenv("SMTP_PASS", "")
    email_to = os.getenv("EMAIL_TO", smtp_user)

    if not smtp_user or not smtp_pass:
        print("\n[!] Email not configured. Set SMTP_USER and SMTP_PASS in your .env file.")
        print("    See .env.example for instructions.")
        return

    today = datetime.now().strftime("%d %b %Y")
    subject = f"Procurement Digest — {len(opportunities)} opportunities — {today}"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = email_to

    html_content = build_html_email(opportunities, days_back)
    msg.attach(MIMEText(html_content, "html"))

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.sendmail(smtp_user, email_to, msg.as_string())
        print(f"\n✓ Email digest sent to {email_to}")
    except Exception as e:
        print(f"\n[!] Failed to send email: {e}")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="UK Procurement Opportunity Trawler — Acoustics & Air Quality")
    parser.add_argument("--days", type=int, default=7, help="How many days back to search (default: 7)")
    parser.add_argument("--email", action="store_true", help="Send results as email digest")
    parser.add_argument("--output", type=str, help="Save results to a CSV file (e.g. results.csv)")
    args = parser.parse_args()

    print(f"\n{'='*70}")
    print(f"  UK PROCUREMENT TRAWLER — Acoustics, NVH & Air Quality")
    print(f"  Searching the last {args.days} day(s)")
    print(f"  Run at: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*70}")

    # Fetch from both sources
    cf_results = fetch_contracts_finder(args.days)
    fts_results = fetch_find_a_tender(args.days)

    all_results = cf_results + fts_results

    # Deduplicate by title similarity (simple exact-title dedup)
    seen_titles = set()
    unique_results = []
    for opp in all_results:
        key = opp.title.lower().strip()
        if key not in seen_titles:
            seen_titles.add(key)
            unique_results.append(opp)

    print(f"\n{'='*70}")
    print(f"  TOTAL: {len(unique_results)} unique relevant opportunities found")
    print(f"{'='*70}")

    # Output
    print_to_console(unique_results)

    if args.output:
        save_to_csv(unique_results, args.output)

    if args.email:
        # Load .env if present
        try:
            from dotenv import load_dotenv
            load_dotenv()
        except ImportError:
            pass
        send_email_digest(unique_results, args.days)

    return len(unique_results)


if __name__ == "__main__":
    main()
