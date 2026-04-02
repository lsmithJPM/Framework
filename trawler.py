"""
UK Procurement Opportunity Trawler
===================================
Searches Contracts Finder and Find a Tender for acoustics,
noise, vibration, and air quality related opportunities.

Usage:
    python trawler.py                    # search last 7 days, print to console
    python trawler.py --days 7 --email   # weekly run
    python trawler.py --output results.csv
"""

import requests
import csv
import argparse
import smtplib
import os
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dataclasses import dataclass, field

# ─────────────────────────────────────────────
# KEYWORDS
# Split into MUST_MATCH (high confidence) and BROAD (wider net).
# An opportunity is included if it matches ANY keyword in either list.
# ─────────────────────────────────────────────

KEYWORDS = [
    # Direct matches — almost certainly relevant
    "acoustics", "acoustic",
    "noise assessment", "noise survey", "noise impact",
    "noise monitoring", "noise vibration", "noise and vibration",
    "vibration assessment", "vibration monitoring", "vibration survey",
    "air quality", "air quality assessment", "air pollution",
    "dust assessment", "dust management plan",
    "sound insulation", "sound noise", "sound vibration",
    "noise nuisance", "statutory nuisance",
    "construction noise", "construction vibration",
    "section 61", "BS4142", "BS 4142", "NPSE", "PPG24", "CRTN",
    "noise mapping", "acoustic design", "acoustic consultant",
    "occupational noise", "workplace noise",
    # Broader environmental — may include acoustics/AQ as a lot
    "environmental survey", "environmental assessment",
    "environmental consultancy", "environmental impact",
    "environmental framework", "environmental services",
    "EIA", "environmental impact assessment",
    "planning consultancy", "planning environmental",
    "ecological survey", "ecological assessment",
]

CPV_CODES = [
    "71313000",  # Environmental engineering consultancy
    "90711000",  # Environmental impact assessment
    "90720000",  # Environmental protection
    "71318000",  # Engineering advisory and consultancy
    "90711500",  # Environmental monitoring
    "90712000",  # Environmental planning
    "71351500",  # Geological and geophysical consultancy
]

# ─────────────────────────────────────────────
# DATA MODEL
# ─────────────────────────────────────────────

@dataclass
class Opportunity:
    title: str
    buyer: str
    source: str
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
            "Description": (self.description[:300] + "...") if len(self.description) > 300 else self.description,
        }

def find_matching_keywords(text: str) -> list:
    text_lower = text.lower()
    return [kw for kw in KEYWORDS if kw.lower() in text_lower]

# ─────────────────────────────────────────────
# CONTRACTS FINDER API
# ─────────────────────────────────────────────

def fetch_contracts_finder(days_back: int) -> list:
    print(f"\n[Contracts Finder] Searching last {days_back} days...")
    published_from = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%dT%H:%M:%SZ")
    base_url = "https://www.contractsfinder.service.gov.uk/Published/Notices/OCDS/Search"
    opportunities = []
    seen_ids = set()
    page = 1
    total_checked = 0

    while True:
        try:
            response = requests.get(
                base_url,
                params={"publishedFrom": published_from, "size": 100, "page": page},
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            print(f"  [!] Contracts Finder API error: {e}")
            break

        releases = data.get("releases", [])
        if not releases:
            break

        total_checked += len(releases)

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
                code = item.get("classification", {}).get("id", "")
                if code in CPV_CODES:
                    cpv_match = True
                    break

            matched = find_matching_keywords(f"{title} {description}")
            if not matched and cpv_match:
                matched = ["[CPV match]"]
            if not matched:
                continue

            buyer_name = ""
            for party in release.get("parties", []):
                if "buyer" in party.get("roles", []):
                    buyer_name = party.get("name", "")
                    break

            published_date = release.get("date", "")[:10] if release.get("date") else "Unknown"
            deadline = tender.get("tenderPeriod", {}).get("endDate", "")
            deadline = deadline[:10] if deadline else "Unknown"
            value_obj = tender.get("value", {})
            value = f"GBP {value_obj['amount']:,.0f}" if value_obj and value_obj.get("amount") else "Unknown"
            notice_id = release.get("id", "")
            url = f"https://www.contractsfinder.service.gov.uk/Notice/{notice_id}" if notice_id else "https://www.contractsfinder.service.gov.uk"

            opportunities.append(Opportunity(
                title=title, buyer=buyer_name or "Unknown", source="Contracts Finder",
                published=published_date, deadline=deadline, value=value,
                description=description, url=url, matched_keywords=matched,
            ))

        total = data.get("total", 0)
        print(f"  Checked {total_checked} of {total} notices...")
        if page * 100 >= total or page * 100 >= 1000:
            break
        page += 1

    print(f"  → Found {len(opportunities)} relevant results")
    return opportunities

# ─────────────────────────────────────────────
# FIND A TENDER — keyword search
# ─────────────────────────────────────────────

def fetch_find_a_tender(days_back: int) -> list:
    print(f"\n[Find a Tender] Searching last {days_back} days...")
    published_from = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y-%m-%d")
    opportunities = []
    seen_ids = set()

    search_terms = [
        "acoustics noise vibration",
        "air quality dust",
        "environmental assessment consultancy",
        "noise survey monitoring",
        "environmental impact assessment",
        "planning environmental services",
    ]

    for term in search_terms:
        for page in range(1, 6):
            try:
                response = requests.get(
                    "https://www.find-tender.service.gov.uk/Search/Results",
                    params={"keywords": term, "publishedFrom": published_from, "page": page},
                    timeout=30,
                    headers={"Accept": "application/json"},
                )
                if response.status_code != 200:
                    break
                try:
                    data = response.json()
                except Exception:
                    break

                releases = data.get("releases", data.get("results", []))
                if not releases:
                    break

                for release in releases:
                    ocid = release.get("ocid", release.get("id", ""))
                    if not ocid or ocid in seen_ids:
                        continue
                    seen_ids.add(ocid)

                    tender = release.get("tender", {})
                    title = tender.get("title", release.get("title", "No title"))
                    description = tender.get("description", release.get("description", ""))
                    matched = find_matching_keywords(f"{title} {description}") or [f"[search: {term}]"]

                    buyer_name = ""
                    for party in release.get("parties", []):
                        if "buyer" in party.get("roles", []):
                            buyer_name = party.get("name", "")
                            break

                    published_date = release.get("date", "")[:10] if release.get("date") else "Unknown"
                    deadline = tender.get("tenderPeriod", {}).get("endDate", "")
                    deadline = deadline[:10] if deadline else "Unknown"
                    value_obj = tender.get("value", {})
                    value = f"GBP {value_obj['amount']:,.0f}" if value_obj and value_obj.get("amount") else "Unknown"
                    notice_id = release.get("id", "")
                    url = f"https://www.find-tender.service.gov.uk/Notice/{notice_id}" if notice_id else "https://www.find-tender.service.gov.uk"

                    opportunities.append(Opportunity(
                        title=title, buyer=buyer_name or "Unknown", source="Find a Tender",
                        published=published_date, deadline=deadline, value=value,
                        description=description, url=url, matched_keywords=matched,
                    ))

            except requests.RequestException as e:
                print(f"  [!] Find a Tender error for '{term}': {e}")
                break

    print(f"  → Found {len(opportunities)} relevant results")
    return opportunities

# ─────────────────────────────────────────────
# OUTPUT
# ─────────────────────────────────────────────

def print_to_console(opportunities: list):
    if not opportunities:
        print("\n  No relevant opportunities found.")
        return
    print(f"\n{'='*70}")
    print(f"  FOUND {len(opportunities)} RELEVANT OPPORTUNITIES")
    print(f"{'='*70}")
    for i, opp in enumerate(sorted(opportunities, key=lambda x: x.published, reverse=True), 1):
        print(f"\n[{i}] {opp.title}")
        print(f"    Buyer:     {opp.buyer}")
        print(f"    Source:    {opp.source}")
        print(f"    Published: {opp.published}")
        print(f"    Deadline:  {opp.deadline}")
        print(f"    Value:     {opp.value}")
        print(f"    Keywords:  {', '.join(opp.matched_keywords)}")
        print(f"    URL:       {opp.url}")
        if opp.description:
            print(f"    Desc:      {opp.description[:200].replace(chr(10), ' ')}...")
        print(f"    {'-'*60}")

def save_to_csv(opportunities: list, filepath: str):
    if not opportunities:
        print("No results to save.")
        return
    fieldnames = ["Title", "Buyer", "Source", "Published", "Deadline", "Estimated Value", "Matched Keywords", "URL", "Description"]
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for opp in opportunities:
            writer.writerow(opp.to_dict())
    print(f"\nResults saved to: {filepath}")

def build_html_email(opportunities: list, days_back: int) -> str:
    today = datetime.now().strftime("%d %B %Y")
    rows = ""
    for opp in sorted(opportunities, key=lambda x: x.published, reverse=True):
        kw_tags = "".join(
            f'<span style="background:#e8f4f8;color:#1a6b8a;padding:2px 7px;border-radius:10px;font-size:11px;margin-right:4px;">{kw}</span>'
            for kw in opp.matched_keywords
        )
        snippet = (opp.description[:250].replace("\n", " ") + "...") if len(opp.description) > 250 else opp.description
        rows += f"""
        <tr style="border-bottom:1px solid #eee;">
          <td style="padding:14px 10px;vertical-align:top;">
            <a href="{opp.url}" style="font-weight:bold;color:#1a6b8a;text-decoration:none;font-size:14px;">{opp.title}</a><br>
            <span style="color:#666;font-size:12px;">{opp.buyer} &nbsp;|&nbsp; {opp.source}</span><br>
            <div style="margin:6px 0;">{kw_tags}</div>
            <p style="color:#444;font-size:12px;margin:4px 0 0;">{snippet}</p>
          </td>
          <td style="padding:14px 10px;vertical-align:top;white-space:nowrap;font-size:12px;color:#555;min-width:110px;">
            <b>Published:</b> {opp.published}<br><b>Deadline:</b> {opp.deadline}<br><b>Value:</b> {opp.value}
          </td>
        </tr>"""
    if not rows:
        rows = '<tr><td colspan="2" style="padding:20px;color:#888;text-align:center;">No matching opportunities found this period.</td></tr>'
    return f"""<html><body style="font-family:Arial,sans-serif;max-width:900px;margin:0 auto;color:#333;">
    <div style="background:#1a6b8a;color:white;padding:20px 24px;border-radius:6px 6px 0 0;">
      <h2 style="margin:0;">🔊 Acoustics & Air Quality Procurement Digest</h2>
      <p style="margin:4px 0 0;opacity:0.85;">{today} &nbsp;|&nbsp; {len(opportunities)} opportunities — last {days_back} day{'s' if days_back != 1 else ''}</p>
    </div>
    <table style="width:100%;border-collapse:collapse;background:white;border:1px solid #ddd;border-top:none;">{rows}</table>
    <p style="font-size:11px;color:#aaa;margin-top:12px;text-align:center;">Sources: Contracts Finder &amp; Find a Tender</p>
    </body></html>"""

def send_email_digest(opportunities: list, days_back: int):
    smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER", "")
    smtp_pass = os.getenv("SMTP_PASS", "")
    email_to = os.getenv("EMAIL_TO", smtp_user)

    if not smtp_user or not smtp_pass:
        print("\n[!] Email not configured. Set SMTP_USER and SMTP_PASS environment variables.")
        return

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"Procurement Digest — {len(opportunities)} opportunities — {datetime.now().strftime('%d %b %Y')}"
    msg["From"] = smtp_user
    msg["To"] = email_to
    msg.attach(MIMEText(build_html_email(opportunities, days_back), "html"))

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
    parser = argparse.ArgumentParser(description="UK Procurement Trawler — Acoustics & Air Quality")
    parser.add_argument("--days", type=int, default=7, help="Days to search back (default: 7)")
    parser.add_argument("--email", action="store_true", help="Send results as email digest")
    parser.add_argument("--output", type=str, help="Save results to CSV file")
    args = parser.parse_args()

    print(f"\n{'='*70}")
    print(f"  UK PROCUREMENT TRAWLER — Acoustics, NVH & Air Quality")
    print(f"  Searching the last {args.days} day(s)")
    print(f"  Run at: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*70}")

    all_results = fetch_contracts_finder(args.days) + fetch_find_a_tender(args.days)

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

    print_to_console(unique_results)

    if args.output:
        save_to_csv(unique_results, args.output)

    if args.email:
        try:
            from dotenv import load_dotenv
            load_dotenv()
        except ImportError:
            pass
        send_email_digest(unique_results, args.days)

if __name__ == "__main__":
    main()
