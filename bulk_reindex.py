#!/usr/bin/env python3
"""
bulk_reindex.py — Daily multi-site submission to Google's Indexing API.

Built for the MimusJobs country network. Reads a list of sites from
sites_config.csv (one sitemap per site), pulls every /job/ URL from each,
and submits new/never-submitted ones to Google's Indexing API -- spread
fairly across all sites so no single site can eat the whole daily quota.

Google's Indexing API officially supports JobPosting and BroadcastEvent
pages, which is exactly what MimusJobs job pages are.

------------------------------------------------------------------------------
ONE-TIME SETUP
------------------------------------------------------------------------------
1. Google Cloud Console -> create/select a project.
2. APIs & Services -> Library -> enable "Web Search Indexing API".
3. APIs & Services -> Credentials -> Create Credentials -> Service Account.
4. Service account -> Keys -> Add Key -> JSON. Save it (e.g. service-account.json).
5. For EVERY site/property in sites_config.csv:
   Search Console -> select that property -> Settings -> Users and permissions
   -> Add user -> paste the service account's email
   (xxxxx@your-project.iam.gserviceaccount.com) -> permission: Owner.
   The API call for a site will fail with 403 if this step is skipped for it.
6. pip install google-auth requests --break-system-packages

------------------------------------------------------------------------------
HOW THE DAILY QUOTA IS DIVIDED
------------------------------------------------------------------------------
Google's Indexing API quota (default 200/day) is per Google Cloud PROJECT,
not per site -- so all 184 sites share one pool if they use the same service
account/project. This script:

  1. Builds a queue of not-yet-submitted URLs for EVERY site.
  2. Goes round-robin: site 1's next URL, site 2's next URL, site 3's next
     URL... and loops back to site 1, until either the daily quota is hit
     or every site's queue is empty.
  3. Remembers what it already submitted (data/state.json) so tomorrow's run
     never resubmits the same URL -- it naturally works through backlogs
     over several days, and once caught up just handles each day's new
     postings (which is a small trickle compared to the 200/day quota).

This means on a big backlog day, every site gets a roughly equal slice of
today's quota rather than the first few sites in the file eating it all.

------------------------------------------------------------------------------
USAGE
------------------------------------------------------------------------------
python3 bulk_reindex.py --key service-account.json --sites-config sites_config.csv

# Cap total daily submissions (default 190, keeping headroom under 200)
python3 bulk_reindex.py --key service-account.json --sites-config sites_config.csv --daily-quota 190

# See what would be submitted without calling the API
python3 bulk_reindex.py --key service-account.json --sites-config sites_config.csv --dry-run

# Less console output (still logs everything to CSV + state.json)
python3 bulk_reindex.py --key service-account.json --sites-config sites_config.csv --quiet

Files this produces/uses:
- data/state.json       Persistent record of every URL ever submitted + result.
                         Committed back to the repo by the GitHub Action so
                         state survives between daily runs.
- reindex_log.csv        This run's submissions only (url, site, result, timestamp).
"""

import argparse
import csv
import json
import os
import sys
import time
import xml.etree.ElementTree as ET
from collections import deque
from datetime import datetime, timezone

import requests

try:
    from google.oauth2 import service_account
    from google.auth.transport.requests import AuthorizedSession
except ImportError:
    print("Missing dependency. Run: pip install google-auth requests --break-system-packages")
    sys.exit(1)

GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
RESET = "\033[0m"

SCOPES = ["https://www.googleapis.com/auth/indexing"]
ENDPOINT = "https://indexing.googleapis.com/v3/urlNotifications:publish"

DEFAULT_DAILY_QUOTA = 190
DELAY_SECONDS = 1.0
STATE_PATH_DEFAULT = "data/state.json"

# A generic python-requests user-agent looks like bot traffic to Cloudflare
# and can get 403'd by the same kind of rules covered earlier in this chat.
# This is honest about what it is (a script, not a browser) but won't be
# auto-flagged as a scraper by default WAF heuristics.
REQUEST_HEADERS = {
    "User-Agent": "MimusJobs-Reindexer/1.0 (+https://mimusjobs.com; automated sitemap fetch for Google indexing)"
}


def divider():
    print(f"{CYAN}{'-' * 70}{RESET}")


def load_sites_config(path):
    sites = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(row for row in f if row.strip() and not row.strip().startswith("#"))
        for row in reader:
            name = row["site_name"].strip()
            sitemap = row["sitemap_url"].strip()
            if name and sitemap:
                sites.append((name, sitemap))
    return sites


def load_urls_from_sitemap(sitemap_url, filter_contains="/job/"):
    """Handles both a sitemap index and a plain urlset. Keeps only job URLs."""
    urls = []
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    resp = requests.get(sitemap_url, timeout=30, headers=REQUEST_HEADERS)
    resp.raise_for_status()
    root = ET.fromstring(resp.content)

    if root.tag.lower().endswith("sitemapindex"):
        sub_sitemaps = [loc.text.strip() for loc in root.findall(".//sm:loc", ns)]
        for sub in sub_sitemaps:
            try:
                sub_resp = requests.get(sub, timeout=30, headers=REQUEST_HEADERS)
                sub_resp.raise_for_status()
                sub_root = ET.fromstring(sub_resp.content)
                for loc in sub_root.findall(".//sm:loc", ns):
                    u = loc.text.strip()
                    if filter_contains in u:
                        urls.append(u)
            except Exception as e:
                print(f"{YELLOW}    Skipping sub-sitemap {sub}: {e}{RESET}")
    else:
        for loc in root.findall(".//sm:loc", ns):
            u = loc.text.strip()
            if filter_contains in u:
                urls.append(u)

    return urls


def load_state(path):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_state(path, state):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, sort_keys=True)


def get_session(key_path):
    print(f"\n{CYAN}{'=' * 70}{RESET}")
    print(f"{CYAN}AUTHENTICATING WITH GOOGLE{RESET}")
    print(f"{CYAN}{'=' * 70}{RESET}")

    if not os.path.exists(key_path):
        print(f"{RED}Key file not found at: {key_path}{RESET}")
        print(f"{RED}Check that the GOOGLE_INDEXING_KEY secret was written correctly, "
              f"or that --key points to the right path.{RESET}")
        sys.exit(1)

    print(f"  Reading key file: {key_path}")

    try:
        with open(key_path, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception as e:
        print(f"{RED}  Could not parse key file as JSON: {e}{RESET}")
        print(f"{RED}  This usually means the secret wasn't written correctly "
              f"(e.g. quoting issue when echoing it to a file).{RESET}")
        sys.exit(1)

    # Only print non-secret identifying fields. NEVER print private_key.
    client_email = raw.get("client_email", "unknown")
    project_id = raw.get("project_id", "unknown")
    print(f"  Key file parsed successfully.")
    print(f"  Service account email: {GREEN}{client_email}{RESET}")
    print(f"  Google Cloud project:  {project_id}")

    try:
        creds = service_account.Credentials.from_service_account_file(key_path, scopes=SCOPES)
        session = AuthorizedSession(creds)
    except Exception as e:
        print(f"{RED}  Failed to build authenticated session: {e}{RESET}")
        sys.exit(1)

    print(f"{GREEN}  Authenticated session created successfully.{RESET}")
    print(f"{CYAN}{'=' * 70}{RESET}\n")

    return session


def submit_url(session, url, verbose=True):
    payload = {"url": url, "type": "URL_UPDATED"}

    if verbose:
        divider()
        print(f"{CYAN}REQUEST{RESET}  POST {ENDPOINT}")
        print(f"  Body: {json.dumps(payload)}")

    resp = session.post(ENDPOINT, json=payload, timeout=30)

    if verbose:
        print(f"{CYAN}RESPONSE{RESET}  Status: {resp.status_code}")
        try:
            parsed = resp.json()
            print(f"  Body:")
            for line in json.dumps(parsed, indent=2).splitlines():
                print(f"    {line}")
            meta = parsed.get("urlNotificationMetadata")
            if meta and meta.get("latestUpdate"):
                latest = meta["latestUpdate"]
                print(f"  {GREEN}Confirmed by Google — notifyTime: "
                      f"{latest.get('notifyTime', 'unknown')} "
                      f"(type: {latest.get('type', 'unknown')}){RESET}")
        except ValueError:
            print(f"  Body (non-JSON): {resp.text[:500]}")

    return resp.status_code, resp.text


def build_round_robin_queue(per_site_pending, daily_quota):
    """
    per_site_pending: dict site_name -> deque of URLs not yet submitted.
    Returns a flat list of (site_name, url), fairly interleaved, capped at
    daily_quota, skipping sites whose queue has run dry.
    """
    queues = {name: deque(urls) for name, urls in per_site_pending.items() if urls}
    order = list(queues.keys())
    result = []

    while order and len(result) < daily_quota:
        next_order = []
        for name in order:
            if len(result) >= daily_quota:
                break
            q = queues[name]
            if q:
                result.append((name, q.popleft()))
            if q:  # still has items after popping -> keep in rotation
                next_order.append(name)
        order = next_order

    return result


def main():
    parser = argparse.ArgumentParser(description="Daily multi-site submission to Google's Indexing API.")
    parser.add_argument("--key", required=True, help="Path to service account JSON key file")
    parser.add_argument("--sites-config", required=True, help="CSV file: site_name,sitemap_url")
    parser.add_argument("--filter", default="/job/", help="Only submit sitemap URLs containing this substring (default: /job/)")
    parser.add_argument("--daily-quota", type=int, default=DEFAULT_DAILY_QUOTA, help="Max submissions this run (default 190)")
    parser.add_argument("--state", default=STATE_PATH_DEFAULT, help="Path to persistent state JSON (default: data/state.json)")
    parser.add_argument("--log", default="reindex_log.csv", help="Path to this run's CSV log")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be submitted, don't call the API")
    parser.add_argument("--quiet", action="store_true", help="Suppress full request/response detail")
    args = parser.parse_args()
    verbose = not args.quiet

    sites = load_sites_config(args.sites_config)
    if not sites:
        print(f"{RED}No sites found in {args.sites_config}.{RESET}")
        sys.exit(1)
    print(f"{CYAN}Loaded {len(sites)} site(s) from {args.sites_config}.{RESET}")

    # Authenticate FIRST, before spending time fetching 184 sitemaps --
    # if the key is bad, you find out in seconds instead of after a long wait.
    # Skipped for --dry-run since no submission will happen anyway.
    session = None
    if not args.dry_run:
        session = get_session(args.key)

    state = load_state(args.state)

    # ---- Discover pending (never-submitted-successfully) URLs per site ----
    per_site_pending = {}
    total_discovered = 0
    for name, sitemap in sites:
        try:
            urls = load_urls_from_sitemap(sitemap, filter_contains=args.filter)
        except Exception as e:
            print(f"{RED}  [{name}] Failed to read sitemap {sitemap}: {e}{RESET}")
            continue

        pending = [u for u in urls if state.get(u, {}).get("result") != "success"]
        per_site_pending[name] = pending
        total_discovered += len(urls)
        print(f"  [{name}] {len(urls)} job URL(s) in sitemap, {len(pending)} still pending submission.")

    divider()
    total_pending = sum(len(v) for v in per_site_pending.values())
    print(f"{CYAN}Total job URLs across all sites: {total_discovered}. Pending (never successfully submitted): {total_pending}.{RESET}")

    queue = build_round_robin_queue(per_site_pending, args.daily_quota)
    print(f"{CYAN}This run will submit {len(queue)} URL(s) (daily quota cap: {args.daily_quota}), "
          f"round-robin across sites.{RESET}")

    if args.dry_run:
        for site, url in queue:
            print(f"{YELLOW}[DRY RUN]{RESET} [{site}] would submit: {url}")
        print(f"{CYAN}Dry run complete. {len(queue)} URL(s) would have been submitted.{RESET}")
        return

    if not queue:
        print(f"{GREEN}Nothing to submit -- all sites are fully caught up.{RESET}")
        return

    log_rows = []
    success_count = 0
    fail_count = 0

    for i, (site, url) in enumerate(queue, start=1):
        if verbose:
            print(f"\n{CYAN}[{i}/{len(queue)}] [{site}]{RESET} {url}")
        timestamp = datetime.now(timezone.utc).isoformat()
        try:
            status, body = submit_url(session, url, verbose=verbose)

            if status == 200:
                print(f"{GREEN}[{i}/{len(queue)}] [{site}] OK{RESET}  {url}")
                state[url] = {"result": "success", "status": status, "site": site, "timestamp": timestamp}
                log_rows.append([site, url, "success", status, timestamp])
                success_count += 1
            elif status == 429:
                print(f"{RED}[{i}/{len(queue)}] [{site}] QUOTA EXCEEDED — stopping for today.{RESET}")
                state[url] = {"result": "quota_exceeded", "status": status, "site": site, "timestamp": timestamp}
                log_rows.append([site, url, "quota_exceeded", status, timestamp])
                break
            else:
                print(f"{RED}[{i}/{len(queue)}] [{site}] FAILED ({status}){RESET}  {url}  -> {body[:200]}")
                state[url] = {"result": "failed", "status": status, "site": site, "timestamp": timestamp}
                log_rows.append([site, url, "failed", status, timestamp])
                fail_count += 1

        except Exception as e:
            print(f"{RED}[{i}/{len(queue)}] [{site}] ERROR{RESET}  {url}  -> {e}")
            state[url] = {"result": "error", "status": str(e), "site": site, "timestamp": timestamp}
            log_rows.append([site, url, "error", str(e), timestamp])
            fail_count += 1

        # Save state after every single URL, not just at the end -- if the
        # job gets killed/times out mid-run, already-submitted URLs are
        # still recorded and won't be resubmitted tomorrow.
        save_state(args.state, state)
        time.sleep(DELAY_SECONDS)

    with open(args.log, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["site", "url", "result", "status_or_error", "timestamp_utc"])
        writer.writerows(log_rows)

    divider()
    print(f"{CYAN}Done.{RESET} {GREEN}{success_count} succeeded{RESET}, {RED}{fail_count} failed{RESET}. "
          f"Log: {args.log} | State: {args.state}")

    remaining = total_pending - success_count
    if remaining > 0:
        print(f"{YELLOW}{remaining} URL(s) still pending across all sites -- tomorrow's run will pick up where this left off.{RESET}")


if __name__ == "__main__":
    main()
