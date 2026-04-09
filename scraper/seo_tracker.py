#!/usr/bin/env python3
"""
SEO RANK TRACKER
Monitors where Affinity ranks in Google search results for target keywords
vs competitors. Uses Google Custom Search API (free tier: 100 queries/day).

Usage:
    python seo_tracker.py                    # Run full keyword check
    python seo_tracker.py --keyword "dispensary new haven"  # Single keyword
    python seo_tracker.py --report           # Show latest rankings

SETUP:
    1. Go to https://programmablesearchengine.google.com/
    2. Create a new search engine, set it to search the whole web
    3. Copy the Search Engine ID (cx)
    4. Go to https://console.cloud.google.com/
    5. Enable "Custom Search API"
    6. Create an API key
    7. Paste both values below
"""

import json
import os
import sys
import time
import argparse
import requests
from datetime import datetime

# ═══════════════════════════════════════════════════════════════
# CONFIG — EDIT THESE
# ═══════════════════════════════════════════════════════════════

GOOGLE_API_KEY = "YOUR_GOOGLE_API_KEY"           # ← Change this
GOOGLE_CX = "YOUR_SEARCH_ENGINE_ID"              # ← Change this

# Your domains to track
YOUR_DOMAINS = [
    "affinitydispensary.com",
    "affinity",  # catches subdomains and directory listings
]

# Keywords to track — these get checked against Google
TARGET_KEYWORDS = [
    # High-intent local keywords
    "dispensary new haven ct",
    "dispensary bridgeport ct",
    "cannabis dispensary near me ct",
    "best dispensary new haven",
    "best dispensary connecticut",
    "cheapest dispensary ct",
    "medical marijuana new haven ct",
    "recreational dispensary new haven",
    "weed delivery new haven ct",

    # Product keywords
    "buy edibles new haven ct",
    "buy flower new haven ct",
    "thc gummies connecticut",
    "pre rolls new haven ct",
    "vape carts connecticut",

    # Competitor conquest keywords
    "rise dispensary orange ct",
    "fine fettle dispensary ct",
    "zen leaf dispensary ct",
    "curaleaf connecticut",
    "sweetspot dispensary ct",
    "higher collective bridgeport",
    "insa dispensary new haven",
    "lit new haven cannabis",

    # Brand keywords
    "affinity dispensary",
    "affinity dispensary new haven",
    "affinity dispensary bridgeport",
    "affinity cannabis ct",
]

# Competitor domains to track in results
COMPETITOR_DOMAINS = {
    "risecannabis.com": "RISE",
    "finefettle.com": "Fine Fettle",
    "zenleafdispensaries.com": "Zen Leaf",
    "curaleaf.com": "Curaleaf",
    "sweetspotfarms.com": "Sweetspot",
    "highercollective.com": "Higher Collective",
    "insacannabis.com": "Insa",
    "shangri-la.com": "Shangri-La",
    "budr.com": "Budr",
    "trulieve.com": "Trulieve",
    "weedmaps.com": "Weedmaps",
    "leafly.com": "Leafly",
    "dutchie.com": "Dutchie",
}

# ═══════════════════════════════════════════════════════════════


def search_google(query, api_key, cx, num_results=10):
    """Query Google Custom Search API."""
    url = "https://www.googleapis.com/customsearch/v1"
    params = {
        "key": api_key,
        "cx": cx,
        "q": query,
        "num": min(num_results, 10),
        "gl": "us",
        "lr": "lang_en",
    }

    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return data.get("items", [])
    except requests.RequestException as e:
        print(f"  ✗ Search failed for '{query}': {e}")
        return []


def check_keyword(keyword, api_key, cx):
    """Check rankings for a single keyword."""
    results = search_google(keyword, api_key, cx)

    ranking = {
        "keyword": keyword,
        "checked_at": datetime.now().isoformat(),
        "your_position": None,
        "your_url": None,
        "competitors_found": [],
        "top_10": [],
    }

    for i, item in enumerate(results, 1):
        url = item.get("link", "")
        title = item.get("title", "")
        domain = url.split("/")[2] if len(url.split("/")) > 2 else ""

        ranking["top_10"].append({
            "position": i,
            "title": title,
            "url": url,
            "domain": domain,
        })

        # Check if it's us
        if any(yd in url.lower() for yd in YOUR_DOMAINS):
            if ranking["your_position"] is None:  # First occurrence
                ranking["your_position"] = i
                ranking["your_url"] = url

        # Check if it's a competitor
        for comp_domain, comp_name in COMPETITOR_DOMAINS.items():
            if comp_domain in domain.lower():
                ranking["competitors_found"].append({
                    "name": comp_name,
                    "position": i,
                    "url": url,
                })

    return ranking


def run_full_check(keywords=None, api_key=None, cx=None):
    """Run rankings check for all target keywords."""
    api_key = api_key or GOOGLE_API_KEY
    cx = cx or GOOGLE_CX

    if "YOUR_" in api_key or "YOUR_" in cx:
        print("  ⚠ Google API credentials not configured.")
        print("  Edit seo_tracker.py and add your API key and Search Engine ID.")
        print("  See the setup instructions at the top of the file.")
        return None

    keywords = keywords or TARGET_KEYWORDS
    all_rankings = []

    print(f"\n  Checking {len(keywords)} keywords...")
    print(f"  (Free tier: 100 queries/day)\n")

    for i, kw in enumerate(keywords, 1):
        print(f"  [{i}/{len(keywords)}] \"{kw}\"", end="")
        ranking = check_keyword(kw, api_key, cx)

        pos = ranking["your_position"]
        if pos:
            print(f"  → You: #{pos}", end="")
        else:
            print(f"  → You: NOT FOUND", end="")

        comps = ranking["competitors_found"]
        if comps:
            comp_str = ", ".join(f"{c['name']}:#{c['position']}" for c in comps[:3])
            print(f"  | Competitors: {comp_str}")
        else:
            print()

        all_rankings.append(ranking)
        time.sleep(1.5)  # Rate limit

    return {
        "checked_at": datetime.now().isoformat(),
        "total_keywords": len(keywords),
        "rankings": all_rankings,
        "summary": _build_summary(all_rankings),
    }


def _build_summary(rankings):
    """Build a summary of the ranking results."""
    ranked = [r for r in rankings if r["your_position"] is not None]
    not_ranked = [r for r in rankings if r["your_position"] is None]

    positions = [r["your_position"] for r in ranked]
    avg_pos = sum(positions) / len(positions) if positions else None

    # Count competitor appearances
    comp_counts = {}
    for r in rankings:
        for c in r["competitors_found"]:
            name = c["name"]
            comp_counts[name] = comp_counts.get(name, 0) + 1

    return {
        "keywords_ranked": len(ranked),
        "keywords_not_ranked": len(not_ranked),
        "average_position": round(avg_pos, 1) if avg_pos else None,
        "top_3_count": len([p for p in positions if p <= 3]),
        "top_10_count": len(ranked),
        "not_ranked_keywords": [r["keyword"] for r in not_ranked],
        "competitor_appearances": dict(sorted(comp_counts.items(), key=lambda x: -x[1])),
    }


def print_report(data):
    """Print a nice report from ranking data."""
    if not data:
        return

    s = data["summary"]
    print("\n" + "=" * 60)
    print("  📊 SEO RANKING REPORT")
    print(f"  📅 {data['checked_at'][:10]}")
    print("=" * 60)

    print(f"\n  Keywords tracked:     {data['total_keywords']}")
    print(f"  You rank in top 10:   {s['top_10_count']}")
    print(f"  You rank in top 3:    {s['top_3_count']}")
    print(f"  Not ranking:          {s['keywords_not_ranked']}")
    if s["average_position"]:
        print(f"  Average position:     #{s['average_position']}")

    print(f"\n  {'KEYWORD':<40} {'YOUR POS':<10} {'TOP COMPETITOR'}")
    print(f"  {'─'*40} {'─'*10} {'─'*30}")

    for r in data["rankings"]:
        pos = f"#{r['your_position']}" if r["your_position"] else "—"
        comp = ""
        if r["competitors_found"]:
            c = r["competitors_found"][0]
            comp = f"{c['name']} #{c['position']}"
        print(f"  {r['keyword']:<40} {pos:<10} {comp}")

    if s["not_ranked_keywords"]:
        print(f"\n  ⚠ NOT RANKING FOR:")
        for kw in s["not_ranked_keywords"]:
            print(f"    → {kw}")

    if s["competitor_appearances"]:
        print(f"\n  COMPETITOR VISIBILITY (appearances in top 10):")
        for name, count in s["competitor_appearances"].items():
            bar = "█" * count
            print(f"    {name:<20} {bar} {count}")


def main():
    parser = argparse.ArgumentParser(description="Affinity SEO Rank Tracker")
    parser.add_argument("--keyword", type=str, help="Check a single keyword")
    parser.add_argument("--report", action="store_true", help="Show latest report")
    parser.add_argument("--data-dir", type=str, default="data", help="Data directory")
    args = parser.parse_args()

    if args.report:
        files = sorted(
            [f for f in os.listdir(args.data_dir) if f.startswith("seo_rankings")],
            reverse=True
        )
        if files:
            with open(os.path.join(args.data_dir, files[0])) as f:
                data = json.load(f)
            print_report(data)
        else:
            print("  No ranking data found. Run: python seo_tracker.py")
        return

    print("\n🔍 AFFINITY SEO RANK TRACKER")
    print("=" * 50)

    keywords = [args.keyword] if args.keyword else None
    results = run_full_check(keywords)

    if results:
        print_report(results)

        outfile = os.path.join(
            args.data_dir,
            f"seo_rankings_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        os.makedirs(args.data_dir, exist_ok=True)
        with open(outfile, "w") as f:
            json.dump(results, f, indent=2, default=str)
        print(f"\n  ✅ Rankings saved to {outfile}")


if __name__ == "__main__":
    main()
