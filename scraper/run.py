#!/usr/bin/env python3
"""
AFFINITY COMPETITIVE INTELLIGENCE — Main Runner
Scrapes all competitor dispensary menus and generates price comparison reports.

Usage:
    python run.py                  # Full scrape + report
    python run.py --dutchie        # Dutchie dispensaries only
    python run.py --weedmaps       # Weedmaps dispensaries only
    python run.py --report         # Generate report from latest data (no scraping)
    python run.py --single SLUG    # Scrape a single dispensary by Dutchie slug
"""

import argparse
import json
import os
import sys
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import COMPETITORS
from scrapers.dutchie_scraper import scrape_all_dutchie, DutchieScraper
from scrapers.weedmaps_scraper import scrape_all_weedmaps, WeedmapsScraper
from scrapers.analyzer import generate_report


def ensure_dirs():
    os.makedirs("data", exist_ok=True)


def save_results(results, prefix):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    outfile = f"data/{prefix}_scrape_{ts}.json"
    with open(outfile, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\n✓ Saved to {outfile}")
    return outfile


def main():
    parser = argparse.ArgumentParser(description="Affinity Competitive Intelligence Scraper")
    parser.add_argument("--dutchie", action="store_true", help="Scrape Dutchie dispensaries only")
    parser.add_argument("--weedmaps", action="store_true", help="Scrape Weedmaps dispensaries only")
    parser.add_argument("--report", action="store_true", help="Generate report from existing data")
    parser.add_argument("--single", type=str, help="Scrape a single Dutchie dispensary by slug")
    args = parser.parse_args()

    ensure_dirs()

    print("\n" + "=" * 60)
    print("  🌿 AFFINITY COMPETITIVE INTELLIGENCE SCRAPER")
    print(f"  📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  📍 {len(COMPETITORS)} competitors configured")
    print("=" * 60)

    if args.report:
        generate_report()
        return

    if args.single:
        scraper = DutchieScraper()
        data = scraper.scrape_dispensary(args.single)
        save_results({args.single: data}, "single")
        return

    if args.dutchie or (not args.dutchie and not args.weedmaps):
        print("\n▸ Phase 1: Scraping Dutchie-powered dispensaries...")
        dutchie_results = scrape_all_dutchie(COMPETITORS)
        save_results(dutchie_results, "dutchie")

    if args.weedmaps or (not args.dutchie and not args.weedmaps):
        print("\n▸ Phase 2: Scraping Weedmaps dispensaries...")
        weedmaps_results = scrape_all_weedmaps(COMPETITORS)
        save_results(weedmaps_results, "weedmaps")

    if not args.dutchie and not args.weedmaps:
        print("\n▸ Phase 3: Generating competitive report...")
        generate_report()

        print("\n▸ Phase 4: Converting to dashboard format...")
        try:
            from convert import find_latest_file, load_json, convert as convert_data
            d_file = find_latest_file("data", "dutchie_scrape")
            w_file = find_latest_file("data", "weedmaps_scrape")
            result = convert_data(load_json(d_file), load_json(w_file))
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            outfile = f"data/dashboard_data_{ts}.json"
            with open(outfile, "w") as f:
                json.dump(result, f, indent=2, default=str)
            print(f"  ✓ Dashboard data saved to {outfile}")
            print(f"  → Upload this file to your dashboard website")
        except Exception as e:
            print(f"  ⚠ Converter error: {e}")

    print("\n" + "=" * 60)
    print("  ✅ SCRAPE COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
