#!/usr/bin/env python3
"""
AUTO-CONVERTER
Transforms raw Dutchie + Weedmaps scraper output into the dashboard-ready format.

Usage:
    python convert.py                          # Auto-finds latest scrape files
    python convert.py --dutchie d.json --weedmaps w.json   # Specify files
    python convert.py --output my_data.json    # Custom output name
"""

import json
import os
import sys
import argparse
from datetime import datetime
from collections import defaultdict


def find_latest_file(data_dir, prefix):
    """Find the most recent file matching a prefix."""
    if not os.path.exists(data_dir):
        return None
    files = [f for f in sorted(os.listdir(data_dir), reverse=True) if f.startswith(prefix)]
    return os.path.join(data_dir, files[0]) if files else None


def load_json(path):
    """Load a JSON file."""
    if not path or not os.path.exists(path):
        return {}
    with open(path) as f:
        return json.load(f)


def convert(dutchie_data, weedmaps_data, affinity_names=None):
    """
    Convert raw scraper output into dashboard format.

    Dashboard format:
    {
        "products": [
            {
                "name": "Product Name",
                "brand": "Brand",
                "category": "Flower",
                "weight": "3.5g",
                "dispensaries": {
                    "Dispensary A": 45.00,
                    "Dispensary B": 50.00
                }
            }
        ],
        "deals": [...]
    }
    """
    if affinity_names is None:
        affinity_names = ["Affinity - New Haven", "Affinity - Bridgeport"]

    # Step 1: Collect all products from all dispensaries
    # Key = "brand::name::category" → { dispensary: price }
    product_map = defaultdict(lambda: {"brand": "", "category": "", "weight": "", "dispensaries": {}})

    # Process Dutchie data
    for disp_name, data in dutchie_data.items():
        if isinstance(data, dict) and "error" in data:
            print(f"  ⚠ Skipping {disp_name} (scrape error)")
            continue

        products = []
        if isinstance(data, dict):
            products = data.get("products", [])

        for p in products:
            name = (p.get("name") or "").strip()
            brand = (p.get("brand") or "Unknown").strip()
            category = (p.get("category") or "Other").strip()

            if not name:
                continue

            # Determine price (prefer rec, fall back to med, then general)
            price = p.get("price_rec") or p.get("price_med") or p.get("price")
            if not price:
                # Try price_ranges
                ranges = p.get("price_ranges", [])
                if ranges:
                    price = ranges[0].get("recPrice") or ranges[0].get("price")

            if not price or price <= 0:
                continue

            # Determine weight from price ranges or name
            weight = ""
            ranges = p.get("price_ranges", [])
            if ranges:
                weight = ranges[0].get("weight", "")
            if not weight:
                # Try to extract from product name
                for w in ["3.5g", "1g", "0.5g", "0.3g", "7g", "14g", "28g", "100mg", "200mg", "300mg", "30ml"]:
                    if w in name.lower().replace(" ", ""):
                        weight = w
                        break

            key = f"{brand}::{name}::{category}".lower()
            product_map[key]["brand"] = brand
            product_map[key]["name"] = name
            product_map[key]["category"] = category
            if weight:
                product_map[key]["weight"] = weight
            product_map[key]["dispensaries"][disp_name] = round(float(price), 2)

    # Process Weedmaps data
    for disp_name, data in weedmaps_data.items():
        if isinstance(data, dict) and "error" in data:
            print(f"  ⚠ Skipping {disp_name} (scrape error)")
            continue

        products = []
        if isinstance(data, dict):
            products = data.get("products", [])

        for p in products:
            name = (p.get("name") or "").strip()
            brand = (p.get("brand") or "Unknown").strip()
            category = (p.get("category") or "Other").strip()

            if not name:
                continue

            price = p.get("price_min") or p.get("price")
            if not price or price <= 0:
                continue

            weight = ""
            prices_list = p.get("prices", [])
            if prices_list:
                weight = prices_list[0].get("size", "")

            key = f"{brand}::{name}::{category}".lower()
            product_map[key]["brand"] = brand
            product_map[key]["name"] = name
            product_map[key]["category"] = category
            if weight:
                product_map[key]["weight"] = weight
            product_map[key]["dispensaries"][disp_name] = round(float(price), 2)

    # Step 2: Filter to products found at 2+ dispensaries (useful comparisons)
    products = []
    for key, data in product_map.items():
        if len(data["dispensaries"]) >= 2:
            products.append({
                "name": data["name"],
                "brand": data["brand"],
                "category": data["category"],
                "weight": data["weight"],
                "dispensaries": data["dispensaries"],
            })

    # Sort by category then name
    products.sort(key=lambda p: (p["category"], p["name"]))

    # Step 3: Extract deals
    deals = []

    for disp_name, data in dutchie_data.items():
        if isinstance(data, dict) and "error" not in data:
            for s in data.get("specials", []):
                deals.append({
                    "dispensary": disp_name,
                    "title": s.get("name", "Special"),
                    "type": _classify_deal_type(s),
                    "amount": s.get("discount"),
                    "category": "All",
                    "expires": s.get("endDate"),
                })

    for disp_name, data in weedmaps_data.items():
        if isinstance(data, dict) and "error" not in data:
            for d in data.get("deals", []):
                deals.append({
                    "dispensary": disp_name,
                    "title": d.get("title", "Deal"),
                    "type": d.get("discount_type", "other"),
                    "amount": d.get("discount_amount"),
                    "category": "All",
                    "expires": d.get("end_date"),
                })

    return {
        "scraped_at": datetime.now().isoformat(),
        "products": products,
        "deals": deals,
        "stats": {
            "total_products_raw": sum(
                len(d.get("products", [])) for d in list(dutchie_data.values()) + list(weedmaps_data.values()) if isinstance(d, dict) and "error" not in d
            ),
            "comparable_products": len(products),
            "dispensaries_with_data": len(set(
                d for p in products for d in p["dispensaries"]
            )),
            "total_deals": len(deals),
        },
    }


def _classify_deal_type(special):
    """Classify a Dutchie special into a deal type."""
    name = (special.get("name") or "").lower()
    stype = (special.get("type") or "").lower()
    if "percent" in stype or "%" in name:
        return "percent"
    if "dollar" in stype or "$" in name:
        return "dollar"
    if "bogo" in name or "buy" in name:
        return "bogo"
    return "other"


def main():
    parser = argparse.ArgumentParser(description="Convert scraper output to dashboard format")
    parser.add_argument("--dutchie", type=str, help="Path to Dutchie scrape JSON")
    parser.add_argument("--weedmaps", type=str, help="Path to Weedmaps scrape JSON")
    parser.add_argument("--output", type=str, default=None, help="Output file path")
    parser.add_argument("--data-dir", type=str, default="data", help="Data directory")
    args = parser.parse_args()

    print("\n🔄 AUTO-CONVERTER")
    print("=" * 50)

    # Find input files
    dutchie_path = args.dutchie or find_latest_file(args.data_dir, "dutchie_scrape")
    weedmaps_path = args.weedmaps or find_latest_file(args.data_dir, "weedmaps_scrape")

    print(f"  Dutchie file:  {dutchie_path or 'NOT FOUND'}")
    print(f"  Weedmaps file: {weedmaps_path or 'NOT FOUND'}")

    if not dutchie_path and not weedmaps_path:
        print("\n⚠ No scrape data found. Run the scraper first: python run.py")
        sys.exit(1)

    dutchie_data = load_json(dutchie_path)
    weedmaps_data = load_json(weedmaps_path)

    print(f"\n  Dutchie dispensaries:  {len(dutchie_data)}")
    print(f"  Weedmaps dispensaries: {len(weedmaps_data)}")

    # Convert
    result = convert(dutchie_data, weedmaps_data)

    print(f"\n  ✓ {result['stats']['total_products_raw']} total products scraped")
    print(f"  ✓ {result['stats']['comparable_products']} comparable products (found at 2+ stores)")
    print(f"  ✓ {result['stats']['dispensaries_with_data']} dispensaries with data")
    print(f"  ✓ {result['stats']['total_deals']} active deals")

    # Save
    out = args.output or os.path.join(
        args.data_dir,
        f"dashboard_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    )
    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
    with open(out, "w") as f:
        json.dump(result, f, indent=2, default=str)

    print(f"\n  ✅ Dashboard-ready file saved to: {out}")
    print(f"     Upload this file into the dashboard's Import panel.")


if __name__ == "__main__":
    main()
