"""
Price Analysis Engine
Analyzes scraped data to produce competitive intelligence reports.
"""

import json
import os
from datetime import datetime
from collections import defaultdict


def load_latest_scrape(data_dir="data"):
    """Load the most recent scrape files."""
    files = sorted(os.listdir(data_dir), reverse=True)
    dutchie_data = {}
    weedmaps_data = {}

    for f in files:
        if f.startswith("dutchie_scrape") and not dutchie_data:
            with open(os.path.join(data_dir, f)) as fh:
                dutchie_data = json.load(fh)
        elif f.startswith("weedmaps_scrape") and not weedmaps_data:
            with open(os.path.join(data_dir, f)) as fh:
                weedmaps_data = json.load(fh)
        if dutchie_data and weedmaps_data:
            break

    return dutchie_data, weedmaps_data


def normalize_all_products(dutchie_data, weedmaps_data):
    """Merge all scraped data into a unified product format."""
    unified = []

    for disp_name, data in dutchie_data.items():
        if "error" in data:
            continue
        for p in data.get("products", []):
            unified.append({
                "dispensary": disp_name,
                "platform": "dutchie",
                "name": p.get("name", "").strip(),
                "brand": p.get("brand", "Unknown"),
                "category": p.get("category"),
                "strain_type": p.get("strain_type"),
                "price": p.get("price_rec") or p.get("price_med"),
                "price_med": p.get("price_med"),
                "price_rec": p.get("price_rec"),
                "thc": p.get("thc"),
                "cbd": p.get("cbd"),
                "on_special": p.get("on_special", False),
                "special_discount": p.get("special_discount"),
            })

    for disp_name, data in weedmaps_data.items():
        if "error" in data:
            continue
        for p in data.get("products", []):
            unified.append({
                "dispensary": disp_name,
                "platform": "weedmaps",
                "name": p.get("name", "").strip(),
                "brand": p.get("brand", "Unknown"),
                "category": p.get("category"),
                "strain_type": p.get("strain_type"),
                "price": p.get("price_min"),
                "price_min": p.get("price_min"),
                "price_max": p.get("price_max"),
                "thc": p.get("thc"),
                "cbd": p.get("cbd"),
                "on_special": False,
            })

    return unified


def price_comparison_by_category(products):
    """Compare average prices across dispensaries by category."""
    cat_prices = defaultdict(lambda: defaultdict(list))

    for p in products:
        if p["price"] and p["category"]:
            cat_prices[p["category"]][p["dispensary"]].append(p["price"])

    report = {}
    for cat, disps in cat_prices.items():
        report[cat] = {}
        for disp, prices in disps.items():
            report[cat][disp] = {
                "avg_price": round(sum(prices) / len(prices), 2),
                "min_price": min(prices),
                "max_price": max(prices),
                "product_count": len(prices),
            }

    return report


def find_matching_products(products):
    """Find identical or similar products across dispensaries for direct price comparison."""
    # Group by normalized product name + brand
    product_map = defaultdict(list)
    for p in products:
        key = f"{p['brand']}::{p['name']}".lower().strip()
        product_map[key].append(p)

    # Only keep products found at 2+ dispensaries
    matches = {}
    for key, items in product_map.items():
        dispensaries = set(i["dispensary"] for i in items)
        if len(dispensaries) >= 2:
            matches[key] = {
                "product": items[0]["name"],
                "brand": items[0]["brand"],
                "category": items[0]["category"],
                "prices_by_dispensary": {
                    i["dispensary"]: i["price"] for i in items if i["price"]
                },
            }

    return matches


def deal_tracker(dutchie_data, weedmaps_data):
    """Extract all active deals/specials across competitors."""
    deals = []

    for disp_name, data in dutchie_data.items():
        if "error" in data:
            continue
        for s in data.get("specials", []):
            deals.append({
                "dispensary": disp_name,
                "platform": "dutchie",
                "name": s.get("name"),
                "type": s.get("type"),
                "discount": s.get("discount"),
                "description": s.get("description"),
            })

    for disp_name, data in weedmaps_data.items():
        if "error" in data:
            continue
        for d in data.get("deals", []):
            deals.append({
                "dispensary": disp_name,
                "platform": "weedmaps",
                "name": d.get("title"),
                "type": d.get("discount_type"),
                "discount": d.get("discount_amount"),
                "description": d.get("description"),
            })

    return deals


def generate_report(data_dir="data"):
    """Generate a full competitive intelligence report."""
    print("\n" + "=" * 60)
    print("AFFINITY COMPETITIVE INTELLIGENCE REPORT")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    dutchie_data, weedmaps_data = load_latest_scrape(data_dir)

    if not dutchie_data and not weedmaps_data:
        print("\n⚠ No scrape data found. Run the scrapers first.")
        return

    products = normalize_all_products(dutchie_data, weedmaps_data)
    print(f"\n📦 Total products tracked: {len(products)}")
    print(f"📍 Dispensaries with data: {len(set(p['dispensary'] for p in products))}")

    # Category price comparison
    print("\n" + "-" * 40)
    print("PRICE COMPARISON BY CATEGORY")
    print("-" * 40)

    cat_report = price_comparison_by_category(products)
    for cat in ["Flower", "Pre-Rolls", "Vaporizers", "Edibles", "Concentrates"]:
        if cat in cat_report:
            print(f"\n  {cat}:")
            sorted_disps = sorted(
                cat_report[cat].items(),
                key=lambda x: x[1]["avg_price"]
            )
            for disp, stats in sorted_disps[:10]:
                print(f"    ${stats['avg_price']:>7.2f} avg | "
                      f"${stats['min_price']:>6.2f}-${stats['max_price']:>6.2f} | "
                      f"{stats['product_count']:>3} items | {disp}")

    # Direct product matches
    print("\n" + "-" * 40)
    print("IDENTICAL PRODUCTS — PRICE GAPS")
    print("-" * 40)

    matches = find_matching_products(products)
    # Sort by biggest price spread
    sorted_matches = sorted(
        matches.values(),
        key=lambda m: max(m["prices_by_dispensary"].values()) - min(m["prices_by_dispensary"].values())
        if len(m["prices_by_dispensary"]) >= 2 else 0,
        reverse=True
    )

    for m in sorted_matches[:20]:
        prices = m["prices_by_dispensary"]
        if len(prices) < 2:
            continue
        spread = max(prices.values()) - min(prices.values())
        cheapest = min(prices, key=prices.get)
        priciest = max(prices, key=prices.get)
        print(f"\n  {m['brand']} — {m['product']} [{m['category']}]")
        print(f"    Cheapest: ${prices[cheapest]:.2f} @ {cheapest}")
        print(f"    Priciest: ${prices[priciest]:.2f} @ {priciest}")
        print(f"    Spread:   ${spread:.2f}")

    # Active deals
    print("\n" + "-" * 40)
    print("ACTIVE DEALS & SPECIALS")
    print("-" * 40)

    deals = deal_tracker(dutchie_data, weedmaps_data)
    for d in deals[:30]:
        disc = f" ({d['discount']})" if d.get("discount") else ""
        print(f"  🏷  {d['dispensary']}: {d['name']}{disc}")

    # Save report as JSON
    report = {
        "generated_at": datetime.now().isoformat(),
        "total_products": len(products),
        "category_prices": cat_report,
        "product_matches": len(matches),
        "active_deals": len(deals),
        "deals": deals,
    }

    outfile = f"{data_dir}/report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(outfile, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\n✓ Full report saved to {outfile}")

    return report


if __name__ == "__main__":
    generate_report()
