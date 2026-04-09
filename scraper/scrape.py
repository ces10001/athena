#!/usr/bin/env python3
"""
AFFINITY COMPETITIVE INTEL — Firecrawl Scraper v2
"""

import json
import os
import sys
import time
import re
import requests
from datetime import datetime

FIRECRAWL_API = "https://api.firecrawl.dev/v2/scrape"
API_KEY = os.environ.get("FIRECRAWL_API_KEY", "")

AFFINITY_STORES = [
    {"name": "Affinity - New Haven", "url": "https://affinitydispensary.com/new-haven-menu/"},
    {"name": "Affinity - Bridgeport", "url": "https://affinitydispensary.com/bridgeport-menu/"},
]

COMPETITORS = [
    {"name": "Higher Collective Bridgeport", "url": "https://highercollectivect.com/bridgeport-menu/"},
    {"name": "Lit New Haven Cannabis", "url": "https://www.litnhv.com/menu"},
    {"name": "Insa New Haven", "url": "https://www.insacannabis.com/dispensaries/new-haven-ct"},
    {"name": "RISE Dispensary Orange", "url": "https://risecannabis.com/dispensaries/connecticut/orange"},
    {"name": "High Profile Hamden", "url": "https://highprofilecannabisnow.com/locations/hamden-ct"},
    {"name": "Hi! People Derby", "url": "https://hipeople.co/derby"},
    {"name": "Rejoice Seymour", "url": "https://rejoicedispensary.com/seymour"},
    {"name": "Budr Cannabis Stratford", "url": "https://www.budr.com/stratford"},
    {"name": "RISE Dispensary Branford", "url": "https://risecannabis.com/dispensaries/connecticut/branford"},
    {"name": "Zen Leaf Naugatuck", "url": "https://zenleafdispensaries.com/location/naugatuck-ct"},
    {"name": "Shangri-La Waterbury", "url": "https://www.shangriladispensary.com/waterbury-menu"},
    {"name": "Zen Leaf Meriden", "url": "https://zenleafdispensaries.com/location/meriden-ct"},
    {"name": "Curaleaf Stamford", "url": "https://curaleaf.com/locations/connecticut/curaleaf-ct-stamford"},
    {"name": "Sweetspot Stamford", "url": "https://sweetspotfarms.com/stamford-menu"},
    {"name": "Fine Fettle Newington", "url": "https://finefettle.com/locations/newington"},
]

EXTRACT_SCHEMA = {
    "type": "object",
    "properties": {
        "products": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "brand": {"type": "string"},
                    "category": {"type": "string"},
                    "price": {"type": "number"},
                    "weight": {"type": "string"},
                },
                "required": ["name", "price"],
            },
        },
        "deals": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "title": {"type": "string"},
                    "discount_type": {"type": "string"},
                    "category": {"type": "string"},
                },
            },
        },
    },
    "required": ["products"],
}

EXTRACT_PROMPT = (
    "Extract ALL cannabis products from this dispensary menu page. "
    "For each product get: name, brand, category (Flower/Pre-Rolls/Vaporizers/Edibles/Concentrates/Tinctures), "
    "price in dollars, and weight. Also extract any deals or specials shown."
)


def scrape_dispensary(name, url):
    print(f"  -> Scraping {name}...")

    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
    }

    payload = {
        "url": url,
        "formats": ["extract"],
        "extract": {
            "schema": EXTRACT_SCHEMA,
            "prompt": EXTRACT_PROMPT,
        },
    }

    try:
        resp = requests.post(FIRECRAWL_API, json=payload, headers=headers, timeout=90)

        if resp.status_code == 402:
            print(f"  X {name}: Out of credits")
            return None
        if resp.status_code == 429:
            print(f"  X {name}: Rate limited, waiting 30s...")
            time.sleep(30)
            resp = requests.post(FIRECRAWL_API, json=payload, headers=headers, timeout=90)
        if resp.status_code != 200:
            try:
                err = resp.json()
                print(f"  X {name}: HTTP {resp.status_code} -> {json.dumps(err)[:300]}")
            except Exception:
                print(f"  X {name}: HTTP {resp.status_code} -> {resp.text[:300]}")
            return scrape_fallback(name, url, headers)

        data = resp.json()
        if not data.get("success"):
            print(f"  X {name}: success=false -> {data.get('error','unknown')}")
            return scrape_fallback(name, url, headers)

        extract = data.get("data", {}).get("extract", {})
        products = extract.get("products", [])
        deals = extract.get("deals", [])
        products = [p for p in products if p.get("name") and p.get("price") and p["price"] > 0]
        print(f"  OK {name}: {len(products)} products, {len(deals)} deals")
        return {"products": products, "deals": deals}

    except requests.exceptions.Timeout:
        print(f"  X {name}: Timeout")
        return None
    except Exception as e:
        print(f"  X {name}: {type(e).__name__}: {e}")
        return None


def scrape_fallback(name, url, headers):
    print(f"  .. {name}: Trying markdown fallback...")
    payload = {"url": url, "formats": ["markdown"]}
    try:
        resp = requests.post(FIRECRAWL_API, json=payload, headers=headers, timeout=90)
        if resp.status_code != 200:
            print(f"  X {name}: Fallback HTTP {resp.status_code}")
            return None
        data = resp.json()
        if not data.get("success"):
            print(f"  X {name}: Fallback failed")
            return None
        md = data.get("data", {}).get("markdown", "")
        if not md:
            print(f"  X {name}: No markdown")
            return None
        products = parse_markdown_products(md)
        print(f"  OK {name}: {len(products)} products (markdown)")
        return {"products": products, "deals": []}
    except Exception as e:
        print(f"  X {name}: Fallback error: {e}")
        return None


def parse_markdown_products(md):
    products = []
    lines = md.split("\n")
    price_pat = re.compile(r'\$(\d+\.?\d*)')
    current_cat = "Other"
    for i, line in enumerate(lines):
        line = line.strip()
        if not line:
            continue
        lower = line.lower()
        for cat in ["flower","pre-roll","vaporizer","vape","edible","concentrate","tincture"]:
            if cat in lower and len(line) < 40:
                cat_map = {"vape":"Vaporizers","pre-roll":"Pre-Rolls","edible":"Edibles",
                           "concentrate":"Concentrates","tincture":"Tinctures","flower":"Flower","vaporizer":"Vaporizers"}
                current_cat = cat_map.get(cat, cat.title())
                break
        prices = price_pat.findall(line)
        if prices:
            price = float(prices[0])
            if 5 <= price <= 500:
                name_line = re.sub(r'\$[\d.]+','',line).strip()
                name_line = re.sub(r'[#*_\[\]()]','',name_line).strip()
                if len(name_line) < 3 and i > 0:
                    name_line = re.sub(r'[#*_\[\]()]','',lines[i-1]).strip()
                if name_line and len(name_line) >= 3:
                    parts = re.split(r'[-|]', name_line, 1)
                    brand = parts[0].strip() if len(parts) > 1 else "Unknown"
                    name = parts[1].strip() if len(parts) > 1 else name_line
                    products.append({"name":name[:80],"brand":brand[:40],"category":current_cat,"price":price,"weight":""})
    return products


def build_dashboard_data(results):
    product_map = {}
    for disp_name, data in results.items():
        if data is None:
            continue
        for p in data.get("products", []):
            name = (p.get("name") or "").strip()
            brand = (p.get("brand") or "Unknown").strip()
            category = (p.get("category") or "Other").strip()
            price = p.get("price")
            weight = (p.get("weight") or "").strip()
            if not name or not price or price <= 0:
                continue
            key = f"{brand}::{name}".lower()
            if key not in product_map:
                product_map[key] = {"name":name,"brand":brand,"category":category,"weight":weight,"dispensaries":{}}
            product_map[key]["dispensaries"][disp_name] = round(float(price), 2)

    comparable = [v for v in product_map.values() if len(v["dispensaries"]) >= 2]
    all_products = list(product_map.values())
    comparable.sort(key=lambda x: (x["category"], x["name"]))
    all_products.sort(key=lambda x: (x["category"], x["name"]))
    output = comparable if len(comparable) >= 5 else all_products

    all_deals = []
    for disp_name, data in results.items():
        if data is None:
            continue
        for d in data.get("deals", []):
            if d.get("title"):
                all_deals.append({"dispensary":disp_name,"title":d["title"],"type":d.get("discount_type","other"),"category":d.get("category","All"),"expires":None})

    return {
        "scraped_at": datetime.now().isoformat(),
        "products": output,
        "deals": all_deals,
        "stats": {
            "total_products": sum(len(d["products"]) for d in results.values() if d),
            "comparable_products": len(comparable),
            "all_products": len(all_products),
            "dispensaries_scraped": len([k for k,v in results.items() if v]),
            "dispensaries_failed": len([k for k,v in results.items() if v is None]),
            "total_deals": len(all_deals),
        },
    }


def main():
    if not API_KEY:
        print("ERROR: FIRECRAWL_API_KEY not set.")
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"  AFFINITY SCRAPER (Firecrawl)")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    all_stores = AFFINITY_STORES + COMPETITORS
    print(f"  {len(COMPETITORS)} competitors + {len(AFFINITY_STORES)} Affinity stores")
    print(f"{'='*60}\n")

    results = {}
    for store in all_stores:
        data = scrape_dispensary(store["name"], store["url"])
        results[store["name"]] = data
        time.sleep(3)

    dashboard = build_dashboard_data(results)

    print(f"\n{'='*60}")
    print(f"  Products: {dashboard['stats']['total_products']}")
    print(f"  Comparable: {dashboard['stats']['comparable_products']}")
    print(f"  Success: {dashboard['stats']['dispensaries_scraped']}")
    print(f"  Failed: {dashboard['stats']['dispensaries_failed']}")
    print(f"  Deals: {dashboard['stats']['total_deals']}")
    print(f"{'='*60}\n")

    os.makedirs("data", exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    with open(f"data/raw_{ts}.json", "w") as f:
        json.dump(results, f, indent=2, default=str)
    dash_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "dashboard_data.json")
    with open(dash_path, "w") as f:
        json.dump(dashboard, f, indent=2, default=str)
    with open(f"data/dashboard_{ts}.json", "w") as f:
        json.dump(dashboard, f, indent=2, default=str)
    print(f"  DONE\n")


if __name__ == "__main__":
    main()
