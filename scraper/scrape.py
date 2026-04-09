#!/usr/bin/env python3
"""AFFINITY SCRAPER v7 — Leefii.com (renders menu data in HTML, verified URLs)"""

import json, os, sys, time, re, requests
from datetime import datetime

SCRAPE_URL = "https://api.firecrawl.dev/v2/scrape"
API_KEY = os.environ.get("FIRECRAWL_API_KEY", "")

# ALL URLs verified from leefii.com city pages (NH, Bridgeport, Stamford, Hartford)
STORES = [
    # Affinity
    {"name": "Affinity - New Haven", "url": "https://leefii.com/dispensary/affinity"},
    # Competitors (all verified Leefii URLs)
    {"name": "Higher Collective Bridgeport", "url": "https://leefii.com/dispensary/higher-collective-bridgeport"},
    {"name": "Lit New Haven Cannabis", "url": "https://leefii.com/dispensary/lit-new-haven-cannabis"},
    {"name": "Insa New Haven", "url": "https://leefii.com/dispensary/insa-cannabis-dispensary-new-haven"},
    {"name": "RISE Dispensary Orange", "url": "https://leefii.com/dispensary/rise-medical-recreational-cannabis-dispensary-orange"},
    {"name": "RISE Dispensary Branford", "url": "https://leefii.com/dispensary/rise-medical-recreational-cannabis-dispensary-branford"},
    {"name": "High Profile Hamden", "url": "https://weedmaps.com/dispensaries/high-profile-hamden"},
    {"name": "Hi! People Derby", "url": "https://leefii.com/dispensary/hi-people-derby"},
    {"name": "Budr Cannabis Stratford", "url": "https://leefii.com/dispensary/budr-cannabis-stratford"},
    {"name": "Sweetspot Stamford", "url": "https://leefii.com/dispensary/sweetspot-cannabis-dispensary-stamford"},
    {"name": "Fine Fettle Newington", "url": "https://leefii.com/dispensary/fine-fettle-newington-dispensary"},
    {"name": "Curaleaf Stamford", "url": "https://leefii.com/dispensary/curaleaf-dispensary-stamford"},
]

HEADERS = {}

def init():
    global HEADERS
    HEADERS = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}

def scrape_page(name, url):
    """Scrape a single page as markdown and parse products."""
    print(f"  -> {name}")
    print(f"     {url}")
    try:
        resp = requests.post(SCRAPE_URL, json={"url": url}, headers=HEADERS, timeout=90)
        if resp.status_code == 402:
            print(f"     OUT OF CREDITS"); return "no_credits"
        if resp.status_code == 429:
            print(f"     rate limited, waiting 25s..."); time.sleep(25)
            resp = requests.post(SCRAPE_URL, json={"url": url}, headers=HEADERS, timeout=90)
        if resp.status_code != 200:
            print(f"     HTTP {resp.status_code}"); return None
        data = resp.json()
        if not data.get("success"):
            print(f"     failed: {data.get('error','?')[:100]}"); return None
        md = data.get("data", {}).get("markdown", "")
        if not md:
            print(f"     empty page"); return None
        products = parse_products(md)
        deals = parse_deals(md)
        print(f"     OK: {len(products)} products, {len(deals)} deals")
        return {"products": products, "deals": deals}
    except Exception as e:
        print(f"     error: {e}"); return None

def parse_products(md):
    """Parse product names, prices, categories from Leefii markdown."""
    products = []
    lines = md.split("\n")
    price_pat = re.compile(r'\$(\d+\.?\d*)')
    cat = "Other"
    seen = set()

    for i, line in enumerate(lines):
        line = line.strip()
        if not line: continue

        # Detect category from Leefii emoji labels or headers
        lo = line.lower()
        if "flower" in lo and len(line) < 60: cat = "Flower"
        elif "pre-roll" in lo or "preroll" in lo or "pre roll" in lo: cat = "Pre-Rolls"
        elif "vape" in lo or "cartridge" in lo or "pen" in lo: cat = "Vaporizers"
        elif "edible" in lo or "gumm" in lo or "chocolate" in lo or "chew" in lo: cat = "Edibles"
        elif "concentrate" in lo or "wax" in lo or "shatter" in lo or "badder" in lo or "rosin" in lo: cat = "Concentrates"
        elif "tincture" in lo: cat = "Tinctures"
        elif "drink" in lo or "seltzer" in lo or "beverage" in lo: cat = "Edibles"

        # Leefii format: ### Product Name\n\nbrand\nweight\n$price
        # Or inline: Product Name ... $price
        prices = price_pat.findall(line)
        if not prices: continue

        price = float(prices[0])
        if price < 1 or price > 500: continue

        # Get product name — either on this line or a nearby header
        nm = re.sub(r'\$[\d.]+', '', line).strip()
        nm = re.sub(r'[#*_\[\]()>|]', '', nm).strip()
        nm = re.sub(r'\s+', ' ', nm).strip()

        # If this line is just a price, look back for the product name
        if len(nm) < 3:
            for back in range(1, 5):
                if i - back < 0: break
                prev = lines[i - back].strip()
                prev = re.sub(r'[#*_\[\]()>|]', '', prev).strip()
                if len(prev) >= 3 and not price_pat.search(prev) and prev.lower() not in ["each", "1 g", "1/8 oz", "1/2 g", "3/10 g"]:
                    nm = prev
                    break

        if not nm or len(nm) < 3: continue
        if nm.lower() in ["each", "hybrid", "indica", "sativa", "rec & med"]: continue

        # Extract brand — look for line above that's a brand name
        brand = "Unknown"
        for back in range(1, 4):
            if i - back < 0: break
            prev = lines[i - back].strip()
            prev = re.sub(r'[#*_\[\]()>|]', '', prev).strip()
            if prev and len(prev) > 2 and len(prev) < 40 and not price_pat.search(prev):
                if prev.lower() not in ["each", "hybrid", "indica", "sativa", "flower", "pre-rolls", "edibles", "rec & med", "1 g", "1/8 oz"]:
                    brand = prev
                    break

        # Extract weight from nearby lines
        weight = ""
        for near in range(max(0, i-3), min(len(lines), i+2)):
            wl = lines[near].strip().lower()
            wm = re.search(r'(\d+\.?\d*\s*(?:g|oz|mg|ml))', wl)
            if wm:
                weight = wm.group(1); break
            if wl in ["1 g", "1/8 oz", "1/2 g", "3/10 g", "2 g", "2.5 g", "3.5 g", "7 g", "14 g", "28 g"]:
                weight = wl; break

        # Deduplicate
        key = f"{nm}:{price}"
        if key in seen: continue
        seen.add(key)

        # Detect category from Leefii emoji prefixes
        if line.startswith("🌿"): cat = "Flower"
        elif line.startswith("🚬"): cat = "Pre-Rolls"
        elif line.startswith("💎"): cat = "Concentrates"
        elif line.startswith("🍪"): cat = "Edibles"

        products.append({
            "name": nm[:80],
            "brand": brand[:40] if brand != nm else "Unknown",
            "category": cat,
            "price": price,
            "weight": weight,
        })

    return products

def parse_deals(md):
    """Look for deal/special mentions in the markdown."""
    deals = []
    deal_pats = [
        re.compile(r'(\d+%\s*off[^.!]*)', re.I),
        re.compile(r'(buy\s+\d+\s+get\s+\d+[^.!]*)', re.I),
        re.compile(r'(bogo[^.!]*)', re.I),
        re.compile(r'(happy\s+hour[^.!]*)', re.I),
        re.compile(r'(first.time[^.!]*discount[^.!]*)', re.I),
    ]
    for pat in deal_pats:
        for m in pat.finditer(md):
            deals.append({"title": m.group(1).strip()[:100], "discount_type": "percent", "category": "All"})
    return deals

def build_dash(results):
    pm = {}
    for disp, data in results.items():
        if not data or not isinstance(data, dict): continue
        display = disp
        for p in data.get("products", []):
            nm = (p.get("name") or "").strip()
            br = (p.get("brand") or "Unknown").strip()
            ct = (p.get("category") or "Other").strip()
            pr = p.get("price")
            wt = (p.get("weight") or "").strip()
            if not nm or not pr or pr <= 0: continue
            key = f"{br}::{nm}".lower()
            if key not in pm:
                pm[key] = {"name":nm,"brand":br,"category":ct,"weight":wt,"dispensaries":{}}
            if display in pm[key]["dispensaries"]:
                pm[key]["dispensaries"][display] = min(pm[key]["dispensaries"][display], round(float(pr),2))
            else:
                pm[key]["dispensaries"][display] = round(float(pr),2)
    comp = sorted([v for v in pm.values() if len(v["dispensaries"]) >= 2], key=lambda x:(x["category"],x["name"]))
    allp = sorted(pm.values(), key=lambda x:(x["category"],x["name"]))
    out = comp if len(comp) >= 5 else allp
    deals = []
    for disp, data in results.items():
        if not data or not isinstance(data, dict): continue
        for d in data.get("deals", []):
            if d.get("title"):
                deals.append({"dispensary":disp,"title":d["title"],"type":d.get("discount_type","other"),"category":"All","expires":None})
    return {
        "scraped_at": datetime.now().isoformat(),
        "products": out, "deals": deals,
        "stats": {
            "total": sum(len(d["products"]) for d in results.values() if d and isinstance(d, dict)),
            "comparable": len(comp), "all": len(allp),
            "success": len([v for v in results.values() if v and isinstance(v, dict) and v.get("products")]),
            "failed": len([v for v in results.values() if not v or not isinstance(v, dict) or not v.get("products")]),
            "deals": len(deals),
        },
    }

def main():
    if not API_KEY:
        print("ERROR: FIRECRAWL_API_KEY not set"); sys.exit(1)
    init()
    print(f"\n{'='*60}")
    print(f"  AFFINITY SCRAPER v7 (Leefii)")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  {len(STORES)} dispensaries")
    print(f"{'='*60}\n")

    results = {}
    for s in STORES:
        result = scrape_page(s["name"], s["url"])
        if result == "no_credits":
            print("\n  !!! OUT OF CREDITS — stopping !!!\n")
            break
        results[s["name"]] = result
        time.sleep(6)

    dash = build_dash(results)
    print(f"\n{'='*60}")
    for k, v in dash["stats"].items(): print(f"  {k}: {v}")
    print(f"{'='*60}\n")

    os.makedirs("data", exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    with open(f"data/raw_{ts}.json","w") as f: json.dump(results,f,indent=2,default=str)
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)),"..","dashboard_data.json")
    with open(out,"w") as f: json.dump(dash,f,indent=2,default=str)
    print("  DONE\n")

if __name__ == "__main__":
    main()
