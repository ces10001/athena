"""
Weedmaps Menu Scraper
Scrapes product/menu data from Weedmaps dispensary listings.
Weedmaps uses a REST API under the hood that serves menu data as JSON.
"""

import json
import time
import requests
from datetime import datetime

WEEDMAPS_API = "https://api-g.weedmaps.com/discovery/v2"


class WeedmapsScraper:
    """Scrapes Weedmaps dispensary menus via their discovery API."""

    def __init__(self, rate_limit_delay=2.5):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "application/json",
        })
        self.delay = rate_limit_delay

    def _get(self, endpoint, params=None):
        """Make a GET request to the Weedmaps API."""
        url = f"{WEEDMAPS_API}/{endpoint}"
        try:
            resp = self.session.get(url, params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            print(f"  ✗ Request failed: {e}")
            return None
        finally:
            time.sleep(self.delay)

    def get_dispensary_info(self, slug):
        """Get dispensary details from Weedmaps by slug."""
        print(f"  → Fetching Weedmaps info for: {slug}")
        data = self._get(f"listings/{slug}")
        if data and "data" in data:
            listing = data["data"].get("listing", {})
            return {
                "id": listing.get("id"),
                "name": listing.get("name"),
                "slug": listing.get("slug"),
                "address": listing.get("address"),
                "city": listing.get("city"),
                "state": listing.get("state"),
                "phone": listing.get("phone_number"),
                "rating": listing.get("rating"),
                "reviews_count": listing.get("reviews_count"),
                "hours": listing.get("business_hours"),
                "has_delivery": listing.get("has_delivery"),
                "has_pickup": listing.get("online_ordering", {}).get("enabled_for_pickup"),
            }
        return None

    def get_menu(self, slug, category=None, page=1, per_page=100):
        """Get menu items from a Weedmaps dispensary."""
        print(f"  → Fetching menu page {page} for: {slug}")
        params = {
            "page": page,
            "page_size": per_page,
            "sort_by": "name",
            "sort_order": "asc",
        }
        if category:
            params["category"] = category

        data = self._get(f"listings/{slug}/menu_items", params=params)
        if data and "data" in data:
            return data["data"]
        return None

    def get_all_products(self, slug):
        """Paginate through all products."""
        all_products = []
        page = 1

        while True:
            result = self.get_menu(slug, page=page)
            if not result or not result.get("menu_items"):
                break

            items = result["menu_items"]
            all_products.extend(items)

            meta = result.get("meta", {}).get("pagination", {})
            total_pages = meta.get("total_pages", 1)
            if page >= total_pages:
                break
            page += 1

        return all_products

    def get_deals(self, slug):
        """Get current deals/specials from a dispensary."""
        print(f"  → Fetching deals for: {slug}")
        data = self._get(f"listings/{slug}/deals")
        if data and "data" in data:
            return data["data"].get("deals", [])
        return []

    def scrape_dispensary(self, slug):
        """Full scrape of a Weedmaps dispensary."""
        print(f"\n{'='*60}")
        print(f"SCRAPING (Weedmaps): {slug}")
        print(f"{'='*60}")

        info = self.get_dispensary_info(slug)
        products = self.get_all_products(slug)
        deals = self.get_deals(slug)

        print(f"  ✓ Found {len(products)} products, {len(deals)} deals")

        # Normalize
        normalized = []
        for item in products:
            prices = item.get("prices", [])
            # Extract the cheapest and most expensive price points
            price_list = []
            for p in prices:
                if p.get("price"):
                    price_list.append({
                        "size": p.get("label", ""),
                        "price": p["price"],
                    })

            normalized.append({
                "id": item.get("id"),
                "name": item.get("name"),
                "brand": item.get("brand", {}).get("name") if item.get("brand") else "Unknown",
                "category": item.get("category", {}).get("name") if item.get("category") else None,
                "strain_type": item.get("genetics"),
                "prices": price_list,
                "price_min": min((p["price"] for p in price_list), default=None),
                "price_max": max((p["price"] for p in price_list), default=None),
                "thc": item.get("thc_percentage"),
                "cbd": item.get("cbd_percentage"),
                "rating": item.get("rating"),
                "review_count": item.get("reviews_count"),
            })

        deal_list = []
        for d in deals:
            deal_list.append({
                "id": d.get("id"),
                "title": d.get("title"),
                "description": d.get("body"),
                "discount_type": d.get("discount_type"),
                "discount_amount": d.get("discount_amount"),
                "start_date": d.get("start_date"),
                "end_date": d.get("end_date"),
            })

        return {
            "dispensary": slug,
            "platform": "weedmaps",
            "info": info,
            "scraped_at": datetime.now().isoformat(),
            "product_count": len(normalized),
            "products": normalized,
            "deals": deal_list,
        }


def scrape_all_weedmaps(competitors):
    """Scrape all Weedmaps-listed competitors."""
    scraper = WeedmapsScraper()
    results = {}

    wm_disps = [
        c for c in competitors
        if "weedmaps" in c.get("platforms", {})
    ]

    print(f"\nFound {len(wm_disps)} Weedmaps dispensaries to scrape\n")

    for comp in wm_disps:
        slug = comp["platforms"]["weedmaps"]
        try:
            data = scraper.scrape_dispensary(slug)
            results[comp["name"]] = data
        except Exception as e:
            print(f"  ✗ Failed to scrape {comp['name']}: {e}")
            results[comp["name"]] = {"error": str(e), "scraped_at": datetime.now().isoformat()}

    return results


if __name__ == "__main__":
    from config import COMPETITORS
    results = scrape_all_weedmaps(COMPETITORS)

    outfile = f"data/weedmaps_scrape_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(outfile, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\n✓ Results saved to {outfile}")
