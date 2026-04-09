"""
Dutchie Menu Scraper
Scrapes product data from Dutchie-powered dispensary menus via their public GraphQL API.
"""

import json
import time
import requests
from datetime import datetime

DUTCHIE_GQL_URL = "https://dutchie.com/graphql"
DUTCHIE_EMBEDDED_GQL = "https://dutchie.com/graphql"

# The key GraphQL query that pulls the full menu from any Dutchie dispensary.
# This is the same query their embedded menus use client-side.
MENU_QUERY = """
query FilteredProducts(
    $productsFilter: ProductsFilterInput!,
    $page: Int,
    $perPage: Int
) {
    filteredProducts(
        filter: $productsFilter,
        page: $page,
        perPage: $perPage
    ) {
        products {
            id
            name
            slug
            type
            category
            subCategory
            brand {
                id
                name
            }
            pricing {
                price
                medPrice
                recPrice
                discountedPrice
                priceRanges {
                    weight
                    price
                    medPrice
                    recPrice
                }
            }
            potency {
                thc
                cbd
                thca
                cbda
            }
            strainType
            description
            image
            weights {
                value
                label
            }
            special {
                id
                name
                type
                discount
            }
        }
        totalCount
        pageInfo {
            totalPages
            currentPage
            hasNextPage
        }
    }
}
"""

DISPENSARY_QUERY = """
query DispensaryBySlug($slug: String!) {
    dispensaryBySlug(slug: $slug) {
        id
        name
        slug
        address
        city
        state
        phone
        hours {
            day
            open
            close
        }
        menuTypes
    }
}
"""

SPECIALS_QUERY = """
query DispensarySpecials($dispensaryId: String!) {
    dispensarySpecials(dispensaryId: $dispensaryId) {
        specials {
            id
            name
            type
            discount
            description
            startDate
            endDate
            products {
                id
                name
                category
            }
        }
    }
}
"""


class DutchieScraper:
    """Scrapes Dutchie-powered dispensary menus."""

    def __init__(self, rate_limit_delay=2.0):
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "application/json",
        })
        self.delay = rate_limit_delay

    def _gql(self, query, variables=None):
        """Execute a GraphQL query against Dutchie's API."""
        payload = {"query": query}
        if variables:
            payload["variables"] = variables

        try:
            resp = self.session.post(DUTCHIE_GQL_URL, json=payload, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if "errors" in data:
                print(f"  ⚠ GraphQL errors: {data['errors']}")
                return None
            return data.get("data")
        except requests.RequestException as e:
            print(f"  ✗ Request failed: {e}")
            return None
        finally:
            time.sleep(self.delay)

    def get_dispensary_info(self, slug):
        """Get dispensary details by slug."""
        print(f"  → Fetching dispensary info for: {slug}")
        data = self._gql(DISPENSARY_QUERY, {"slug": slug})
        if data:
            return data.get("dispensaryBySlug")
        return None

    def get_menu(self, dispensary_slug, category=None, page=1, per_page=100):
        """Get product menu for a dispensary."""
        filters = {"dispensarySlug": dispensary_slug}
        if category:
            filters["category"] = category

        data = self._gql(MENU_QUERY, {
            "productsFilter": filters,
            "page": page,
            "perPage": per_page,
        })
        if data:
            return data.get("filteredProducts")
        return None

    def get_all_products(self, dispensary_slug):
        """Paginate through all products for a dispensary."""
        all_products = []
        page = 1

        while True:
            print(f"  → Scraping page {page}...")
            result = self.get_menu(dispensary_slug, page=page, per_page=100)
            if not result or not result.get("products"):
                break

            all_products.extend(result["products"])
            page_info = result.get("pageInfo", {})

            if not page_info.get("hasNextPage", False):
                break
            page += 1

        return all_products

    def get_specials(self, dispensary_id):
        """Get current specials/deals for a dispensary."""
        data = self._gql(SPECIALS_QUERY, {"dispensaryId": dispensary_id})
        if data:
            return data.get("dispensarySpecials", {}).get("specials", [])
        return []

    def scrape_dispensary(self, slug):
        """Full scrape of a single Dutchie-powered dispensary."""
        print(f"\n{'='*60}")
        print(f"SCRAPING: {slug}")
        print(f"{'='*60}")

        # Get dispensary info
        info = self.get_dispensary_info(slug)

        # Get all products
        products = self.get_all_products(slug)
        print(f"  ✓ Found {len(products)} products")

        # Get specials if we have the dispensary ID
        specials = []
        if info and info.get("id"):
            specials = self.get_specials(info["id"])
            print(f"  ✓ Found {len(specials)} active specials")

        # Normalize product data
        normalized = []
        for p in products:
            pricing = p.get("pricing", {})
            potency = p.get("potency", {})
            special = p.get("special")

            normalized.append({
                "id": p.get("id"),
                "name": p.get("name"),
                "brand": p.get("brand", {}).get("name", "Unknown"),
                "category": p.get("category"),
                "sub_category": p.get("subCategory"),
                "strain_type": p.get("strainType"),
                "price_rec": pricing.get("recPrice") or pricing.get("price"),
                "price_med": pricing.get("medPrice"),
                "price_discounted": pricing.get("discountedPrice"),
                "price_ranges": pricing.get("priceRanges", []),
                "thc": potency.get("thc"),
                "cbd": potency.get("cbd"),
                "on_special": special is not None,
                "special_name": special.get("name") if special else None,
                "special_discount": special.get("discount") if special else None,
            })

        return {
            "dispensary": slug,
            "info": info,
            "scraped_at": datetime.now().isoformat(),
            "product_count": len(normalized),
            "products": normalized,
            "specials": specials,
        }


def scrape_all_dutchie(competitors):
    """Scrape all Dutchie-powered competitors from config."""
    scraper = DutchieScraper()
    results = {}

    dutchie_disps = [
        c for c in competitors
        if "dutchie_slug" in c.get("platforms", {})
    ]

    print(f"\nFound {len(dutchie_disps)} Dutchie-powered dispensaries to scrape\n")

    for comp in dutchie_disps:
        slug = comp["platforms"]["dutchie_slug"]
        try:
            data = scraper.scrape_dispensary(slug)
            results[comp["name"]] = data
        except Exception as e:
            print(f"  ✗ Failed to scrape {comp['name']}: {e}")
            results[comp["name"]] = {"error": str(e), "scraped_at": datetime.now().isoformat()}

    return results


if __name__ == "__main__":
    from config import COMPETITORS
    results = scrape_all_dutchie(COMPETITORS)

    outfile = f"data/dutchie_scrape_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(outfile, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\n✓ Results saved to {outfile}")
