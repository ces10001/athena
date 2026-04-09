"""
AFFINITY COMPETITIVE INTELLIGENCE - Scraper Configuration
All CT dispensary competitors with their online menu platforms and URLs.
"""

# Dispensary menu URL patterns:
# Dutchie embedded: https://dutchie.com/embedded-menu/{slug}  OR  https://{store}.dutchie.com
# Dutchie storefront: https://store.{dispensary}.com  (proxied Dutchie)
# Jane/iHeartJane: https://www.iheartjane.com/embed/stores/{id}
# Weedmaps: https://weedmaps.com/dispensaries/{slug}
# Leafly: https://www.leafly.com/dispensary-info/{slug}

COMPETITORS = [
    # ── RISE (Dutchie-powered) ──────────────────────────────────────
    {
        "name": "RISE Dispensary Orange",
        "chain": "RISE",
        "address": "175 Boston Post Rd, Orange, CT",
        "platforms": {
            "dutchie_slug": "rise-dispensary-orange",
            "weedmaps": "rise-cannabis-orange",
            "leafly": "rise-cannabis-dispensary-orange",
        },
    },
    {
        "name": "RISE Dispensary Branford",
        "chain": "RISE",
        "address": "471 E Main St, Branford, CT",
        "platforms": {
            "dutchie_slug": "rise-dispensary-branford",
            "weedmaps": "rise-cannabis-branford",
            "leafly": "rise-cannabis-dispensary-branford",
        },
    },

    # ── FINE FETTLE (Dutchie-powered, 7 locations) ──────────────────
    {
        "name": "Fine Fettle Newington",
        "chain": "Fine Fettle",
        "address": "2280 Berlin Tpke, Newington, CT",
        "platforms": {
            "dutchie_slug": "fine-fettle-newington",
            "weedmaps": "fine-fettle-newington",
            "leafly": "fine-fettle-newington",
        },
    },
    {
        "name": "Fine Fettle Manchester",
        "chain": "Fine Fettle",
        "address": "91 Hale Rd, Manchester, CT",
        "platforms": {
            "dutchie_slug": "fine-fettle-manchester",
            "weedmaps": "fine-fettle-manchester",
            "leafly": "fine-fettle-manchester",
        },
    },
    {
        "name": "Fine Fettle Willimantic",
        "chain": "Fine Fettle",
        "address": "1548 W Main St, Willimantic, CT",
        "platforms": {"dutchie_slug": "fine-fettle-willimantic"},
    },
    {
        "name": "Fine Fettle Stamford",
        "chain": "Fine Fettle",
        "address": "12 Research Dr, Stamford, CT",
        "platforms": {"dutchie_slug": "fine-fettle-stamford"},
    },
    {
        "name": "Fine Fettle Norwalk",
        "chain": "Fine Fettle",
        "address": "191 Main St, Norwalk, CT",
        "platforms": {"dutchie_slug": "fine-fettle-norwalk"},
    },
    {
        "name": "Fine Fettle Bristol",
        "chain": "Fine Fettle",
        "address": "1228 Farmington Ave, Bristol, CT",
        "platforms": {"dutchie_slug": "fine-fettle-bristol"},
    },
    {
        "name": "Fine Fettle Waterbury",
        "chain": "Fine Fettle",
        "address": "85 Turnpike Dr, Waterbury, CT",
        "platforms": {"dutchie_slug": "fine-fettle-waterbury"},
    },

    # ── ZEN LEAF (Jane-powered, 7 locations) ────────────────────────
    {
        "name": "Zen Leaf Meriden",
        "chain": "Zen Leaf",
        "address": "1371 E Main St, Meriden, CT",
        "platforms": {"weedmaps": "zen-leaf-meriden", "leafly": "zen-leaf-meriden"},
    },
    {
        "name": "Zen Leaf Waterbury",
        "chain": "Zen Leaf",
        "address": "237 E Aurora St, Waterbury, CT",
        "platforms": {"weedmaps": "zen-leaf-waterbury"},
    },
    {
        "name": "Zen Leaf Norwich",
        "chain": "Zen Leaf",
        "address": "606 W Main St, Norwich, CT",
        "platforms": {"weedmaps": "zen-leaf-norwich"},
    },
    {
        "name": "Zen Leaf Naugatuck",
        "chain": "Zen Leaf",
        "address": "585 S Main St, Naugatuck, CT",
        "platforms": {"weedmaps": "zen-leaf-naugatuck"},
    },
    {
        "name": "Zen Leaf Enfield",
        "chain": "Zen Leaf",
        "address": "98 Elm St, Enfield, CT",
        "platforms": {"weedmaps": "zen-leaf-enfield"},
    },
    {
        "name": "Zen Leaf Newington",
        "chain": "Zen Leaf",
        "address": "2903 Berlin Tpke, Newington, CT",
        "platforms": {"weedmaps": "zen-leaf-newington"},
    },
    {
        "name": "Zen Leaf Ashford",
        "chain": "Zen Leaf",
        "address": "55 Nott Hwy, Ashford, CT",
        "platforms": {"weedmaps": "zen-leaf-ashford"},
    },

    # ── CURALEAF (Dutchie-powered, 3 locations) ─────────────────────
    {
        "name": "Curaleaf Stamford",
        "chain": "Curaleaf",
        "address": "814 E Main St, Stamford, CT",
        "platforms": {"dutchie_slug": "curaleaf-ct-stamford", "weedmaps": "curaleaf-stamford"},
    },
    {
        "name": "Curaleaf Manchester",
        "chain": "Curaleaf",
        "address": "240 Buckland St, Manchester, CT",
        "platforms": {"dutchie_slug": "curaleaf-ct-manchester"},
    },
    {
        "name": "Curaleaf Groton",
        "chain": "Curaleaf",
        "address": "79 Gold Star Hwy, Groton, CT",
        "platforms": {"dutchie_slug": "curaleaf-ct-groton"},
    },

    # ── BUDR (Jane-powered, 6 locations) ────────────────────────────
    {
        "name": "Budr Cannabis Stratford",
        "chain": "Budr",
        "address": "7365 Main St, Stratford, CT",
        "platforms": {"weedmaps": "budr-cannabis-stratford"},
    },
    {
        "name": "Budr Cannabis Danbury (Mill Plain)",
        "chain": "Budr",
        "address": "105 Mill Plain Rd, Danbury, CT",
        "platforms": {"weedmaps": "budr-cannabis-danbury"},
    },
    {
        "name": "Budr Cannabis Danbury (Federal)",
        "chain": "Budr",
        "address": "108 Federal Rd, Danbury, CT",
        "platforms": {"weedmaps": "budr-cannabis-danbury-federal-road"},
    },
    {
        "name": "Budr Cannabis Vernon",
        "chain": "Budr",
        "address": "234 Talcottville Rd, Vernon, CT",
        "platforms": {"weedmaps": "budr-cannabis-vernon"},
    },
    {
        "name": "Budr Cannabis Montville",
        "chain": "Budr",
        "address": "887 Norwich-NL Tpke, Uncasville, CT",
        "platforms": {"weedmaps": "budr-cannabis-montville"},
    },
    {
        "name": "Budr Cannabis Tolland",
        "chain": "Budr",
        "address": "9 Fieldstone Cmns, Tolland, CT",
        "platforms": {"weedmaps": "budr-cannabis-tolland"},
    },

    # ── SHANGRI-LA (4 locations) ────────────────────────────────────
    {
        "name": "Shangri-La Norwalk (Main)",
        "chain": "Shangri-La",
        "address": "430 Main Ave, Norwalk, CT",
        "platforms": {"weedmaps": "shangri-la-dispensary-norwalk"},
    },
    {
        "name": "Shangri-La Norwalk (CT Ave)",
        "chain": "Shangri-La",
        "address": "75 Connecticut Ave, Norwalk, CT",
        "platforms": {"weedmaps": "shangri-la-dispensary-south-norwalk"},
    },
    {
        "name": "Shangri-La Waterbury",
        "chain": "Shangri-La",
        "address": "53 Interstate Ln, Waterbury, CT",
        "platforms": {"weedmaps": "shangri-la-dispensary-waterbury"},
    },
    {
        "name": "Shangri-La Plainville",
        "chain": "Shangri-La",
        "address": "359 New Britain Ave, Plainville, CT",
        "platforms": {"weedmaps": "shangri-la-dispensary-plainville"},
    },

    # ── SWEETSPOT (2 locations) ─────────────────────────────────────
    {
        "name": "Sweetspot West Hartford",
        "chain": "Sweetspot",
        "address": "2 Park Rd, West Hartford, CT",
        "platforms": {"dutchie_slug": "sweetspot-west-hartford"},
    },
    {
        "name": "Sweetspot Stamford",
        "chain": "Sweetspot",
        "address": "111 High Ridge Rd, Stamford, CT",
        "platforms": {"dutchie_slug": "sweetspot-stamford"},
    },

    # ── HIGHER COLLECTIVE (2 locations) ─────────────────────────────
    {
        "name": "Higher Collective Bridgeport",
        "chain": "Higher Collective",
        "address": "3369 Fairfield Ave, Bridgeport, CT",
        "platforms": {"weedmaps": "higher-collective-bridgeport"},
    },
    {
        "name": "Higher Collective New London",
        "chain": "Higher Collective",
        "address": "595 Bank St, New London, CT",
        "platforms": {"weedmaps": "higher-collective-new-london"},
    },

    # ── SINGLE / SMALLER CHAINS ─────────────────────────────────────
    {
        "name": "Insa New Haven",
        "chain": "Insa",
        "address": "222 Sargent Dr, New Haven, CT",
        "platforms": {"dutchie_slug": "insa-new-haven", "weedmaps": "insa-new-haven"},
    },
    {
        "name": "Insa Hartford",
        "chain": "Insa",
        "address": "167 Brainard Rd, Hartford, CT",
        "platforms": {"dutchie_slug": "insa-hartford"},
    },
    {
        "name": "Crisp Cannabis Cromwell",
        "chain": "Crisp",
        "address": "33 Berlin Rd, Cromwell, CT",
        "platforms": {"dutchie_slug": "crisp-cannabis-cromwell"},
    },
    {
        "name": "Crisp Cannabis East Hartford",
        "chain": "Crisp",
        "address": "500 Main St, East Hartford, CT",
        "platforms": {"dutchie_slug": "crisp-cannabis-east-hartford"},
    },
    {
        "name": "Venu Flower Collective",
        "chain": "Venu",
        "address": "895 Washington St, Middletown, CT",
        "platforms": {"dutchie_slug": "venu-flower-collective"},
    },
    {
        "name": "Lit New Haven Cannabis",
        "chain": "Lit",
        "address": "169 East St, New Haven, CT",
        "platforms": {"weedmaps": "lit-new-haven-cannabis"},
    },
    {
        "name": "Trulieve Bristol",
        "chain": "Trulieve",
        "address": "820 Farmington Ave, Bristol, CT",
        "platforms": {"dutchie_slug": "trulieve-bristol", "weedmaps": "trulieve-bristol"},
    },
    {
        "name": "Hi! People Derby",
        "chain": "Hi! People",
        "address": "90 Pershing Dr, Derby, CT",
        "platforms": {"weedmaps": "hi-people-derby"},
    },
    {
        "name": "High Profile Hamden",
        "chain": "High Profile",
        "address": "2607 Whitney Ave, Hamden, CT",
        "platforms": {"dutchie_slug": "high-profile-hamden"},
    },
    {
        "name": "Rejoice Meriden",
        "chain": "Rejoice",
        "address": "834 Broad St, Meriden, CT",
        "platforms": {"weedmaps": "rejoice-meriden"},
    },
    {
        "name": "Rejoice Seymour",
        "chain": "Rejoice",
        "address": "39 New Haven Rd, Seymour, CT",
        "platforms": {"weedmaps": "rejoice-seymour"},
    },
    {
        "name": "Nightjar East Lyme",
        "chain": "Nightjar",
        "address": "15 Colton Rd, East Lyme, CT",
        "platforms": {"weedmaps": "nightjar-east-lyme"},
    },
    {
        "name": "Still River Wellness",
        "chain": "Still River",
        "address": "3568 Winsted Rd, Torrington, CT",
        "platforms": {"dutchie_slug": "still-river-wellness"},
    },
    {
        "name": "Octane Enfield",
        "chain": "Octane",
        "address": "9 Hazard Ave, Enfield, CT",
        "platforms": {"weedmaps": "octane-enfield"},
    },
    {
        "name": "Nova Farms New Britain",
        "chain": "Nova Farms",
        "address": "623 Hartford Rd, New Britain, CT",
        "platforms": {"dutchie_slug": "nova-farms-new-britain"},
    },
]

# Product categories to track
CATEGORIES = [
    "Flower",
    "Pre-Rolls",
    "Vaporizers",
    "Edibles",
    "Concentrates",
    "Tinctures",
    "Topicals",
    "Accessories",
]
