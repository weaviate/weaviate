#!/usr/bin/env python3
"""
Common Indexing Mistakes Demo - Import Only

Creates a single product-catalog collection where five properties are
deliberately misconfigured, then bulk-loads 1,000,000 synthetic objects.
The companion UI runs pre-canned queries against this collection to make
the cost of each misconfiguration visible (slow latency, false positives,
or both). No reindex migrations are run by this script - the viewer is
expected to fix the schema from the Weaviate console and re-run the same
queries from the UI to see the difference.

The deliberate mistakes are:

  1. price_cents (int)        filterable=False, rangeable=False
     -> Range queries (price between X and Y) fall back to brute force.

  2. spec_sheet_path (text)   tokenization=WORD
     -> Path-shaped searches like "/products/cameras/gopro" tokenize on
        '/' and '.', producing false positives that share the word
        'gopro' anywhere in their path.

  3. sku (text)               tokenization=WORD
     -> SKUs like "GP-HERO12-BLK-001" tokenize on '-' into noisy tokens.
        Exact-match or BM25 lookup returns dozens of unrelated SKUs.

  4. category (text)          filterable=False, tokenization=FIELD
     -> Equality filter on category has no inverted bucket and falls back
        to a row scan.

  5. support_email (text)     tokenization=WORD
     -> Emails like "support@gopro.com" tokenize on '@' and '.', so a
        BM25 search returns every product whose support address shares a
        domain token.

Usage:
    .venv/bin/python tools/dev/bench/demo_indexing_mistakes_import.py

Connects to a local Weaviate by default (localhost:8080 / grpc 50051).
Set WCD_URL and WCD_API_KEY to target a Weaviate Cloud cluster instead.
"""

import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone

import weaviate
from weaviate.classes.config import (
    Configure,
    DataType,
    Property,
    Tokenization,
)
from weaviate.classes.init import Auth

COLLECTION_NAME = "IndexingMistakesDemo"
NUM_OBJECTS = 1_000_000
BATCH_SIZE = 1_000
SEED = 42
PROGRESS_EVERY = 50_000

# -- Schema --------------------------------------------------------------------
#
# Five of the properties are deliberately misconfigured. The remaining ones
# (name, description, brand, in_stock, release_date, weight_grams) are
# configured sensibly so the result cards in the UI look like a real product
# catalog and not just numbers.

PROPERTIES = [
    # --- sensible properties (so result cards read like a catalog row) -----
    Property(
        name="name",
        data_type=DataType.TEXT,
        index_searchable=True,
        index_filterable=True,
        tokenization=Tokenization.WORD,
    ),
    Property(
        name="description",
        data_type=DataType.TEXT,
        index_searchable=True,
        index_filterable=False,
        tokenization=Tokenization.WORD,
    ),
    Property(
        name="brand",
        data_type=DataType.TEXT,
        index_searchable=True,
        index_filterable=True,
        tokenization=Tokenization.FIELD,
    ),
    Property(
        name="in_stock",
        data_type=DataType.BOOL,
        index_filterable=True,
    ),
    Property(
        name="release_date",
        data_type=DataType.DATE,
        index_filterable=True,
        index_range_filters=True,
    ),
    Property(
        name="weight_grams",
        data_type=DataType.INT,
        index_filterable=True,
        index_range_filters=True,
    ),

    # --- mistake 1: int with no rangeable, no filterable -------------------
    # Range queries on this property fall back to brute force.
    Property(
        name="price_cents",
        data_type=DataType.INT,
        index_filterable=False,
        index_range_filters=False,
    ),

    # --- mistake 2: file path with WORD tokenization -----------------------
    # Path-shaped searches will splinter on '/' and '.', matching noise.
    Property(
        name="spec_sheet_path",
        data_type=DataType.TEXT,
        index_searchable=True,
        index_filterable=True,
        tokenization=Tokenization.WORD,
    ),

    # --- mistake 3: SKU with WORD tokenization -----------------------------
    # SKUs splinter on '-' so BM25 lookup returns many unrelated SKUs.
    Property(
        name="sku",
        data_type=DataType.TEXT,
        index_searchable=True,
        index_filterable=True,
        tokenization=Tokenization.WORD,
    ),

    # --- mistake 4: category equality filter with no inverted index --------
    # filterable=False means equality filter falls back to a row scan.
    Property(
        name="category",
        data_type=DataType.TEXT,
        index_searchable=False,
        index_filterable=False,
        tokenization=Tokenization.FIELD,
    ),

    # --- mistake 5: email with WORD tokenization ---------------------------
    # Emails splinter on '@' and '.', so any BM25 lookup matches every
    # product that shares a single domain word ("support", "gopro", "com").
    Property(
        name="support_email",
        data_type=DataType.TEXT,
        index_searchable=True,
        index_filterable=True,
        tokenization=Tokenization.WORD,
    ),
]


# -- Fake data ------------------------------------------------------------------
#
# Mostly procedurally generated, but seeded so reruns produce identical output.
# Brand and category names recur with realistic frequency. SKUs and paths are
# templated off the brand/category so the demo queries land on something
# recognisable.

BRANDS = [
    "GoPro", "Sony", "Canon", "Nikon", "Fujifilm", "Panasonic", "DJI",
    "Insta360", "Apple", "Samsung", "Google", "OnePlus", "Xiaomi",
    "Bose", "Sonos", "JBL", "Sennheiser", "AudioTechnica", "Shure",
    "Logitech", "Razer", "Corsair", "SteelSeries", "HyperX",
    "Dell", "HP", "Lenovo", "Asus", "Acer", "MSI",
    "Anker", "Belkin", "UGreen", "Sandisk", "Kingston", "WD", "Seagate",
    "Garmin", "Fitbit", "Suunto", "Polar",
    "Philips", "Dyson", "Roomba", "Shark", "Bosch", "DeWalt", "Makita",
    "Nest", "Ring", "Arlo", "Eufy", "TPLink", "Netgear", "Asus",
]

CATEGORIES = [
    "Cameras > Action",
    "Cameras > Mirrorless",
    "Cameras > DSLR",
    "Cameras > Compact",
    "Cameras > 360",
    "Drones > Consumer",
    "Drones > Professional",
    "Phones > Flagship",
    "Phones > Midrange",
    "Phones > Budget",
    "Audio > Headphones",
    "Audio > Earbuds",
    "Audio > Speakers",
    "Audio > Microphones",
    "Computing > Laptops",
    "Computing > Desktops",
    "Computing > Monitors",
    "Computing > Keyboards",
    "Computing > Mice",
    "Storage > SSD",
    "Storage > HDD",
    "Storage > Flash",
    "Wearables > Smartwatch",
    "Wearables > Fitness Tracker",
    "Home > Vacuum",
    "Home > Power Tools",
    "Home > Lighting",
    "Smart Home > Cameras",
    "Smart Home > Doorbells",
    "Smart Home > Hubs",
    "Networking > Routers",
    "Networking > Switches",
    "Networking > Mesh",
]

# Category -> short tag used in SKUs and paths.
CATEGORY_TAGS = {
    "Cameras > Action":         ("cameras",   "ACT"),
    "Cameras > Mirrorless":     ("cameras",   "MIR"),
    "Cameras > DSLR":           ("cameras",   "DSL"),
    "Cameras > Compact":        ("cameras",   "CMP"),
    "Cameras > 360":            ("cameras",   "360"),
    "Drones > Consumer":        ("drones",    "DRC"),
    "Drones > Professional":    ("drones",    "DRP"),
    "Phones > Flagship":        ("phones",    "FLG"),
    "Phones > Midrange":        ("phones",    "MID"),
    "Phones > Budget":          ("phones",    "BDG"),
    "Audio > Headphones":       ("audio",     "HDP"),
    "Audio > Earbuds":          ("audio",     "EAR"),
    "Audio > Speakers":         ("audio",     "SPK"),
    "Audio > Microphones":      ("audio",     "MIC"),
    "Computing > Laptops":      ("computing", "LAP"),
    "Computing > Desktops":     ("computing", "DSK"),
    "Computing > Monitors":     ("computing", "MON"),
    "Computing > Keyboards":    ("computing", "KBD"),
    "Computing > Mice":         ("computing", "MSE"),
    "Storage > SSD":            ("storage",   "SSD"),
    "Storage > HDD":            ("storage",   "HDD"),
    "Storage > Flash":          ("storage",   "FLS"),
    "Wearables > Smartwatch":   ("wearables", "WSM"),
    "Wearables > Fitness Tracker": ("wearables", "WFT"),
    "Home > Vacuum":            ("home",      "VAC"),
    "Home > Power Tools":       ("home",      "PWR"),
    "Home > Lighting":          ("home",      "LGT"),
    "Smart Home > Cameras":     ("smarthome", "SHC"),
    "Smart Home > Doorbells":   ("smarthome", "SHD"),
    "Smart Home > Hubs":        ("smarthome", "SHH"),
    "Networking > Routers":     ("networking", "RTR"),
    "Networking > Switches":    ("networking", "SWT"),
    "Networking > Mesh":        ("networking", "MSH"),
}

MODEL_PREFIXES = [
    "Pro", "Air", "Max", "Ultra", "Plus", "Lite", "X", "Elite", "Studio",
    "Edge", "Core", "Prime", "Flex", "Nova", "Quantum", "Aero",
]
MODEL_SUFFIXES = [
    "II", "III", "IV", "V", "VI", "S", "SE", "FE", "Z", "G2", "G3",
]

COLOR_CODES = ["BLK", "WHT", "SLV", "GLD", "BLU", "RED", "GRN", "GRY"]

DESCRIPTION_BITS = [
    "Compact and lightweight",
    "Weatherproof for outdoor use",
    "Studio-grade build",
    "Ships with a 24-month warranty",
    "Designed for content creators",
    "Pro-level performance in a consumer body",
    "Optimised for low-light scenes",
    "Tested down to -10C",
    "Bluetooth 5.3 with multipoint pairing",
    "USB-C charging at up to 65W",
    "Voice assistant ready",
    "Compatible with the companion mobile app",
    "Recycled aluminium chassis",
    "Capable of 4K60 recording",
    "Wi-Fi 6E onboard",
    "Hot-swappable battery",
    "IP68 rated against dust and water",
    "Tool-free assembly",
]

# Most brands use their main domain for support, but a small minority use a
# subdomain like help.brand.com. This keeps WORD tokenization confused.
SUPPORT_DOMAIN_PREFIXES = ["support", "help", "service", "care"]


def _model_name(rng: random.Random) -> str:
    prefix = rng.choice(MODEL_PREFIXES)
    number = rng.randint(1, 999)
    if rng.random() < 0.4:
        suffix = " " + rng.choice(MODEL_SUFFIXES)
    else:
        suffix = ""
    return f"{prefix} {number}{suffix}"


def _description(rng: random.Random, brand: str, category: str) -> str:
    bits = rng.sample(DESCRIPTION_BITS, k=2)
    return f"{brand} {category.split('>')[-1].strip().lower()}. {bits[0]}. {bits[1]}."


def _make_object(i: int, rng: random.Random) -> dict:
    brand = rng.choice(BRANDS)
    category = rng.choice(CATEGORIES)
    category_dir, sku_tag = CATEGORY_TAGS[category]

    model = _model_name(rng)
    color = rng.choice(COLOR_CODES)
    serial = rng.randint(1, 999)

    name = f"{brand} {model}"
    sku = f"{brand[:2].upper()}-{sku_tag}-{color}-{serial:03d}"
    # Path includes the brand slug so demo searches for a brand
    # produce false-positive matches across other category subtrees.
    spec_sheet_path = (
        f"/products/{category_dir}/{brand.lower()}/{model.lower().replace(' ', '-')}/spec.pdf"
    )

    support_prefix = (
        "support" if rng.random() < 0.85 else rng.choice(SUPPORT_DOMAIN_PREFIXES)
    )
    support_email = f"{support_prefix}@{brand.lower()}.com"

    release = datetime(2022, 1, 1, tzinfo=timezone.utc) + timedelta(
        days=rng.randint(0, 1460), seconds=rng.randint(0, 86_399)
    )

    return {
        "name": name,
        "description": _description(rng, brand, category),
        "brand": brand,
        "in_stock": rng.random() < 0.7,
        "release_date": release.isoformat(),
        "weight_grams": rng.randint(20, 5_000),
        # Mistake fields -------------------------------------------------------
        "price_cents": rng.randint(999, 199_999),
        "spec_sheet_path": spec_sheet_path,
        "sku": sku,
        "category": category,
        "support_email": support_email,
    }


# -- Main ----------------------------------------------------------------------


def main() -> int:
    sys.stdout.reconfigure(line_buffering=True)
    rng = random.Random(SEED)

    wcd_url = os.environ.get("WCD_URL")
    wcd_key = os.environ.get("WCD_API_KEY")
    if wcd_url:
        if not wcd_key:
            print("WCD_URL is set but WCD_API_KEY is empty", file=sys.stderr)
            return 2
        print(f"Connecting to Weaviate Cloud at {wcd_url}...")
        client = weaviate.connect_to_weaviate_cloud(
            cluster_url=wcd_url,
            auth_credentials=Auth.api_key(wcd_key),
        )
    else:
        print("Connecting to local Weaviate at localhost:8080 (grpc 50051)...")
        client = weaviate.connect_to_local(port=8080, grpc_port=50051)
    try:
        if client.collections.exists(COLLECTION_NAME):
            print(f"Deleting existing collection {COLLECTION_NAME}...")
            client.collections.delete(COLLECTION_NAME)

        print(
            f"Creating collection {COLLECTION_NAME} with {len(PROPERTIES)} properties..."
        )
        client.collections.create(
            name=COLLECTION_NAME,
            vectorizer_config=Configure.Vectorizer.none(),
            properties=PROPERTIES,
        )

        collection = client.collections.get(COLLECTION_NAME)
        print(f"Importing {NUM_OBJECTS:,} objects...")
        t0 = time.monotonic()
        with collection.batch.fixed_size(batch_size=BATCH_SIZE) as batch:
            for i in range(NUM_OBJECTS):
                batch.add_object(properties=_make_object(i, rng))
                if (i + 1) % PROGRESS_EVERY == 0:
                    done = i + 1
                    rate = done / (time.monotonic() - t0)
                    print(f"  {done:>10,} / {NUM_OBJECTS:,}  ({rate:,.0f} obj/s)")
        errors = collection.batch.failed_objects
        if errors:
            print(f"  {len(errors)} failed objects. First error: {errors[0]}")
            return 1
        elapsed = time.monotonic() - t0
        print(
            f"  Import complete in {elapsed:.1f}s ({NUM_OBJECTS / elapsed:,.0f} obj/s)"
        )

        target = wcd_url if wcd_url else "localhost:8080"
        print()
        print(f"Ready. The UI can now point at {target} and load the")
        print(f"'{COLLECTION_NAME}' collection to demonstrate each indexing mistake.")
        print("No reindex migrations have been triggered - do that from the Weaviate console.")
        return 0
    finally:
        client.close()


if __name__ == "__main__":
    sys.exit(main())
