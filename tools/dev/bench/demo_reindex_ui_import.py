#!/usr/bin/env python3
"""
Runtime Reindex UI Demo — Import Only

Creates a single collection with a broad mix of property types and indexing
states, then imports a small dataset. Does NOT run any reindex migrations —
this is intended to populate a Weaviate instance that the runtime-reindex UI
can then point at, so every combination the UI renders has live data behind
it.

Covers, per property:
  - text / text[] with every tokenization (word, lowercase, whitespace, field)
  - text with searchable ON / OFF and filterable ON / OFF (each combination)
  - int / number / date with filterable ON / OFF and rangeable ON / OFF
  - boolean, uuid

Usage:
    .venv/bin/python tools/dev/bench/demo_reindex_ui_import.py

Requires a Weaviate already running on localhost:8080 (grpc 50051) with
`DISTRIBUTED_TASKS_ENABLED=true` if you want to exercise reindex from the UI
afterwards.
"""

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

COLLECTION_NAME = "ReindexUIDemo"
NUM_OBJECTS = 1_000_000
BATCH_SIZE = 1_000
SEED = 42
PROGRESS_EVERY = 50_000

# -- Schema --------------------------------------------------------------------
#
# Each property is named after its configuration so it's obvious in the UI
# what state it's in *before* any reindex migration is run.
#
# Naming scheme:
#   <type>_<indexing-state>
# where indexing-state encodes searchable/filterable/rangeable and, for text,
# the tokenization.

PROPERTIES = [
    # --- text: all four tokenizations, searchable + filterable on -----------
    Property(
        name="text_word_search_filter",
        data_type=DataType.TEXT,
        index_searchable=True,
        index_filterable=True,
        tokenization=Tokenization.WORD,
    ),
    Property(
        name="text_lowercase_search_filter",
        data_type=DataType.TEXT,
        index_searchable=True,
        index_filterable=True,
        tokenization=Tokenization.LOWERCASE,
    ),
    Property(
        name="text_whitespace_search_filter",
        data_type=DataType.TEXT,
        index_searchable=True,
        index_filterable=True,
        tokenization=Tokenization.WHITESPACE,
    ),
    Property(
        name="text_field_search_filter",
        data_type=DataType.TEXT,
        index_searchable=True,
        index_filterable=True,
        tokenization=Tokenization.FIELD,
    ),
    # --- text: mixed searchable / filterable states -------------------------
    Property(
        name="text_searchable_only",
        data_type=DataType.TEXT,
        index_searchable=True,
        index_filterable=False,
        tokenization=Tokenization.WORD,
    ),
    Property(
        name="text_filterable_only",
        data_type=DataType.TEXT,
        index_searchable=False,
        index_filterable=True,
        tokenization=Tokenization.WORD,
    ),
    Property(
        name="text_no_index",
        data_type=DataType.TEXT,
        index_searchable=False,
        index_filterable=False,
    ),
    # --- text array ---------------------------------------------------------
    Property(
        name="text_array_word_search_filter",
        data_type=DataType.TEXT_ARRAY,
        index_searchable=True,
        index_filterable=True,
        tokenization=Tokenization.WORD,
    ),
    Property(
        name="text_array_field_filterable_only",
        data_type=DataType.TEXT_ARRAY,
        index_searchable=False,
        index_filterable=True,
        tokenization=Tokenization.FIELD,
    ),
    # --- int: every filterable/rangeable combination ------------------------
    Property(
        name="int_filter_range",
        data_type=DataType.INT,
        index_filterable=True,
        index_range_filters=True,
    ),
    Property(
        name="int_filter_only",
        data_type=DataType.INT,
        index_filterable=True,
        index_range_filters=False,
    ),
    Property(
        name="int_range_only",
        data_type=DataType.INT,
        index_filterable=False,
        index_range_filters=True,
    ),
    Property(
        name="int_no_index",
        data_type=DataType.INT,
        index_filterable=False,
        index_range_filters=False,
    ),
    # --- number -------------------------------------------------------------
    Property(
        name="number_filter_range",
        data_type=DataType.NUMBER,
        index_filterable=True,
        index_range_filters=True,
    ),
    Property(
        name="number_filter_only",
        data_type=DataType.NUMBER,
        index_filterable=True,
        index_range_filters=False,
    ),
    # --- date ---------------------------------------------------------------
    Property(
        name="date_filter_range",
        data_type=DataType.DATE,
        index_filterable=True,
        index_range_filters=True,
    ),
    Property(
        name="date_filter_only",
        data_type=DataType.DATE,
        index_filterable=True,
        index_range_filters=False,
    ),
    # --- boolean ------------------------------------------------------------
    Property(
        name="bool_filter",
        data_type=DataType.BOOL,
        index_filterable=True,
    ),
    Property(
        name="bool_no_index",
        data_type=DataType.BOOL,
        index_filterable=False,
    ),
    # --- uuid ---------------------------------------------------------------
    Property(
        name="uuid_filter",
        data_type=DataType.UUID,
        index_filterable=True,
    ),
    # NB: no geoCoordinates — it builds a per-shard HNSW index that isn't
    # covered by runtime reindex and massively slows down ingest. If the UI
    # needs to render geo props too, add them in a separate demo.
]


# -- Fake data -----------------------------------------------------------------

WORDS = [
    "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel",
    "india", "juliet", "kilo", "lima", "mike", "november", "oscar", "papa",
    "quebec", "romeo", "sierra", "tango", "uniform", "victor", "whiskey",
    "xray", "yankee", "zulu",
]
CATEGORIES = ["books", "music", "film", "games", "tools", "food", "travel"]
STATUSES = ["draft", "review", "published", "archived"]


def _sentence(rng: random.Random, n: int = 6) -> str:
    return " ".join(rng.choices(WORDS, k=n))


def _make_object(i: int, rng: random.Random) -> dict:
    created = datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(
        days=rng.randint(0, 730), seconds=rng.randint(0, 86_399)
    )
    updated = created + timedelta(days=rng.randint(0, 30))
    return {
        "text_word_search_filter": _sentence(rng),
        "text_lowercase_search_filter": _sentence(rng).upper(),
        "text_whitespace_search_filter": _sentence(rng),
        "text_field_search_filter": rng.choice(CATEGORIES),
        "text_searchable_only": _sentence(rng, n=12),
        "text_filterable_only": rng.choice(STATUSES),
        "text_no_index": f"note #{i}: " + _sentence(rng, n=4),
        "text_array_word_search_filter": rng.sample(WORDS, k=4),
        "text_array_field_filterable_only": rng.sample(CATEGORIES, k=2),
        "int_filter_range": rng.randint(0, 1_000_000),
        "int_filter_only": rng.randint(0, 1000),
        "int_range_only": rng.randint(-1000, 1000),
        "int_no_index": rng.randint(0, 10_000),
        "number_filter_range": round(rng.uniform(0.0, 10_000.0), 2),
        "number_filter_only": round(rng.uniform(0.0, 100.0), 4),
        "date_filter_range": created.isoformat(),
        "date_filter_only": updated.isoformat(),
        "bool_filter": rng.random() < 0.5,
        "bool_no_index": rng.random() < 0.3,
        "uuid_filter": str(__import__("uuid").uuid4()),
    }


# -- Main ----------------------------------------------------------------------


def main() -> int:
    sys.stdout.reconfigure(line_buffering=True)
    rng = random.Random(SEED)

    client = weaviate.connect_to_local(port=8080, grpc_port=50051)
    try:
        if client.collections.exists(COLLECTION_NAME):
            print(f"Deleting existing collection {COLLECTION_NAME}...")
            client.collections.delete(COLLECTION_NAME)

        print(f"Creating collection {COLLECTION_NAME} with {len(PROPERTIES)} properties...")
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
        print(f"  Import complete in {elapsed:.1f}s ({NUM_OBJECTS / elapsed:,.0f} obj/s)")

        print()
        print("Ready. The UI can now point at localhost:8080 and load the")
        print(f"'{COLLECTION_NAME}' collection to exercise every index-state combination.")
        print("No reindex migrations have been triggered — do that from the UI.")
        return 0
    finally:
        client.close()


if __name__ == "__main__":
    sys.exit(main())
