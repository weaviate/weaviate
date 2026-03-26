"""
Test script for creating a collection with asciiFold enabled
and updating the asciiFoldIgnore list.

Usage:
    Requires a running Weaviate instance (e.g. via `make local`).
    pip install weaviate-client
    python test_ascii_fold_ignore.py
"""

import json
import urllib.request

import weaviate


BASE_URL = "http://localhost:8080"
COLLECTION_NAME = "ASCIIFoldIgnoreTest"


def _rest_get(path: str) -> dict:
    """GET JSON from the Weaviate REST API (stdlib only)."""
    with urllib.request.urlopen(f"{BASE_URL}{path}") as resp:
        return json.loads(resp.read())


def _rest_put(path: str, body: dict) -> None:
    """PUT JSON to the Weaviate REST API (stdlib only)."""
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        f"{BASE_URL}{path}",
        data=data,
        method="PUT",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req) as resp:
        resp.read()
COLLECTION_NAME_TOKENIZATIONS = "ASCIIFoldTokenizationsTest"


def create_collection_with_ascii_fold(client: weaviate.WeaviateClient) -> None:
    """Create a collection with asciiFold enabled and an initial ignore list."""
    client.collections.delete(COLLECTION_NAME)

    client.collections.create_from_dict(
        {
            "class": COLLECTION_NAME,
            "vectorizer": "none",
            "properties": [
                {
                    "name": "title",
                    "dataType": ["text"],
                    "tokenization": "word",
                    "textAnalyser": {
                        "asciiFold": True,
                        "asciiFoldIgnore": ["é"],
                    },
                },
                {
                    "name": "body",
                    "dataType": ["text"],
                    "tokenization": "word",
                    "textAnalyser": {
                        "asciiFold": True,
                        "asciiFoldIgnore": [],
                    },
                },
            ],
        }
    )
    print(f"Created collection '{COLLECTION_NAME}' with asciiFold enabled.")


def verify_collection_config(client: weaviate.WeaviateClient) -> None:
    """Verify the collection config reflects the textAnalyser settings."""
    # Use the REST API directly to check raw schema since the Python client
    # may not expose textAnalyser fields yet.
    schema = _rest_get(f"/v1/schema/{COLLECTION_NAME}")
    raw_props = {p["name"]: p for p in schema["properties"]}

    title_analyser = raw_props["title"].get("textAnalyser", {})
    body_analyser = raw_props["body"].get("textAnalyser", {})

    print("\n--- Property: title ---")
    print(json.dumps(title_analyser, indent=2))
    assert title_analyser.get("asciiFold") is True, "title should have asciiFold=true"
    assert title_analyser.get("asciiFoldIgnore") == [
        "é"
    ], "title should ignore 'é'"

    print("\n--- Property: body ---")
    print(json.dumps(body_analyser, indent=2))
    assert body_analyser.get("asciiFold") is True, "body should have asciiFold=true"
    assert (
        body_analyser.get("asciiFoldIgnore") is None
        or body_analyser.get("asciiFoldIgnore") == []
    ), "body should have empty ignore list"

    print("\nConfig verification passed.")


def insert_test_data(client: weaviate.WeaviateClient) -> None:
    """Insert test objects with accented text."""
    collection = client.collections.get(COLLECTION_NAME)

    collection.data.insert_many(
        [
            # title has asciiFoldIgnore=["é"], so 'é' is preserved in title index
            # body has no ignore list, so everything is folded
            {"title": "L'école est fermée", "body": "L'école est fermée"},
            {"title": "cafe résumé", "body": "cafe résumé"},
            {"title": "São Paulo café", "body": "São Paulo café"},
        ]
    )
    print(f"\nInserted 3 test objects into '{COLLECTION_NAME}'.")


def search_and_verify(client: weaviate.WeaviateClient) -> None:
    """Run single-property BM25 searches to verify accent folding behavior."""
    collection = client.collections.get(COLLECTION_NAME)

    print("\n--- BM25 Single-Property Searches ---")

    # Search body (full fold, no ignore): "ecole" should match "école"
    results = collection.query.bm25(query="ecole", query_properties=["body"], limit=5)
    print(f"\nSearch 'ecole' in body: {len(results.objects)} results")
    for obj in results.objects:
        print(f"  - title: {obj.properties['title']}, body: {obj.properties['body']}")
    assert len(results.objects) == 1, f"Expected 1 result for 'ecole' in body, got {len(results.objects)}"

    # Search title (ignore é): "ecole" should NOT match since é is preserved
    results = collection.query.bm25(query="ecole", query_properties=["title"], limit=5)
    print(f"\nSearch 'ecole' in title: {len(results.objects)} results")
    for obj in results.objects:
        print(f"  - title: {obj.properties['title']}")
    assert len(results.objects) == 0, f"Expected 0 results for 'ecole' in title (é ignored), got {len(results.objects)}"

    # Search title with the accent: "école" should match
    results = collection.query.bm25(query="école", query_properties=["title"], limit=5)
    print(f"\nSearch 'école' in title: {len(results.objects)} results")
    for obj in results.objects:
        print(f"  - title: {obj.properties['title']}")
    assert len(results.objects) == 1, f"Expected 1 result for 'école' in title, got {len(results.objects)}"

    # Search body for "cafe" - should match both "café" and "cafe" since body folds all
    results = collection.query.bm25(query="cafe", query_properties=["body"], limit=5)
    print(f"\nSearch 'cafe' in body (full fold): {len(results.objects)} results")
    for obj in results.objects:
        print(f"  - body: {obj.properties['body']}")
    assert len(results.objects) == 2, f"Expected 2 results for 'cafe' in body, got {len(results.objects)}"

    print("\nSingle-property BM25 tests passed.")


def search_and_verify_bm25f(client: weaviate.WeaviateClient) -> None:
    """Run multi-property BM25F searches to verify accent folding with ignore."""
    collection = client.collections.get(COLLECTION_NAME)

    print("\n--- BM25F Multi-Property Searches ---")

    # BM25F across both title and body for "ecole"
    # body (full fold): matches "école" → 1 hit
    # title (ignore é): "ecole" != "école" → 0 hits
    # Combined: should return 1 result (matched via body only)
    results = collection.query.bm25(
        query="ecole", query_properties=["title", "body"], limit=5
    )
    print(f"\nBM25F 'ecole' in [title, body]: {len(results.objects)} results")
    for obj in results.objects:
        print(f"  - title: {obj.properties['title']}, body: {obj.properties['body']}")
    assert len(results.objects) == 1, (
        f"Expected 1 result for 'ecole' in [title, body], got {len(results.objects)}"
    )

    # BM25F across both for "école"
    # body (full fold): "école" → folded to "ecole" → matches indexed "ecole" → 1 hit
    # title (ignore é): "école" stays "école" → matches indexed "école" → 1 hit
    # Same document, so combined: 1 result (with higher score from both properties)
    results = collection.query.bm25(
        query="école", query_properties=["title", "body"], limit=5
    )
    print(f"\nBM25F 'école' in [title, body]: {len(results.objects)} results")
    for obj in results.objects:
        print(f"  - title: {obj.properties['title']}, body: {obj.properties['body']}")
    assert len(results.objects) == 1, (
        f"Expected 1 result for 'école' in [title, body], got {len(results.objects)}"
    )

    # BM25F across both for "cafe"
    # body (full fold): "cafe" matches "café" and "cafe" → 2 docs
    # title (ignore é): "cafe" stays "cafe", "café" stays "café" in index
    #   → "cafe" matches "cafe" (from "cafe résumé") → 1 doc
    # Combined: should find objects that have "cafe" in either property
    results = collection.query.bm25(
        query="cafe", query_properties=["title", "body"], limit=5
    )
    print(f"\nBM25F 'cafe' in [title, body]: {len(results.objects)} results")
    for obj in results.objects:
        print(f"  - title: {obj.properties['title']}, body: {obj.properties['body']}")
    assert len(results.objects) == 2, (
        f"Expected 2 results for 'cafe' in [title, body], got {len(results.objects)}"
    )

    # BM25F for "café" across both
    # body (full fold): "café" → folded to "cafe" → matches 2 docs
    # title (ignore é): "café" stays "café" → matches "São Paulo café" → 1 doc
    # Combined: 2 unique docs
    results = collection.query.bm25(
        query="café", query_properties=["title", "body"], limit=5
    )
    print(f"\nBM25F 'café' in [title, body]: {len(results.objects)} results")
    for obj in results.objects:
        print(f"  - title: {obj.properties['title']}, body: {obj.properties['body']}")
    assert len(results.objects) == 2, (
        f"Expected 2 results for 'café' in [title, body], got {len(results.objects)}"
    )

    # BM25F for "resume" (no accent) across both
    # body (full fold): "resume" matches "résumé" (folded to "resume") → 1 doc
    # title (ignore é): "resume" != "résumé" (é preserved) → 0 hits
    # Combined: 1 result
    results = collection.query.bm25(
        query="resume", query_properties=["title", "body"], limit=5
    )
    print(f"\nBM25F 'resume' in [title, body]: {len(results.objects)} results")
    for obj in results.objects:
        print(f"  - title: {obj.properties['title']}, body: {obj.properties['body']}")
    assert len(results.objects) == 1, (
        f"Expected 1 result for 'resume' in [title, body], got {len(results.objects)}"
    )

    # BM25F for "résumé" (with accents) across both
    # body (full fold): "résumé" → folded to "resume" → matches 1 doc
    # title (ignore é): "résumé" → é preserved, other accents folded → "résumé" → matches 1 doc
    # Same document, so combined: 1 result
    results = collection.query.bm25(
        query="résumé", query_properties=["title", "body"], limit=5
    )
    print(f"\nBM25F 'résumé' in [title, body]: {len(results.objects)} results")
    for obj in results.objects:
        print(f"  - title: {obj.properties['title']}, body: {obj.properties['body']}")
    assert len(results.objects) == 1, (
        f"Expected 1 result for 'résumé' in [title, body], got {len(results.objects)}"
    )

    print("\nBM25F multi-property tests passed.")


def update_ignore_list_and_verify(client: weaviate.WeaviateClient) -> None:
    """Update the asciiFoldIgnore list and verify old documents are not re-indexed."""
    collection = client.collections.get(COLLECTION_NAME)

    print("\n--- Update Ignore List Test ---")

    # Phase 1: Verify baseline before update
    print("\n[Phase 1] Baseline with asciiFoldIgnore=['é']")

    results = collection.query.bm25(query="école", query_properties=["title"], limit=5)
    print(f"  Search 'école' in title: {len(results.objects)} results")
    assert len(results.objects) == 1, f"Expected 1 (é preserved in index), got {len(results.objects)}"

    results = collection.query.bm25(query="ecole", query_properties=["title"], limit=5)
    print(f"  Search 'ecole' in title: {len(results.objects)} results")
    assert len(results.objects) == 0, f"Expected 0 (é preserved, ecole≠école), got {len(results.objects)}"

    # Phase 2: Update schema — remove é from ignore list via REST API
    print("\n[Phase 2] Updating asciiFoldIgnore to [] (remove é)")
    class_schema = _rest_get(f"/v1/schema/{COLLECTION_NAME}")

    # Update the title property's ignore list
    for prop in class_schema["properties"]:
        if prop["name"] == "title":
            prop["textAnalyser"]["asciiFoldIgnore"] = []

    _rest_put(f"/v1/schema/{COLLECTION_NAME}", class_schema)
    print("  Schema updated successfully.")

    # Verify the config was updated
    updated_schema = _rest_get(f"/v1/schema/{COLLECTION_NAME}")
    updated_props = {p["name"]: p for p in updated_schema["properties"]}
    title_ignore = updated_props["title"].get("textAnalyser", {}).get("asciiFoldIgnore")
    assert title_ignore is None or title_ignore == [], (
        f"Expected empty ignore list after update, got {title_ignore}"
    )
    print("  Config verified: asciiFoldIgnore is now empty.")

    # Phase 3: Old documents are NOT re-indexed
    # Old docs have 'école' in the index (é was preserved at index time).
    # Now queries fold é→e, so 'école' query → 'ecole' — doesn't match old 'école' in index.
    print("\n[Phase 3] Verify old documents are NOT re-indexed")

    results = collection.query.bm25(query="ecole", query_properties=["title"], limit=5)
    print(f"  Search 'ecole' in title: {len(results.objects)} results")
    assert len(results.objects) == 0, (
        f"Expected 0 (old docs still have 'école' in index, query 'ecole' doesn't match), got {len(results.objects)}"
    )

    results = collection.query.bm25(query="école", query_properties=["title"], limit=5)
    print(f"  Search 'école' in title: {len(results.objects)} results")
    assert len(results.objects) == 0, (
        f"Expected 0 (query 'école' now folded to 'ecole', old index has 'école' — mismatch), got {len(results.objects)}"
    )

    print("  Confirmed: old documents were NOT re-indexed.")

    # Phase 4: New documents use updated config
    print("\n[Phase 4] Insert new document and verify it uses updated config")

    collection.data.insert({"title": "nouvelle école", "body": "nouvelle école"})
    print("  Inserted new document with 'école'.")

    # New doc indexed with full fold (é no longer ignored): 'école' → 'ecole'
    results = collection.query.bm25(query="ecole", query_properties=["title"], limit=5)
    print(f"  Search 'ecole' in title: {len(results.objects)} results")
    assert len(results.objects) == 1, (
        f"Expected 1 (new doc indexed as 'ecole'), got {len(results.objects)}"
    )

    results = collection.query.bm25(query="école", query_properties=["title"], limit=5)
    print(f"  Search 'école' in title: {len(results.objects)} results")
    assert len(results.objects) == 1, (
        f"Expected 1 (query 'école'→'ecole' matches new doc), got {len(results.objects)}"
    )

    print("\n  Update test passed.")


def test_tokenization_variants(client: weaviate.WeaviateClient) -> None:
    """Test asciiFold + ignore across different tokenization strategies."""
    client.collections.delete(COLLECTION_NAME_TOKENIZATIONS)

    # Create collection with properties using different tokenizations,
    # each with asciiFold=true and asciiFoldIgnore=["é"]
    client.collections.create_from_dict(
        {
            "class": COLLECTION_NAME_TOKENIZATIONS,
            "vectorizer": "none",
            "properties": [
                {
                    "name": "wordProp",
                    "dataType": ["text"],
                    "tokenization": "word",
                    "textAnalyser": {
                        "asciiFold": True,
                        "asciiFoldIgnore": ["é"],
                    },
                },
                {
                    "name": "lowercaseProp",
                    "dataType": ["text"],
                    "tokenization": "lowercase",
                    "textAnalyser": {
                        "asciiFold": True,
                        "asciiFoldIgnore": ["é"],
                    },
                },
                {
                    "name": "whitespaceProp",
                    "dataType": ["text"],
                    "tokenization": "whitespace",
                    "textAnalyser": {
                        "asciiFold": True,
                        "asciiFoldIgnore": ["é"],
                    },
                },
                {
                    "name": "fieldProp",
                    "dataType": ["text"],
                    "tokenization": "field",
                    "textAnalyser": {
                        "asciiFold": True,
                        "asciiFoldIgnore": ["é"],
                    },
                },
                {
                    "name": "trigramProp",
                    "dataType": ["text"],
                    "tokenization": "trigram",
                    "textAnalyser": {
                        "asciiFold": True,
                        "asciiFoldIgnore": ["é"],
                    },
                },
                {
                    "name": "wordNoIgnore",
                    "dataType": ["text"],
                    "tokenization": "word",
                    "textAnalyser": {
                        "asciiFold": True,
                    },
                },
                {
                    "name": "lowercaseNoIgnore",
                    "dataType": ["text"],
                    "tokenization": "lowercase",
                    "textAnalyser": {
                        "asciiFold": True,
                    },
                },
                {
                    "name": "whitesspaceNoIgnore",
                    "dataType": ["text"],
                    "tokenization": "whitespace",
                    "textAnalyser": {
                        "asciiFold": True,
                    },
                },
                {
                    "name": "fieldNoIgnore",
                    "dataType": ["text"],
                    "tokenization": "field",
                    "textAnalyser": {
                        "asciiFold": True,
                    },
                },
                {
                    "name": "trigramNoIgnore",
                    "dataType": ["text"],
                    "tokenization": "trigram",
                    "textAnalyser": {
                        "asciiFold": True,
                    },
                },
            ],
        }
    )
    print(f"\nCreated collection '{COLLECTION_NAME_TOKENIZATIONS}' with multiple tokenizations.")

    collection = client.collections.get(COLLECTION_NAME_TOKENIZATIONS)
    text = "L'école est fermée"
    collection.data.insert_many(
        [
            {
                "wordProp": text,
                "lowercaseProp": text,
                "whitespaceProp": text,
                "fieldProp": text,
                "trigramProp": text,
                "wordNoIgnore": text,
                "lowercaseNoIgnore": text,
                "whitesspaceNoIgnore": text,
                "fieldNoIgnore": text,
                "trigramNoIgnore": text,
            },
        ]
    )
    print("  Inserted test data.")

    print("\n--- Tokenization Variant Tests ---")

    # Each tokenization splits text differently:
    #   word:       "L'école est fermée" → ["l", "école", "est", "fermée"]
    #   lowercase:  "L'école est fermée" → ["l'école", "est", "fermée"]
    #   whitespace: "L'école est fermée" → ["L'école", "est", "fermée"]
    #   field:      "L'école est fermée" → ["L'école est fermée"]
    #   trigram:    "L'école est fermée" → 3-char sliding windows
    #
    # Queries must use tokens appropriate for each tokenization.

    # --- word tokenization (splits on non-alphanumeric, lowercased) ---
    print("\n  word tokenization:")

    def bm25(q, props):
        return collection.query.bm25(
            query=q, query_properties=props, limit=5
        )

    # With ignore=["é"]: é preserved in index
    r = bm25("école", ["wordProp"])
    print(f"    wordProp: 'école' → {len(r.objects)} results")
    assert len(r.objects) == 1

    r = bm25("ecole", ["wordProp"])
    print(f"    wordProp: 'ecole' → {len(r.objects)} results")
    assert len(r.objects) == 0, "é preserved, ecole≠école"

    r = bm25("fermee", ["wordProp"])
    print(f"    wordProp: 'fermee' → {len(r.objects)} results")
    assert len(r.objects) == 0, "é preserved in fermée"

    # Without ignore: full fold
    r = bm25("école", ["wordNoIgnore"])
    print(f"    wordNoIgnore: 'école' → {len(r.objects)} results")
    assert len(r.objects) == 1

    r = bm25("ecole", ["wordNoIgnore"])
    print(f"    wordNoIgnore: 'ecole' → {len(r.objects)} results")
    assert len(r.objects) == 1

    r = bm25("fermee", ["wordNoIgnore"])
    print(f"    wordNoIgnore: 'fermee' → {len(r.objects)} results")
    assert len(r.objects) == 1

    # --- lowercase tokenization (splits on whitespace, lowercased) ---
    # Tokens: ["l'école", "est", "fermée"]
    print("\n  lowercase tokenization:")

    r = bm25("l'école", ["lowercaseProp"])
    print(f"    lowercaseProp: \"l'école\" → {len(r.objects)} results")
    assert len(r.objects) == 1

    r = bm25("l'ecole", ["lowercaseProp"])
    print(f"    lowercaseProp: \"l'ecole\" → {len(r.objects)} results")
    assert len(r.objects) == 0, "é preserved"

    r = bm25("fermée", ["lowercaseProp"])
    print(f"    lowercaseProp: 'fermée' → {len(r.objects)} results")
    assert len(r.objects) == 1

    r = bm25("fermee", ["lowercaseProp"])
    print(f"    lowercaseProp: 'fermee' → {len(r.objects)} results")
    assert len(r.objects) == 0, "é preserved in fermée"

    # Without ignore: full fold
    r = bm25("l'ecole", ["lowercaseNoIgnore"])
    print(f"    lowercaseNoIgnore: \"l'ecole\" → {len(r.objects)} results")
    assert len(r.objects) == 1

    r = bm25("l'école", ["lowercaseNoIgnore"])
    print(f"    lowercaseNoIgnore: \"l'école\" → {len(r.objects)} results")
    assert len(r.objects) == 1

    r = bm25("fermee", ["lowercaseNoIgnore"])
    print(f"    lowercaseNoIgnore: 'fermee' → {len(r.objects)} results")
    assert len(r.objects) == 1

    # --- whitespace tokenization (splits on whitespace, case-preserved) ---
    # Tokens: ["L'école", "est", "fermée"]
    print("\n  whitespace tokenization:")

    r = bm25("L'école", ["whitespaceProp"])
    print(f"    whitespaceProp: \"L'école\" → {len(r.objects)} results")
    assert len(r.objects) == 1

    r = bm25("L'ecole", ["whitespaceProp"])
    print(f"    whitespaceProp: \"L'ecole\" → {len(r.objects)} results")
    assert len(r.objects) == 0, "é preserved"

    # Without ignore: full fold (but case preserved)
    r = bm25("L'ecole", ["whitesspaceNoIgnore"])
    print(
        f"    whitesspaceNoIgnore: \"L'ecole\" → "
        f"{len(r.objects)} results"
    )
    assert len(r.objects) == 1

    r = bm25("L'école", ["whitesspaceNoIgnore"])
    print(
        f"    whitesspaceNoIgnore: \"L'école\" → "
        f"{len(r.objects)} results"
    )
    assert len(r.objects) == 1

    # --- field tokenization (entire value as one token) ---
    print("\n  field tokenization:")

    r = bm25("L'école est fermée", ["fieldProp"])
    print(f"    fieldProp: exact text → {len(r.objects)} results")
    assert len(r.objects) == 1

    r = bm25("L'ecole est fermee", ["fieldProp"])
    print(f"    fieldProp: folded text → {len(r.objects)} results")
    assert len(r.objects) == 0, "é preserved in field"

    # Field tokenization preserves case, so folded query must match case
    r = bm25("L'ecole est fermee", ["fieldNoIgnore"])
    print(f"    fieldNoIgnore: folded text → {len(r.objects)} results")
    assert len(r.objects) == 1

    r = bm25("L'école est fermée", ["fieldNoIgnore"])
    print(f"    fieldNoIgnore: original text → {len(r.objects)} results")
    assert len(r.objects) == 1

    # --- trigram tokenization (3-char sliding windows) ---
    print("\n  trigram tokenization:")

    r = bm25("éco", ["trigramProp"])
    print(f"    trigramProp: 'éco' → {len(r.objects)} results")
    assert len(r.objects) == 1

    r = bm25("eco", ["trigramProp"])
    print(f"    trigramProp: 'eco' → {len(r.objects)} results")
    assert len(r.objects) == 0, "é preserved in trigrams"

    r = bm25("eco", ["trigramNoIgnore"])
    print(f"    trigramNoIgnore: 'eco' → {len(r.objects)} results")
    assert len(r.objects) == 1

    r = bm25("éco", ["trigramNoIgnore"])
    print(f"    trigramNoIgnore: 'éco' → {len(r.objects)} results")
    assert len(r.objects) == 1

    # --- Cross-tokenization BM25F ---
    print("\n  Cross-tokenization BM25F:")

    # word(ignore) + lowercase(no ignore): "ecole" matches via word
    # on lowercaseNoIgnore, but "ecole" is not a token there
    # (lowercase has "l'ecole"). Only wordNoIgnore would match.
    r = bm25("ecole", ["wordProp", "wordNoIgnore"])
    print(
        f"    'ecole' in [wordProp(ignore), wordNoIgnore]: "
        f"{len(r.objects)} results"
    )
    assert len(r.objects) == 1, "matched via wordNoIgnore"

    r = bm25("école", ["wordProp", "wordNoIgnore"])
    print(
        f"    'école' in [wordProp(ignore), wordNoIgnore]: "
        f"{len(r.objects)} results"
    )
    assert len(r.objects) == 1, "matched via both"

    r = bm25("eco", ["trigramProp", "trigramNoIgnore"])
    print(
        f"    'eco' in [trigramProp(ignore), trigramNoIgnore]: "
        f"{len(r.objects)} results"
    )
    assert len(r.objects) == 1, "matched via trigramNoIgnore"

    client.collections.delete(COLLECTION_NAME_TOKENIZATIONS)
    print("\n  Tokenization variant tests passed.")


def cleanup(client: weaviate.WeaviateClient) -> None:
    """Delete the test collection."""
    client.collections.delete(COLLECTION_NAME)
    print(f"\nDeleted collection '{COLLECTION_NAME}'.")


def main() -> None:
    with weaviate.connect_to_local() as client:
        print(f"Connected to Weaviate {client.get_meta()['version']}\n")

        create_collection_with_ascii_fold(client)
        verify_collection_config(client)
        insert_test_data(client)
        search_and_verify(client)
        search_and_verify_bm25f(client)
        update_ignore_list_and_verify(client)
        cleanup(client)
        test_tokenization_variants(client)

    print("\nDone.")


if __name__ == "__main__":
    main()
