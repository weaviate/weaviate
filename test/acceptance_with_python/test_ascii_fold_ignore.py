"""
Tests for asciiFold and asciiFoldIgnore across tokenization strategies.

Requires a running Weaviate instance (e.g. via `make local`).
"""

import json
import urllib.request

import weaviate

BASE_URL = "http://localhost:8080"
COLLECTION_NAME = "ASCIIFoldIgnoreTest"
COLLECTION_NAME_TOKENIZATIONS = "ASCIIFoldTokenizationsTest"


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


def test_ascii_fold_ignore_config() -> None:
    """Verify collection config reflects textAnalyser settings."""
    with weaviate.connect_to_local() as client:
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

        schema = _rest_get(f"/v1/schema/{COLLECTION_NAME}")
        raw_props = {p["name"]: p for p in schema["properties"]}

        title_analyser = raw_props["title"].get("textAnalyser", {})
        assert title_analyser.get("asciiFold") is True
        assert title_analyser.get("asciiFoldIgnore") == ["é"]

        body_analyser = raw_props["body"].get("textAnalyser", {})
        assert body_analyser.get("asciiFold") is True
        assert (
            body_analyser.get("asciiFoldIgnore") is None
            or body_analyser.get("asciiFoldIgnore") == []
        )

        client.collections.delete(COLLECTION_NAME)


def _create_and_populate(client: weaviate.WeaviateClient) -> None:
    """Helper: create the test collection and insert test data."""
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
    collection = client.collections.get(COLLECTION_NAME)
    collection.data.insert_many(
        [
            {"title": "L'école est fermée", "body": "L'école est fermée"},
            {"title": "cafe résumé", "body": "cafe résumé"},
            {"title": "São Paulo café", "body": "São Paulo café"},
        ]
    )


def test_bm25_single_property() -> None:
    """Single-property BM25 searches with accent folding."""
    with weaviate.connect_to_local() as client:
        _create_and_populate(client)
        collection = client.collections.get(COLLECTION_NAME)

        # body (full fold, no ignore): "ecole" matches "école"
        r = collection.query.bm25(query="ecole", query_properties=["body"], limit=5)
        assert len(r.objects) == 1

        # title (ignore é): "ecole" does NOT match
        r = collection.query.bm25(query="ecole", query_properties=["title"], limit=5)
        assert len(r.objects) == 0

        # title: "école" matches (é preserved)
        r = collection.query.bm25(query="école", query_properties=["title"], limit=5)
        assert len(r.objects) == 1

        # body: "cafe" matches both "café" and "cafe"
        r = collection.query.bm25(query="cafe", query_properties=["body"], limit=5)
        assert len(r.objects) == 2

        client.collections.delete(COLLECTION_NAME)


def test_bm25f_multi_property() -> None:
    """Multi-property BM25F searches with mixed ignore lists."""
    with weaviate.connect_to_local() as client:
        _create_and_populate(client)
        collection = client.collections.get(COLLECTION_NAME)

        # "ecole" across [title, body]: matches via body only
        r = collection.query.bm25(
            query="ecole", query_properties=["title", "body"], limit=5
        )
        assert len(r.objects) == 1

        # "école" across [title, body]: matches via both properties
        r = collection.query.bm25(
            query="école", query_properties=["title", "body"], limit=5
        )
        assert len(r.objects) == 1

        # "cafe" across both: 2 docs
        r = collection.query.bm25(
            query="cafe", query_properties=["title", "body"], limit=5
        )
        assert len(r.objects) == 2

        # "café" across both: 2 docs
        r = collection.query.bm25(
            query="café", query_properties=["title", "body"], limit=5
        )
        assert len(r.objects) == 2

        # "resume" across both: 1 doc (via body only)
        r = collection.query.bm25(
            query="resume", query_properties=["title", "body"], limit=5
        )
        assert len(r.objects) == 1

        # "résumé" across both: 1 doc
        r = collection.query.bm25(
            query="résumé", query_properties=["title", "body"], limit=5
        )
        assert len(r.objects) == 1

        client.collections.delete(COLLECTION_NAME)


def test_update_ignore_list() -> None:
    """Updating asciiFoldIgnore does not re-index existing documents."""
    with weaviate.connect_to_local() as client:
        _create_and_populate(client)
        collection = client.collections.get(COLLECTION_NAME)

        # Baseline: é preserved in title index
        r = collection.query.bm25(query="école", query_properties=["title"], limit=5)
        assert len(r.objects) == 1

        r = collection.query.bm25(query="ecole", query_properties=["title"], limit=5)
        assert len(r.objects) == 0

        # Update schema: remove é from ignore list
        class_schema = _rest_get(f"/v1/schema/{COLLECTION_NAME}")
        for prop in class_schema["properties"]:
            if prop["name"] == "title":
                prop["textAnalyser"]["asciiFoldIgnore"] = []
        _rest_put(f"/v1/schema/{COLLECTION_NAME}", class_schema)

        # Verify config updated
        updated = _rest_get(f"/v1/schema/{COLLECTION_NAME}")
        updated_props = {p["name"]: p for p in updated["properties"]}
        ignore = updated_props["title"].get("textAnalyser", {}).get("asciiFoldIgnore")
        assert ignore is None or ignore == []

        # Old docs NOT re-indexed: query now folds é→e, but old index has "école"
        r = collection.query.bm25(query="ecole", query_properties=["title"], limit=5)
        assert len(r.objects) == 0, "old docs still have 'école' in index"

        r = collection.query.bm25(query="école", query_properties=["title"], limit=5)
        assert len(r.objects) == 0, "query 'école' now folded to 'ecole', mismatches old index"

        # New documents use updated config
        collection.data.insert({"title": "nouvelle école", "body": "nouvelle école"})

        r = collection.query.bm25(query="ecole", query_properties=["title"], limit=5)
        assert len(r.objects) == 1, "new doc indexed as 'ecole'"

        r = collection.query.bm25(query="école", query_properties=["title"], limit=5)
        assert len(r.objects) == 1, "query 'école'→'ecole' matches new doc"

        client.collections.delete(COLLECTION_NAME)


def test_tokenization_variants() -> None:
    """asciiFold + ignore across word, lowercase, whitespace, field, trigram."""
    with weaviate.connect_to_local() as client:
        client.collections.delete(COLLECTION_NAME_TOKENIZATIONS)
        client.collections.create_from_dict(
            {
                "class": COLLECTION_NAME_TOKENIZATIONS,
                "vectorizer": "none",
                "properties": [
                    {
                        "name": "wordProp",
                        "dataType": ["text"],
                        "tokenization": "word",
                        "textAnalyser": {"asciiFold": True, "asciiFoldIgnore": ["é"]},
                    },
                    {
                        "name": "lowercaseProp",
                        "dataType": ["text"],
                        "tokenization": "lowercase",
                        "textAnalyser": {"asciiFold": True, "asciiFoldIgnore": ["é"]},
                    },
                    {
                        "name": "whitespaceProp",
                        "dataType": ["text"],
                        "tokenization": "whitespace",
                        "textAnalyser": {"asciiFold": True, "asciiFoldIgnore": ["é"]},
                    },
                    {
                        "name": "fieldProp",
                        "dataType": ["text"],
                        "tokenization": "field",
                        "textAnalyser": {"asciiFold": True, "asciiFoldIgnore": ["é"]},
                    },
                    {
                        "name": "trigramProp",
                        "dataType": ["text"],
                        "tokenization": "trigram",
                        "textAnalyser": {"asciiFold": True, "asciiFoldIgnore": ["é"]},
                    },
                    {
                        "name": "wordNoIgnore",
                        "dataType": ["text"],
                        "tokenization": "word",
                        "textAnalyser": {"asciiFold": True},
                    },
                    {
                        "name": "lowercaseNoIgnore",
                        "dataType": ["text"],
                        "tokenization": "lowercase",
                        "textAnalyser": {"asciiFold": True},
                    },
                    {
                        "name": "whitespaceNoIgnore",
                        "dataType": ["text"],
                        "tokenization": "whitespace",
                        "textAnalyser": {"asciiFold": True},
                    },
                    {
                        "name": "fieldNoIgnore",
                        "dataType": ["text"],
                        "tokenization": "field",
                        "textAnalyser": {"asciiFold": True},
                    },
                    {
                        "name": "trigramNoIgnore",
                        "dataType": ["text"],
                        "tokenization": "trigram",
                        "textAnalyser": {"asciiFold": True},
                    },
                ],
            }
        )

        text = "L'école est fermée"
        collection = client.collections.get(COLLECTION_NAME_TOKENIZATIONS)
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
                    "whitespaceNoIgnore": text,
                    "fieldNoIgnore": text,
                    "trigramNoIgnore": text,
                },
            ]
        )

        def bm25(q, props):
            return collection.query.bm25(query=q, query_properties=props, limit=5)

        # --- word: splits on non-alphanumeric, lowercased ---
        # Tokens: ["l", "école", "est", "fermée"]
        assert len(bm25("école", ["wordProp"]).objects) == 1
        assert len(bm25("ecole", ["wordProp"]).objects) == 0, "é preserved"
        assert len(bm25("fermee", ["wordProp"]).objects) == 0, "é preserved"
        assert len(bm25("école", ["wordNoIgnore"]).objects) == 1
        assert len(bm25("ecole", ["wordNoIgnore"]).objects) == 1
        assert len(bm25("fermee", ["wordNoIgnore"]).objects) == 1

        # --- lowercase: splits on whitespace, lowercased ---
        # Tokens: ["l'école", "est", "fermée"]
        assert len(bm25("l'école", ["lowercaseProp"]).objects) == 1
        assert len(bm25("l'ecole", ["lowercaseProp"]).objects) == 0, "é preserved"
        assert len(bm25("fermée", ["lowercaseProp"]).objects) == 1
        assert len(bm25("fermee", ["lowercaseProp"]).objects) == 0, "é preserved"
        assert len(bm25("l'ecole", ["lowercaseNoIgnore"]).objects) == 1
        assert len(bm25("l'école", ["lowercaseNoIgnore"]).objects) == 1
        assert len(bm25("fermee", ["lowercaseNoIgnore"]).objects) == 1

        # --- whitespace: splits on whitespace, case-preserved ---
        # Tokens: ["L'école", "est", "fermée"]
        assert len(bm25("L'école", ["whitespaceProp"]).objects) == 1
        assert len(bm25("L'ecole", ["whitespaceProp"]).objects) == 0, "é preserved"
        assert len(bm25("L'ecole", ["whitespaceNoIgnore"]).objects) == 1
        assert len(bm25("L'école", ["whitespaceNoIgnore"]).objects) == 1

        # --- field: entire value as one token, case-preserved ---
        assert len(bm25("L'école est fermée", ["fieldProp"]).objects) == 1
        assert len(bm25("L'ecole est fermee", ["fieldProp"]).objects) == 0, "é preserved"
        assert len(bm25("L'ecole est fermee", ["fieldNoIgnore"]).objects) == 1
        assert len(bm25("L'école est fermée", ["fieldNoIgnore"]).objects) == 1

        # --- trigram: 3-char sliding windows ---
        assert len(bm25("éco", ["trigramProp"]).objects) == 1
        assert len(bm25("eco", ["trigramProp"]).objects) == 0, "é preserved"
        assert len(bm25("eco", ["trigramNoIgnore"]).objects) == 1
        assert len(bm25("éco", ["trigramNoIgnore"]).objects) == 1

        # --- Cross-tokenization BM25F ---
        assert len(bm25("ecole", ["wordProp", "wordNoIgnore"]).objects) == 1
        assert len(bm25("école", ["wordProp", "wordNoIgnore"]).objects) == 1
        assert len(bm25("eco", ["trigramProp", "trigramNoIgnore"]).objects) == 1

        client.collections.delete(COLLECTION_NAME_TOKENIZATIONS)
