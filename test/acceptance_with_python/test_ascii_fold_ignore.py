"""
Tests for asciiFold and asciiFoldIgnore across tokenization strategies.

Requires a running Weaviate instance (e.g. via `make local`).
"""

import json
import urllib.error
import urllib.request
from typing import Generator

import os
import pytest
import weaviate.classes as wvc


BASE_URL = os.environ.get("WEAVIATE_URL", "http://localhost:8080")


def _sanitize_collection_name(name: str) -> str:
    name = (
        name.replace("[", "")
        .replace("]", "")
        .replace("-", "")
        .replace(" ", "")
        .replace(".", "")
        .replace("{", "")
        .replace("}", "")
    )
    return name[0].upper() + name[1:]


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
    try:
        with urllib.request.urlopen(req) as resp:
            resp.read()
    except urllib.error.HTTPError as e:
        err_body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"PUT {path} failed with {e.code}: {err_body}"
        ) from e


def _create_and_populate(client, collection_name: str) -> None:
    """Helper: create the test collection and insert test data."""
    client.collections.delete(collection_name)
    client.collections.create_from_dict(
        {
            "class": collection_name,
            "vectorizer": "none",
            "properties": [
                {
                    "name": "title",
                    "dataType": ["text"],
                    "tokenization": "word",
                    "textAnalyzer": {
                        "asciiFold": True,
                        "asciiFoldIgnore": ["é", "Ł"],
                    },
                },
                {
                    "name": "body",
                    "dataType": ["text"],
                    "tokenization": "word",
                    "textAnalyzer": {
                        "asciiFold": True,
                        "asciiFoldIgnore": [],
                    },
                },
            ],
        }
    )
    collection = client.collections.get(collection_name)
    collection.data.insert_many(
        [
            {"title": "L'école est fermée", "body": "L'école est fermée"},
            {"title": "cafe résumé", "body": "cafe résumé"},
            {"title": "São Paulo café", "body": "São Paulo café"},
            {"title": "Łódź", "body": "Łódź"},
        ]
    )


class TestASCIIFoldConfig:
    """Tests for asciiFold configuration."""

    @pytest.fixture(autouse=True)
    def setup(self, request, weaviate_client) -> Generator[None, None, None]:
        self.client = weaviate_client()
        self.collection_name = _sanitize_collection_name(request.node.name)
        self.client.collections.delete(self.collection_name)
        yield
        self.client.collections.delete(self.collection_name)
        self.client.close()

    def test_ascii_fold_ignore_config(self) -> None:
        """Verify collection config reflects textAnalyzer settings."""
        self.client.collections.create_from_dict(
            {
                "class": self.collection_name,
                "vectorizer": "none",
                "properties": [
                    {
                        "name": "title",
                        "dataType": ["text"],
                        "tokenization": "word",
                        "textAnalyzer": {
                            "asciiFold": True,
                            "asciiFoldIgnore": ["é"],
                        },
                    },
                    {
                        "name": "body",
                        "dataType": ["text"],
                        "tokenization": "word",
                        "textAnalyzer": {
                            "asciiFold": True,
                            "asciiFoldIgnore": [],
                        },
                    },
                ],
            }
        )

        schema = _rest_get(f"/v1/schema/{self.collection_name}")
        raw_props = {p["name"]: p for p in schema["properties"]}

        title_analyzer = raw_props["title"].get("textAnalyzer", {})
        assert title_analyzer.get("asciiFold") is True
        assert title_analyzer.get("asciiFoldIgnore") == ["é"]

        body_analyzer = raw_props["body"].get("textAnalyzer", {})
        assert body_analyzer.get("asciiFold") is True
        assert (
            body_analyzer.get("asciiFoldIgnore") is None
            or body_analyzer.get("asciiFoldIgnore") == []
        )


class TestASCIIFoldBM25:
    """Tests for BM25 searches with accent folding."""

    @pytest.fixture(autouse=True)
    def setup(self, request, weaviate_client) -> Generator[None, None, None]:
        self.client = weaviate_client()
        self.collection_name = _sanitize_collection_name(request.node.name)
        _create_and_populate(self.client, self.collection_name)
        self.collection = self.client.collections.get(self.collection_name)
        yield
        self.client.collections.delete(self.collection_name)
        self.client.close()

    def test_bm25_single_property(self) -> None:
        """Single-property BM25 searches with accent folding."""
        # body (full fold, no ignore): "ecole" matches "école"
        r = self.collection.query.bm25(query="ecole", query_properties=["body"], limit=5)
        assert len(r.objects) == 1

        # title (ignore é): "ecole" does NOT match
        r = self.collection.query.bm25(query="ecole", query_properties=["title"], limit=5)
        assert len(r.objects) == 0

        # title: "école" matches (é preserved)
        r = self.collection.query.bm25(query="école", query_properties=["title"], limit=5)
        assert len(r.objects) == 1

        # body: "cafe" matches both "café" and "cafe"
        r = self.collection.query.bm25(query="cafe", query_properties=["body"], limit=5)
        assert len(r.objects) == 2

        assert len(self.collection.query.bm25("Łódź", query_properties=["title"]).objects) == 1
        assert len(self.collection.query.bm25("Lodz", query_properties=["title"]).objects) == 0 # Ł preserved
        assert len(self.collection.query.bm25("Lodz", query_properties=["body"]).objects) == 1 # Ł folded to L
        assert len(self.collection.query.bm25("Łódź", query_properties=["body"]).objects) == 1


    def test_bm25f_multi_property(self) -> None:
        """Multi-property BM25F searches with mixed ignore lists."""
        # "ecole" across [title, body]: matches via body only
        r = self.collection.query.bm25(
            query="ecole", query_properties=["title", "body"], limit=5
        )
        assert len(r.objects) == 1

        # "école" across [title, body]: matches via both properties
        r = self.collection.query.bm25(
            query="école", query_properties=["title", "body"], limit=5
        )
        assert len(r.objects) == 1

        # "cafe" across both: 2 docs
        r = self.collection.query.bm25(
            query="cafe", query_properties=["title", "body"], limit=5
        )
        assert len(r.objects) == 2

        # "café" across both: 2 docs
        r = self.collection.query.bm25(
            query="café", query_properties=["title", "body"], limit=5
        )
        assert len(r.objects) == 2

        # "resume" across both: 1 doc (via body only)
        r = self.collection.query.bm25(
            query="resume", query_properties=["title", "body"], limit=5
        )
        assert len(r.objects) == 1

        # "résumé" across both: 1 doc
        r = self.collection.query.bm25(
            query="résumé", query_properties=["title", "body"], limit=5
        )
        assert len(r.objects) == 1


class TestASCIIFoldUpdateIgnoreList:
    """Tests for updating asciiFoldIgnore."""

    @pytest.fixture(autouse=True)
    def setup(self, request, weaviate_client) -> Generator[None, None, None]:
        self.client = weaviate_client()
        self.collection_name = _sanitize_collection_name(request.node.name)
        _create_and_populate(self.client, self.collection_name)
        self.collection = self.client.collections.get(self.collection_name)
        yield
        self.client.collections.delete(self.collection_name)
        self.client.close()

    def test_update_ignore_list(self) -> None:
        """Updating asciiFoldIgnore does not re-index existing documents."""
        # Baseline: é preserved in title index
        r = self.collection.query.bm25(query="école", query_properties=["title"], limit=5)
        assert len(r.objects) == 1

        r = self.collection.query.bm25(query="ecole", query_properties=["title"], limit=5)
        assert len(r.objects) == 0

        # Update schema: remove é from ignore list
        class_schema = _rest_get(f"/v1/schema/{self.collection_name}")
        for prop in class_schema["properties"]:
            if prop["name"] == "title":
                prop["textAnalyzer"]["asciiFoldIgnore"] = []
        _rest_put(f"/v1/schema/{self.collection_name}", class_schema)

        # Verify config updated
        updated = _rest_get(f"/v1/schema/{self.collection_name}")
        updated_props = {p["name"]: p for p in updated["properties"]}
        ignore = updated_props["title"].get("textAnalyzer", {}).get("asciiFoldIgnore")
        assert ignore is None or ignore == []

        # Old docs NOT re-indexed: query now folds é→e, but old index has "école"
        r = self.collection.query.bm25(query="ecole", query_properties=["title"], limit=5)
        assert len(r.objects) == 0, "old docs still have 'école' in index"

        r = self.collection.query.bm25(query="école", query_properties=["title"], limit=5)
        assert len(r.objects) == 0, "query 'école' now folded to 'ecole', mismatches old index"

        # New documents use updated config
        self.collection.data.insert({"title": "nouvelle école", "body": "nouvelle école"})

        r = self.collection.query.bm25(query="ecole", query_properties=["title"], limit=5)
        assert len(r.objects) == 1, "new doc indexed as 'ecole'"

        r = self.collection.query.bm25(query="école", query_properties=["title"], limit=5)
        assert len(r.objects) == 1, "query 'école'→'ecole' matches new doc"


class TestASCIIFoldTokenizationVariants:
    """asciiFold + ignore across word, lowercase, whitespace, field, trigram."""

    @pytest.fixture(autouse=True)
    def setup(self, request, weaviate_client) -> Generator[None, None, None]:
        self.client = weaviate_client()
        self.collection_name = _sanitize_collection_name(request.node.name)
        self.client.collections.delete(self.collection_name)
        self.client.collections.create_from_dict(
            {
                "class": self.collection_name,
                "vectorizer": "none",
                "properties": [
                    {
                        "name": "wordProp",
                        "dataType": ["text"],
                        "tokenization": "word",
                        "textAnalyzer": {"asciiFold": True, "asciiFoldIgnore": ["é"]},
                    },
                    {
                        "name": "lowercaseProp",
                        "dataType": ["text"],
                        "tokenization": "lowercase",
                        "textAnalyzer": {"asciiFold": True, "asciiFoldIgnore": ["é"]},
                    },
                    {
                        "name": "whitespaceProp",
                        "dataType": ["text"],
                        "tokenization": "whitespace",
                        "textAnalyzer": {"asciiFold": True, "asciiFoldIgnore": ["é"]},
                    },
                    {
                        "name": "fieldProp",
                        "dataType": ["text"],
                        "tokenization": "field",
                        "textAnalyzer": {"asciiFold": True, "asciiFoldIgnore": ["é"]},
                    },
                    {
                        "name": "trigramProp",
                        "dataType": ["text"],
                        "tokenization": "trigram",
                        "textAnalyzer": {"asciiFold": True, "asciiFoldIgnore": ["é"]},
                    },
                    {
                        "name": "wordNoIgnore",
                        "dataType": ["text"],
                        "tokenization": "word",
                        "textAnalyzer": {"asciiFold": True},
                    },
                    {
                        "name": "lowercaseNoIgnore",
                        "dataType": ["text"],
                        "tokenization": "lowercase",
                        "textAnalyzer": {"asciiFold": True},
                    },
                    {
                        "name": "whitespaceNoIgnore",
                        "dataType": ["text"],
                        "tokenization": "whitespace",
                        "textAnalyzer": {"asciiFold": True},
                    },
                    {
                        "name": "fieldNoIgnore",
                        "dataType": ["text"],
                        "tokenization": "field",
                        "textAnalyzer": {"asciiFold": True},
                    },
                    {
                        "name": "trigramNoIgnore",
                        "dataType": ["text"],
                        "tokenization": "trigram",
                        "textAnalyzer": {"asciiFold": True},
                    },
                ],
            }
        )

        text = "L'école est fermée"
        self.collection = self.client.collections.get(self.collection_name)
        self.collection.data.insert_many(
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
        yield
        self.client.collections.delete(self.collection_name)
        self.client.close()

    def _bm25(self, q, props):
        return self.collection.query.bm25(query=q, query_properties=props, limit=5)

    def test_word_tokenization(self) -> None:
        """word: splits on non-alphanumeric, lowercased."""
        assert len(self._bm25("école", ["wordProp"]).objects) == 1
        assert len(self._bm25("ecole", ["wordProp"]).objects) == 0, "é preserved"
        assert len(self._bm25("fermee", ["wordProp"]).objects) == 0, "é preserved"
        assert len(self._bm25("école", ["wordNoIgnore"]).objects) == 1
        assert len(self._bm25("ecole", ["wordNoIgnore"]).objects) == 1
        assert len(self._bm25("fermee", ["wordNoIgnore"]).objects) == 1

    def test_lowercase_tokenization(self) -> None:
        """lowercase: splits on whitespace, lowercased."""
        assert len(self._bm25("l'école", ["lowercaseProp"]).objects) == 1
        assert len(self._bm25("l'ecole", ["lowercaseProp"]).objects) == 0, "é preserved"
        assert len(self._bm25("fermée", ["lowercaseProp"]).objects) == 1
        assert len(self._bm25("fermee", ["lowercaseProp"]).objects) == 0, "é preserved"
        assert len(self._bm25("l'ecole", ["lowercaseNoIgnore"]).objects) == 1
        assert len(self._bm25("l'école", ["lowercaseNoIgnore"]).objects) == 1
        assert len(self._bm25("fermee", ["lowercaseNoIgnore"]).objects) == 1

    def test_whitespace_tokenization(self) -> None:
        """whitespace: splits on whitespace, case-preserved."""
        assert len(self._bm25("L'école", ["whitespaceProp"]).objects) == 1
        assert len(self._bm25("L'ecole", ["whitespaceProp"]).objects) == 0, "é preserved"
        assert len(self._bm25("L'ecole", ["whitespaceNoIgnore"]).objects) == 1
        assert len(self._bm25("L'école", ["whitespaceNoIgnore"]).objects) == 1

    def test_field_tokenization(self) -> None:
        """field: entire value as one token, case-preserved."""
        assert len(self._bm25("L'école est fermée", ["fieldProp"]).objects) == 1
        assert len(self._bm25("L'ecole est fermee", ["fieldProp"]).objects) == 0, "é preserved"
        assert len(self._bm25("L'ecole est fermee", ["fieldNoIgnore"]).objects) == 1
        assert len(self._bm25("L'école est fermée", ["fieldNoIgnore"]).objects) == 1

    def test_trigram_tokenization(self) -> None:
        """trigram: 3-char sliding windows."""
        assert len(self._bm25("éco", ["trigramProp"]).objects) == 1
        assert len(self._bm25("eco", ["trigramProp"]).objects) == 0, "é preserved"
        assert len(self._bm25("eco", ["trigramNoIgnore"]).objects) == 1
        assert len(self._bm25("éco", ["trigramNoIgnore"]).objects) == 1

    def test_cross_tokenization_bm25f(self) -> None:
        """Cross-tokenization BM25F."""
        assert len(self._bm25("ecole", ["wordProp", "wordNoIgnore"]).objects) == 1
        assert len(self._bm25("école", ["wordProp", "wordNoIgnore"]).objects) == 1
        assert len(self._bm25("eco", ["trigramProp", "trigramNoIgnore"]).objects) == 1


class TestASCIIFoldMultiCharIgnore:
    """Tests for asciiFoldIgnore with multiple characters in the ignore list."""

    @pytest.fixture(autouse=True)
    def setup(self, request, weaviate_client) -> Generator[None, None, None]:
        self.client = weaviate_client()
        self.collection_name = _sanitize_collection_name(request.node.name)
        self.client.collections.delete(self.collection_name)
        self.client.collections.create_from_dict(
            {
                "class": self.collection_name,
                "vectorizer": "none",
                "properties": [
                    {
                        "name": "multiIgnore",
                        "dataType": ["text"],
                        "tokenization": "word",
                        "textAnalyzer": {
                            "asciiFold": True,
                            "asciiFoldIgnore": ["é", "ü", "ñ", "ø"],
                        },
                    },
                    {
                        "name": "noIgnore",
                        "dataType": ["text"],
                        "tokenization": "word",
                        "textAnalyzer": {
                            "asciiFold": True,
                            "asciiFoldIgnore": [],
                        },
                    },
                ],
            }
        )
        collection = self.client.collections.get(self.collection_name)
        collection.data.insert_many(
            [
                {"multiIgnore": "résumé", "noIgnore": "résumé"},
                {"multiIgnore": "über", "noIgnore": "über"},
                {"multiIgnore": "El Niño", "noIgnore": "El Niño"},
                {"multiIgnore": "Ørsted", "noIgnore": "Ørsted"},
                {"multiIgnore": "São Paulo", "noIgnore": "São Paulo"},
                {"multiIgnore": "naïve café", "noIgnore": "naïve café"},
            ]
        )
        self.collection = collection
        yield
        self.client.collections.delete(self.collection_name)
        self.client.close()

    def _bm25(self, q, props):
        return self.collection.query.bm25(query=q, query_properties=props, limit=10)

    def test_ignored_chars_preserved(self) -> None:
        """Characters in the ignore list are NOT folded."""
        # é preserved: "resume" should not match
        assert len(self._bm25("resume", ["multiIgnore"]).objects) == 0
        assert len(self._bm25("résumé", ["multiIgnore"]).objects) == 1

        # ü preserved: "uber" should not match
        assert len(self._bm25("uber", ["multiIgnore"]).objects) == 0
        assert len(self._bm25("über", ["multiIgnore"]).objects) == 1

        # ñ preserved: "nino" should not match
        assert len(self._bm25("nino", ["multiIgnore"]).objects) == 0
        assert len(self._bm25("niño", ["multiIgnore"]).objects) == 1

        # ø preserved: "orsted" should not match
        assert len(self._bm25("orsted", ["multiIgnore"]).objects) == 0
        assert len(self._bm25("ørsted", ["multiIgnore"]).objects) == 1

    def test_non_ignored_chars_still_folded(self) -> None:
        """Characters NOT in the ignore list are still folded."""
        # ã is not in ignore list, so "sao" matches "São"
        assert len(self._bm25("sao", ["multiIgnore"]).objects) == 1
        assert len(self._bm25("são", ["multiIgnore"]).objects) == 1

        # ï is not in ignore list, so "naive" matches "naïve"
        assert len(self._bm25("naive", ["multiIgnore"]).objects) == 1
        assert len(self._bm25("naïve", ["multiIgnore"]).objects) == 1

    def test_no_ignore_folds_everything(self) -> None:
        """With empty ignore list, all characters are folded."""
        assert len(self._bm25("resume", ["noIgnore"]).objects) == 1
        assert len(self._bm25("uber", ["noIgnore"]).objects) == 1
        assert len(self._bm25("nino", ["noIgnore"]).objects) == 1
        assert len(self._bm25("orsted", ["noIgnore"]).objects) == 1
        assert len(self._bm25("sao", ["noIgnore"]).objects) == 1
        assert len(self._bm25("naive", ["noIgnore"]).objects) == 1

    def test_mixed_ignored_and_folded_in_same_word(self) -> None:
        """Words with both ignored and non-ignored accented chars."""
        # "naïve café": ï folded (not in ignore), é preserved (in ignore)
        # So "naive" matches but "cafe" does not
        assert len(self._bm25("naive", ["multiIgnore"]).objects) == 1
        assert len(self._bm25("cafe", ["multiIgnore"]).objects) == 0
        assert len(self._bm25("café", ["multiIgnore"]).objects) == 1

    def test_config_reflects_multi_char_ignore(self) -> None:
        """Schema config returns all characters in the ignore list."""
        schema = _rest_get(f"/v1/schema/{self.collection_name}")
        props = {p["name"]: p for p in schema["properties"]}
        ignore = props["multiIgnore"].get("textAnalyzer", {}).get("asciiFoldIgnore")
        assert set(ignore) == {"é", "ü", "ñ", "ø"}


class TestASCIIFoldFilters:
    """Filters (Equal, Like) respect asciiFold and asciiFoldIgnore."""

    @pytest.fixture(autouse=True)
    def setup(self, request, weaviate_client) -> Generator[None, None, None]:
        self.client = weaviate_client()
        self.collection_name = _sanitize_collection_name(request.node.name)
        _create_and_populate(self.client, self.collection_name)
        self.collection = self.client.collections.get(self.collection_name)
        yield
        self.client.collections.delete(self.collection_name)
        self.client.close()

    def test_equal_filter_body_full_fold(self) -> None:
        """Equal filter on body (full fold, no ignore)."""
        r = self.collection.query.fetch_objects(
            filters=wvc.query.Filter.by_property("body").equal("ecole"),
            limit=5,
        )
        assert len(r.objects) == 1, (
            f"body Equal 'ecole': expected 1, got {len(r.objects)}"
        )

        r = self.collection.query.fetch_objects(
            filters=wvc.query.Filter.by_property("body").equal("école"),
            limit=5,
        )
        assert len(r.objects) == 1, (
            f"body Equal 'école': expected 1, got {len(r.objects)}"
        )

    def test_equal_filter_title_ignore_e_accent(self) -> None:
        """Equal filter on title (ignore é)."""
        r = self.collection.query.fetch_objects(
            filters=wvc.query.Filter.by_property("title").equal("école"),
            limit=5,
        )
        assert len(r.objects) == 1, (
            f"title Equal 'école': expected 1, got {len(r.objects)}"
        )

        r = self.collection.query.fetch_objects(
            filters=wvc.query.Filter.by_property("title").equal("ecole"),
            limit=5,
        )
        assert len(r.objects) == 0, (
            f"title Equal 'ecole': expected 0 (é preserved), "
            f"got {len(r.objects)}"
        )

    def test_like_filter_body_full_fold(self) -> None:
        """Like filter on body (full fold)."""
        r = self.collection.query.fetch_objects(
            filters=wvc.query.Filter.by_property("body").like("ecol*"),
            limit=5,
        )
        assert len(r.objects) == 1, (
            f"body Like 'ecol*': expected 1, got {len(r.objects)}"
        )

    def test_like_filter_title_ignore_e_accent(self) -> None:
        """Like filter on title (ignore é)."""
        r = self.collection.query.fetch_objects(
            filters=wvc.query.Filter.by_property("title").like("écol*"),
            limit=5,
        )
        assert len(r.objects) == 1, (
            f"title Like 'écol*': expected 1, got {len(r.objects)}"
        )

        r = self.collection.query.fetch_objects(
            filters=wvc.query.Filter.by_property("title").like("ecol*"),
            limit=5,
        )
        assert len(r.objects) == 0, (
            f"title Like 'ecol*': expected 0 (é preserved), "
            f"got {len(r.objects)}"
        )

    def test_equal_filter_cafe_body_full_fold(self) -> None:
        """Equal filter for "cafe" on body (full fold)."""
        r = self.collection.query.fetch_objects(
            filters=wvc.query.Filter.by_property("body").equal("cafe"),
            limit=5,
        )
        assert len(r.objects) == 2, (
            f"body Equal 'cafe': expected 2 (café+cafe), "
            f"got {len(r.objects)}"
        )

    def test_equal_filter_cafe_title_ignore_e_accent(self) -> None:
        """Equal filter for "café" on title (ignore é)."""
        r = self.collection.query.fetch_objects(
            filters=wvc.query.Filter.by_property("title").equal("café"),
            limit=5,
        )
        assert len(r.objects) == 1, (
            f"title Equal 'café': expected 1, got {len(r.objects)}"
        )
