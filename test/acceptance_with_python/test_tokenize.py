import json
import urllib.request
import urllib.error
from typing import Any, Generator
import os

import pytest


WEAVIATE_URL = os.environ.get("WEAVIATE_URL", "http://localhost:8080")


REQUEST_TIMEOUT = 10.0
def post_json(
    url: str,
    data: dict[str, Any],
    timeout: float = REQUEST_TIMEOUT,
) -> tuple[int, dict[str, Any] | None]:
    """POST JSON using urllib and return (status_code, parsed_body_or_None)."""
    body = json.dumps(data).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        resp_body = None
        try:
            resp_body = json.loads(e.read())
        except Exception:
            pass
        return e.code, resp_body


def delete(url: str, timeout: float = REQUEST_TIMEOUT) -> int:
    """DELETE request using urllib and return status code."""
    req = urllib.request.Request(url, method="DELETE")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status
    except urllib.error.HTTPError as e:
        return e.code


def get_json(
    url: str, timeout: float = REQUEST_TIMEOUT
) -> tuple[int, dict[str, Any] | None]:
    """GET JSON using urllib and return (status_code, parsed_body_or_None)."""
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        resp_body = None
        try:
            resp_body = json.loads(e.read())
        except Exception:
            pass
        return e.code, resp_body


def put_json(
    url: str,
    data: dict[str, Any],
    timeout: float = REQUEST_TIMEOUT,
) -> tuple[int, dict[str, Any] | None]:
    """PUT JSON using urllib and return (status_code, parsed_body_or_None)."""
    body = json.dumps(data).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="PUT",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        resp_body = None
        try:
            resp_body = json.loads(e.read())
        except Exception:
            pass
        return e.code, resp_body


class TestGenericTokenize:
    """Tests for POST /v1/tokenize."""

    @pytest.mark.parametrize(
        "tokenization,text,expected_tokens",
        [
            ("word", "The quick brown fox", ["the", "quick", "brown", "fox"]),
            ("lowercase", "Hello World Test", ["hello", "world", "test"]),
            ("whitespace", "Hello World Test", ["Hello", "World", "Test"]),
            ("field", "  Hello World  ", ["Hello World"]),
            ("trigram", "Hello", ["hel", "ell", "llo"]),
        ],
    )
    def test_tokenization_methods(
        self, tokenization: str, text: str, expected_tokens: list[str]
    ) -> None:
        status, body = post_json(
            f"{WEAVIATE_URL}/v1/tokenize",
            {"text": text, "tokenization": tokenization},
        )
        assert status == 200
        assert body["tokenization"] == tokenization
        assert body["indexed"] == expected_tokens
        assert body["query"] == expected_tokens

    def test_missing_text(self) -> None:
        status, _ = post_json(f"{WEAVIATE_URL}/v1/tokenize", {"tokenization": "word"})
        assert status == 422

    def test_missing_tokenization(self) -> None:
        status, _ = post_json(f"{WEAVIATE_URL}/v1/tokenize", {"text": "hello"})
        assert status == 422

    def test_invalid_tokenization(self) -> None:
        status, _ = post_json(
            f"{WEAVIATE_URL}/v1/tokenize",
            {"text": "hello", "tokenization": "nope"},
        )
        assert status == 422

    def test_empty_body(self) -> None:
        status, _ = post_json(f"{WEAVIATE_URL}/v1/tokenize", {})
        assert status == 422

    def test_ascii_fold(self) -> None:
        status, body = post_json(
            f"{WEAVIATE_URL}/v1/tokenize",
            {
                "text": "L'école est fermée",
                "tokenization": "word",
                "analyzerConfig": {"asciiFold": True},
            },
        )
        assert status == 200
        assert body["indexed"] == ["l", "ecole", "est", "fermee"]
        assert body["query"] == ["l", "ecole", "est", "fermee"]

    def test_ascii_fold_with_ignore(self) -> None:
        status, body = post_json(
            f"{WEAVIATE_URL}/v1/tokenize",
            {
                "text": "L'école est fermée",
                "tokenization": "word",
                "analyzerConfig": {"asciiFold": True, "asciiFoldIgnore": ["é"]},
            },
        )
        assert status == 200
        assert body["indexed"] == ["l", "école", "est", "fermée"]
        assert body["query"] == ["l", "école", "est", "fermée"]

    def test_stopword_preset(self) -> None:
        status, body = post_json(
            f"{WEAVIATE_URL}/v1/tokenize",
            {
                "text": "The quick brown fox",
                "tokenization": "word",
                "analyzerConfig": {"stopwordPreset": "en"},
            },
        )
        assert status == 200
        assert body is not None
        assert body["indexed"] == ["the", "quick", "brown", "fox"]
        assert "the" not in body["query"]
        assert "quick" in body["query"]

    def test_stopword_custom_additions(self) -> None:
        status, body = post_json(
            f"{WEAVIATE_URL}/v1/tokenize",
            {
                "text": "hello world test",
                "tokenization": "word",
                "analyzerConfig": {"stopwordPreset": "custom"},
                "stopwordPresets": {
                    "custom": {"additions": ["test"]},
                },
            },
        )
        assert status == 200
        assert body["indexed"] == ["hello", "world", "test"]
        assert body["query"] == ["hello", "world"]

    def test_stopword_preset_with_removals(self) -> None:
        status, body = post_json(
            f"{WEAVIATE_URL}/v1/tokenize",
            {
                "text": "the quick",
                "tokenization": "word",
                "analyzerConfig": {"stopwordPreset": "en-no-the"},
                "stopwordPresets": {
                    "en-no-the": {"preset": "en", "removals": ["the"]},
                },
            },
        )
        assert status == 200
        assert body["indexed"] == ["the", "quick"]
        assert body["query"] == ["the", "quick"]

    def test_analyzer_stopword_preset_en(self) -> None:
        """analyzerConfig.stopwordPreset='en' applies the built-in English stopword list."""
        status, body = post_json(
            f"{WEAVIATE_URL}/v1/tokenize",
            {
                "text": "The quick brown fox",
                "tokenization": "word",
                "analyzerConfig": {"stopwordPreset": "en"},
            },
        )
        assert status == 200
        assert body["indexed"] == ["the", "quick", "brown", "fox"]
        assert "the" not in body["query"]
        assert "quick" in body["query"]

    def test_analyzer_stopword_preset_none(self) -> None:
        """analyzerConfig.stopwordPreset='none' keeps all tokens."""
        status, body = post_json(
            f"{WEAVIATE_URL}/v1/tokenize",
            {
                "text": "The quick brown fox",
                "tokenization": "word",
                "analyzerConfig": {"stopwordPreset": "none"},
            },
        )
        assert status == 200
        assert body["indexed"] == ["the", "quick", "brown", "fox"]
        assert body["query"] == ["the", "quick", "brown", "fox"]

    def test_analyzer_stopword_preset_with_ascii_fold(self) -> None:
        """stopwordPreset combined with asciiFold in analyzerConfig."""
        status, body = post_json(
            f"{WEAVIATE_URL}/v1/tokenize",
            {
                "text": "The école est fermée",
                "tokenization": "word",
                "analyzerConfig": {"asciiFold": True, "stopwordPreset": "en"},
            },
        )
        assert status == 200
        assert body["indexed"] == ["the", "ecole", "est", "fermee"]
        assert "the" not in body["query"]
        assert "ecole" in body["query"]

    def test_analyzer_invalid_stopword_preset(self) -> None:
        """Invalid stopwordPreset in analyzerConfig returns error."""
        status, body = post_json(
            f"{WEAVIATE_URL}/v1/tokenize",
            {
                "text": "hello",
                "tokenization": "word",
                "analyzerConfig": {"stopwordPreset": "nonexistent"},
            },
        )
        assert status == 422

    def test_custom_preset_via_stopword_presets(self) -> None:
        """Custom preset defined inline via stopwordPresets, referenced by analyzerConfig."""
        status, body = post_json(
            f"{WEAVIATE_URL}/v1/tokenize",
            {
                "text": "le chat et la souris",
                "tokenization": "word",
                "analyzerConfig": {"stopwordPreset": "fr"},
                "stopwordPresets": {
                    "fr": {"additions": ["le", "la", "les"]},
                },
            },
        )
        assert status == 200
        assert body["indexed"] == ["le", "chat", "et", "la", "souris"]
        assert "le" not in body["query"]
        assert "la" not in body["query"]
        assert "chat" in body["query"]
        assert "et" in body["query"]

    def test_custom_preset_with_base_and_removals(self) -> None:
        """Custom preset using 'en' as base with removals."""
        status, body = post_json(
            f"{WEAVIATE_URL}/v1/tokenize",
            {
                "text": "the quick and the fox",
                "tokenization": "word",
                "analyzerConfig": {"stopwordPreset": "custom"},
                "stopwordPresets": {
                    "custom": {"preset": "en", "removals": ["the"]},
                },
            },
        )
        assert status == 200
        assert body["indexed"] == ["the", "quick", "and", "the", "fox"]
        # 'the' was removed from en stopwords, so it stays in query
        assert "the" in body["query"]
        # 'and' is still an en stopword
        assert "and" not in body["query"]

    def test_request_preset_overrides_builtin_of_same_name(self) -> None:
        """A request-level stopwordPreset sharing a name with a built-in
        replaces the built-in entirely (matches collection-level semantics
        where invertedIndexConfig.stopwordPresets['en'] overrides the
        built-in 'en' for properties of that collection)."""
        status, body = post_json(
            f"{WEAVIATE_URL}/v1/tokenize",
            {
                "text": "the quick hello world",
                "tokenization": "word",
                "analyzerConfig": {"stopwordPreset": "en"},
                "stopwordPresets": {
                    # No explicit base → defaults to "none". The user-defined
                    # "en" is used as-is, so the built-in en list is ignored.
                    "en": {"additions": ["hello"]},
                },
            },
        )
        assert status == 200
        assert body is not None
        assert body["indexed"] == ["the", "quick", "hello", "world"]
        # built-in 'en' is no longer applied, so 'the' stays in query
        assert "the" in body["query"]
        # 'hello' was added by the override, so it is filtered
        assert "hello" not in body["query"]
        assert "quick" in body["query"]
        assert "world" in body["query"]

    def test_custom_preset_not_found(self) -> None:
        """Referencing a preset not in stopwordPresets returns 422."""
        status, body = post_json(
            f"{WEAVIATE_URL}/v1/tokenize",
            {
                "text": "hello",
                "tokenization": "word",
                "analyzerConfig": {"stopwordPreset": "missing"},
                "stopwordPresets": {
                    "other": {"additions": ["hello"]},
                },
            },
        )
        assert status == 422

    def test_ascii_fold_combined_with_stopwords(self) -> None:
        status, body = post_json(
            f"{WEAVIATE_URL}/v1/tokenize",
            {
                "text": "The école est fermée",
                "tokenization": "word",
                "analyzerConfig": {"asciiFold": True, "stopwordPreset": "en"},
            },
        )
        assert status == 200
        assert body is not None
        assert body["indexed"] == ["the", "ecole", "est", "fermee"]
        assert "the" not in body["query"]
        assert "ecole" in body["query"]

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


class TestPropertyTokenize:
    """Tests for POST /v1/schema/{className}/properties/{propertyName}/tokenize."""

    @pytest.fixture(autouse=True)
    def setup_collection(self, request, weaviate_client) -> Generator[None, None, None]:
        self.client = weaviate_client()
        self.collection_name = _sanitize_collection_name(request.node.name)
        self.client.collections.delete(self.collection_name)
        self.client.collections.create_from_dict(
            {
                "class": self.collection_name,
                "vectorizer": "none",
                "invertedIndexConfig": {
                    "stopwords": {
                        "preset": "none",
                        "additions": ["le", "la", "les"],
                    },
                    "stopwordPresets": {
                        "fr": ["le", "la", "les", "un", "une", "des", "du", "de"],
                    },
                },
                "properties": [
                    {
                        "name": "title",
                        "dataType": ["text"],
                        "tokenization": "word",
                        "textAnalyzer": {
                            "stopwordPreset": "en",
                        },
                    },
                    {
                        "name": "title_fr",
                        "dataType": ["text"],
                        "tokenization": "word",
                        "textAnalyzer": {
                            "stopwordPreset": "fr",
                        },
                    },
                    {
                        "name": "tag",
                        "dataType": ["text"],
                        "tokenization": "field",
                    },
                    {
                        "name": "count",
                        "dataType": ["int"],
                    },
                ],
            }
        )
        yield
        self.client.collections.delete(self.collection_name)
        self.client.close()

    def test_word_property(self) -> None:
        status, body = post_json(
            f"{WEAVIATE_URL}/v1/schema/{self.collection_name}/properties/title/tokenize",
            {"text": "The quick brown fox"},
        )
        assert status == 200
        assert body["tokenization"] == "word"
        assert body["indexed"] == ["the", "quick", "brown", "fox"]

    def test_field_property(self) -> None:
        status, body = post_json(
            f"{WEAVIATE_URL}/v1/schema/{self.collection_name}/properties/tag/tokenize",
            {"text": "  Hello World  "},
        )
        assert status == 200
        assert body["tokenization"] == "field"
        assert body["indexed"] == ["Hello World"]

    def test_lowercase_first_letter_class_name(self) -> None:
        """UppercaseClassName only capitalizes the first letter, so 'tokenizeTestCollection' should work."""
        lowered = self.collection_name[0].lower() + self.collection_name[1:]
        status, _ = post_json(
            f"{WEAVIATE_URL}/v1/schema/{lowered}/properties/title/tokenize",
            {"text": "hello world"},
        )
        assert status == 200

    def test_case_insensitive_property_name(self) -> None:
        status, _ = post_json(
            f"{WEAVIATE_URL}/v1/schema/{self.collection_name}/properties/Title/tokenize",
            {"text": "hello world"},
        )
        assert status == 200

    def test_property_user_defined_preset_fr(self) -> None:
        """title_fr uses user-defined 'fr' preset from invertedIndexConfig.stopwordPresets."""
        status, body = post_json(
            f"{WEAVIATE_URL}/v1/schema/{self.collection_name}/properties/title_fr/tokenize",
            {"text": "le chat et la souris"},
        )
        assert status == 200
        assert body["indexed"] == ["le", "chat", "et", "la", "souris"]
        # 'le' and 'la' are in the user-defined 'fr' preset, so filtered from query
        assert "le" not in body["query"]
        assert "la" not in body["query"]
        assert "chat" in body["query"]
        assert "et" in body["query"]
        assert "souris" in body["query"]

    def test_property_stopword_preset_en(self) -> None:
        """title has stopwordPreset='en', overriding the collection-level French additions."""
        status, body = post_json(
            f"{WEAVIATE_URL}/v1/schema/{self.collection_name}/properties/title/tokenize",
            {"text": "the quick brown fox"},
        )
        assert status == 200
        assert body["indexed"] == ["the", "quick", "brown", "fox"]
        # stopwordPreset="en" filters English stopwords
        assert "the" not in body["query"]
        assert "quick" in body["query"]
        assert "brown" in body["query"]
        assert "fox" in body["query"]

    def test_property_stopword_preset_en_does_not_use_collection_additions(self) -> None:
        """title has stopwordPreset='en'; collection-level additions (le, la, les) should NOT apply."""
        status, body = post_json(
            f"{WEAVIATE_URL}/v1/schema/{self.collection_name}/properties/title/tokenize",
            {"text": "le chat la souris"},
        )
        assert status == 200
        # 'le' and 'la' are NOT English stopwords, so they should appear in query
        assert "le" in body["query"]
        assert "la" in body["query"]

    def test_property_tokenize_via_alias(self) -> None:
        """The endpoint should resolve a collection alias to the underlying class."""
        alias_name = self.collection_name + "Alias"
        # Ensure no stale alias from a prior failed run
        delete(f"{WEAVIATE_URL}/v1/aliases/{alias_name}")
        status, _ = post_json(
            f"{WEAVIATE_URL}/v1/aliases",
            {"alias": alias_name, "class": self.collection_name},
        )
        assert status == 200
        try:
            status, body = post_json(
                f"{WEAVIATE_URL}/v1/schema/{alias_name}/properties/title/tokenize",
                {"text": "the quick brown fox"},
            )
            assert status == 200
            assert body["tokenization"] == "word"
            assert body["indexed"] == ["the", "quick", "brown", "fox"]
            # title has stopwordPreset='en' so 'the' should be filtered from the query
            assert "the" not in body["query"]
            assert "quick" in body["query"]
        finally:
            delete(f"{WEAVIATE_URL}/v1/aliases/{alias_name}")

    def test_cannot_remove_stopword_preset_in_use(self) -> None:
        """Removing a stopwordPreset still referenced by a property must be rejected."""
        # Fetch the current class definition
        status, class_def = get_json(
            f"{WEAVIATE_URL}/v1/schema/{self.collection_name}"
        )
        assert status == 200
        assert class_def is not None
        # Sanity check: title_fr references the 'fr' user-defined preset
        assert "fr" in class_def["invertedIndexConfig"]["stopwordPresets"]

        # Attempt to drop the 'fr' preset while title_fr still references it
        class_def["invertedIndexConfig"]["stopwordPresets"] = {}
        status, body = put_json(
            f"{WEAVIATE_URL}/v1/schema/{self.collection_name}",
            class_def,
        )
        assert status == 422, f"expected 422, got {status}: {body}"
        # Error message should mention the preset name and the offending property
        err = json.dumps(body)
        assert "fr" in err
        assert "title_fr" in err

        # The original preset should still be present after the rejected update
        status, class_def_after = get_json(
            f"{WEAVIATE_URL}/v1/schema/{self.collection_name}"
        )
        assert status == 200
        assert class_def_after is not None
        assert "fr" in class_def_after["invertedIndexConfig"]["stopwordPresets"]

        # Tokenization through title_fr should still work using the 'fr' preset
        status, body = post_json(
            f"{WEAVIATE_URL}/v1/schema/{self.collection_name}/properties/title_fr/tokenize",
            {"text": "le chat"},
        )
        assert status == 200
        assert body is not None
        assert "le" not in body["query"]
        assert "chat" in body["query"]

    def test_update_stopword_preset_contents(self) -> None:
        """Updating words in an existing stopword preset takes effect at query time."""
        # Baseline: 'le' is in the 'fr' preset and should be filtered, 'yo' is not.
        status, body = post_json(
            f"{WEAVIATE_URL}/v1/schema/{self.collection_name}/properties/title_fr/tokenize",
            {"text": "le yo chat"},
        )
        assert status == 200
        assert body is not None
        assert body["indexed"] == ["le", "yo", "chat"]
        assert "le" not in body["query"]
        assert "yo" in body["query"]
        assert "chat" in body["query"]

        # Update the 'fr' preset: remove 'le', add 'yo'.
        status, class_def = get_json(
            f"{WEAVIATE_URL}/v1/schema/{self.collection_name}"
        )
        assert status == 200
        assert class_def is not None
        class_def["invertedIndexConfig"]["stopwordPresets"]["fr"] = [
            "la",
            "les",
            "yo",
        ]
        status, _ = put_json(
            f"{WEAVIATE_URL}/v1/schema/{self.collection_name}",
            class_def,
        )
        assert status == 200

        # After update: 'le' should pass through, 'yo' should be filtered.
        status, body = post_json(
            f"{WEAVIATE_URL}/v1/schema/{self.collection_name}/properties/title_fr/tokenize",
            {"text": "le yo chat"},
        )
        assert status == 200
        assert body is not None
        assert body["indexed"] == ["le", "yo", "chat"]
        assert "le" in body["query"]
        assert "yo" not in body["query"]
        assert "chat" in body["query"]

        # 'la' is still in the preset, so it should still be filtered.
        status, body = post_json(
            f"{WEAVIATE_URL}/v1/schema/{self.collection_name}/properties/title_fr/tokenize",
            {"text": "la souris"},
        )
        assert status == 200
        assert body is not None
        assert "la" not in body["query"]
        assert "souris" in body["query"]

    def test_class_not_found(self) -> None:
        status, _ = post_json(
            f"{WEAVIATE_URL}/v1/schema/NonExistentClass/properties/title/tokenize",
            {"text": "hello"},
        )
        assert status == 404

    def test_property_not_found(self) -> None:
        status, _ = post_json(
            f"{WEAVIATE_URL}/v1/schema/{self.collection_name}/properties/nonexistent/tokenize",
            {"text": "hello"},
        )
        assert status == 404

    def test_non_text_property(self) -> None:
        status, _ = post_json(
            f"{WEAVIATE_URL}/v1/schema/{self.collection_name}/properties/count/tokenize",
            {"text": "hello"},
        )
        assert status == 422

    def test_missing_text(self) -> None:
        status, _ = post_json(
            f"{WEAVIATE_URL}/v1/schema/{self.collection_name}/properties/title/tokenize",
            {},
        )
        assert status == 422


class TestStopwordPresetReferenceValidation:
    """Explicit coverage for validateStopwordPresetsStillReferenced.

    Each test creates its own collection so the scenarios are independent.
    """

    @pytest.fixture
    def collection_name(self, request, weaviate_client) -> Generator[str, None, None]:
        client = weaviate_client()
        name = _sanitize_collection_name(request.node.name)
        client.collections.delete(name)
        yield name
        client.collections.delete(name)
        client.close()

    def _create(self, name: str, body: dict[str, Any]) -> None:
        body = {**body, "class": name}
        status, resp = post_json(f"{WEAVIATE_URL}/v1/schema", body)
        assert status == 200, f"failed to create collection: {status} {resp}"

    def _update(self, name: str, body: dict[str, Any]) -> tuple[int, dict[str, Any] | None]:
        return put_json(f"{WEAVIATE_URL}/v1/schema/{name}", body)

    def test_remove_unused_preset_is_allowed(self, collection_name: str) -> None:
        """Removing a preset that no property references must succeed."""
        self._create(
            collection_name,
            {
                "vectorizer": "none",
                "invertedIndexConfig": {
                    "stopwordPresets": {
                        "fr": ["le", "la", "les"],
                        "es": ["el", "la", "los"],
                    },
                },
                "properties": [
                    {
                        "name": "title",
                        "dataType": ["text"],
                        "tokenization": "word",
                        "textAnalyzer": {"stopwordPreset": "fr"},
                    },
                ],
            },
        )
        # Drop only 'es' (unused). 'fr' is still referenced by title.
        status, class_def = get_json(f"{WEAVIATE_URL}/v1/schema/{collection_name}")
        assert status == 200 and class_def is not None
        class_def["invertedIndexConfig"]["stopwordPresets"] = {"fr": ["le", "la", "les"]}
        status, body = self._update(collection_name, class_def)
        assert status == 200, f"expected 200, got {status}: {body}"

    def test_remove_preset_referenced_by_top_level_property_is_rejected(
        self, collection_name: str
    ) -> None:
        """A removed preset still referenced by a top-level property must be rejected,
        and the rejection must mention both the preset name and the property name."""
        self._create(
            collection_name,
            {
                "vectorizer": "none",
                "invertedIndexConfig": {
                    "stopwordPresets": {"fr": ["le", "la", "les"]},
                },
                "properties": [
                    {
                        "name": "title",
                        "dataType": ["text"],
                        "tokenization": "word",
                        "textAnalyzer": {"stopwordPreset": "fr"},
                    },
                ],
            },
        )
        status, class_def = get_json(f"{WEAVIATE_URL}/v1/schema/{collection_name}")
        assert status == 200 and class_def is not None
        class_def["invertedIndexConfig"]["stopwordPresets"] = {}
        status, body = self._update(collection_name, class_def)
        assert status == 422, f"expected 422, got {status}: {body}"
        err = json.dumps(body)
        assert "fr" in err
        assert "title" in err

    def test_remove_preset_referenced_by_nested_property_is_rejected(
        self, collection_name: str
    ) -> None:
        """A removed preset still referenced by a nested property must be rejected,
        with the dotted nested-property path in the error message."""
        self._create(
            collection_name,
            {
                "vectorizer": "none",
                "invertedIndexConfig": {
                    "stopwordPresets": {"fr": ["le", "la", "les"]},
                },
                "properties": [
                    {
                        "name": "doc",
                        "dataType": ["object"],
                        "nestedProperties": [
                            {
                                "name": "body",
                                "dataType": ["text"],
                                "tokenization": "word",
                                "textAnalyzer": {"stopwordPreset": "fr"},
                            },
                        ],
                    },
                ],
            },
        )
        status, class_def = get_json(f"{WEAVIATE_URL}/v1/schema/{collection_name}")
        assert status == 200 and class_def is not None
        class_def["invertedIndexConfig"]["stopwordPresets"] = {}
        status, body = self._update(collection_name, class_def)
        assert status == 422, f"expected 422, got {status}: {body}"
        err = json.dumps(body)
        assert "fr" in err
        # Error should reference the nested property by dotted path
        assert "doc.body" in err

    def test_rename_preset_breaks_reference_and_is_rejected(
        self, collection_name: str
    ) -> None:
        """Renaming a preset (effectively removing the old name) while a property
        still references the old name must be rejected."""
        self._create(
            collection_name,
            {
                "vectorizer": "none",
                "invertedIndexConfig": {
                    "stopwordPresets": {"fr": ["le", "la", "les"]},
                },
                "properties": [
                    {
                        "name": "title",
                        "dataType": ["text"],
                        "tokenization": "word",
                        "textAnalyzer": {"stopwordPreset": "fr"},
                    },
                ],
            },
        )
        status, class_def = get_json(f"{WEAVIATE_URL}/v1/schema/{collection_name}")
        assert status == 200 and class_def is not None
        # Replace 'fr' with 'french' — same words, different name. The property
        # still points at 'fr', so this should be rejected.
        class_def["invertedIndexConfig"]["stopwordPresets"] = {
            "french": ["le", "la", "les"]
        }
        status, body = self._update(collection_name, class_def)
        assert status == 422, f"expected 422, got {status}: {body}"
        err = json.dumps(body)
        assert "fr" in err
        assert "title" in err

    def test_builtin_preset_reference_does_not_block_user_preset_removal(
        self, collection_name: str
    ) -> None:
        """A property using the built-in 'en' preset must not block removal of an
        unrelated user-defined preset."""
        self._create(
            collection_name,
            {
                "vectorizer": "none",
                "invertedIndexConfig": {
                    "stopwordPresets": {"fr": ["le", "la", "les"]},
                },
                "properties": [
                    {
                        "name": "title",
                        "dataType": ["text"],
                        "tokenization": "word",
                        # Built-in 'en' — never appears in user-defined presets.
                        "textAnalyzer": {"stopwordPreset": "en"},
                    },
                ],
            },
        )
        status, class_def = get_json(f"{WEAVIATE_URL}/v1/schema/{collection_name}")
        assert status == 200 and class_def is not None
        # 'fr' is unused (only 'en' is referenced) → removing it must succeed.
        class_def["invertedIndexConfig"]["stopwordPresets"] = {}
        status, body = self._update(collection_name, class_def)
        assert status == 200, f"expected 200, got {status}: {body}"
