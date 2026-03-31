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

    def test_with_stopwords(self) -> None:
        status, body = post_json(
            f"{WEAVIATE_URL}/v1/tokenize",
            {
                "text": "The quick brown fox jumps over the lazy dog",
                "tokenization": "word",
                "analyzerConfig": {"stopwordPreset": "en"},
            },
        )
        assert status == 200
        assert body["indexed"] == [
            "the", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog",
        ]
        # "the" should be filtered from query tokens
        assert "the" not in body["query"]
        assert "quick" in body["query"]

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
                },
                "properties": [
                    {
                        "name": "title",
                        "dataType": ["text"],
                        "tokenization": "word",
                        "analyzerConfig": {
                            "stopwordPreset": "en",
                        },
                    },
                    {
                        "name": "title_fr",
                        "dataType": ["text"],
                        "tokenization": "word",
                        "analyzerConfig": {
                            "stopwordPreset": "none",
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

    @pytest.mark.skip(reason="Currently, the property-level analyzerConfig does not override the class-level stopword preset. This test will be enabled once that functionality is implemented.")
    def test_with_en_stopword_preset(self) -> None:
        status, body = post_json(
            f"{WEAVIATE_URL}/v1/schema/{self.collection_name}/properties/title/tokenize",
            {"text": "le chat et la souris"},
        )
        assert status == 200
        assert body["indexed"] == ["le", "chat", "et", "la", "souris"]
        assert body["query"] == ["le", "chat", "et", "la", "souris"]

    def test_with_custom_stopword_preset(self) -> None:
        status, body = post_json(
            f"{WEAVIATE_URL}/v1/schema/{self.collection_name}/properties/title_fr/tokenize",
            {"text": "le chat et la souris"},
        )
        assert status == 200
        assert body["indexed"] == ["le", "chat", "et", "la", "souris"]
        assert body["query"] == ["chat", "et", "souris"]

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
