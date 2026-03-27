import json
import urllib.request
import urllib.error
from typing import Any

import pytest
import weaviate
import weaviate.classes as wvc
from weaviate.collections.classes.config import Property, DataType, Configure


WEAVIATE_URL = "http://localhost:8080"


def post_json(url: str, data: dict[str, Any]) -> tuple[int, dict[str, Any] | None]:
    """POST JSON using urllib and return (status_code, parsed_body_or_None)."""
    body = json.dumps(data).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req) as resp:
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
                "analyzerConfig": {"stopwords": {"preset": "en"}},
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


class TestPropertyTokenize:
    """Tests for POST /v1/schema/{className}/properties/{propertyName}/tokenize."""

    COLLECTION_NAME = "TokenizeTestCollection"

    @pytest.fixture(autouse=True)
    def setup_collection(self) -> None:
        client = weaviate.connect_to_local()
        try:
            client.collections.delete(self.COLLECTION_NAME)
            client.collections.create(
                self.COLLECTION_NAME,
                vectorizer_config=Configure.Vectorizer.none(),
                properties=[
                    Property(name="title", data_type=DataType.TEXT, tokenization=wvc.config.Tokenization.WORD),
                    Property(name="tag", data_type=DataType.TEXT, tokenization=wvc.config.Tokenization.FIELD),
                    Property(name="count", data_type=DataType.INT),
                ],
            )
        finally:
            client.close()

        yield

        client = weaviate.connect_to_local()
        try:
            client.collections.delete(self.COLLECTION_NAME)
        finally:
            client.close()

    def test_word_property(self) -> None:
        status, body = post_json(
            f"{WEAVIATE_URL}/v1/schema/{self.COLLECTION_NAME}/properties/title/tokenize",
            {"text": "The quick brown fox"},
        )
        assert status == 200
        assert body["tokenization"] == "word"
        assert body["indexed"] == ["the", "quick", "brown", "fox"]

    def test_field_property(self) -> None:
        status, body = post_json(
            f"{WEAVIATE_URL}/v1/schema/{self.COLLECTION_NAME}/properties/tag/tokenize",
            {"text": "  Hello World  "},
        )
        assert status == 200
        assert body["tokenization"] == "field"
        assert body["indexed"] == ["Hello World"]

    def test_lowercase_first_letter_class_name(self) -> None:
        """UppercaseClassName only capitalizes the first letter, so 'tokenizeTestCollection' should work."""
        lowered = self.COLLECTION_NAME[0].lower() + self.COLLECTION_NAME[1:]
        status, _ = post_json(
            f"{WEAVIATE_URL}/v1/schema/{lowered}/properties/title/tokenize",
            {"text": "hello world"},
        )
        assert status == 200

    def test_case_insensitive_property_name(self) -> None:
        status, _ = post_json(
            f"{WEAVIATE_URL}/v1/schema/{self.COLLECTION_NAME}/properties/Title/tokenize",
            {"text": "hello world"},
        )
        assert status == 200

    def test_class_not_found(self) -> None:
        status, _ = post_json(
            f"{WEAVIATE_URL}/v1/schema/NonExistentClass/properties/title/tokenize",
            {"text": "hello"},
        )
        assert status == 404

    def test_property_not_found(self) -> None:
        status, _ = post_json(
            f"{WEAVIATE_URL}/v1/schema/{self.COLLECTION_NAME}/properties/nonexistent/tokenize",
            {"text": "hello"},
        )
        assert status == 404

    def test_non_text_property(self) -> None:
        status, _ = post_json(
            f"{WEAVIATE_URL}/v1/schema/{self.COLLECTION_NAME}/properties/count/tokenize",
            {"text": "hello"},
        )
        assert status == 400

    def test_missing_text(self) -> None:
        status, _ = post_json(
            f"{WEAVIATE_URL}/v1/schema/{self.COLLECTION_NAME}/properties/title/tokenize",
            {},
        )
        assert status == 422
