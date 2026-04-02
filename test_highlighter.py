import pytest
from unittest.mock import MagicMock
from weaviate_highlighter import WeaviateHighlighter, Document

@pytest.fixture
def highlighter():
    return WeaviateHighlighter(fragment_size=50, use_cross_encoder=False)

def test_extract_snippet_basic(highlighter):
    text = "This is a simple text meant for testing search features."
    query = "search"
    snippet = highlighter.extract_snippet(query, text)
    assert "<em>search</em>" in snippet
    
def test_casing_and_partial(highlighter):
    text = "The SearchEngine is searching for SEARCHes."
    query = "search"
    snippet = highlighter.extract_snippet(query, text)
    assert "<em>Search</em>Engine" in snippet
    assert "<em>search</em>ing" in snippet
    assert "<em>SEARCH</em>es" in snippet

def test_no_matches(highlighter):
    text = "A completely unrelated document here."
    query = "elephant"
    snippet = highlighter.extract_snippet(query, text)
    assert "<em>" not in snippet
    assert text.startswith(snippet)

def test_empty_string(highlighter):
    assert highlighter.extract_snippet("query", "") == ""
    assert "some text".startswith(highlighter.extract_snippet("", "some text"))

def test_short_text(highlighter):
    text = "Short."
    snippet = highlighter.extract_snippet("short", text)
    assert "<em>Short</em>." == snippet

def test_cross_encoder_mock():
    # Instantiate with use_cross_encoder flag
    hl = WeaviateHighlighter(use_cross_encoder=True)
    hl.use_cross_encoder = True # Force true in case missing deps set it to False
    
    # Mock the sentence_transformers CrossEncoder predict method
    hl.cross_encoder = MagicMock()
    # doc 1 low score, doc 2 high score
    hl.cross_encoder.predict.return_value = [0.1, 0.9] 
    
    query = "test"
    texts = ["irrelevant document here", "a very good test document"]
    
    ranked = hl.process_and_rank_snippets(query, texts)
    
    # We expect the snippet from doc 2 to be ranked first due to 0.9 score
    assert "<em>test</em>" in ranked[0]
    assert "irrelevant" in ranked[1]
    
def test_transform_documents(highlighter):
    docs = [
        Document(page_content="The first text contains the keyword.", metadata={"id": 1}),
        Document(page_content="The second text has nothing.", metadata={"id": 2})
    ]
    
    transformed = highlighter.transform_documents(docs, search_query="keyword")
    
    assert len(transformed) == 2
    
    # Doc 1 (Keyword present)
    metadata_1 = transformed[0].metadata
    assert "_additional" in metadata_1
    assert "highlight" in metadata_1["_additional"]
    assert "<em>keyword</em>" in metadata_1["_additional"]["highlight"]["content"]
    assert metadata_1["id"] == 1
    
    # Doc 2 (No keyword present)
    metadata_2 = transformed[1].metadata
    assert "<em>" not in metadata_2["_additional"]["highlight"]["content"]
    assert metadata_2["id"] == 2
