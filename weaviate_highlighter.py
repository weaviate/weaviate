import re
from typing import List, Dict, Any, Sequence

try:
    from langchain_core.documents import Document
    from langchain_core.documents.transformers import BaseDocumentTransformer
    LANGCHAIN_AVAILABLE = True
except ImportError:
    LANGCHAIN_AVAILABLE = False
    class BaseDocumentTransformer:
        pass
    class Document:
        def __init__(self, page_content: str, metadata: dict = None):
            self.page_content = page_content
            self.metadata = metadata or {}

class WeaviateHighlighter(BaseDocumentTransformer):
    """
    A Document Transformer that implements Weaviate-like search highlighting.
    Extracts the most keyword-dense 200-character fragment and wraps matches in <em>.
    """
    def __init__(self, fragment_size: int = 200, use_cross_encoder: bool = False):
        self.fragment_size = fragment_size
        self.use_cross_encoder = use_cross_encoder
        self.cross_encoder = None
        
        if self.use_cross_encoder:
            try:
                from sentence_transformers import CrossEncoder
                print("Loading cross-encoder model...")
                self.cross_encoder = CrossEncoder('cross-encoder/ms-marco-TinyBERT-L-2-v2')
            except ImportError:
                print("Warning: 'sentence_transformers' is not installed. CrossEncoder features disabled.")
                self.use_cross_encoder = False
                
    def extract_snippet(self, query: str, text: str) -> str:
        """
        Extracts the most relevant fragment_size snippet from text for the given query.
        Wraps matches in <em> tags.
        """
        # Extract alphanumeric words to match
        keywords = [k for k in re.split(r'\W+', query.lower()) if k]
        if not keywords:
            return text[:self.fragment_size]
            
        # Match longer keywords first so we match partials effectively without overshadowing
        # e.g., 'highlight' before 'high'
        keywords.sort(key=len, reverse=True)
        pattern = re.compile(r'(' + '|'.join(map(re.escape, keywords)) + r')', re.IGNORECASE)
        
        matches = list(pattern.finditer(text))
        if not matches:
            return text[:self.fragment_size]
            
        max_score = -1
        best_window = (0, min(len(text), self.fragment_size))
        
        # Test several window positions around each match to find the densest fragment
        for match in matches:
            start_pos = match.start()
            
            for offset in [0, self.fragment_size // 4, self.fragment_size // 2, int(self.fragment_size * 0.75)]:
                window_start = max(0, start_pos - offset)
                window_end = min(len(text), window_start + self.fragment_size)
                
                # If window hits the end boundary, optionally shift it to preserve full fragment_size
                if window_end - window_start < self.fragment_size and len(text) > self.fragment_size:
                    window_start = max(0, window_end - self.fragment_size)
                    
                window_text = text[window_start:window_end]
                # Dense area = maximum number of substring matches
                score = len(pattern.findall(window_text))
                
                if score > max_score:
                    max_score = score
                    best_window = (window_start, window_end)
                    
        best_text = text[best_window[0]:best_window[1]]
        highlighted = pattern.sub(r'<em>\1</em>', best_text)
        
        prefix = "..." if best_window[0] > 0 else ""
        suffix = "..." if best_window[1] < len(text) else ""
        
        return f"{prefix}{highlighted}{suffix}"

    def process_and_rank_snippets(self, query: str, texts: List[str]) -> List[str]:
        """
        Takes a query string and a list of document strings, returns highlighted snippets.
        Optionally ranks them using the Cross-Encoder.
        """
        snippets = [self.extract_snippet(query, t) for t in texts]
        
        if self.use_cross_encoder and self.cross_encoder and snippets:
            # Strip tags for ranking evaluation
            clean_tag = re.compile(r'</?em>')
            clean_snippets = [clean_tag.sub('', s) for s in snippets]
            pairs = [(query, cs) for cs in clean_snippets]
            
            scores = self.cross_encoder.predict(pairs)
            scored_snippets = list(zip(snippets, scores))
            # Sort descending by relevancy score
            scored_snippets.sort(key=lambda x: x[1], reverse=True)
            return [s for s, _ in scored_snippets]
            
        return snippets

    def transform_documents(self, documents: Sequence[Any], **kwargs: Any) -> Sequence[Any]:
        """
        Transformers LangChain Document objects by inserting highlighted text 
        into metadata mimicking Weaviate's GraphQL _additional response.
        Pass 'search_query' in kwargs.
        """
        query = kwargs.get('search_query', '')
        if not query:
            return documents
            
        transformed_docs = []
        for doc in documents:
            content = getattr(doc, 'page_content', '')
            snippet = self.extract_snippet(query, content)
            
            # Copy and nest metadata schema
            new_metadata = getattr(doc, 'metadata', {}).copy()
            if '_additional' not in new_metadata:
                new_metadata['_additional'] = {}
            if 'highlight' not in new_metadata['_additional']:
                new_metadata['_additional']['highlight'] = {}
                
            new_metadata['_additional']['highlight']['content'] = snippet
            
            # Form return struct
            if LANGCHAIN_AVAILABLE:
                transformed_docs.append(Document(page_content=content, metadata=new_metadata))
            else:
                transformed_docs.append(Document(page_content=content, metadata=new_metadata))
                
        # Support Cross-Encoder Document Re-ranking
        if self.use_cross_encoder and self.cross_encoder and transformed_docs:
            pairs = [(query, getattr(doc, 'page_content', '')) for doc in transformed_docs]
            scores = self.cross_encoder.predict(pairs)
            for doc, score in zip(transformed_docs, scores):
                doc.metadata['_additional']['cross_encoder_score'] = float(score)
            transformed_docs.sort(
                key=lambda d: getattr(d, 'metadata', {}).get('_additional', {}).get('cross_encoder_score', 0.0), 
                reverse=True
            )

        return transformed_docs

    async def atransform_documents(self, documents: Sequence[Any], **kwargs: Any) -> Sequence[Any]:
        return self.transform_documents(documents, **kwargs)


def mock_graphql_response(highlighted_texts: List[str]) -> Dict[str, Any]:
    """Generates a mock GraphQL response showing _additional { highlight { content } }"""
    return {
        "data": {
            "Get": {
                "Article": [
                    {
                        "_additional": {
                            "highlight": {
                                "content": text
                            }
                        },
                        "title": f"Mock Title {i+1}",
                        "content": "Original content here..."
                    }
                    for i, text in enumerate(highlighted_texts)
                ]
            }
        }
    }


if __name__ == "__main__":
    import json
    
    highlighter = WeaviateHighlighter(fragment_size=100)
    
    query = "Search engines evaluate highlight"
    doc_texts = [
        ("This is an extra long text that does not have anything to do with our keywords at first. "
         "But eventually we will mention how a search engine can add amazing features. "
         "It highlights the most important keywords and helps the user find the information quickly. "
         "A good search engine is quite complex."),
        ("Different document entirely. It also talks about how developers highlight text using regex "
         "when integrating a Search engine into their product.")
    ]
    
    # 1. Base function: Query and multiple docs -> list of snippets
    print("--- Core Extract & Rank Logic ---")
    snippets = highlighter.process_and_rank_snippets(query, doc_texts)
    for i, snip in enumerate(snippets):
        print(f"[{i}] {snip}")
    print("\n" + "="*50 + "\n")
    
    # 2. Langchain RAG Integration Test
    print("--- LangChain Document Transformation ---")
    docs = [Document(page_content=t, metadata={"id": i}) for i, t in enumerate(doc_texts)]
    transformed = highlighter.transform_documents(docs, search_query=query)
    
    print(json.dumps(transformed[0].metadata, indent=2))
    print("\n" + "="*50 + "\n")
    
    # 3. Mock GraphQL response Output
    print("--- Mock GraphQL Response ---")
    highlights = [d.metadata['_additional']['highlight']['content'] for d in transformed]
    res = mock_graphql_response(highlights)
    print(json.dumps(res, indent=2))
