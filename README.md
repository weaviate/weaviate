# Weaviate <img alt='Weaviate logo' src='https://weaviate.io/img/site/weaviate-logo-light.png' width='148' align='right' />

[![GitHub Repo stars](https://img.shields.io/github/stars/weaviate/weaviate?style=social)](https://github.com/weaviate/weaviate)
[![Go Reference](https://pkg.go.dev/badge/github.com/weaviate/weaviate.svg)](https://pkg.go.dev/github.com/weaviate/weaviate)
[![Build Status](https://github.com/weaviate/weaviate/actions/workflows/.github/workflows/pull_requests.yaml/badge.svg?branch=main)](https://github.com/weaviate/weaviate/actions/workflows/.github/workflows/pull_requests.yaml)
[![Go Report Card](https://goreportcard.com/badge/github.com/weaviate/weaviate)](https://goreportcard.com/report/github.com/weaviate/weaviate)
[![Coverage Status](https://codecov.io/gh/weaviate/weaviate/branch/main/graph/badge.svg)](https://codecov.io/gh/weaviate/weaviate)
[![Slack](https://img.shields.io/badge/slack--channel-blue?logo=slack)](https://weaviate.io/slack)

Weaviate is an open-source vector database designed for AI applications. It combines vector search with structured filtering and supports production-scale workloads with features like horizontal scaling, replication, and multi-tenancy.

## Installation

- [Docker](https://docs.weaviate.io/deploy/installation-guides/docker-installation)
- [Kubernetes](https://docs.weaviate.io/deploy/installation-guides/k8s-installation)
- [Weaviate Cloud](https://console.weaviate.cloud)

See [installation docs](https://docs.weaviate.io/deploy) for more options like embedded Weaviate, and cloud providers like [AWS](https://docs.weaviate.io/deploy/installation-guides/aws-marketplace) and [GCP](https://docs.weaviate.io/deploy/installation-guides/gcp-marketplace).

## Getting Started

```python
import os
import weaviate
from weaviate.classes.config import Configure, DataType, Property

# Connect to Weaviate
client = weaviate.connect_to_local(
    headers={"X-OpenAI-Api-Key": os.environ["OPENAI_API_KEY"]}
)

# Create a collection
client.collections.create(
    name="Article",
    properties=[Property(name="content", data_type=DataType.TEXT)],
    vector_config=Configure.Vectors.text2vec_openai(),  # Use vectorizer or bring your own vectors
)

# Insert objects
articles = client.collections.get("Article")
articles.data.insert_many(
    [
        {"content": "Vector databases enable semantic search"},
        {"content": "Machine learning models generate embeddings"},
        {"content": "Weaviate supports hybrid search capabilities"},
    ]
)
while len(articles) < 3: pass  # Wait for vectorization to complete

# Perform semantic search
results = articles.query.near_text(query="Search objects by meaning", limit=1)
print(results.objects[0])

client.close()
```

## Weaviate Features

- **[Vector search](https://docs.weaviate.io/weaviate/search/similarity)**: Sub-50ms semantic search on billions of vectors using HNSW algorithm
- **[Keyword search](https://docs.weaviate.io/weaviate/search/bm25)**
- **[Hybrid search](https://docs.weaviate.io/weaviate/search/hybrid)**: Combine vector search with keyword (BM25) and filter operators
- **[Integrated vectorizers](https://docs.weaviate.io/weaviate/model-providers)**: Built-in support for OpenAI, Cohere, HuggingFace, Google PaLM, Anthropic, and other embedding model providers
- **[Generative search - RAG](https://docs.weaviate.io/weaviate/search/generative)**: RAG pipelines with integrated LLMs for answer generation
- **[Multi-tenancy](https://docs.weaviate.io/weaviate/manage-collections/multi-tenancy)**: Isolate data per tenant with built-in access control
- **[Horizontal scaling](https://docs.weaviate.io/deploy/configuration/horizontal-scaling)**: Distributed architecture for production workloads

For a complete list of all functionalities, visit the [official Weaviate documentation](https://docs.weaviate.io).

## Useful Resources

- [What is a Vector Database](https://weaviate.io/blog/what-is-a-vector-database)
- [What is Vector Search](https://weaviate.io/blog/vector-search-explained)
- [What is Hybrid Search](https://weaviate.io/blog/hybrid-search-explained)
- [How to Choose an Embedding Model](https://weaviate.io/blog/how-to-choose-an-embedding-model)
- [What is RAG](https://weaviate.io/blog/introduction-to-rag)
- [RAG Evaluation](https://weaviate.io/blog/rag-evaluation)
- [Advanced RAG Techniques](https://weaviate.io/blog/advanced-rag)
- [What is Multimodal RAG](https://weaviate.io/blog/multimodal-rag)
- [What is Agentic RAG](https://weaviate.io/blog/what-is-agentic-rag)
- [What is Graph RAG](https://weaviate.io/blog/graph-rag)
- [Overview of Late Interaction Models](https://weaviate.io/blog/late-interaction-overview)

## Contributing

We welcome contributions! Please see our [Contributor Guide](https://docs.weaviate.io/contributor-guide) for the development setup, code style guidelines, testing requirements and the pull request process.

Join our [Slack community](https://weaviate.io/slack) to discuss ideas and get help.

## License

BSD 3-Clause License. See [LICENSE](./LICENSE) for details.
