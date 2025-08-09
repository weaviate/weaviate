# Weaviate <img alt='Weaviate logo' src='https://weaviate.io/img/site/weaviate-logo-light.png' width='148' align='right' />

[![GitHub Repo stars](https://img.shields.io/github/stars/weaviate/weaviate?style=social)](https://github.com/weaviate/weaviate)
[![Go Reference](https://pkg.go.dev/badge/github.com/weaviate/weaviate.svg)](https://pkg.go.dev/github.com/weaviate/weaviate)
[![Build Status](https://github.com/weaviate/weaviate/actions/workflows/.github/workflows/pull_requests.yaml/badge.svg?branch=main)](https://github.com/weaviate/weaviate/actions/workflows/.github/workflows/pull_requests.yaml)
[![Go Report Card](https://goreportcard.com/badge/github.com/weaviate/weaviate)](https://goreportcard.com/report/github.com/weaviate/weaviate)
[![Coverage Status](https://codecov.io/gh/weaviate/weaviate/branch/main/graph/badge.svg)](https://codecov.io/gh/weaviate/weaviate)
[![Slack](https://img.shields.io/badge/slack--channel-blue?logo=slack)](https://weaviate.io/slack)

## Overview

Weaviate is an open-source vector database designed for AI applications. It combines vector search with structured filtering and supports production-scale workloads with features like horizontal scaling, replication, and multi-tenancy.

## Installation

- [Docker]()
- [Kubernetes]()
- [Weaviate Cloud](https://console.weaviate.cloud)

See [installation docs](https://weaviate.io/developers/weaviate/installation) for embedded Weaviate, local development, and custom configurations.

## Getting Started

```python
import weaviate
import weaviate.classes as wvc

# Connect to Weaviate
client = weaviate.connect_to_local()

# Create a collection
client.collections.create(
    name="Article",
    properties=[
        wvc.Property(name="content", data_type=wvc.DataType.TEXT)
    ]
)

# Insert objects
articles = client.collections.get("Article")
articles.data.insert_many([
    {"content": "Vector databases enable semantic search"},
    {"content": "Machine learning models generate embeddings"},
    {"content": "Weaviate supports hybrid search capabilities"}
])

# Perform semantic search
results = articles.query.near_text(
    query="AI and embeddings",
    limit=2
)

for item in results.objects:
    print(item.properties["content"])

client.close()
```

## Weaviate Features

- **Vector Search**: Sub-50ms semantic search on billions of vectors using HNSW algorithm
- **Hybrid Search**: Combine vector search with keyword (BM25) and filter operators
- **Integrated Vectorizers**: Built-in support for OpenAI, Cohere, HuggingFace, Google PaLM, Anthropic, and more
- **Generative Search**: RAG pipelines with integrated LLMs for answer generation
- **Multi-Modal**: Search across text, images, and other data types
- **CRUD Operations**: Full support for Create, Read, Update, Delete operations
- **Multi-Tenancy**: Isolate data per tenant with built-in access control
- **Horizontal Scaling**: Distributed architecture for production workloads

## Useful Resources

- [What is a Vector Database](https://weaviate.io/blog/what-is-a-vector-database)
- [What is Vector Search](https://weaviate.io/blog/vector-search-explained)
- [What is Hybrid Search](https://weaviate.io/blog/hybrid-search-explained)
- [How to Choose an Embedding Model](https://weaviate.io/blog/how-to-choose-an-embedding-model)
- [What is RAG]()
- [RAG Evaluation]()
- [Advanced RAG Techniques]()
- [What is Multimodal RAG]()
- [What is Agentic RAG]()
- [What is Graph RAG]()
- [Overview of Late Interaction Models]()

## Contributing to Weaviate

We welcome contributions! Please see our [Contributor Guide](https://weaviate.io/developers/contributor-guide) for the development setup, code style guidelines, testing requirements and the pull request process.

Join our [Slack community](https://weaviate.io/slack) to discuss ideas and get help.

## License

BSD 3-Clause License. See [LICENSE](./LICENSE) for details.
