# Weaviate <img alt='Weaviate logo' src='https://weaviate.io/img/site/weaviate-logo-light.png' width='148' align='right' />

[![GitHub Repo stars](https://img.shields.io/github/stars/weaviate/weaviate?style=social)](https://github.com/weaviate/weaviate)
[![Go Reference](https://pkg.go.dev/badge/github.com/weaviate/weaviate.svg)](https://pkg.go.dev/github.com/weaviate/weaviate)
[![Build Status](https://github.com/weaviate/weaviate/actions/workflows/.github/workflows/pull_requests.yaml/badge.svg?branch=main)](https://github.com/weaviate/weaviate/actions/workflows/.github/workflows/pull_requests.yaml)
[![Go Report Card](https://goreportcard.com/badge/github.com/weaviate/weaviate)](https://goreportcard.com/report/github.com/weaviate/weaviate)
[![Coverage Status](https://codecov.io/gh/weaviate/weaviate/branch/main/graph/badge.svg)](https://codecov.io/gh/weaviate/weaviate)
[![Slack](https://img.shields.io/badge/slack--channel-blue?logo=slack)](https://weaviate.io/slack)

Weaviate is an open-source cloud-native vector database designed for building AI applications. It combines vector similarity search with keyword filtering, retrieval-augmented generation (RAG), and reranking within a single system. These features provide a reliable foundation for AI-powered applications such as RAG pipelines, chatbots, recommendation engines, summarizers, and classification systems.

Weaviate can vectorize your data at import time using AI models (OpenAI, Cohere, HuggingFace, etc.) or you can upload your own vector embeddings. Weaviate supports production-scale workloads by offering multi-tenancy, replication, authorization and [many more features](#weaviate-features).

To get started quickly, have a look at one of these tutorials:

- [Quickstart - Weaviate Cloud](https://docs.weaviate.io/weaviate/quickstart)
- [Quickstart - local Docker instance](https://docs.weaviate.io/weaviate/quickstart/local)

<!--
## Why Weaviate?

Weaviate uses state-of-the-art machine learning (ML) models to turn your data - text, images, and more - into a searchable vector database.

Here are some highlights.

### Speed

Weaviate is fast. The core engine can run a 10-NN nearest neighbor search on millions of objects in milliseconds. See [benchmarks](https://docs.weaviate.io/weaviate/benchmarks).

### Flexibility

Weaviate can **vectorize your data at import time**. Or, if you have already vectorized your data, you can **upload your own vectors** instead.

Modules give you the flexibility to tune Weaviate for your needs. More than two dozen modules connect you to popular services and model hubs such as OpenAI, Cohere, VoyageAI and HuggingFace. Use custom modules to work with your own models or third party services.

### Production-readiness

Weaviate is built with [scaling](https://docs.weaviate.io/weaviate/concepts/cluster), [replication](https://docs.weaviate.io/weaviate/concepts/replication-architecture), and [security](https://docs.weaviate.io/weaviate/configuration/authentication) in mind so you can go smoothly from **rapid prototyping** to **production at scale**.

### Beyond search

Weaviate doesn't just power lightning-fast vector searches. Other superpowers include **recommendation**, **summarization**, and **integration with neural search frameworks**.

## Who uses Weaviate?

- **Software Engineers**

  - Weaviate is an ML-first database engine
  - Out-of-the-box modules for AI-powered searches, automatic classification, and LLM integration
   - Full CRUD support
   - Cloud-native, distributed system that runs well on Kubernetes
   - Scales with your workloads

- **Data Engineers**

  - Weaviate is a fast, flexible vector database
  - Use your own ML model or third party models
  - Run locally or with an inference service

- **Data Scientists**

   - Seamless handover of Machine Learning models to engineers and MLOps
   - Deploy and maintain your ML models in production reliably and efficiently
   - Easily package custom trained models
-->

## Installation

Weaviate offers a multitude of installation and deployment options:

- [Docker](https://docs.weaviate.io/deploy/installation-guides/docker-installation)
- [Kubernetes](https://docs.weaviate.io/deploy/installation-guides/k8s-installation)
- [Weaviate Cloud](https://console.weaviate.cloud)

See the [installation docs](https://docs.weaviate.io/deploy) for more deployment options like [AWS](https://docs.weaviate.io/deploy/installation-guides/aws-marketplace) and [GCP](https://docs.weaviate.io/deploy/installation-guides/gcp-marketplace).

## Getting started

The following Python example shows how easy it is to populate a Weaviate database with data, create vector embeddings and perform semantic search:

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
    vector_config=Configure.Vectors.text2vec_openai(),  # Use integrated vectorizers or bring your own vectors
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

## Client libraries and APIs

Weaviate provides client libraries for several programming languages:

- [Python](https://docs.weaviate.io/weaviate/client-libraries/python)
- [JavaScript/TypeScript](https://docs.weaviate.io/weaviate/client-libraries/typescript)
- [Java](https://docs.weaviate.io/weaviate/client-libraries/java)
- [Go](https://docs.weaviate.io/weaviate/client-libraries/go)
- C# (🚧 Coming soon 🚧)

There are also additional [community-maintained libraries](https://docs.weaviate.io/weaviate/client-libraries/community).

Weaviate also exposes [REST API](https://docs.weaviate.io/weaviate/api/rest), [gRPC API](https://docs.weaviate.io/weaviate/api/grpc) and [GraphQL API](https://docs.weaviate.io/weaviate/api/graphql) to communicate with the database server.

## Weaviate features

- **[Vector search](https://docs.weaviate.io/weaviate/search/similarity)**: Perform fast semantic searches on billions of vectors
- **[Keyword search](https://docs.weaviate.io/weaviate/search/bm25)**: Keyword search is an exact matching-based search using strings
- **[Hybrid search](https://docs.weaviate.io/weaviate/search/hybrid)**: Combine vector search with keyword (BM25) and filter operators
- **[Integrated vectorizers](https://docs.weaviate.io/weaviate/model-providers)**: Built-in support for OpenAI, Cohere, HuggingFace, Google, Anthropic, and many other embedding model providers
- **[Compression](https://docs.weaviate.io/weaviate/configuration/compression)**: Vector quantization and encoding can significantly lower resource consumption
- **[RAG (generative search)](https://docs.weaviate.io/weaviate/search/generative)**: RAG pipelines with integrated LLMs for answer generation
- **[Reranking](https://docs.weaviate.io/weaviate/search/rerank)**: Reorder search results according to a specific criteria
- **[Multi-tenancy](https://docs.weaviate.io/weaviate/manage-collections/multi-tenancy)**: Isolate data per tenant with built-in access control
- **[Horizontal scaling](https://docs.weaviate.io/deploy/configuration/horizontal-scaling)**: Distributed architecture for production workloads
- **[Role-based access control (RBAC)](https://docs.weaviate.io/weaviate/configuration/rbac)**: Restrict access based on roles and users

For a complete list of all functionalities, visit the [official Weaviate documentation](https://docs.weaviate.io).

## Integrations

Weaviate integrates with many external services:

| <!-- -->                                                                                   | <!-- -->                                                   | <!-- -->                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| ------------------------------------------------------------------------------------------ | ---------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **[Cloud Hyperscalers](https://docs.weaviate.io/integrations/cloud-hyperscalers)**         | Large-scale computing and storage                          | [AWS](https://docs.weaviate.io/integrations/cloud-hyperscalers/aws), [Google](https://docs.weaviate.io/integrations/cloud-hyperscalers/google)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| **[Compute Infrastructure](https://docs.weaviate.io/integrations/compute-infrastructure)** | Run and scale containerized applications                   | [Modal](https://docs.weaviate.io/integrations/compute-infrastructure/modal), [Replicate](https://docs.weaviate.io/integrations/compute-infrastructure/replicate), [Replicated](https://docs.weaviate.io/integrations/compute-infrastructure/replicated)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| **[Data Platforms](https://docs.weaviate.io/integrations/data-platforms)**                 | Data ingestion and web scraping                            | [Airbyte](https://docs.weaviate.io/integrations/data-platforms/airbyte), [Aryn](https://docs.weaviate.io/integrations/data-platforms/aryn), [Boomi](https://docs.weaviate.io/integrations/data-platforms/boomi), [Box](https://docs.weaviate.io/integrations/data-platforms/box), [Confluent](https://docs.weaviate.io/integrations/data-platforms/confluent), [Astronomer](https://docs.weaviate.io/integrations/data-platforms/astronomer), [Context Data](https://docs.weaviate.io/integrations/data-platforms/context-data), [Databricks](https://docs.weaviate.io/integrations/data-platforms/databricks), [Firecrawl](https://docs.weaviate.io/integrations/data-platforms/firecrawl), [IBM](https://docs.weaviate.io/integrations/data-platforms/ibm), [Unstructured](https://docs.weaviate.io/integrations/data-platforms/unstructured)                |
| **[LLM and Agent Frameworks](https://docs.weaviate.io/integrations/llm-agent-frameworks)** | Build agents and generative AI applications                | [Agno](https://docs.weaviate.io/integrations/llm-agent-frameworks/agno), [Composio](https://docs.weaviate.io/integrations/llm-agent-frameworks/composio), [CrewAI](https://docs.weaviate.io/integrations/llm-agent-frameworks/crewai), [DSPy](https://docs.weaviate.io/integrations/llm-agent-frameworks/dspy), [Dynamiq](https://docs.weaviate.io/integrations/llm-agent-frameworks/dynamiq), [Haystack](https://docs.weaviate.io/integrations/llm-agent-frameworks/haystack), [LangChain](https://docs.weaviate.io/integrations/llm-agent-frameworks/langchain), [LlamaIndex](https://docs.weaviate.io/integrations/llm-agent-frameworks/llamaindex), [N8n](https://docs.weaviate.io/integrations/llm-agent-frameworks/n8n), [Semantic Kernel](https://docs.weaviate.io/integrations/llm-agent-frameworks/semantic-kernel)                                   |
| **[Operations](https://docs.weaviate.io/integrations/operations)**                         | Tools for monitoring and analyzing generative AI workflows | [AIMon](https://docs.weaviate.io/integrations/operations/aimon), [Arize](https://docs.weaviate.io/integrations/operations/arize), [Cleanlab](https://docs.weaviate.io/integrations/operations/cleanlab), [Comet](https://docs.weaviate.io/integrations/operations/comet), [DeepEval](https://docs.weaviate.io/integrations/operations/deepeval), [Langtrace](https://docs.weaviate.io/integrations/operations/langtrace), [LangWatch](https://docs.weaviate.io/integrations/operations/langwatch), [Nomic](https://docs.weaviate.io/integrations/operations/nomic), [Patronus AI](https://docs.weaviate.io/integrations/operations/patronus), [Ragas](https://docs.weaviate.io/integrations/operations/ragas), [TruLens](https://docs.weaviate.io/integrations/operations/trulens), [Weights & Biases](https://docs.weaviate.io/integrations/operations/wandb) |

## Useful blog posts

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

We welcome and appreciate contributions! Please see our [Contributor Guide](https://docs.weaviate.io/contributor-guide) for the development setup, code style guidelines, testing requirements and the pull request process.

Join our [Slack community](https://weaviate.io/slack) or [Community forum](https://forum.weaviate.io/) to discuss ideas and get help.

## License

BSD 3-Clause License. See [LICENSE](./LICENSE) for details.
