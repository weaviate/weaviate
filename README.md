# Weaviate <img alt='Weaviate logo' src='https://weaviate.io/img/site/weaviate-logo-light.png' width='148' align='right' />

[![GitHub Repo stars](https://img.shields.io/github/stars/weaviate/weaviate?style=social)](https://github.com/weaviate/weaviate)
[![Go Reference](https://pkg.go.dev/badge/github.com/weaviate/weaviate.svg)](https://pkg.go.dev/github.com/weaviate/weaviate)
[![Build Status](https://github.com/weaviate/weaviate/actions/workflows/.github/workflows/pull_requests.yaml/badge.svg?branch=main)](https://github.com/weaviate/weaviate/actions/workflows/.github/workflows/pull_requests.yaml)
[![Go Report Card](https://goreportcard.com/badge/github.com/weaviate/weaviate)](https://goreportcard.com/report/github.com/weaviate/weaviate)
[![Coverage Status](https://codecov.io/gh/weaviate/weaviate/branch/main/graph/badge.svg)](https://codecov.io/gh/weaviate/weaviate)
[![Slack](https://img.shields.io/badge/slack--channel-blue?logo=slack)](https://weaviate.io/slack)

Weaviate is an open-source cloud-native vector database designed for building AI applications. It combines vector similarity search with keyword filtering, retrieval-augmented generation (RAG), and reranking within a single system. This provides a reliable foundation for AI-powered applications such as RAG pipelines, chatbots, recommendation engines, summarizers, and classification systems.

Weaviate can vectorize your data at import time using AI models (OpenAI, Cohere, HuggingFace, etc.) or you can upload your own vector embeddings. Weaviate supports production-scale workloads by offering multi-tenancy, replication, authorization and [many more features](#weaviate-features).

To get started quickly, have a look at one of these tutorials:

- [Quickstart - Weaviate Cloud](https://docs.weaviate.io/weaviate/quickstart)
- [Quickstart - local Docker instance](https://docs.weaviate.io/weaviate/quickstart/local)

## Installation

Weaviate offers multiple installation and deployment options:

- [Docker](https://docs.weaviate.io/deploy/installation-guides/docker-installation)
- [Kubernetes](https://docs.weaviate.io/deploy/installation-guides/k8s-installation)
- [Weaviate Cloud](https://console.weaviate.cloud)

See the [installation docs](https://docs.weaviate.io/deploy) for more deployment options like [AWS](https://docs.weaviate.io/deploy/installation-guides/aws-marketplace) and [GCP](https://docs.weaviate.io/deploy/installation-guides/gcp-marketplace).

## Getting started

You can easily start Weaviate with docker:

```
docker run -p 8080:8080 -p 50051:50051 -e ENABLE_API_BASED_MODULES=true cr.weaviate.io/semitechnologies/weaviate:1.32.2
```

Install the Python client:

```
pip install -U weaviate-client

```

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

Weaviate exposes [REST API](https://docs.weaviate.io/weaviate/api/rest), [gRPC API](https://docs.weaviate.io/weaviate/api/grpc) and [GraphQL API](https://docs.weaviate.io/weaviate/api/graphql) to communicate with the database server.

## Weaviate features

These features enable you to build AI-powered applications:

- **⚡ Fast Search Performance**: Perform complex semantic [searches](https://docs.weaviate.io/weaviate/search/similarity) over billions of vectors in milliseconds. Weaviate's architecture is built in Go for speed and reliability, ensuring your AI applications are highly responsive even under heavy load. See our [ANN benchmarks](https://docs.weaviate.io/weaviate/benchmarks/ann) for more info.

- **🔌 Flexible Vectorization**: Seamlessly vectorize data at import time with [integrated vectorizers](https://docs.weaviate.io/weaviate/model-providers) from OpenAI, Cohere, HuggingFace, Google, and more. Or you can import [your own vector embeddings](https://docs.weaviate.io/weaviate/starter-guides/custom-vectors).

- **🔍 Advanced Hybrid & Image Search**: Combine the power of semantic search with traditional [keyword (BM25) search](https://docs.weaviate.io/weaviate/search/bm25), [image search](https://docs.weaviate.io/weaviate/search/image) and [advanced filtering](https://docs.weaviate.io/weaviate/search/filters) to get the best results with a single API call.

- **🤖 Integrated RAG & Reranking**: Go beyond simple retrieval with built-in [generative search (RAG)](https://docs.weaviate.io/weaviate/search/generative) and [reranking](https://docs.weaviate.io/weaviate/search/rerank) capabilities. Power sophisticated Q&A systems, chatbots, and summarizers directly from your database without additional tooling.

- **📈 Production-Ready & Scalable**: Weaviate is built for mission-critical applications. Go from rapid prototyping to production at scale with native support for [horizontal scaling](https://docs.weaviate.io/deploy/configuration/horizontal-scaling), [multi-tenancy](https://docs.weaviate.io/weaviate/manage-collections/multi-tenancy), replication, and fine-grained [role-based access control (RBAC)](https://docs.weaviate.io/weaviate/configuration/rbac).

- **💰 Cost-Efficient Operations**: Radically lower resource consumption and operational costs with built-in [vector compression](https://docs.weaviate.io/weaviate/configuration/compression). Vector quantization and multi-vector encoding reduce memory usage with minimal impact on search performance.

For a complete list of all functionalities, visit the [official Weaviate documentation](https://docs.weaviate.io).

## Useful resources

### Integrations

Weaviate integrates with many external services:

| Category                                                                                   | Description                                                | Integrations                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| ------------------------------------------------------------------------------------------ | ---------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **[Cloud Hyperscalers](https://docs.weaviate.io/integrations/cloud-hyperscalers)**         | Large-scale computing and storage                          | [AWS](https://docs.weaviate.io/integrations/cloud-hyperscalers/aws), [Google](https://docs.weaviate.io/integrations/cloud-hyperscalers/google)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| **[Compute Infrastructure](https://docs.weaviate.io/integrations/compute-infrastructure)** | Run and scale containerized applications                   | [Modal](https://docs.weaviate.io/integrations/compute-infrastructure/modal), [Replicate](https://docs.weaviate.io/integrations/compute-infrastructure/replicate), [Replicated](https://docs.weaviate.io/integrations/compute-infrastructure/replicated)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| **[Data Platforms](https://docs.weaviate.io/integrations/data-platforms)**                 | Data ingestion and web scraping                            | [Airbyte](https://docs.weaviate.io/integrations/data-platforms/airbyte), [Aryn](https://docs.weaviate.io/integrations/data-platforms/aryn), [Boomi](https://docs.weaviate.io/integrations/data-platforms/boomi), [Box](https://docs.weaviate.io/integrations/data-platforms/box), [Confluent](https://docs.weaviate.io/integrations/data-platforms/confluent), [Astronomer](https://docs.weaviate.io/integrations/data-platforms/astronomer), [Context Data](https://docs.weaviate.io/integrations/data-platforms/context-data), [Databricks](https://docs.weaviate.io/integrations/data-platforms/databricks), [Firecrawl](https://docs.weaviate.io/integrations/data-platforms/firecrawl), [IBM](https://docs.weaviate.io/integrations/data-platforms/ibm), [Unstructured](https://docs.weaviate.io/integrations/data-platforms/unstructured)                |
| **[LLM and Agent Frameworks](https://docs.weaviate.io/integrations/llm-agent-frameworks)** | Build agents and generative AI applications                | [Agno](https://docs.weaviate.io/integrations/llm-agent-frameworks/agno), [Composio](https://docs.weaviate.io/integrations/llm-agent-frameworks/composio), [CrewAI](https://docs.weaviate.io/integrations/llm-agent-frameworks/crewai), [DSPy](https://docs.weaviate.io/integrations/llm-agent-frameworks/dspy), [Dynamiq](https://docs.weaviate.io/integrations/llm-agent-frameworks/dynamiq), [Haystack](https://docs.weaviate.io/integrations/llm-agent-frameworks/haystack), [LangChain](https://docs.weaviate.io/integrations/llm-agent-frameworks/langchain), [LlamaIndex](https://docs.weaviate.io/integrations/llm-agent-frameworks/llamaindex), [N8n](https://docs.weaviate.io/integrations/llm-agent-frameworks/n8n), [Semantic Kernel](https://docs.weaviate.io/integrations/llm-agent-frameworks/semantic-kernel)                                   |
| **[Operations](https://docs.weaviate.io/integrations/operations)**                         | Tools for monitoring and analyzing generative AI workflows | [AIMon](https://docs.weaviate.io/integrations/operations/aimon), [Arize](https://docs.weaviate.io/integrations/operations/arize), [Cleanlab](https://docs.weaviate.io/integrations/operations/cleanlab), [Comet](https://docs.weaviate.io/integrations/operations/comet), [DeepEval](https://docs.weaviate.io/integrations/operations/deepeval), [Langtrace](https://docs.weaviate.io/integrations/operations/langtrace), [LangWatch](https://docs.weaviate.io/integrations/operations/langwatch), [Nomic](https://docs.weaviate.io/integrations/operations/nomic), [Patronus AI](https://docs.weaviate.io/integrations/operations/patronus), [Ragas](https://docs.weaviate.io/integrations/operations/ragas), [TruLens](https://docs.weaviate.io/integrations/operations/trulens), [Weights & Biases](https://docs.weaviate.io/integrations/operations/wandb) |

### Demo projects

These demos are working applications that highlight some of Weaviate's capabilities. Their source code is available on GitHub.

- [Verba](https://verba.weaviate.io) ([GitHub](https://github.com/weaviate/verba)): A community-driven open-source application designed to offer an end-to-end, streamlined, and user-friendly interface for Retrieval-Augmented Generation (RAG) out of the box.
- [Healthsearch](https://healthsearch.weaviate.io) ([GitHub](https://github.com/weaviate/healthsearch-demo)): An open-source project aimed at showcasing the potential of leveraging user-written reviews and queries to retrieve supplement products based on specific health effects.
- [Awesome-Moviate](https://awesome-moviate.weaviate.io/) ([GitHub](https://github.com/weaviate-tutorials/awesome-moviate)): A movie search and recommendation engine that allows keyword-based (BM25), semantic, and hybrid searches.

### Blog posts

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

We welcome and appreciate contributions! Please see our [Contributor guide](https://docs.weaviate.io/contributor-guide) for the development setup, code style guidelines, testing requirements and the pull request process.

Join our [Slack community](https://weaviate.io/slack) or [Community forum](https://forum.weaviate.io/) to discuss ideas and get help.

## License

BSD 3-Clause License. See [LICENSE](./LICENSE) for details.
