<h1>Weaviate <img alt='Weaviate logo' src='https://weaviate.io/img/site/weaviate-logo-light.png' width='148' align='right' /></h1>

[![Go Reference](https://pkg.go.dev/badge/github.com/weaviate/weaviate.svg)](https://pkg.go.dev/github.com/weaviate/weaviate)
[![Build Status](https://github.com/weaviate/weaviate/actions/workflows/.github/workflows/pull_requests.yaml/badge.svg?branch=main)](https://github.com/weaviate/weaviate/actions/workflows/.github/workflows/pull_requests.yaml)
[![Go Report Card](https://goreportcard.com/badge/github.com/weaviate/weaviate)](https://goreportcard.com/report/github.com/weaviate/weaviate)
[![Coverage Status](https://codecov.io/gh/weaviate/weaviate/branch/main/graph/badge.svg)](https://codecov.io/gh/weaviate/weaviate)
[![Slack](https://img.shields.io/badge/slack--channel-blue?logo=slack)](https://weaviate.io/slack)
[![GitHub Tutorials](https://img.shields.io/badge/Weaviate_Tutorials-green)](https://github.com/weaviate-tutorials/)

## Overview

Weaviate is a cloud-native, **open source vector database** that is robust, fast, and scalable.

To get started quickly, have a look at one of these pages:

- [Quickstart tutorial](https://weaviate.io/developers/weaviate/quickstart) To see Weaviate in action
- [Contributor guide](https://weaviate.io/developers/contributor-guide) To contribute to this project

For more details, read through the summary on this page or see the system [documentation](https://weaviate.io/developers/weaviate/).

---

## Why Weaviate?

Weaviate uses state-of-the-art machine learning (ML) models to turn your data - text, images, and more - into a searchable vector database.

Here are some highlights.

### Speed

Weaviate is fast. The core engine can run a 10-NN nearest neighbor search on millions of objects in milliseconds. See [benchmarks](https://weaviate.io/developers/weaviate/benchmarks).

### Flexibility

Weaviate can **vectorize your data at import time**. Or, if you have already vectorized your data, you can **upload your own vectors** instead.

Modules give you the flexibility to tune Weaviate for your needs. More than two dozen modules connect you to popular services and model hubs such as [OpenAI](https://weaviate.io/developers/weaviate/modules/retriever-vectorizer-modules/text2vec-openai), [Cohere](https://weaviate.io/developers/weaviate/modules/retriever-vectorizer-modules/text2vec-cohere), [VoyageAI](https://weaviate.io/developers/weaviate/modules/retriever-vectorizer-modules/text2vec-voyageai) and [HuggingFace](https://weaviate.io/developers/weaviate/modules/retriever-vectorizer-modules/text2vec-huggingface). Use custom modules to work with your own models or third party services.

### Production-readiness

Weaviate is built with [scaling](https://weaviate.io/developers/weaviate/concepts/cluster), [replication](https://weaviate.io/developers/weaviate/concepts/replication-architecture), and [security](https://weaviate.io/developers/weaviate/configuration/authentication) in mind so you can go smoothly from **rapid prototyping** to **production at scale**.

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

## What can you build with Weaviate?

A Weaviate vector database can search text, images, or a combination of both. Fast vector search provides a foundation for chatbots, recommendation systems, summarizers, and classification systems.

Here are some examples that show how Weaviate integrates with other AI and ML tools:

### Use Weaviate with third party embeddings

- [Cohere](https://weaviate.io/developers/weaviate/modules/retriever-vectorizer-modules/text2vec-cohere) ([blogpost](https://txt.cohere.com/embedding-archives-wikipedia/))
- [Hugging Face](https://weaviate.io/developers/weaviate/modules/retriever-vectorizer-modules/text2vec-huggingface)
- [OpenAI](https://github.com/openai/openai-cookbook/tree/main/examples/vector_databases/weaviate)

### Use Weaviate as a document store

- [DocArray](https://docarray.jina.ai/advanced/document-store/weaviate/)
- [Haystack](https://docs.haystack.deepset.ai/reference/integrations-weaviate#weaviatedocumentstore) ([blogpost](https://www.deepset.ai/weaviate-vector-search-engine-integration))

### Use Weaviate as a memory backend

- [Auto-GPT](https://github.com/Significant-Gravitas/Auto-GPT/blob/master/docs/configuration/memory.md#weaviate-setup) ([blogpost](https://weaviate.io/blog/autogpt-and-weaviate))
- [LangChain](https://python.langchain.com/docs/integrations/providers/weaviate) ([blogpost](https://weaviate.io/blog/combining-langchain-and-weaviate))
- [LlamaIndex](https://gpt-index.readthedocs.io/en/latest/how_to/integrations/vector_stores.html) ([blogpost](https://weaviate.io/blog/llamaindex-and-weaviate))
- [OpenAI - ChatGPT retrieval plugin](https://github.com/openai/chatgpt-retrieval-plugin/blob/main/docs/providers/weaviate/setup.md)

### Demos

These demos are working applications that highlight some of Weaviate's capabilities. Their source code is available on GitHub.

- [Verba, the Golden RAGtreiver](https://verba.weaviate.io) ([GitHub](https://github.com/weaviate/verba))
- [Healthsearch](https://healthsearch.weaviate.io) ([GitHub](https://github.com/weaviate/healthsearch-demo))
- [Awesome-Moviate](https://awesome-moviate.weaviate.io/) ([GitHub](https://github.com/weaviate-tutorials/awesome-moviate))

## How can you connect to Weaviate?

Weaviate exposes a [GraphQL API](https://weaviate.io/developers/weaviate/api/graphql) and a [REST API](https://weaviate.io/developers/weaviate/api/rest). Starting in v1.23, a new [gRPC API](https://weaviate.io/developers/weaviate/api/grpc) provides even faster access to your data.

Weaviate provides client libraries for several popular languages:

- [Python](https://weaviate.io/developers/weaviate/client-libraries/python)
- [JavaScript/TypeScript](https://weaviate.io/developers/weaviate/client-libraries/typescript)
- [Go](https://weaviate.io/developers/weaviate/client-libraries/go)
- [Java](https://weaviate.io/developers/weaviate/client-libraries/java)

There are also [community supported libraries](https://weaviate.io/developers/weaviate/client-libraries/community) for additional languages.

## Where can You learn more?

Free, self-paced courses in [Weaviate Academy](https://weaviate.io/developers/academy) teach you how to use Weaviate. The [Tutorials repo](https://github.com/weaviate-tutorials) has code for example projects. The [Recipes repo](https://github.com/weaviate/recipes) has even more project code to get you started.

The [Weaviate blog](https://weaviate.io/blog) and [podcast](https://weaviate.io/podcast) regularly post stories on Weaviate and AI.

Here are some popular posts:

### Blogs

- [What to expect from Weaviate in 2023](https://weaviate.io/blog/what-to-expect-from-weaviate-in-2023)
- [Why is vector search so fast?](https://weaviate.io/blog/Why-is-Vector-Search-so-fast)
- [Cohere Multilingual ML Models with Weaviate](https://weaviate.io/blog/Cohere-multilingual-with-weaviate)
- [Vamana vs. HNSW - Exploring ANN algorithms Part 1](https://weaviate.io/blog/ann-algorithms-vamana-vs-hnsw)
- [HNSW+PQ - Exploring ANN algorithms Part 2.1](https://weaviate.io/blog/ann-algorithms-hnsw-pq)
- [The Tile Encoder - Exploring ANN algorithms Part 2.2](https://weaviate.io/blog/ann-algorithms-tiles-enocoder)
- [How GPT4.0 and other Large Language Models Work](https://weaviate.io/blog/what-are-llms)
- [Monitoring Weaviate in Production](https://weaviate.io/blog/monitoring-weaviate-in-production)
- [The ChatGPT Retrieval Plugin - Weaviate as a Long-term Memory Store for Generative AI](https://weaviate.io/blog/weaviate-retrieval-plugin)
- [Combining LangChain and Weaviate](https://weaviate.io/blog/combining-langchain-and-weaviate)
- [How to build an Image Search Application with Weaviate](https://weaviate.io/blog/how-to-build-an-image-search-application-with-weaviate)
- [Building Multimodal AI in TypeScript](https://weaviate.io/blog/multimodal-search-in-typescript)
- [Giving Auto-GPT Long-Term Memory with Weaviate](https://weaviate.io/blog/autogpt-and-weaviate)

### Podcasts

- [Neural Magic in Weaviate](https://www.youtube.com/watch?v=leGgjIQkVYo)
- [BERTopic](https://www.youtube.com/watch?v=IwXOaHanfUU)
- [Jina AI's Neural Search Framework](https://www.youtube.com/watch?v=o6MD0tWl0SM)

### Other reading

- [Weaviate is an open-source search engine powered by ML, vectors, graphs, and GraphQL (ZDNet)](https://www.zdnet.com/article/weaviate-an-open-source-search-engine-powered-by-machine-learning-vectors-graphs-and-graphql/)
- [Weaviate, an ANN Database with CRUD support (DB-Engines.com)](https://db-engines.com/en/blog_post/87)
- [A sub-50ms neural search with DistilBERT and Weaviate (Towards Datascience)](https://towardsdatascience.com/a-sub-50ms-neural-search-with-distilbert-and-weaviate-4857ae390154)
- [Getting Started with Weaviate Python Library (Towards Datascience)](https://towardsdatascience.com/getting-started-with-weaviate-python-client-e85d14f19e4f)

## Join our community!

At Weaviate, we love to connect with our community. We love helping amazing people build cool things. And, we love to talk with you about you passion for vector databases and AI.

Please reach out, and join our community:

- [Community forum](https://forum.weaviate.io)
- [GitHub](https://github.com/weaviate/weaviate)
- [Slack](https://weaviate.io/slack)
- [X (Twitter)](https://twitter.com/weaviate_io)

To keep up to date with new releases, meetup news, and more, subscribe to our [newsletter](https://newsletter.weaviate.io/)
