<h1>Weaviate <img alt='Weaviate logo' src='https://weaviate.io/img/site/weaviate-logo-light.png' width='148' align='right' /></h1>

[![Build Status](https://github.com/weaviate/weaviate/actions/workflows/.github/workflows/pull_requests.yaml/badge.svg?branch=master)](https://github.com/weaviate/weaviate/actions/workflows/.github/workflows/pull_requests.yaml)
[![Go Report Card](https://goreportcard.com/badge/github.com/weaviate/weaviate)](https://goreportcard.com/report/github.com/weaviate/weaviate)
[![Coverage Status](https://codecov.io/gh/weaviate/weaviate/branch/master/graph/badge.svg)](https://codecov.io/gh/weaviate/weaviate)
[![Slack](https://img.shields.io/badge/slack--channel-blue?logo=slack)](https://weaviate.io/slack)

## Overview

Weaviate is an **open source ​vector database** that is robust, scalable, cloud-native, and fast.

If you just want to get started, great! Try:
- the [quickstart tutorial](https://weaviate.io/developers/weaviate/quickstart) if you are looking to use Weaviate, or
- the [contributor guide](https://weaviate.io/developers/contributor-guide) if you are looking to contribute to the project.

And you can find our [documentation here](https://weaviate.io/developers/weaviate/).

If you have a bit more time, stick around and check out our summary below 😉

-----

## Why Weaviate?

With Weaviate, you can turn your text, images and more into a searchable vector database using state-of-the-art ML models.

Some of its highlights are:

### Speed

Weaviate typically performs a 10-NN neighbor search out of millions of objects in single-digit milliseconds. See [benchmarks](https://weaviate.io/developers/weaviate/benchmarks).

### Flexibility

You can use Weaviate to conveniently **vectorize your data at import time**, or alternatively you can **upload your own vectors**.

These vectorization options are enabled by Weaviate modules. Modules enable use of popular services and model hubs such as [OpenAI](https://weaviate.io/developers/weaviate/modules/retriever-vectorizer-modules/text2vec-openai), [Cohere](https://weaviate.io/developers/weaviate/modules/retriever-vectorizer-modules/text2vec-cohere) or [HuggingFace](https://weaviate.io/developers/weaviate/modules/retriever-vectorizer-modules/text2vec-huggingface) and much more, including use of local and custom models.

### Production-readiness

Weaviate is designed to take you from **rapid prototyping** all the way to **production at scale**.

To this end, Weaviate is built with [scaling](https://weaviate.io/developers/weaviate/concepts/cluster), [replication](https://weaviate.io/developers/weaviate/concepts/replication-architecture), and [security](https://weaviate.io/developers/weaviate/configuration/authentication) in mind, among others.

### Beyond search

Weaviate powers lightning-fast vector searches, but it is capable of much more. Some of its other superpowers include **recommendation**, **summarization**, and **integrations with neural search frameworks**.

## What can you build with Weaviate?

For starters, you can build vector databases with text, images, or a combination of both.

You can also build question and answer extraction, summarization and classification systems.

You can find [code examples here](https://github.com/weaviate/weaviate-examples), and you might these blog posts useful:

- [The ChatGPT Retrieval Plugin - Weaviate as a Long-term Memory Store for Generative AI](https://weaviate.io/blog/weaviate-retrieval-plugin)
- [Giving Auto-GPT Long-Term Memory with Weaviate](https://weaviate.io/blog/autogpt-and-weaviate)
- [Combining LangChain and Weaviate](https://weaviate.io/blog/combining-langchain-and-weaviate)
- [How to build an Image Search Application with Weaviate](https://weaviate.io/blog/how-to-build-an-image-search-application-with-weaviate)
- [Cohere Multilingual ML Models with Weaviate](https://weaviate.io/blog/cohere-multilingual-with-weaviate)
- [Weaviate Podcast Search](https://weaviate.io/blog/weaviate-podcast-search)
- [The Sphere Dataset in Weaviate](https://weaviate.io/blog/sphere-dataset-in-weaviate)

### Integrations

Examples and/or documentation of Weaviate integrations (a-z).

- [Auto-GPT](https://github.com/Significant-Gravitas/Auto-GPT/blob/master/docs/configuration/memory.md#weaviate-setup) ([blogpost](https://weaviate.io/blog/autogpt-and-weaviate)) – use Weaviate as a memory backend for Auto-GPT
- [Cohere](https://weaviate.io/developers/weaviate/modules/retriever-vectorizer-modules/text2vec-cohere) ([blogpost](https://txt.cohere.com/embedding-archives-wikipedia/)) - Use Cohere embeddings with Weaviate.
- [DocArray](https://docarray.jina.ai/advanced/document-store/weaviate/) - Use Weaviate as a document store in DocArray.
- [Haystack](https://docs.haystack.deepset.ai/reference/document-store-api#weaviatedocumentstore) ([blogpost](https://www.deepset.ai/weaviate-vector-search-engine-integration)) - Use Weaviate as a document store in Haystack.
- [Hugging Face](https://weaviate.io/developers/weaviate/modules/retriever-vectorizer-modules/text2vec-huggingface) - Use Hugging Face models with Weaviate.
- [LangChain](https://python.langchain.com/en/latest/ecosystem/weaviate.html#weaviate) ([blogpost](https://weaviate.io/blog/combining-langchain-and-weaviate)) - Use Weaviate as a memory backend for LangChain.
- [LlamaIndex](https://gpt-index.readthedocs.io/en/latest/how_to/integrations/vector_stores.html) ([blogpost](https://weaviate.io/blog/llamaindex-and-weaviate))- Use Weaviate as a memory backend for LlamaIndex.
- [OpenAI - ChatGPT retrieval plugin](https://github.com/openai/chatgpt-retrieval-plugin/blob/main/docs/providers/weaviate/setup.md) - Use Weaviate as a memory backend for ChatGPT.
- [OpenAI](https://github.com/openai/openai-cookbook/tree/main/examples/vector_databases/weaviate) - use OpenAI embeddings with Weaviate.
- [yours?](https://weaviate.io/slack)

## Weaviate content

Speaking of content - we love connecting with our community through these. We love helping amazing people build cool things with Weaviate, and we love getting to know them as well as talking to them about their passions.

To this end, our team does an amazing job with our [blog](https://weaviate.io/blog) and [podcast](https://weaviate.io/podcast).

Some of our past favorites include:

### 📝 Blogs

- [What to expect from Weaviate in 2023](https://weaviate.io/blog/what-to-expect-from-weaviate-in-2023)
- [Why is vector search so fast?](https://weaviate.io/blog/Why-is-Vector-Search-so-fast)
- [Cohere Multilingual ML Models with Weaviate](https://weaviate.io/blog/Cohere-multilingual-with-weaviate)
- [Vamana vs. HNSW - Exploring ANN algorithms Part 1](https://weaviate.io/blog/ann-algorithms-vamana-vs-hnsw)
- [HNSW+PQ - Exploring ANN algorithms Part 2.1](https://weaviate.io/blog/ann-algorithms-hnsw-pq)
- [The Tile Encoder - Exploring ANN algorithms Part 2.2](https://weaviate.io/blog/ann-algorithms-tiles-enocoder)
- [How GPT4.0 and other Large Language Models Work](https://weaviate.io/blog/what-are-llms)
- [Monitoring Weaviate in Production](https://weaviate.io/blog/monitoring-weaviate-in-production)

### 🎙️ Podcasts

- [Neural Magic in Weaviate](https://www.youtube.com/watch?v=leGgjIQkVYo)
- [BERTopic](https://www.youtube.com/watch?v=IwXOaHanfUU)
- [Jina AI's Neural Search Framework](https://www.youtube.com/watch?v=o6MD0tWl0SM)

### 📰 Newsletter

Subscribe to our [🗞️ newsletter](https://newsletter.weaviate.io/) to keep up to date including new releases, meetup news and of course all of the content,.

## Join our community!

We invite you to:
- Join our [Slack](https://weaviate.io/slack) community, and
- Ask questions at our [forum](https://forum.weaviate.io/).

You can also say hi to us below:
- [Twitter](https://twitter.com/weaviate_io)
- [LinkedIn](https://www.linkedin.com/company/weaviate-io)

-----

## Weaviate helps ...

1. **Software Engineers** - Who use Weaviate as an ML-first database for your applications.
    * Out-of-the-box modules for: AI-powered searches, Q&A, integrating LLMs with your data, and automatic classification.
    * With full CRUD support like you're used to from other OSS databases.
    * Cloud-native, distributed, runs well on Kubernetes and scales with your workloads.

2. **Data Engineers** - Who use Weaviate as fast, flexible vector database
    * Use your own ML mode or out-of-the-box ML models, locally or with an inference service.
    * Weaviate takes care of the scalability, so that you don't have to.

3. **Data Scientists** - Who use Weaviate for a seamless handover of their Machine Learning models to MLOps.
    * Deploy and maintain your ML models in production reliably and efficiently.
    * Easily package any custom trained model you want.
    * Smooth and accelerated handover of your ML models to engineers.

Read more in our [documentation](https://weaviate.io/developers/weaviate)

## Interfaces

You can use Weaviate with any of these clients:

- [Python](https://weaviate.io/developers/weaviate/client-libraries/python)
- [JavaScript/TypeScript](https://weaviate.io/developers/weaviate/client-libraries/typescript)
- [Go](https://weaviate.io/developers/weaviate/client-libraries/go)
- [Java](https://weaviate.io/developers/weaviate/client-libraries/java)

You can also use its GraphQL API to retrieve objects and properties.

### GraphQL interface demo

<a href="https://weaviate.io/developers/weaviate/current/" target="_blank"><img src="https://weaviate.io/img/site/weaviate-demo.gif?i=9" alt="Demo of Weaviate" width="100%"></a>


## Additional material

### Reading

- [Weaviate is an open-source search engine powered by ML, vectors, graphs, and GraphQL (ZDNet)](https://www.zdnet.com/article/weaviate-an-open-source-search-engine-powered-by-machine-learning-vectors-graphs-and-graphql/)
- [Weaviate, an ANN Database with CRUD support (DB-Engines.com)](https://db-engines.com/en/blog_post/87)
- [A sub-50ms neural search with DistilBERT and Weaviate (Towards Datascience)](https://towardsdatascience.com/a-sub-50ms-neural-search-with-distilbert-and-weaviate-4857ae390154)
- [Getting Started with Weaviate Python Library (Towards Datascience)](https://towardsdatascience.com/getting-started-with-weaviate-python-client-e85d14f19e4f)
