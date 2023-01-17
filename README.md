<h1>Weaviate <img alt='Weaviate logo' src='https://raw.githubusercontent.com/semi-technologies/weaviate/19de0956c69b66c5552447e84d016f4fe29d12c9/docs/assets/weaviate-logo.png' width='124' align='right' /></h1>

[![Build Status](https://github.com/semi-technologies/weaviate/actions/workflows/.github/workflows/pull_requests.yaml/badge.svg?branch=master)](https://github.com/semi-technologies/weaviate/actions/workflows/.github/workflows/pull_requests.yaml)
[![Go Report Card](https://goreportcard.com/badge/github.com/semi-technologies/weaviate)](https://goreportcard.com/report/github.com/semi-technologies/weaviate)
[![Coverage Status](https://codecov.io/gh/semi-technologies/weaviate/branch/master/graph/badge.svg)](https://codecov.io/gh/semi-technologies/weaviate)
[![Slack](https://img.shields.io/badge/slack--channel-blue?logo=slack)](https://join.slack.com/t/weaviate/shared_invite/zt-goaoifjr-o8FuVz9b1HLzhlUfyfddhw)
[![Newsletter](https://img.shields.io/badge/newsletter-blue?logo=revue)](http://weaviate-newsletter.semi.technology/)

## Overview

Weaviate is an **open source ​vector search engine** that is robust, scalable, cloud-native, and fast. 

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

Weaviate is a search engine that is capable of much more. Some of its other superpowers include **recommendation**, **summarization**, and **integrations with neural search frameworks**.

## What can you build with Weaviate?

For starters, you can build vector search engines with text, images, or a combination of both. 

You can also build question and answer extraction, summarization and classification systems.

You can find [code examples here](https://github.com/semi-technologies/weaviate-examples), and you might blog posts like these useful:

- [How to build an Image Search Application with Weaviate](https://weaviate.io/blog/how-to-build-an-image-search-application-with-weaviate)
- [Cohere Multilingual ML Models with Weaviate](https://weaviate.io/blog/cohere-multilingual-with-weaviate)
- [The Sphere Dataset in Weaviate](https://weaviate.io/blog/sphere-dataset-in-weaviate)

## Content

Speaking of content - we love connecting with our community through these. We love helping amazing people build cool things with Weaviate, and we love getting to know them as well as talking to them about their passions. 

To this end, our team does an amazing job with our [blog](https://weaviate.io/blog) and [podcast](https://weaviate.io/podcast). 

Some of our past favorites include:

### 📝 Blogs

- [Why is vector search so fast?](https://weaviate.io/blog/Why-is-Vector-Search-so-fast)
- [Cohere Multilingual ML Models with Weaviate](https://weaviate.io/blog/Cohere-multilingual-with-weaviate)
- [Vamana vs. HNSW - Exploring ANN algorithms Part 1](https://weaviate.io/blog/ann-algorithms-vamana-vs-hnsw)

### 🎙️ Podcasts

- [Neural Magic in Weaviate](https://www.youtube.com/watch?v=leGgjIQkVYo)
- [BERTopic](https://www.youtube.com/watch?v=IwXOaHanfUU)
- [Jina AI's Neural Search Framework](https://www.youtube.com/watch?v=o6MD0tWl0SM)

Both our [📝 blogs](https://weaviate.io/blog) and [🎙️ podcasts](https://weaviate.io/podcast) are updated regularly. To keep up to date with all things Weaviate including new software releases, meetup news and of course all of the content, you can subscribe to our [🗞️ newsletter](https://newsletter.weaviate.io/).

## Community

Also, we invite you to join our [Slack](https://join.slack.com/t/weaviate/shared_invite/zt-goaoifjr-o8FuVz9b1HLzhlUfyfddhw) community. There, you can meet other Weaviate users and members of the Weaviate team to talk all things Weaviate and AI (and other topics!).

You can also say hi to us below:
- [Twitter](https://twitter.com/weaviate_io) 
- [LinkedIn](https://www.linkedin.com/company/semi-technologies)

Or connect to us via:
- [Stack Overflow for questions](https://stackoverflow.com/questions/tagged/weaviate)
- [GitHub for issues](https://github.com/semi-technologies/weaviate/issues)

-----

## Weaviate helps ...

1. **Software Engineers** ([docs](https://weaviate.io/developers/weaviate/current/)) - Who use Weaviate as an ML-first database for your applications. 
    * Out-of-the-box modules for: NLP/semantic search, automatic classification and image similarity search.
    * Easy to integrate into your current architecture, with full CRUD support like you're used to from other OSS databases.
    * Cloud-native, distributed, runs well on Kubernetes and scales with your workloads.

2. **Data Engineers** ([docs](https://weaviate.io/developers/weaviate/current/)) - Who use Weaviate as a vector database that is built up from the ground with ANN at its core, and with the same UX they love from Lucene-based search engines.
    * Weaviate has a modular setup that allows you to use your ML models inside Weaviate, but you can also use out-of-the-box ML models (e.g., SBERT, ResNet, fasttext, etc).
    * Weaviate takes care of the scalability, so that you don't have to.
    * Deploy and maintain ML models in production reliably and efficiently.

3. **Data Scientists** ([docs](https://weaviate.io/developers/weaviate/current/)) - Who use Weaviate for a seamless handover of their Machine Learning models to MLOps.
    * Deploy and maintain your ML models in production reliably and efficiently.
    * Weaviate's modular design allows you to easily package any custom trained model you want.
    * Smooth and accelerated handover of your Machine Learning models to engineers.

## Interfaces

You can use Weaviate with any of these clients:

- [Python](https://weaviate.io/developers/weaviate/client-libraries/python)
- [Javascript](https://weaviate.io/developers/weaviate/client-libraries/javascript)
- [Go](https://weaviate.io/developers/weaviate/client-libraries/go)
- [Java](https://weaviate.io/developers/weaviate/client-libraries/java)

You can also use its GraphQL API to retrieve objects and properties.

### GraphQL interface demo

<a href="https://weaviate.io/developers/weaviate/current/" target="_blank"><img src="https://weaviate.io/img/weaviate-demo.gif?i=8" alt="Demo of Weaviate" width="100%"></a>

<sup>Weaviate GraphQL demo on news article dataset containing: Transformers module, GraphQL usage, semantic search, _additional{} features, Q&A, and Aggregate{} function. You can the demo on this dataset in the GUI here: <a href="https://console.semi.technology/console/query#weaviate_uri=https://demo.dataset.playground.semi.technology&graphql_query=%7B%0A%20%20Get%20%7B%0A%20%20%20%20Article(%0A%20%20%20%20%20%20nearText%3A%20%7B%0A%20%20%20%20%20%20%20%20concepts%3A%20%5B%22Housing%20prices%22%5D%0A%20%20%20%20%20%20%7D%0A%20%20%20%20%20%20where%3A%20%7B%0A%20%20%20%20%20%20%20%20operator%3A%20Equal%0A%20%20%20%20%20%20%20%20path%3A%20%5B%22inPublication%22%2C%20%22Publication%22%2C%20%22name%22%5D%0A%20%20%20%20%20%20%20%20valueString%3A%20%22The%20Economist%22%0A%20%20%20%20%20%20%7D%0A%20%20%20%20)%20%7B%0A%20%20%20%20%20%20title%0A%20%20%20%20%20%20inPublication%20%7B%0A%20%20%20%20%20%20%20%20...%20on%20Publication%20%7B%0A%20%20%20%20%20%20%20%20%20%20name%0A%20%20%20%20%20%20%20%20%7D%0A%20%20%20%20%20%20%7D%0A%20%20%20%20%20%20_additional%20%7B%0A%20%20%20%20%20%20%20%20certainty%0A%20%20%20%20%20%20%7D%0A%20%20%20%20%7D%0A%20%20%7D%0A%7D" target="_blank">semantic search</a>, <a href="https://console.semi.technology/console/query#weaviate_uri=https://demo.dataset.playground.semi.technology&graphql_query=%7B%0A%20%20Get%7B%0A%20%20%20%20Article(%0A%20%20%20%20%20%20ask%3A%20%7B%0A%20%20%20%20%20%20%20%20question%3A%20%22What%20did%20Jemina%20Packington%20predict%3F%22%0A%20%20%20%20%20%20%20%20properties%3A%20%5B%22summary%22%5D%0A%20%20%20%20%20%20%7D%0A%20%20%20%20%20%20limit%3A%201%0A%20%20%20%20)%7B%0A%20%20%20%20%20%20title%0A%20%20%20%20%20%20inPublication%20%7B%0A%20%20%20%20%20%20%20%20...%20on%20Publication%20%7B%0A%20%20%20%20%20%20%20%20%20%20name%0A%20%20%20%20%20%20%20%20%7D%0A%20%20%20%20%20%20%7D%0A%20%20%20%20%20%20_additional%20%7B%0A%20%20%20%20%20%20%20%20answer%20%7B%0A%20%20%20%20%20%20%20%20%20%20endPosition%0A%20%20%20%20%20%20%20%20%20%20property%0A%20%20%20%20%20%20%20%20%20%20result%0A%20%20%20%20%20%20%20%20%20%20startPosition%0A%20%20%20%20%20%20%20%20%7D%0A%20%20%20%20%20%20%7D%0A%20%20%20%20%7D%0A%20%20%7D%0A%7D" target="_blank">Q&A</a>, <a href="https://console.semi.technology/console/query#weaviate_uri=https://demo.dataset.playground.semi.technology&graphql_query=%7B%0A%20%20Aggregate%20%7B%0A%20%20%20%20Article%20%7B%0A%20%20%20%20%20%20meta%20%7B%0A%20%20%20%20%20%20%20%20count%0A%20%20%20%20%20%20%7D%0A%20%20%20%20%7D%0A%20%20%7D%0A%7D" target="_blank">Aggregate</a>.</sup>

## Additional material

### Reading

- [Weaviate is an open-source search engine powered by ML, vectors, graphs, and GraphQL (ZDNet)](https://www.zdnet.com/article/weaviate-an-open-source-search-engine-powered-by-machine-learning-vectors-graphs-and-graphql/)
- [Weaviate, an ANN Database with CRUD support (DB-Engines.com)](https://db-engines.com/en/blog_post/87)
- [A sub-50ms neural search with DistilBERT and Weaviate (Towards Datascience)](https://towardsdatascience.com/a-sub-50ms-neural-search-with-distilbert-and-weaviate-4857ae390154)
- [Getting Started with Weaviate Python Library (Towards Datascience)](https://towardsdatascience.com/getting-started-with-weaviate-python-client-e85d14f19e4f)
