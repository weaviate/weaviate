<h1>Weaviate <img alt='Weaviate logo' src='https://raw.githubusercontent.com/semi-technologies/weaviate/19de0956c69b66c5552447e84d016f4fe29d12c9/docs/assets/weaviate-logo.png' width='124' align='right' /></h1>

## The ML-first vector search engine

[![Build Status](https://api.travis-ci.org/semi-technologies/weaviate.svg?branch=master)](https://travis-ci.org/semi-technologies/weaviate/branches)
[![Go Report Card](https://goreportcard.com/badge/github.com/semi-technologies/weaviate)](https://goreportcard.com/report/github.com/semi-technologies/weaviate)
[![Coverage Status](https://codecov.io/gh/semi-technologies/weaviate/branch/master/graph/badge.svg)](https://codecov.io/gh/semi-technologies/weaviate)
[![Slack](https://img.shields.io/badge/slack--channel-blue?logo=slack)](https://join.slack.com/t/weaviate/shared_invite/zt-goaoifjr-o8FuVz9b1HLzhlUfyfddhw)
[![Newsletter](https://img.shields.io/badge/newsletter-blue?logo=revue)](http://weaviate-newsletter.semi.technology/)

## Description

**Weaviate in a nutshell**: Weaviate is a vector search engine and vector database. Weaviate uses machine learning to vectorize and store data, and to find answers to natural language queries. With Weaviate you can also bring your custom ML models to production scale.

**Weaviate in detail**: Weaviate is a low-latency vector search engine with out-of-the-box support for different media types (text, images, etc.). It offers Semantic Search, Question-Answer-Extraction, Classification, Customizable Models (PyTorch/TensorFlow/Keras), and more. Built from scratch in Go, Weaviate stores both objects and vectors, allowing for combining vector search with structured filtering with the fault-tolerance of a cloud-native database, all accessible through GraphQL, REST, and various language clients.

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

## GraphQL interface demo

<a href="https://weaviate.io/developers/weaviate/current/" target="_blank"><img src="https://weaviate.io/img/weaviate-demo.gif?i=8" alt="Demo of Weaviate" width="100%"></a>

<sup>Weaviate GraphQL demo on news article dataset containing: Transformers module, GraphQL usage, semantic search, _additional{} features, Q&A, and Aggregate{} function. You can the demo on this dataset in the GUI here: <a href="https://console.semi.technology/console/query#weaviate_uri=https://demo.dataset.playground.semi.technology&graphql_query=%7B%0A%20%20Get%20%7B%0A%20%20%20%20Article(%0A%20%20%20%20%20%20nearText%3A%20%7B%0A%20%20%20%20%20%20%20%20concepts%3A%20%5B%22Housing%20prices%22%5D%0A%20%20%20%20%20%20%7D%0A%20%20%20%20%20%20where%3A%20%7B%0A%20%20%20%20%20%20%20%20operator%3A%20Equal%0A%20%20%20%20%20%20%20%20path%3A%20%5B%22inPublication%22%2C%20%22Publication%22%2C%20%22name%22%5D%0A%20%20%20%20%20%20%20%20valueString%3A%20%22The%20Economist%22%0A%20%20%20%20%20%20%7D%0A%20%20%20%20)%20%7B%0A%20%20%20%20%20%20title%0A%20%20%20%20%20%20inPublication%20%7B%0A%20%20%20%20%20%20%20%20...%20on%20Publication%20%7B%0A%20%20%20%20%20%20%20%20%20%20name%0A%20%20%20%20%20%20%20%20%7D%0A%20%20%20%20%20%20%7D%0A%20%20%20%20%20%20_additional%20%7B%0A%20%20%20%20%20%20%20%20certainty%0A%20%20%20%20%20%20%7D%0A%20%20%20%20%7D%0A%20%20%7D%0A%7D" target="_blank">semantic search</a>, <a href="https://console.semi.technology/console/query#weaviate_uri=https://demo.dataset.playground.semi.technology&graphql_query=%7B%0A%20%20Get%7B%0A%20%20%20%20Article(%0A%20%20%20%20%20%20ask%3A%20%7B%0A%20%20%20%20%20%20%20%20question%3A%20%22What%20did%20Jemina%20Packington%20predict%3F%22%0A%20%20%20%20%20%20%20%20properties%3A%20%5B%22summary%22%5D%0A%20%20%20%20%20%20%7D%0A%20%20%20%20%20%20limit%3A%201%0A%20%20%20%20)%7B%0A%20%20%20%20%20%20title%0A%20%20%20%20%20%20inPublication%20%7B%0A%20%20%20%20%20%20%20%20...%20on%20Publication%20%7B%0A%20%20%20%20%20%20%20%20%20%20name%0A%20%20%20%20%20%20%20%20%7D%0A%20%20%20%20%20%20%7D%0A%20%20%20%20%20%20_additional%20%7B%0A%20%20%20%20%20%20%20%20answer%20%7B%0A%20%20%20%20%20%20%20%20%20%20endPosition%0A%20%20%20%20%20%20%20%20%20%20property%0A%20%20%20%20%20%20%20%20%20%20result%0A%20%20%20%20%20%20%20%20%20%20startPosition%0A%20%20%20%20%20%20%20%20%7D%0A%20%20%20%20%20%20%7D%0A%20%20%20%20%7D%0A%20%20%7D%0A%7D" target="_blank">Q&A</a>, <a href="https://console.semi.technology/console/query#weaviate_uri=https://demo.dataset.playground.semi.technology&graphql_query=%7B%0A%20%20Aggregate%20%7B%0A%20%20%20%20Article%20%7B%0A%20%20%20%20%20%20meta%20%7B%0A%20%20%20%20%20%20%20%20count%0A%20%20%20%20%20%20%7D%0A%20%20%20%20%7D%0A%20%20%7D%0A%7D" target="_blank">Aggregate</a>.</sup>

## Features

Weaviate makes it easy to use state-of-the-art ML models while giving you the scalability, ease of use, safety and cost-effectiveness of a purpose-built vector database. Most notably:

* **Fast queries**<br>
   Weaviate typically performs a 10-NN neighbor search out of millions of objects in considerably less than 100ms.
   <br><sub></sub>

* **Any media type with Weaviate Modules**<br>
  Use State-of-the-Art ML model inference (e.g. Transformers) for Text, Images, etc. at search and query time to let Weaviate manage the process of vectorizing your data for you - or import your own vectors.

* **Combine vector and scalar search**<br>
  Weaviate allows for efficient combined vector and scalar searches, e.g “articles related to the COVID 19 pandemic published within the past 7 days”. Weaviate stores both your objects and the vectors and make sure the retrieval of both is always efficient. There is no need for third party object storage.
* **Real-time and persistent**<br>
Weaviate lets you search through your data even if it’s currently being imported or updated. In addition, every write is written to a Write-Ahead-Log (WAL) for immediately persisted writes - even when a crash occurs.

* **Horizontal Scalability**<br>
  Scale Weaviate for your exact needs, e.g. High-Availability, maximum ingestion, largest possible dataset size, maximum queries per second, etc. (Multi-Node sharding since `v1.8.0`, Replication under development) 

* **Cost-Effectiveness**<br>
  Very large datasets do not need to be kept entirely in memory in Weaviate. At the same time available memory can be used to increase the speed of queries. This allows for a conscious speed/cost trade-off to suit every use case.

* **Graph-like connections between objects**<br>
  Make arbitrary connections between your objects in a graph-like fashion to resemble real-life connections between your data points. Traverse those connections using GraphQL.

## Documentation

You can find detailed documentation in the [developers section of our website](https://weaviate.io/developers/weaviate/current/) or directly go to one of the docs using the links in the list below.

## Additional material

### Video

- [Weaviate introduction video](https://www.youtube.com/watch?v=IExopg1r4fw)

### Reading

- [Weaviate is an open-source search engine powered by ML, vectors, graphs, and GraphQL (ZDNet)](https://www.zdnet.com/article/weaviate-an-open-source-search-engine-powered-by-machine-learning-vectors-graphs-and-graphql/)
- [Weaviate, an ANN Database with CRUD support (DB-Engines.com)](https://db-engines.com/en/blog_post/87)
- [A sub-50ms neural search with DistilBERT and Weaviate (Towards Datascience)](https://towardsdatascience.com/a-sub-50ms-neural-search-with-distilbert-and-weaviate-4857ae390154)
- [Getting Started with Weaviate Python Library (Towards Datascience)](https://towardsdatascience.com/getting-started-with-weaviate-python-client-e85d14f19e4f)

## Examples

You can find [code examples here](https://github.com/semi-technologies/weaviate-examples)

## Support

- [Stackoverflow for questions](https://stackoverflow.com/questions/tagged/weaviate)
- [Github for issues](https://github.com/semi-technologies/weaviate/issues)
- [Slack channel to connect](https://join.slack.com/t/weaviate/shared_invite/zt-goaoifjr-o8FuVz9b1HLzhlUfyfddhw)
- [Newsletter to stay in the know](http://weaviate-newsletter.semi.technology/)

## Contributing

- [How to Contribute](https://weaviate.io/developers/contributor-guide/current/)
