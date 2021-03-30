<h1>Weaviate <img alt='Weaviate logo' src='https://raw.githubusercontent.com/semi-technologies/weaviate/19de0956c69b66c5552447e84d016f4fe29d12c9/docs/assets/weaviate-logo.png' width='52' align='right' /></h1>

[![Build Status](https://api.travis-ci.org/semi-technologies/weaviate.svg?branch=master)](https://travis-ci.org/semi-technologies/weaviate/branches)
[![Go Report Card](https://goreportcard.com/badge/github.com/semi-technologies/weaviate)](https://goreportcard.com/report/github.com/semi-technologies/weaviate)
[![Coverage Status](https://codecov.io/gh/semi-technologies/weaviate/branch/master/graph/badge.svg)](https://codecov.io/gh/semi-technologies/weaviate)
[![Slack](https://img.shields.io/badge/slack--channel-blue?logo=slack)](https://codecov.io/gh/semi-technologies/weaviate)
[![Newsletter](https://img.shields.io/badge/newsletter-blue?logo=mailchimp)](https://www.semi.technology/newsletter/)

## About

Weaviate is a cloud-native, real-time vector search engine (aka _neural search engine_ or _deep search engine_). There are modules for specific use cases such as semantic search, plugins to integrate Weaviate in any application of your choice, and a [console](https://console.semi.technology/) to visualize your data.

Keywords: _GraphQL - RESTful - vector search engine - vector database - neural search engine - semantic search - HNSW - deep search - machine learning - kNN - transformers - contextionary - FastText - GloVe_

## Background

What if you could search through your data like Google Search and integrate it into any application with an API? That's our core goal with Weaviate.
- Weaviate has a graph-like data model
- You can use GraphQL to query Weaviate based on your own schema 
- Every data-object (i.e., graph-node) is represented by a vector, this can be a vector of any kind (NLP, image, audio, etc)
- Weaviate modules can take care of the vectorization for your or you can create your own vectors.   
  - [Out-of-the-box transformer modules](https://www.semi.technology/developers/weaviate/current/modules/text2vec-transformers.html)
  - [Out-of-the-box contextionary (i.e., FastText and GloVe) modules](https://www.semi.technology/developers/weaviate/current/modules/text2vec-contextionary.html)
- Weaviate modules can simply add functionality to Weaviate based on your needs.
- [Weaviate is a full-fledged CRUD database](https://db-engines.com/en/blog_post/87)
- [Speed up to sub-50ms regardless of the size of your dataset](https://towardsdatascience.com/a-sub-50ms-neural-search-with-distilbert-and-weaviate-4857ae390154)

## Modules

<table style="width:100%">
  <tr>
    <th>Module</th>
    <th>Description</th>
    <th>GraphQL demo with module</th>
  </tr>
  <tr>
    <td>
      <a href="https://www.semi.technology/developers/weaviate/current/modules/text2vec-contextionary.html">text2vec-contextionary</a>
    </td>
    <td>The module text2vec-contextionary is Weaviate’s own language vectorizer. It gives context to the language used in your dataset (there are Contextionary versions available for multiple languages). The text2vec-contextionary is trained using fastText on Wiki and CommonCrawl data. We aim to make the Contextionary available for use cases in any domain, regardless if they are business-related, academic or other. </td>
    <td><img src="https://www.semi.technology/img/demo-c11y.gif?i=1" alt="Demo of Weaviate" width="100%"></td>
  </tr>
  <tr>
    <td>
      <a href="https://www.semi.technology/developers/weaviate/current/modules/text2vec-transformers.html">text2vec-transformers</a>
    </td>
    <td>The text2vec-transformers module allows you to use a pre-trained language transformer model as Weaviate vectorization module. Transformer models differ from the Contextionary as they allow you to plug in a pretrained NLP module specific to your use case. This means models like BERT, DilstBERT, RoBERTa, DilstilROBERTa, etc. can be used out-of-the box with Weaviate.</td>
    <td><img src="https://www.semi.technology/img/demo-transformers.gif?i=1" alt="Demo of Weaviate" width="100%"></td>
  </tr>
  <tr>
    <td>t
      <a href="https://www.semi.technology/developers/weaviate/current/modules/custom-modules.html">custom modules</a>
    </td>
    <td>
      <i>soon available</i>
    </td>
    <td>n.a.</td>
  </tr>
</table>

## Documentation

- Complete documentation is available on [semi.technology/developers/weaviate](https://www.semi.technology/developers/weaviate/current/).

## Examples

- You can find [code examples here](https://github.com/semi-technologies/weaviate-examples)
- You can find [tutorials here](https://www.semi.technology/developers/weaviate/current/tutorials/)

## Support

- [Stackoverflow for questions](https://stackoverflow.com/questions/tagged/weaviate)
- [Github for issues](https://github.com/semi-technologies/weaviate/issues)
- [Slack channel](https://join.slack.com/t/weaviate/shared_invite/zt-goaoifjr-o8FuVz9b1HLzhlUfyfddhw)

## Contributing

- [How to Contribute](https://www.semi.technology/developers/contributor-guide/current/)
