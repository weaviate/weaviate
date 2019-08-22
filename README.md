# Weaviate <img alt='Weaviate logo' src='https://raw.githubusercontent.com/semi-technologies/weaviate/19de0956c69b66c5552447e84d016f4fe29d12c9/docs/assets/weaviate-logo.png' width='180' align='right' />

_The Decentralized Knowledge Graph & The Decentralized Knowledge P2P Network_

## Index

- [Introduction](#introduction)
- [References & Docs](#references)
- [Questions](#questions)
- [Commercial use](#commercial-use)
- [Build Status](#build-status)
- [Query Example](#query-example)

## Introduction

Weaviate is a knowledge graph which meshes all your data and makes it available as one seamless source for contextualized research, reporting, and re-use. Our aim is to transform static (big-)data into a natural language queryable knowledge base which you can access directly or over a peer-to-peer network.

### Technical features

Weaviate comes with a variety of features and is based on specific design principles.

Key features of Weaviate include:

<table>
  <tr>
    <td width="25%"><b>Contextual</b></td>
    <td>The Contextionary is a fast and powerful natural language processing tool which combines multiple uni-directional word embeddings, bi-directional word embeddings and other NLP tools to achieve production-ready machine comprehension tasks. You can read more about the Contextionary <a href="./docs/en/contribute/contextionary.md">here</a> and the roadmap for new features is located <a href="./docs/en/contribute/roadmap.md">here</a>.</td> 
  </tr>
  <tr>
    <td><b>Contextual ontologies</b></td>
    <td>Weaviate's ontologies are completely contextual, meaning that you not only use words to describe what your data means but also the context in which your data appears.</td> 
  </tr>
  <tr>
    <td><b>Easy to use</b></td>
    <td>We rely heavily on vanilla GraphQL and RESTful interfaces. We believe in a superior developer user experience and try to make it as easy as possible to use Weaviate</td> 
  </tr>
  <tr>
    <td><b>Decentralized knowledge graphs (P2P-network)</b></td>
    <td>If desired, you can create a decentralized P2P-network of many Weaviates. This is especially handy when you want to share data or to make specific knowledge graph architectures.</td> 
  </tr>
  <tr>
    <td><b>Modular data storage</b></td>
    <td>Because Weaviate can be used for many use cases where <a href="https://en.wikipedia.org/wiki/CAP_theorem" target="_blank">consistency, availability and partition tolerance</a> play different roles, we want to make it as easy as possible to use multiple databases to store your data.</td> 
  </tr>
</table>

## References

> Note: Weaviate is currently only available as an unstable release. We hope to release a first stable version in the coming months.

Weaviate's documentation is available in the `./docs` folder on Github and divided into two sections:

1. [documenation for users](#usage-docs) and;
2. [documentation for contributors](#contribution-docs).

We highly recommend starting with the _Getting Started_ (for [users](./docs/en/use/getting-started.md) or for [contributors](./docs/en/contribute/getting-started.md)).

### Usage docs

> We are updating our docs and they will be moved to [www.semi.technology](https://www.semi.technology) soon.
> You can leave your email [here](http://eepurl.com/gye_bX) to get a notification when they are live.

- [Index of all documentation for users](./docs/en/use/index.md)
- [Getting Started](./docs/en/use/getting-started.md)

### Contribution docs

- [Index of documentation for contributors](./docs/en/contribute/index.md)
- [Getting Started](./docs/en/contribute/getting-started.md)

### Miscellaneous docs

- [FAQ](./docs/en/use/FAQ.md)

## Questions

- General questions: [Stackoverflow.com](https://stackoverflow.com/questions/tagged/weaviate).
- Issues: [Github](https://github.com/semi-technologies/weaviate/issues).

## Commercial use

Weaviate is released under a [BSD-3](https://github.com/semi-technologies/weaviate/blob/master/LICENSE.md) license by the Creative Software Foundation. [SeMI](https://www.semi.network) offers a suite of enterprise and whitelabel platform products and managed services based on Weaviate.

More information:

- Products & Services overview: [www.semi.technology](https://www.semi.technology/products/).
- Contact: [hello@semi.technology](mailto:hello@semi.technology).

## Build Status

| Branch   | Status        |
| -------- |:-------------:|
| Master   | [![Build Status](https://api.travis-ci.org/semi-technologies/weaviate.svg?branch=master)](https://travis-ci.org/semi-technologies/weaviate/branches)
| Develop  | [![Build Status](https://api.travis-ci.org/semi-technologies/weaviate.svg?branch=develop)](https://travis-ci.org/semi-technologies/weaviate/branches)

## Query Example

![Weaviate query example](./docs/assets/demo.gif)
