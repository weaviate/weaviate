# Weaviate <img alt='Weaviate logo' src='https://raw.githubusercontent.com/creativesoftwarefdn/weaviate/19de0956c69b66c5552447e84d016f4fe29d12c9/docs/assets/weaviate-logo.png' width='200' align='right' />

_Decentralised Knowledge Graph & Decentralised Knowledge P2P Network_

## About

Weaviate is a knowledge graph which meshes all your data and makes it available as one seamless source for contextualised research, reporting, and re-use. Our aim is to transform static (big-)data into a natural language queriable knowledge base which you can access directly or over a Peer-to-Peer network.

### Technical Features

Weaviate comes with a variety of features and is based on specific design principles;

Key features of Weaviate include;

<table>
  <tr>
    <td width="25%"><b>Contextual</b></td>
    <td>The contextionary is a fast and powerful natural language processing tool which combines multiple one directional word embeddings, bi-directional word embeddings and other NLP tools to achieve production ready machine comprehension tasks. You can read more about the contextionary <a href="./docs/en/contribute/contextionary.md">here</a> and the roadmap for new features is located <a href="./docs/en/contribute/roadmap.md">here</a>.</td> 
  </tr>
  <tr>
    <td><b>Contextual Ontologies</b></td>
    <td>Weaviate's ontologies are completely contextual, meaning that you not only use words to describe what your data means but also the context in which your data appears.</td> 
  </tr>
  <tr>
    <td><b>Easy to use</b></td>
    <td>We relay heavely on vanilla GraphQL and RESTful interfaces. We believe in a superior developer user experience an try to make it as easy as possible to use Weaviate</td> 
  </tr>
  <tr>
    <td><b>Decentrilized Knowledge Graphs (P2P-network)</b></td>
    <td>If desired, you can create a decentrilized P2P-network of many Weaviates. This is especially handy when you want to share data or to make specific knowledge graph architectures.</td> 
  </tr>
  <tr>
    <td><b>Modulair Datastorage</b></td>
    <td>Because Weaviate can be used for many use cases where <a href="https://en.wikipedia.org/wiki/CAP_theorem" target="_blank">consistency, availability and partition tolerance</a> play different roles, we want to make it as easy as possible to use multiple databases to store your data.</td> 
  </tr>
</table>

## Documentation

Weaviate's documentation is available in the `./docs` folder on Github and divided into two sections:

1. [documenation for users](#usage-docs) and;
2. [documentation for contributors](#contribution-docs).

We highly recommend to start with the _Getting Started_ (for [users](./docs/en/use/getting-started.md) or for [contributors](./docs/en/contribute/getting-started.md)) or the [Weaviate Concept](./docs/en/use/weaviate-concept.md).

### Usage Docs

- [Getting Started](./docs/en/use/getting-started.md)
- [Index of all documentation for users](./docs/en/use/index.md)

### Contribution Docs

- [Getting Started](./docs/en/contribute/getting-started.md)
- [Index of documentation for contributors](./docs/en/contribute/index.md)

### Miscellaneous Docs

- [Weaviate Concept](./docs/en/use/weaviate-concept.md)
- [FAQ](./docs/en/use/FAQ.md)

## Questions

- General questions: [Stackoverflow.com](https://stackoverflow.com/questions/tagged/weaviate).
- Issues: [Github](https://github.com/creativesoftwarefdn/weaviate/issues).

## Commercial Use

Weaviate is released under a [BSD-3](https://github.com/creativesoftwarefdn/weaviate/blob/master/LICENSE.md) license by the Creative Software Foundation. [SeMI](https://www.semi.network) offers a suite of enterprise and turnkey platform products and services based on Weaviate.

More information:

- Products & Services overview: [www.semi.networ](https://www.semi.network/products/).
- Contact: [hello@semi.network](mailto:hello@semi.network).

## Build Status

| Branch   | Status        |
| -------- |:-------------:|
| Master   | [![Build Status](https://api.travis-ci.org/creativesoftwarefdn/weaviate.svg?branch=master)](https://travis-ci.org/creativesoftwarefdn/weaviate/branches)
| Develop  | [![Build Status](https://api.travis-ci.org/creativesoftwarefdn/weaviate.svg?branch=develop)](https://travis-ci.org/creativesoftwarefdn/weaviate/branches)