<h1>Weaviate <img alt='Weaviate logo' src='https://raw.githubusercontent.com/semi-technologies/weaviate/19de0956c69b66c5552447e84d016f4fe29d12c9/docs/assets/weaviate-logo.png' width='52' align='right' /></h1>

[![Build Status](https://api.travis-ci.org/semi-technologies/weaviate.svg?branch=master)](https://travis-ci.org/semi-technologies/weaviate/branches)
[![Go Report Card](https://goreportcard.com/badge/github.com/semi-technologies/weaviate)](https://goreportcard.com/report/github.com/semi-technologies/weaviate)
[![Coverage Status](https://codecov.io/gh/semi-technologies/weaviate/branch/master/graph/badge.svg)](https://codecov.io/gh/semi-technologies/weaviate)
[![Slack](https://img.shields.io/badge/slack--channel-blue?logo=slack)](https://codecov.io/gh/semi-technologies/weaviate)
[![Newsletter](https://img.shields.io/badge/newsletter-blue?logo=mailchimp)](https://www.semi.technology/newsletter/)

<a href="https://semi.technology/developers/weaviate/current/" target="_blank"><img src="https://www.semi.technology/img/weaviate-demo.gif?i=4" alt="Demo of Weaviate" width="100%"></a>

## Description

Weaviate is a cloud-native, real-time vector search engine (aka _neural search engine_ or _deep search engine_). There are modules for specific use cases such as semantic search, plugins to integrate Weaviate in any application of your choice, and a [console](https://console.semi.technology/) to visualize your data.

_GraphQL - RESTful - vector search engine - vector database - neural search engine - semantic search - HNSW - deep search - machine learning - kNN_

## Documentation

You can find detailed documentation in the [developers section of our website](https://www.semi.technology/developers/weaviate/current/) or directly go to one of the docs using the links in the list below.

- [Documentation](https://semi.technology/developers/weaviate/current/)
  - About Weaviate
    - [Introduction video](https://www.semi.technology/developers/weaviate/current/index.html#introduction-video)
    - [What is Weaviate](https://www.semi.technology/developers/weaviate/current/index.html#what-is-weaviate)
    - [Why a vector search engine?](https://www.semi.technology/developers/weaviate/current/index.html#why-a-vector-search-engine)
    - [How does Weaviate work?](https://www.semi.technology/developers/weaviate/current/index.html#how-does-weaviate-work)
    - [When should I use Weaviate?](https://www.semi.technology/developers/weaviate/current/index.html#when-should-i-use-weaviate)
  - Installation
    - [Docker Compose](https://semi.technology/developers/weaviate/current/getting-started/installation.html#docker-compose)
    - [Cloud Deployment](https://semi.technology/developers/weaviate/current/getting-started/installation.html#cloud-deployment)
    - [Weaviate Cluster Service (beta)](https://semi.technology/developers/weaviate/current/getting-started/installation.html#weaviate-cluster-service)
    - [Kubernetes](https://semi.technology/developers/weaviate/current/getting-started/installation.html#kubernetes)
  - [Modules](https://www.semi.technology/developers/weaviate/current/modules/)
  - [RESTful API references](https://www.semi.technology/developers/weaviate/current/restful-api-references/)
  - [GraphQL references](https://www.semi.technology/developers/weaviate/current/graphql-references/)
  - Client Libraries
    - [Python](https://semi.technology/developers/weaviate/current/client-libraries/python.html)
    - [JavaScript](https://semi.technology/developers/weaviate/current/client-libraries/javascript.html)
    - [Go](https://www.semi.technology/developers/weaviate/current/client-libraries/go.html)
  - [How To's](https://semi.technology/developers/weaviate/current/how-tos/)
  - [RESTful API references](https://semi.technology/developers/weaviate/current/restful-api-references/)
  - [GraphQL references](https://semi.technology/developers/weaviate/current/graphql-references/)
  - [More resources](https://semi.technology/developers/weaviate/current/more-resources/)

## Examples

You can find [more examples here](https://github.com/semi-technologies/weaviate-examples)

### Unmask Superheroes in 5 steps using the NLP module

A simple example in Python (you can also use other [client libs](https://www.semi.technology/developers/weaviate/current/client-libraries/)) showing how Weaviate can help you unmask superheroes thanks to its vector search capabilities 🦸

1. Connect to a Weaviate

```python
import weaviate
client = weaviate.Client("http://localhost:8080")
```

2. Add a class to the schema

```python
classObj = {
    "class": "Superhero",
    "description": "A class describing a super hero",
    "properties": [
        {
            "name": "name",
            "dataType": [
                "string"
            ],
            "description": "Name of the super hero"
        }
    ],
    "vectorizer": "text2vec-contextionary" # Tell Weaviate to vectorize the content
}
client.schema.create_class(classObj) # returns None if successful
```

Step 3. Add the superheroes with a batch request

```python

batman = {
    "name": "Batman"
}
superman = {
    "name": "Superman"
}
batch = weaviate.ObjectsBatchRequest()
batch.add(batman, "Superhero")
batch.add(superman, "Superhero")
client.batch.create(batch)
```

Step 4. Try to find superheroes in the vectorspace

```python
def findAlterego(alterEgo):
    whoIsIt = client.query.get(
        "Superhero",
        ["name", "_additional {certainty, id } "]
    ).with_near_text({
        "concepts": [alterEgo] # query that gets vectorized 🪄
    }).do()

    print(
        alterEgo, "is", whoIsIt['data']['Get']['Superhero'][0]['name'],
        "with a certainy of", whoIsIt['data']['Get']['Superhero'][0]['_additional']['certainty']
    )

findAlterego("Clark Kent")  # prints something like: Clark Kent is Superman with a certainy of 0.6026741
findAlterego("Bruce Wayne") # prints something like: Bruce Wayne is Batman with a certainy of 0.6895526
```

Step 5. See how the superheroes are represented in the vectorspace

```python
def showVectorForAlterego(alterEgo):
    whoIsIt = client.query.get(
        "Superhero",
        ["_additional {id} "]
    ).with_near_text({
        "concepts": [alterEgo] # query that gets vectorized 🪄
    }).do()

    getVector = client.data_object.get_by_id(
        whoIsIt['data']['Get']['Superhero'][0]['_additional']['id'],
        additional_properties=["vector"]
    )

    print(
        "The vector for",
        alterEgo,
        "is",
        getVector['vector']
    )

showVectorForAlterego("Clark Kent") # prints something like: The vector for Clark Kent is [-0.05484624, 0.08283167, -0.3002325, ...etc...
```

## Support

- [Stackoverflow for questions](https://stackoverflow.com/questions/tagged/weaviate)
- [Github for issues](https://github.com/semi-technologies/weaviate/issues)
- [Slack channel](https://join.slack.com/t/weaviate/shared_invite/zt-goaoifjr-o8FuVz9b1HLzhlUfyfddhw)

## Contributing

- [How to Contribute](https://www.semi.technology/developers/contributor-guide/current/)
