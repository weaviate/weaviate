# Getting Started

> Getting started with Weaviate's Docker-compose setup.

## Index

#### Getting Started with a Local Weaviate

- [Preparation](#preparation)
- [Running Weaviate with Docker-compose](#running-weaviate-with-docker-compose)
- [Creating an Ontology](#creating-an-ontology)
- [Adding Thing and Action Nodes](#adding-thing-and-action-nodes)
- [Crawling the Knowledge Graph with GraphQL](#crawling-the-knowledge-graph-with-graphql)
- [Querying Individual Nodes with the RESTful API](#querying-individual-nodes-with-the-restful-api)
- [Adding a full blown Zoo with Weaviate-CLI](#adding-a-full-blown-zoo-with-weaviate-cli)

#### Getting Started with a Network of Weaviates

> Soon Online

## Preparation

If you want to learn more about the general concept of the Knowledge Graphs and Knowledge Networks you can read that [here](#). This getting started guide uses the dockerized versions of Weaviate. For more advanced usage or configuration, you can use the binaries and data connectors directly. This is covered in the [running Weaviate](running-weaviate.md#run-weaviate-stand-alone-with-docker) documentation.

Check you have Docker and Docker-compose installed:

```bash
$ docker --version
$ docker-compose --version
```

If you do not have Docker installed, you can read [here](https://docs.docker.com/install/) how to do this on a multitude of operating systems.

You are now ready to get started! If you run into issues, please use the;
1. [Knowledge base of old issues](https://github.com/creativesoftwarefdn/weaviate/issues?utf8=%E2%9C%93&q=label%3Abug). Or,
2. Regarding questions: [Stackoverflow](https://stackoverflow.com/questions/tagged/weaviate) . Or,
3. Regarding issues: [Github](//github.com/creativesoftwarefdn/weaviate/issues).

## Running Weaviate with Docker-compose

All elements inside Weaviate are loosely coupled, meaning that you can use or [create](https://github.com/creativesoftwarefdn/weaviate/blob/develop/docs/en/contribute/custom-connectors.md) multiple database connectors. In this setup, we will be using the JanusGraph connector because it contains the most features for scaling Weaviate.

Important to know;
1. The whole setup uses a decent amount of memory. In case of issues, try increasing the memory limit that Docker has available. The setup runs from 2 CPU 12Gig memory. In case of issues check [this issue](https://github.com/creativesoftwarefdn/weaviate/issues/742).
2. It takes some time to start up the whole infrastructure.

#### Starting Weaviate with Docker-compose

You can find [here](running-weaviate.md#run-full-stack-with-docker-compose) how to run Weaviate with Docker-compose.

Check if Weaviate is up and running by using the following curl command:

```bash
$ curl http://localhost:8080/weaviate/v1/meta
```

The result should be an empty Weaviate:

```json
{
    "actionsSchema": {
        "classes": [],
        "type": "action"
    },
    "hostname": "http://[::]:8080",
    "thingsSchema": {
        "classes": [],
        "type": "thing"
    }
}
```

You are now ready to start using Weaviate!

## Creating an Ontology

The ontology describes how the knowledge graph is structured and based on which semantic elements, you can read more in-depth what the Weaviate ontology entails [here](ontology-schema.md).

In this example, we are going to create a [knowledge graph of a zoo](https://github.com/creativesoftwarefdn/weaviate-demo-zoo). Although you can [automate the import of an ontology](#adding-a-full-blown-zoo-with-weaviate-cli) we will do it manually now to get an understanding of how the ontology is structured.

First, we will create the `Zoo` as a Weaviate-thing like this;

```bash
$ curl -X POST http://localhost:8080/weaviate/v1/schema/things -H "Content-Type: application/json" -d '{
    "class": "Zoo",
    "keywords": [{
        "keyword": "park",
        "weight": 0.01
    }, {
        "keyword": "animals",
        "weight": 0.01
    }],
    "description": "Animal park",
    "properties": [{
        "@dataType": [
            "string"
        ],
        "cardinality": "atMostOne",
        "description": "Name of the Zoo",
        "name": "name",
        "keywords": [{
            "keyword": "identifier",
            "weight": 0.01
        }]
    }]
}'
```

Next, we want to create a `City` class to describe which city a zoo is in.

```bash
$ curl -X POST http://localhost:8080/weaviate/v1/schema/things -H "Content-Type: application/json" -d '{
    "class": "City",
    "keywords": [{
        "keyword": "village",
        "weight": 0.01
    }],
    "description": "City",
    "properties": [{
        "@dataType": [
            "string"
        ],
        "cardinality": "atMostOne",
        "description": "Name of the City",
        "name": "name",
        "keywords": [{
            "keyword": "identifier",
            "weight": 0.01
        }]
    }]
}'
```

Now we have two classes but nowhere we have defined where the point to, this is because you can't create a cross-reference to a nonexisting class.

Let's update both classes to include cross-references.

First, we update the `Zoo` class:

```bash
$ curl -X POST http://localhost:8080/weaviate/v1/schema/things/Zoo/properties -H "Content-Type: application/json" -d '{
  "@dataType": [
    "City"
  ],
  "cardinality": "atMostOne",
  "description": "In which city this zoo is located",
  "name": "inCity",
  "keywords": []
}'
```

Secondly, we update the `City` class:

```bash
$ curl -X POST http://localhost:8080/weaviate/v1/schema/things/City/properties -H "Content-Type: application/json" -d '{
  "@dataType": [
    "Zoo"
  ],
  "cardinality": "atMostOne",
  "description": "Which zoos are in this city?",
  "name": "hasZoo",
  "keywords": []
}'
```

#### Adding Special Types To The Ontology

Besides cross-references, Weaviate has a variety of special data types like geocoding. Click [here](./ontology-schema.md#property-data-types) for an overview of available types.

Let's update the `City` class with a location.

```bash
$ curl -X POST http://localhost:8080/weaviate/v1/schema/things/City/properties -H "Content-Type: application/json" -d '{
  "@dataType": [
    "geoCoordinates"
  ],
  "cardinality": "atMostOne",
  "description": "Where is the city located on the earth?",
  "name": "location",
  "keywords": []
}'
```

## Adding Thing and Action Nodes

Now we have defined the ontology, it is time to start adding data.

We are going to add two cities and two zoos.

First the city of Amsterdam

```bash
$ curl -X POST http://localhost:8080/weaviate/v1/things -H "Content-Type: application/json" -d '{
    "thing": {
        "@class": "City",
        "@context": "http://myzoo.org",
        "schema": {
            "name": "Amsterdam",
            "location": {
                "latitude": 52.22,
                "longitude": 4.54
            }
        }
    }
}'
```

Results in:

```json
{
    "@class": "City",
    "@context": "http://myzoo.org",
    "schema": {
        "location": {
            "latitude": 52.22,
            "longitude": 4.54
        },
        "name": "Amsterdam"
    },
    "creationTimeUnix": 1551613377976,
    "thingId": "6406759e-f6fb-47ba-a537-1a62728d2f55"
}
```

Next, the city of Berlin

```bash
$ curl -X POST http://localhost:8080/weaviate/v1/things -H "Content-Type: application/json" -d '{
    "thing": {
        "@class": "City",
        "@context": "http://myzoo.org",
        "schema": {
            "name": "Berlin",
            "location": {
                "latitude": 52.31,
                "longitude": 13.23
            }
        }
    }
}'
```

which results in:

```json
{
    "@class": "City",
    "@context": "http://myzoo.org",
    "schema": {
        "location": {
            "latitude": 52.31,
            "longitude": 13.23
        },
        "name": "Berlin"
    },
    "creationTimeUnix": 1551613417125,
    "thingId": "f15ba7e7-0635-4009-828b-7a631cd6840e"
}
```

Now, we are going to add the Zoos, note how we are defining the cross-reference.

```bash
$ curl -X POST http://localhost:8080/weaviate/v1/things -H "Content-Type: application/json" -d '{
    "thing": {
        "@class": "Zoo",
        "@context": "http://myzoo.org",
        "schema": {
            "name": "Artis",
            "inCity": {
                "$cref": "weaviate://localhost/things/6406759e-f6fb-47ba-a537-1a62728d2f55"
            }
        }
    }
}'
```

Which results in:

```json
{
    "@class": "Zoo",
    "@context": "http://myzoo.org",
    "schema": {
        "inCity": {
            "$cref": "weaviate://localhost/things/6406759e-f6fb-47ba-a537-1a62728d2f55"
        },
        "name": "Artis"
    },
    "creationTimeUnix": 1551613482193,
    "thingId": "3c6ac167-d7e5-4479-a726-8341b9113e40"
}
```

Note how we are using the UUID of the city Amsterdam (which we created above) to point to the correct thing in the graph.

We will do the same for the Berlin zoo.

```bash
$ curl -X POST http://localhost:8080/weaviate/v1/things -H "Content-Type: application/json" -d '{
    "thing": {
        "@class": "Zoo",
        "@context": "http://myzoo.org",
        "schema": {
            "name": "The Berlin Zoological Garden",
            "inCity": {
                "$cref": "weaviate://localhost/things/f15ba7e7-0635-4009-828b-7a631cd6840e"
            }
        }
    }
}'
```

Which results in:

```json
{
    "@class": "Zoo",
    "@context": "http://myzoo.org",
    "schema": {
        "inCity": {
            "$cref": "weaviate://localhost/things/f15ba7e7-0635-4009-828b-7a631cd6840e"
        },
        "name": "The Berlin Zoological Garden"
    },
    "creationTimeUnix": 1551613546490,
    "thingId": "5322d290-9682-4e7a-ae65-270cefeba8e5"
}
```

We now have created a mini-graph of 4 nodes. Let's now explore the graph!

#### Many References

In some cases, you want to set multiple references. In the following example, we are going to create animals that live in one of the zoos. We are going to extend the zoos to point to multiple animals that live in the zoo.

First, we are going to create the `Animals` class.

```bash
$ curl -X POST http://localhost:8080/weaviate/v1/schema/things -H "Content-Type: application/json" -d '{
    "class": "Animal",
    "keywords": [{
        "keyword": "beast",
        "weight": 0.01
    }],
    "description": "Animals",
    "properties": [{
        "@dataType": [
            "string"
        ],
        "description": "Name of the Animal",
        "name": "name",
        "keywords": [{
            "keyword": "identifier",
            "weight": 0.01
        }]
    },{
        "@dataType": [
            "string"
        ],
        "description": "Species of the Animal",
        "name": "species",
        "keywords": [{
            "keyword": "family",
            "weight": 0.01
        }]
    }]
}'
```

Next, we can extend the `Zoo` class to include many animals. Note the `cardinality` which is set to `many`.

```bash
$ curl -X POST http://localhost:8080/weaviate/v1/schema/things/Zoo/properties -H "Content-Type: application/json" -d '{
  "@dataType": [
    "Animal"
  ],
  "cardinality": "many",
  "description": "Animals that live in this zoo",
  "name": "hasAnimals",
  "keywords": []
}'
```

We will add two `Animals`, the elephants Alphonso and Bert and we are going to store their UUID's

```bash
$ curl -X POST http://localhost:8080/weaviate/v1/things -H "Content-Type: application/json" -d '{
    "thing": {
        "@class": "Animal",
        "@context": "http://myzoo.org",
        "schema": {
            "name": "Alphonso",
            "species": "elephant"
        }
    }
}'
```

```bash
$ curl -X POST http://localhost:8080/weaviate/v1/things -H "Content-Type: application/json" -d '{
    "thing": {
        "@class": "Animal",
        "@context": "http://myzoo.org",
        "schema": {
            "name": "Bert",
            "species": "elephant"
        }
    }
}'
```

The UUID's that are returned are;

- **Alphonso**: `ac19c17b-63df-4d6f-9015-9086bb3466c4`
- **Bert**: `82f91e01-37b4-431c-98d1-43ebb48bca0f`

We can now add Alphonso and Bert to the Amsterdam `Zoo`.

```bash
$ curl -X POST http://localhost:8080/weaviate/v1/things/3c6ac167-d7e5-4479-a726-8341b9113e40/properties/hasAnimals -H "Content-Type: application/json" -d '{
  "$cref": "weaviate://localhost/things/ac19c17b-63df-4d6f-9015-9086bb3466c4", # UUID of Alphonso
  "locationUrl": "localhost",
  "type": "Thing"
}'
```

```bash
$ curl -X POST http://localhost:8080/weaviate/v1/things/3c6ac167-d7e5-4479-a726-8341b9113e40/properties/hasAnimals -H "Content-Type: application/json" -d '{
  "$cref": "weaviate://localhost/things/82f91e01-37b4-431c-98d1-43ebb48bca0f", # UUID of Bert
  "locationUrl": "localhost",
  "type": "Thing"
}'
```

## Crawling the Knowledge Graph with GraphQL

One of the features of Weaviate is the use of GraphQL to query the graph. The docker-compose setup comes with the Weaviate Playground which we will use to crawl the graph.

1. Open the Weaviate Playground by going to [http://localhost](http://localhost).
2. Fill in the URL of your Weaviate instance: `http://localhost:8080/weaviate/v1/graphql`.
3. When the Weaviate Playground is loaded, click "GraphQL" in the right bottom of the screen.

You can learn more about all functionalities [here](index.md).

Getting an overview of all Zoo's would look like this:

```graphql
{
  Local{
    Get{
      Things{
        Zoo{
          name
        }
      }
    }
  }
}
```

We have added the Zoo to a city, get to the name of the City, we have to use the `InCity{}` reference and define that we want to get the results based on a city. But before we can do this, we need to know what the available classes of `InCity{}` are. We can get these insights by clicking `InCity` and inspecting the type (`[ZooInCityObj]`). Because there is only one possible type (`City`) we now know that `InCity{}` ingests the class: `City`.

```graphql
{
  Local {
    Get {
      Things {
        Zoo {
          name
          InCity{
            ... on City{
              name
            }
          }
        }
      }
    }
  }
}
```

Because we have set the `HasZoo` property, we can also reverse the query:

```graphql
{
  Local {
    Get {
      Things {
        City {
          HasZoo {
            ... on Zoo {
              name
            }
          }
        }
      }
    }
  }
}
```

# Querying Individual Nodes with the RESTful API

You can also query individual nodes through the RESTful API.

The following example results in all Things that are added to Weaviate;

```bash
$ curl http://localhost:8080/weaviate/v1/things
```

[Click here](RESTful.md) for a complete overview of RESTful API endpoint.

# Adding a full blown Zoo with Weaviate-CLI

...