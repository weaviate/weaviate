# Getting Started

> Getting started with Weaviate's Docker-compose setup.

## Index

- [Preparation](#preparation)
- [Running Weaviate with Docker-compose](#running-weaviate-with-docker-compose)
- [Creating an Ontology](#creating-an-ontology)
- [Adding Thing and Action Nodes](#adding-thing-and-action-nodes)
- [Crawling the Knowledge Graph with GraphQL](#crawling-the-knowledge-graph-with-graphql)

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
1. The whole setup uses a decent amount of memory. In case of issues, try increasing the memory limit that Docker has available. The setup runs from 2 CPU 9Gig. In case of issues check [this issue](https://github.com/creativesoftwarefdn/weaviate/issues/742).
2. It takes some time to start up the whole infrastructure. It is best to test the end-point that comes available.

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

In this example, we are going to create a [knowledge graph of a zoo](https://github.com/creativesoftwarefdn/weaviate-demo-zoo). Although you can [automate the import of an ontology](#using-the-weaviate-cli) we will do it manually now to get an understanding of how the ontology is structured.

First, we will create the `Zoo` as a Weaviate-thing like this;

```bash
$ curl -X POST http://35.204.8.121:8080/weaviate/v1/schema/things -H "Content-Type: application/json" -d '{
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
$ curl -X POST http://35.204.8.121:8080/weaviate/v1/schema/things -H "Content-Type: application/json" -d '{
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

First, we update the Zoo class:

```bash
$ curl -X POST http://35.204.8.121:8080/weaviate/v1/schema/things/Zoo/properties -H "Content-Type: application/json" -d '{
  "@dataType": [
    "City"
  ],
  "cardinality": "atMostOne",
  "description": "In which city this zoo is located",
  "name": "inCity",
  "keywords": []
}'
```

Secondly, we update the City class:

```bash
$ curl -X POST http://35.204.8.121:8080/weaviate/v1/schema/things/City/properties -H "Content-Type: application/json" -d '{
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

Besides cross-references, Weaviate has a variety of special data types like geocoding. Using such a type has advantages when querying the knowledge graph.

Let's update the `City` class with a location.

```bash
$ curl -X POST http://35.204.8.121:8080/weaviate/v1/schema/things/City/properties -H "Content-Type: application/json" -d '{
  "@dataType": [
    "geoCoordinates"
  ],
  "cardinality": "atMostOne",
  "description": "Where is the city located on the earth?",
  "name": "location",
  "keywords": []
}'
```

## Adding Data

Now we have defined the ontology, it is time to start adding data.

We are going to add two cities and two zoos.

First the city of Amsterdam

```bash
$ curl -X POST http://35.204.8.121:8080/weaviate/v1/things -H "Content-Type: application/json" -d '{
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
        "name": "Amsterdam"
    },
    "creationTimeUnix": 1551471528306,
    "thingId": "c1b714dc-8639-4d76-8e6d-eb44132acaf8"
}
```

Next, the city of Berlin

```bash
$ curl -X POST http://35.204.8.121:8080/weaviate/v1/things -H "Content-Type: application/json" -d '{
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

which results in:

```json
{
    "@class": "City",
    "@context": "http://myzoo.org",
    "schema": {
        "name": "Berlin"
    },
    "creationTimeUnix": 1551471528306,
    "thingId": "f3cf2411-0256-468c-a5a6-2cd7196d6a83"
}
```

Now, we are going to add the Zoos, note how we are defining the cross-reference.

```bash
$ curl -X POST http://35.204.8.121:8080/weaviate/v1/things -H "Content-Type: application/json" -d '{
    "thing": {
        "@class": "Zoo",
        "@context": "http://myzoo.org",
        "schema": {
            "name": "Artis",
            "inCity": {
                "$cref": "weaviate://localhost/things/c1b714dc-8639-4d76-8e6d-eb44132acaf8"
            }
        }
    }
}'

Note how we are using the UUID of the city Amsterdam (which we created above) to point to the correct thing in the graph.

We will do the same for the Berlin zoo.

```bash
$ curl -X POST http://35.204.8.121:8080/weaviate/v1/things -H "Content-Type: application/json" -d '{
    "thing": {
        "@class": "Zoo",
        "@context": "http://myzoo.org",
        "schema": {
            "name": "The Berlin Zoological Garden",
            "inCity": {
                "$cref": "weaviate://localhost/things/f3cf2411-0256-468c-a5a6-2cd7196d6a83"
            }
        }
    }
}'
```

We now have created a mini-graph of 4 nodes. Let's now explore the graph!

## Crawling the Knowledge Graph with GraphQL

One of the features of Weaviate is the use of GraphQL to query the graph. The docker-compose setup comes with the Weaviate Playground which we will use to crawl the graph.

1. Open the Weaviate Playground by going to [http://localhost](http://localhost).
2. Fill in the URL of your Weaviate instance: `http://localhost:8080/weaviate/v1/graphql`.
3. When the Weaviate Playground is loaded, click "GraphQL" in the right bottom of the screen.

...