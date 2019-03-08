# RESTful API

> An overview of all RESTful API endpoints.

If you want to create, manipulate or inspect individual nodes in Weaviate or if you want to create an ontology, you can use the RESTful API. The getting started manual outlines a few examples and the OpenAPI specs can be found here. For traversing the knowledge graph, it is best to use the [GraphQL interface](graphql_introduction.md).

**Useful Links:**<br>
- [SwaggerHub publication](https://app.swaggerhub.com/apis/bobvanluijt/weaviate/)
- [Original OpenAPI specs](https://github.com/creativesoftwarefdn/weaviate/blob/master/openapi-specs/schema.json)

## Overview

- [Working with the ontology schema](#ontology-schema).
- Working with things and actions.
- Meta and knowledge tools.
- Batching.
- PATCH semantics.

## Ontology Schema

Before you can add thing and action nodes to Weaviate you need to create an ontology schema first, you can learn more about the ontology and what it does [here](ontology-schema.md).

#### Dump the ontology schema

(<a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/0.12.65#/schema/weaviate.schema.dump" target="_blank">Definition on Swaggerhub</a>)

The complete things and actions schema can be dumped as follows:

```bash
$ curl http://localhost:8080/weaviate/v1/meta
```

Example result:

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

#### Define a _thing_ in the ontology schema

(<a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/0.12.65#/schema/weaviate.schema.things.create" target="_blank">Definition on Swaggerhub</a>)

You can learn more about what should be part of the POST body [here](ontology-schema.md).

Example request:

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

#### Update a _thing_'s keywords

(<a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/0.12.65#/schema/weaviate.schema.things.update" target="_blank">Definition on Swaggerhub</a>)

Example request:

```bash
$ curl -X PUT http://localhost:8080/weaviate/v1/schema/things/Zoo -H "Content-Type: application/json" -d '{
  "newName": "Menagerie",
  "keywords": [
    {
      "keyword": "zoo",
      "weight": 1.0
    }
  ]
}'
```

#### Delete a _thing_ from the ontology schema

(<a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/0.12.65#/schema/weaviate.schema.things.delete" target="_blank">Definition on Swaggerhub</a>)

Example request:

```bash
$ curl -X DELETE http://localhost:8080/weaviate/v1/schema/things/Zoo
```

#### Add a _thing_ property to the ontology schema

(<a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/0.12.65#/schema/weaviate.schema.things.properties.add" target="_blank">Definition on Swaggerhub</a>)

You can learn more about what should be part of the POST body [here](ontology-schema.md#property-datatypes).

Example request:

```bash
$ curl -X PUT http://localhost:8080/weaviate/v1/schema/things/Zoo/properties -H "Content-Type: application/json" -d '{
  "@dataType": [
    "City"
  ],
  "cardinality": "atMostOne",
  "description": "In which city this zoo is located",
  "name": "inCity",
  "keywords": []
}'
```

#### Delete a _thing_'s property from the ontology schema

(<a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/0.12.65#/schema/weaviate.schema.things.properties.delete" target="_blank">Definition on Swaggerhub</a>)

Example request:

```bash
$ curl -X DELETE http://localhost:8080/weaviate/v1/schema/things/Zoo/properties/inCity
```

### Define an _action_ in the ontology schema

(<a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/0.12.65#/schema/weaviate.schema.actions.create" target="_blank">Definition on Swaggerhub</a>)

You can learn more about what should be part of the POST body [here](ontology-schema.md).

Example request:

```bash
$ curl -X POST http://localhost:8080/weaviate/v1/schema/actions -H "Content-Type: application/json" -d '{
    "class": "BuyAction",
    "keywords": [{
        "keyword": "consume",
        "weight": 0.01
    }],
    "description": "When something is bought in one of the Zoo shops",
    "properties": [{
        "@dataType": [
            "number"
        ],
        "description": "Which amount is spend?",
        "name": "amount",
        "keywords": [{
            "keyword": "money",
            "weight": 0.01
        }]
    }]
}'
```

#### Update an _action_'s keywords

(<a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/0.12.65#/schema/weaviate.schema.actions.update" target="_blank">Definition on Swaggerhub</a>)

Example request:

```bash
$ curl -X PUT http://localhost:8080/weaviate/v1/schema/actions/BuyAction -H "Content-Type: application/json" -d '{
  "newName": "PurchaseAction",
  "keywords": [
    {
      "keyword": "buyAction",
      "weight": 1.0
    }
  ]
}'
```

#### Delete an _action_ from the ontology schema

(<a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/0.12.65#/schema/weaviate.schema.actions.delete" target="_blank">Definition on Swaggerhub</a>)

Example request:

```bash
$ curl -X DELETE http://localhost:8080/weaviate/v1/schema/things/BuyAction
```

#### Add an _action_ property to the ontology schema

(<a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/0.12.65#/schema/weaviate.schema.actions.properties.add" target="_blank">Definition on Swaggerhub</a>)

You can learn more about what should be part of the POST body [here](ontology-schema.md#property-datatypes).

Example request:

```bash
$ curl -X PUT http://localhost:8080/weaviate/v1/schema/actions/BuyAction/properties -H "Content-Type: application/json" -d '{
  "@dataType": [
    "Shop"
  ],
  "cardinality": "atMostOne",
  "description": "In which shop was this purchase done?",
  "name": "inShop",
  "keywords": []
}'
```

#### Delete an _action_'s property from the ontology schema

(<a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/0.12.65#/schema/weaviate.schema.things.properties.delete" target="_blank">Definition on Swaggerhub</a>)

Example request:

```bash
$ curl -X DELETE http://localhost:8080/weaviate/v1/schema/actions/BuyAction/properties/inShop
```