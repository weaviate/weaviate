> We are updating our docs and they will be moved to [www.semi.technology](https://www.semi.technology) soon.
> You can leave your email [here](http://eepurl.com/gye_bX) to get a notification when they are live.

# RESTful API

> An overview of all RESTful API endpoints.

If you want to create, manipulate or inspect individual nodes in Weaviate or if you want to create an ontology, you can use the RESTful API. The getting started manual outlines a few examples and the OpenAPI specs can be found here. For traversing the knowledge graph, it is best to use the [GraphQL interface](graphql_introduction.md).

**Useful Links:**<br>
- <a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/" target="_blank">SwaggerHub publication</a>
- <a href="https://github.com/semi-technologies/weaviate/blob/master/openapi-specs/schema.json" target="_blank">Original OpenAPI specs</a>

## Overview

- [Working with the ontology schema](#ontology-schema)
- [Working with things and actions](##things--actions)
- [Meta and contextionary tools](#meta-and-contextionary-tools)

## Ontology Schema

Before you can add thing and action nodes to Weaviate you need to create an ontology schema first, you can learn more about the ontology and what it does [here](ontology-schema.md).

### Dump the ontology schema

(<a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/0.12.70#/schema/weaviate.schema.dump" target="_blank">Definition on Swaggerhub</a>)

The complete things and actions schema can be dumped as follows:

```bash
$ curl http://localhost:8080/v1/meta
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

### Define a _thing_ in the ontology schema

(<a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/0.12.70#/schema/weaviate.schema.things.create" target="_blank">Definition on Swaggerhub</a>)

You can learn more about what should be part of the POST body [here](ontology-schema.md).

Example request:

```bash
$ curl -X POST http://localhost:8080/v1/schema/things -H "Content-Type: application/json" -d '{
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
        "dataType": [
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

### Update a _thing_'s name and keywords

(<a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/0.12.70#/schema/weaviate.schema.things.update" target="_blank">Definition on Swaggerhub</a>)

Example request:

```bash
$ curl -X PUT http://localhost:8080/v1/schema/things/Zoo -H "Content-Type: application/json" -d '{
  "newName": "Menagerie",
  "keywords": [
    {
      "keyword": "zoo",
      "weight": 1.0
    }
  ]
}'
```

### Update an _thing_ property name and keywords

(<a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/0.12.70#/schema/weaviate.schema.things.properties.update" target="_blank">Definition on Swaggerhub</a>)

Example request:

```bash
$ curl -X PUT http://localhost:8080/v1/schema/actions/Zoo/properties/name -H "Content-Type: application/json" -d '{
  "newName": "identifier",
  "keywords": [
    {
      "keyword": "name",
      "weight": 1.0
    }
  ]
}'
```

### Delete a _thing_ from the ontology schema

(<a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/0.12.70#/schema/weaviate.schema.things.delete" target="_blank">Definition on Swaggerhub</a>)

Example request:

```bash
$ curl -X DELETE http://localhost:8080/v1/schema/things/Zoo
```

### Add a _thing_ property to the ontology schema

(<a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/0.12.70#/schema/weaviate.schema.things.properties.add" target="_blank">Definition on Swaggerhub</a>)

You can learn more about what should be part of the POST body [here](ontology-schema.md#property-datatypes).

Example request:

```bash
$ curl -X PUT http://localhost:8080/v1/schema/things/Zoo/properties -H "Content-Type: application/json" -d '{
  "dataType": [
    "City"
  ],
  "cardinality": "atMostOne",
  "description": "In which city this zoo is located",
  "name": "inCity",
  "keywords": []
}'
```

### Delete a _thing_'s property from the ontology schema

(<a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/0.12.70#/schema/weaviate.schema.things.properties.delete" target="_blank">Definition on Swaggerhub</a>)

Example request:

```bash
$ curl -X DELETE http://localhost:8080/v1/schema/things/Zoo/properties/inCity
```

### Define an _action_ in the ontology schema

(<a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/0.12.70#/schema/weaviate.schema.actions.create" target="_blank">Definition on Swaggerhub</a>)

You can learn more about what should be part of the POST body [here](ontology-schema.md).

Example request:

```bash
$ curl -X POST http://localhost:8080/v1/schema/actions -H "Content-Type: application/json" -d '{
    "class": "BuyAction",
    "keywords": [{
        "keyword": "consume",
        "weight": 0.01
    }],
    "description": "When something is bought in one of the Zoo shops",
    "properties": [{
        "dataType": [
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

### Update an _action_'s name and keywords

(<a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/0.12.70#/schema/weaviate.schema.actions.update" target="_blank">Definition on Swaggerhub</a>)

Example request:

```bash
$ curl -X PUT http://localhost:8080/v1/schema/actions/BuyAction -H "Content-Type: application/json" -d '{
  "newName": "PurchaseAction",
  "keywords": [
    {
      "keyword": "buyAction",
      "weight": 1.0
    }
  ]
}'
```

### Update an _action_ property name and keywords

(<a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/0.12.70#/schema/weaviate.schema.actions.properties.update" target="_blank">Definition on Swaggerhub</a>)

Example request:

```bash
$ curl -X PUT http://localhost:8080/v1/schema/actions/BuyAction/properties/amount -H "Content-Type: application/json" -d '{
  "newName": "quantity",
  "keywords": [
    {
      "keyword": "amount",
      "weight": 1.0
    }
  ]
}'
```

### Delete an _action_ from the ontology schema

(<a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/0.12.70#/schema/weaviate.schema.actions.delete" target="_blank">Definition on Swaggerhub</a>)

Example request:

```bash
$ curl -X DELETE http://localhost:8080/v1/schema/things/BuyAction
```

### Add an _action_ property to the ontology schema

(<a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/0.12.70#/schema/weaviate.schema.actions.properties.add" target="_blank">Definition on Swaggerhub</a>)

You can learn more about what should be part of the POST body [here](ontology-schema.md#property-datatypes).

Example request:

```bash
$ curl -X PUT http://localhost:8080/v1/schema/actions/BuyAction/properties -H "Content-Type: application/json" -d '{
  "dataType": [
    "Shop"
  ],
  "cardinality": "atMostOne",
  "description": "In which shop was this purchase done?",
  "name": "inShop",
  "keywords": []
}'
```

### Delete an _action_'s property from the ontology schema

(<a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/0.12.70#/schema/weaviate.schema.things.properties.delete" target="_blank">Definition on Swaggerhub</a>)

Example request:

```bash
$ curl -X DELETE http://localhost:8080/v1/schema/actions/BuyAction/properties/inShop
```

## Things & Actions

The Weaviate graph contains two root-level concepts: things (noun-based) and actions (verb-based) which can be added to the graph.

### Get a list of _things_

(<a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/0.12.70#/things/weaviate.things.list" target="_blank">Definition on Swaggerhub</a>)

Example request:

```bash
$ curl http://localhost:8080/v1/things
```

### Validate a _thing_

(<a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/0.12.70#/things/weaviate.things.validate" target="_blank">Definition on Swaggerhub</a>)

Can be used to validate if a concept is properly structured.

Example request:

```bash
$ curl http://localhost:8080/v1/things/validate -H "Content-Type: application/json" -d '{
    "class": "Animal",
    "schema": {
        "name": "Bert",
        "species": "elephant"
    }
}'
```

### Create a _thing_

(<a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/0.12.70#/things/weaviate.things.create" target="_blank">Definition on Swaggerhub</a>)

Returns the unique ID of the newly created concept.

Example request:

```bash
$ curl http://localhost:8080/v1/things -H "Content-Type: application/json" -d '{
    "class": "Animal",
    "schema": {
        "name": "Bert",
        "species": "elephant"
    }
}'
```

### Update a _thing_

(<a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/0.12.70#/things/weaviate.things.update" target="_blank">Definition on Swaggerhub</a>)

Example request:

```bash
$ curl http://localhost:8080/v1/things/{uuid} -H "Content-Type: application/json" -d '{
    "class": "Animal",
    "schema": {
        "name": "Charles",
        "species": "monkey"
    }
}'
```

### Update a _thing_ using PATCH semantics

(<a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/0.12.70#/things/weaviate.things.patch" target="_blank">Definition on Swaggerhub</a>)

> Weaviate supports [RFC 6902](https://tools.ietf.org/html/rfc6902) patching ([learn](http://jsonpatch.com/)).

Example where a `beacon` is added to the `inZoo` property:

```bash
$ curl PATCH http://localhost:8080/v1/actions/c354ba34-432e-4e51-97ef-f33e39f39e55 -H "Content-Type: application/json" -d '[
  {
    "op": "add",
    "path": "/schema/inZoo",
    "value": {
      "beacon": "weaviate://localhost/things/52eeba34-f562-4211-96b2-ea24d112b3d1"
    }
  }
]'
```

### Delete a _thing_

(<a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/0.12.70#/things/weaviate.things.delete" target="_blank">Definition on Swaggerhub</a>)

Example request:

```bash
$ curl -X DELETE http://localhost:8080/v1/things/{uuid}
```

### Create a single _thing_ property

(<a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/0.12.70#/things/weaviate.things.properties.create" target="_blank">Definition on Swaggerhub</a>)

Add a single reference to a class-property when cardinality is set to 'hasMany'. You can learn more about Weaviate `beacon` definitions [here](ontology-schema.md#crossref-data-type).

```bash
$ curl -X POST http://localhost:8080/v1/things/{uuid}/properties/{propertyName} -H "Content-Type: application/json" -d '{
    "beacon": "weaviate://localhost/things/82f91e01-37b4-431c-98d1-43ebb48bca0f"
}'
```

### Update a single _thing_ property

(<a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/0.12.70#/things/weaviate.things.properties.update" target="_blank">Definition on Swaggerhub</a>)

Add an array of references to a class-property when cardinality is set to 'hasMany'. You can learn more about Weaviate `beacon` definitions [here](ontology-schema.md#crossref-data-type).

```bash
$ curl -X PUT http://localhost:8080/v1/things/{uuid}/properties/{propertyName} -H "Content-Type: application/json" -d '[
  {
    "beacon": "weaviate://localhost/things/82f91e01-37b4-431c-98d1-43ebb48bca0f"
  }
]'
```

### Delete a single _thing_ property

(<a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/0.12.70#/things/weaviate.things.properties.delete" target="_blank">Definition on Swaggerhub</a>)

```bash
$ curl -X DELETE http://localhost:8080/v1/things/{uuid}/properties/{propertyName} -H "Content-Type: application/json" -d '{
    "beacon": "weaviate://localhost/things/82f91e01-37b4-431c-98d1-43ebb48bca0f"
}'
```

### Get a list of _actions_

(<a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/0.12.70#/actions/weaviate.actions.list" target="_blank">Definition on Swaggerhub</a>)

Example request:

```bash
$ curl http://localhost:8080/v1/actions
```

### Validate a _action_

(<a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/0.12.70#/actions/weaviate.actions.validate" target="_blank">Definition on Swaggerhub</a>)

Can be used to validate if a concept is properly structured.

Example request:

```bash
$ curl http://localhost:8080/v1/actions/validate -H "Content-Type: application/json" -d '{
    "class": "Animal",
    "schema": {
        "name": "Bert",
        "species": "elephant"
    }
}'
```

### Create a _action_

(<a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/0.12.70#/actions/weaviate.actions.create" target="_blank">Definition on Swaggerhub</a>)

Returns the unique ID of the newly created concept.

Example request:

```bash
$ curl http://localhost:8080/v1/actions -H "Content-Type: application/json" -d '{
    "class": "Animal",
    "schema": {
        "name": "Bert",
        "species": "elephant"
    }
}'
```

### Update a _action_

(<a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/0.12.70#/actions/weaviate.actions.update" target="_blank">Definition on Swaggerhub</a>)

Example request:

```bash
$ curl http://localhost:8080/v1/actions/{uuid} -H "Content-Type: application/json" -d '{
    "class": "Animal",
    "schema": {
        "name": "Charles",
        "species": "monkey"
    }
}'
```

### Update a _action_ using PATCH semantics

(<a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/0.12.70#/actions/weaviate.actions.patch" target="_blank">Definition on Swaggerhub</a>)

> Weaviate supports [RFC 6902](https://tools.ietf.org/html/rfc6902) patching ([learn](http://jsonpatch.com/)).

Example where a `beacon` is added to the `inZoo` property:

```bash
$ curl PATCH http://localhost:8080/v1/actions/c354ba34-432e-4e51-97ef-f33e39f39e55 -H "Content-Type: application/json" -d '[
  {
    "op": "add",
    "path": "/schema/inZoo",
    "value": {
      "beacon": "weaviate://localhost/actions/52eeba34-f562-4211-96b2-ea24d112b3d1"
    }
  }
]'
```

### Delete a _action_

(<a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/0.12.70#/actions/weaviate.actions.delete" target="_blank">Definition on Swaggerhub</a>)

Example request:

```bash
$ curl -X DELETE http://localhost:8080/v1/actions/{uuid}
```

### Create a single _action_ property

(<a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/0.12.70#/actions/weaviate.actions.properties.create" target="_blank">Definition on Swaggerhub</a>)

Add a single reference to a class-property when cardinality is set to 'hasMany'. You can learn more about Weaviate `beacon` definitions [here](ontology-schema.md#crossref-data-type).

```bash
$ curl -X POST http://localhost:8080/v1/actions/{uuid}/properties/{propertyName} -H "Content-Type: application/json" -d '{
    "beacon": "weaviate://localhost/actions/82f91e01-37b4-431c-98d1-43ebb48bca0f"
}'
```

### Update a single _action_ property

(<a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/0.12.70#/actions/weaviate.actions.properties.update" target="_blank">Definition on Swaggerhub</a>)

Add an array of references to a class-property when cardinality is set to 'hasMany'. You can learn more about Weaviate `beacon` definitions [here](ontology-schema.md#crossref-data-type).

```bash
$ curl -X PUT http://localhost:8080/v1/actions/{uuid}/properties/{propertyName} -H "Content-Type: application/json" -d '[
  {
    "beacon": "weaviate://localhost/actions/82f91e01-37b4-431c-98d1-43ebb48bca0f"
  }
]'
```

### Delete a single _action_ property

(<a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/0.12.70#/actions/weaviate.actions.properties.delete" target="_blank">Definition on Swaggerhub</a>)

```bash
$ curl -X DELETE http://localhost:8080/v1/actions/{uuid}/properties/{propertyName} -H "Content-Type: application/json" -d '{
    "beacon": "weaviate://localhost/actions/82f91e01-37b4-431c-98d1-43ebb48bca0f"
}'
```

# Meta and Contextionary Tools

Tools to inspect the knowledge graph or to get meta information about the local Weaviate.

### Contextionary Tools

### Get Words from the Contextionary

(<a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/0.12.70#/contextionary-API/weaviate.c11y.words" target="_blank">Definition on Swaggerhub</a>)

Gives information about a set of words like nearest neighbors, relations to other words and vector spaces. Words should be concatenated [CamelCase](ontology-schema.md#camelcase).

_Note: When only a single word is provided, one `individualWords` is returned._

```bash
$ curl http://localhost:8080/v1/c11y/words/monkeyZooBanana -H "Content-Type: application/json"
```

### Meta

(<a href="https://app.swaggerhub.com/apis/bobvanluijt/weaviate/0.12.70#/meta/weaviate.meta.get" target="_blank">Definition on Swaggerhub</a>)

Gives meta information about the server and can be used to provide information to another Weaviate instance that wants to interact with the current instance.

```bash
$ curl http://localhost:8080/v1/meta
```
