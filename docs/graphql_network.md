---
publishedOnWebsite: false
title: GraphQL Network
subject: OSS
---

## Index

- [Query a Weaviate network](#query-a-weaviate-network)
- [Query structure](#query-structure)
- [Fetch function](#fetch-function)
- [Introspect function](#introspect-function)
	- [Introspect Things and Actions](#introspect-things-and-actions)
	- [Introspect a beacon](#introspect-a-beacon)
- [Filters](#filters)


## Query a Weaviate network

The networked fetch allows fetching graph data over the network. These fetches will be somewhat fuzzy because of all graphs having unique ontologies. This makes querying a network of Weaviates different from a single local Weaviate instance. In a network query, you want to fetch data or information about data (metadata) that you don't know the semantic schema of. With help of [Natural Language Processing (NLP)](https://en.wikipedia.org/wiki/Natural_language_processing) techniques, querying other ontologies without knowing the actual naming in ontologies is possible. Keywords for names of classes and properties in ontologies enables finding relations with similar classes and properties of other ontologies.


### Beacons

Beacons are contractions of singleRefs into a single string. For example: `weaviate://zoo/8569c0aa-3e8a-4de4-86a7-89d010152ad6`. They are used to pin-point nodes in the networked graph.

## Query structure

The overall query structure of the networked query is:

```graphql
{
  Network{
    Fetch{
      Things
      Actions
    }
    Introspection{
      Things
      Actions
      Beacon
    }
  }
}
```

For the fields of `Fetch` as well as the `Introspection` query, arguments are required. This is because you are 'searching and fetching' for nodes in the network, rather than 'getting' exactly what you want in a local Weaviate.


## Fetch function

The goal of the `Fetch` function is to find a beacon in the network. This function actually fetches data, which is different from exploring ontologies with the `Introspection` query. Returned beacons can be used to get actual data from the network or to search what kind of information is avaiable around this beacon. 

Example request:
```graphql
{
  Network{
    Fetch{
      Things(where: {
        class: [{
          name: "Animal",
          keywords: [{
            value: "Mammal",
            weight: 0.9
          }],
          certainty: 0.8
        }],
        properties: [{
          name: "name",
          keywords: [{
            value: "identifier",
            weight: 0.9
          }],
          certainty: 0.8,
          operator: Equal,
          valueString: "Bella"
        }]
      }) {
        beacon
        certainty
      }
    }
  }
}
```

With this request, beacons that match the queried information are returned. In this case, we are looking for Animals with the name 'Bella', where we give the additional naming context information of that we mean to find classes that may also match the context of `Mammal`, and that the property `name` may also be called something in the context of `identifier` with 0.8 on the scale of 0.0-1.0 certainty.


## Introspect function

The goal of the Introspection query is to discover what is in the network. The query results show what is the likelihood that something is available. The introspection query is designed for fetching data schema infomation based on the own ontology, and for fetching data schema information about beacons. The introspection query is thus not designed for fetching (meta) data of actual data values and nodes in the network.

### Introspect Things and Actions

If you want to introspect `Things` or `Actions` in the network, you need to specify this in the query. This function can be used to discover, based on the local ontology, a clear map of what is available in the network.

```graphql
{
  Network{
    Introspect{
      Things(where: {
        class: [{
          name: "Animal",
          keywords: [{
            value: "Mammal",
            weight: 0.9
          }],
          certainty: 0.9
        }],
        properties: [{
          name: "name",
          keywords: [{
            value: "identifier",
            weight: 0.9
          }],
          certainty: 0.8
        }]
      }){
        className
        certainty
        properties{
          propertyName
          certainty
        }
      }
    }
  }
}
```

This will return a list of `classNames`s, `properties` and the `certainty` that they match with the context of the queried `Thing` or `Action`. 


### Introspect a Beacon

If the location (i.e. beacon) of a `Thing` or `Action` in the network is known, it is possible to introspect the context of this node in terms of what the class and properties might be.

The following example request returns a list of possible classes and properties with the certainty of these class and property names.

```graphql
{
  Network{
    Introspect{
      Beacon(id:"weaviate://zoo/8569c0aa-3e8a-4de4-86a7-89d010152ad6"){
        className
        certainty
        properties{
          propertyName
          certainty
        }
      }
    }
  }
}
```


## Filters

Different from the `Local` queries is that the filter arguments in the `Network` search are required to get results. 