# GraphQL - Network Queries

> About GraphQL's Network{} queries.

## Purpose: Show how to query a Weaviate Network

## Index

- [Query a Weaviate network](#query-a-weaviate-network)
- [Query structure](#query-structure)
- [Get function](#get-function)
- [Fetch function](#fetch-function)
  - [Fetch Things and Actions](#fetch-things-and-actions)
  - [Fuzzy Fetch](#fuzzy-fetch)
- [Introspect function](#introspect-function)
	- [Introspect Things and Actions](#introspect-things-and-actions)
	- [Introspect a beacon](#introspect-a-beacon)
- [GetMeta function](#getmeta-function)
- [Filters](#filters)


## Query a Weaviate network

The networked fetch allows fetching graph data over the network. These fetches will be somewhat fuzzy because of all graphs having unique ontologies. This makes querying a network of Weaviates different from a single local Weaviate instance. In a network query, you want to fetch data or information about data (metadata) that you don't know the semantic schema of. With help of [Natural Language Processing (NLP)](https://en.wikipedia.org/wiki/Natural_language_processing) techniques, querying other ontologies without knowing the actual naming in ontologies is possible. Keywords for names of classes and properties in ontologies enables finding relations with similar classes and properties of other ontologies.


### Beacons

Beacons are contractions of singleRefs into a single string. For example: `weaviate://zoo/8569c0aa-3e8a-4de4-86a7-89d010152ad6`. They are used to pin-point nodes in the networked graph.

## Query structure

The overall query structure of the networked query is:

```graphql
{
  Network {
    Get {
      <weaviate_instance> {
        Things
        Actions
      }
    }
    Fetch {
      Things
      Actions
    }
    Introspection {
      Things
      Actions
      Beacon
    }
  }
}
```

The `Get` function requires the name of the Weaviate instance you are pointing to. For the fields of `Fetch` as well as the `Introspection` query, filter arguments are required. This is because you are 'searching and fetching' for nodes in the network, rather than 'getting' exactly what you want in a local Weaviate.


## Get function
Via the P2P network, it will be possible to request ontologies of the Weaviate instances in a network. This means that Things and Actions can be requested directly from these Weaviate instances, with the terminology and definitions of the original ontology. This function works in a similar way as the Local `Get` function, but requires the name of the `Weaviate` instance as step between `Get` and `Things` or `Actions` in the GraphQL query structure. The classes and properties of the pointed Weaviate instance can then be addressed in the same way as the Local `Get` function. Also, the data filter (`where`) has the same design. The `where` filter should still be put on the level of the `Get` function, but make sure to state the name of the Weaviate instance is mentioned in the path, like `["weaviateB", "Things", "City", "name"]`. Note that a Weaviate name can only start with a letter, but has the following regex rules for the rest of the name: `^[a-zA-Z_][a-zA-Z0-9_.-]*$` An example request could look like this:

```graphql
{
  Network {
    Get {
      weaviateB(where: {
        path: ["Airport", "place"],
        operator: Equal,
        valueString: "Amsterdam"
      }) {
      	Things {
          Airport {
            code
            place
          }
        }
      }
      weaviateC {
      	Things {
          Municipality {
            name
          }
        }
      }
    }
  }
}
```


## Fetch function

The goal of the `Fetch` function is to find a beacon in the network. This function actually fetches data, which is different from exploring ontologies with the `Introspection` query. Returned beacons can be used to get actual data from the network or to search what kind of information is avaiable around this beacon. 
With the `Fetch` function Things and Actions can be fetched using a `where` filter. In addition, a `Fuzzy` fetch can be performed to do look for nodes with less information provided.


### Fetch Things and Actions
Example request:
```graphql
{
  Network {
    Fetch {
      Things(where: {
        class: {
          name: "Animal",
          keywords: [{
            value: "Mammal",
            weight: 0.9
          }],
          certainty: 0.8
        },
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


### Fuzzy Fetch
Next to the above introduced `Things` and `Actions` search where at least the class name, property name and property value should be provided to fetch nodes, a `Fuzzy` `Fetch` is provided which gives the option to to a search only based on a property value. A search could look like this:

```graphql
{
  Network{
    Fetch{
      Fuzzy(value:"Amsterdam", certainty:0.95){ // value is always a string, because needs to be in contextionary
        beacon
        certainty
      }
    }
  }
}
```
Where the property value `Amsterdam` and the minimal certainty of `0.95` are provided in the query. Note that the property value is always a `string`, and needs to exists in the Contextionary. The result will be, like the Fetch for Things and Actions, a list of beacons and corresponding certainty levels. With this information, more information could be requested by using the `Introspect` function.

## Introspect function

The goal of the Introspection query is to discover what is in the network. The query results show what is the likelihood that something is available. The introspection query is designed for fetching data schema infomation based on the own ontology, and for fetching data schema information about beacons. The introspection query is thus not designed for fetching (meta) data of actual data values and nodes in the network.

### Introspect Things and Actions

If you want to introspect `Things` or `Actions` in the network, you need to specify this in the query. This function can be used to discover, based on the local ontology, a clear map of what is available in the network.

```graphql
{
  Network {
    Introspect {
      Things(where: {
        class: {
          name: "Animal",
          keywords: [{
            value: "Mammal",
            weight: 0.9
          }],
          certainty: 0.9
        },
        properties: [{
          name: "name",
          keywords: [{
            value: "identifier",
            weight: 0.9
          }],
          certainty: 0.8
        }]
      }) {
        weaviate
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

Which returns a list of nodes that are 'close' (in contextionary) to the requested information.
Where
- `weaviate` is the name of the weavaite instance the node is found in.
- `className` is a string of the name of the class of the node in the other ontology.
- `certainty` is the certainty (0.0-1.0) that indicates the certainty to which the found class (or property) is similar to the provided information in the filter.
- `properties` is a list of properties of the node that is found.
- `propertyName` is the name of the property in the class in the pointed ontology.

This will return a list of `weaviate` (the name of the Weaviate instance) `classNames`s, `properties` and the `certainty` that they match with the context of the queried `Thing` or `Action`. 


### Introspect a Beacon

If the location (i.e. beacon) of a `Thing` or `Action` in the network is known, it is possible to introspect the context of this node in terms of what the class and properties are.

The following example request returns a list of possible classes and properties.

```graphql
{
  Network {
    Introspect {
      Beacon(id:"weaviate://zoo/8569c0aa-3e8a-4de4-86a7-89d010152ad6") {
        className
        properties {
          propertyName
        }
      }
    }
  }
}
```
 
Where
- `className` is a string of the name of the class in the other ontology.
- `properties` is a list of properties.
- `propertyName` is the name of the property in the class in the pointed ontology.

Note that this query does not resolve in a list, but returns only one class name with a list of its properties.


## GetMeta function
For nodes in the network, meta information can be queried just like for nodes in a local Weaviate. 

``` graphql
{
  Network {
    GetMeta {
      weaviateB {
      	Things {
          Airport(where: {
            path: ["place"],
            operator: Equal,
            valueString: "Amsterdam"
          }) {
            meta {
              count
            }
            place {
              type
              count
              topOccurrences(limit:2) {
                value
                occurs
              }
            }
          }
        }
      }
    }
  }
}
```

## Filters

Different from the `Local` queries is that the filter arguments in the `Network` search are required to get results. 