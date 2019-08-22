> We are updating our docs and they will be moved to [www.semi.technology](https://www.semi.technology) soon.
> You can leave your email [here](http://eepurl.com/gye_bX) to get a notification when they are live.

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
- [Meta function](#getmeta-function)
- [Aggregate function](#aggregate-function)
- [Filters](#filters)


## Query a Weaviate network

The networked fetch allows fetching graph data over the network. These fetches will be somewhat fuzzy because of all graphs having unique ontologies. This makes querying a network of Weaviates different from a single local Weaviate instance. In a network query, you want to fetch data or information about data (metadata) that you don't know the semantic schema of. With help of [Natural Language Processing (NLP)](https://en.wikipedia.org/wiki/Natural_language_processing) techniques, querying other ontologies without knowing the actual naming in ontologies is possible. Keywords for names of classes and properties in ontologies enables finding relations with similar classes and properties of other ontologies.


### Beacons

Beacons are contractions of singleRefs into a single string. For example: `weaviate://zoo/things/<uuid>`. They are used to pin-point nodes in the networked graph.


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
      Fuzzy
    }
    Meta {
      <weaviate_instance> {
        Things
        Actions
      }
    }
    Aggregate {
      <weaviate_instance> {
        Things
        Actions
      }
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

The goal of the `Fetch` function is to find a beacon in the network. This function actually fetches data, which is different from exploring ontologies schemas. Returned beacons can be used to get actual data from the network or to search what kind of information is available around this beacon. 
With the `Fetch` function Things and Actions can be fetched using a `where` filter. In addition, a `Fuzzy` fetch can be performed to do look for nodes when less information is provided.


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
        className
        beacon
        certainty
      }
    }
  }
}
```

With this request, class names and beacons that match the queried information are returned. In this case, we are looking for Animals with the name 'Bella', where we give the additional naming context information of that we mean to find classes that may also match the context of `Mammal`, and that the property `name` may also be called something in the context of `identifier` with 0.8 on the scale of 0.0-1.0 certainty. 
The `certainty` of the classes found will also be returned. This value is calculated by a model combining weighted distances of the queried and found information. 


### Fuzzy Fetch
Next to the above introduced `Things` and `Actions` search where at least the class name, property name and property value should be provided to fetch nodes, a `Fuzzy` `Fetch` is provided which gives the option to to a search only based on a property value. A search could look like this:

```graphql
{
  Network{
    Fetch{
      Fuzzy(value:"Amsterdam", certainty:0.95){ // value is always a string
        className
        beacon
        certainty
      }
    }
  }
}
```

Where the property value `Amsterdam` and the minimal certainty of `0.95` are provided in the query. Note that the property value is always a `string`, one word. To get the best results, the value should be present in the Contextionary. If not, the search still works, but searched through the data with a different approach (in the data directly). Moreover, the strings will be matched with data values on "contains" and ["Levenshtein distance"](https://en.wikipedia.org/wiki/Levenshtein_distance) basis. This implies that classes with a property `NotAmsterdam` will be returned if the query contains `Amsterdam`, and that the class with property `Amsterdam` will be returned if the query contains a typo like `Amstedram`. 
The result will be, like the `Fetch` for `Things` and `Actions`, a list of class names, beacons and corresponding certainty levels. With this information, more information could be requested by using the `Introspect` function. To get the data of the result, the beacon can be used in a REST API Get Things or Actions query.


## Meta function
For nodes in the network, meta information can be queried just like for nodes in a [local Weaviate](graphql_local#getmeta-function). 

``` graphql
{
  Network {
    Meta {
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


## Aggregate function

To aggregate and group results, you can use the `Aggregation` function. 

Data can be grouped by a specific property, which can be specified on the class level in the query. The `minimum`, `maximum`, `median`, `sum`, `mode`, and the `mean` of numeric property values can be queried, as well as the number of specific property values by `count`. The returned data is a list of groups, indicated by `groupedBy` `path` (same as the filter), and the actual `value`. 

### Example
The query below groups all the cities in a local Weaviate on the name of the country, and should return the aggregated data values of the specified functions.

``` graphql
{
  Network{
    Aggregate{
      WeaviateB {
        Things {
          Airline(groupBy: ["label"]) {
            hasNumberOfPlanes {
            minimum
            maximum
            median
            mean
            sum
            mode
            count
          }
          label { # This property has no numeric values, but 'string' values instead. Only 'count' can be queried for non-numeric propertie
            count
          }
            groupedBy { #indicates the groups
              path #the path as shown in the filter, will be ["label"]
              value #the property value of the path's property key of the group
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
