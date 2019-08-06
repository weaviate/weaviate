# GraphQL - Network Filters

> About GraphQL's Network{} filters.

## Audience: technical

## Purpose: Show how filters can be used in network queries

This page explains by an example how the Network fetch works. The design of the Network fetch can be found [here](https://github.com/SeMI-network/weaviate-graphql-prototype/wiki/Website:-GraphQL-Network).

## Index
- [Network Get](#network-get)
- [Network Fetch](#network-fetch)
- [Network Meta](#network-getmeta)
- [Network Aggregate](#network-aggregate)
- [Other parameters](#other-parameters)
  - [Limit](#limit)


## Network Get
The filter for the `Network` `Get` function has the same structure as the `Local` `Get` function. This means that you could use the filter as follows:

```graphql
{
  Network {
    Get {
      WeaviateB {
      	Things {
          Airport(where: {
            path: ["place"],
            operator: Equal
            valueString: "Amsterdam"
          }) {
            place
            label
          }
        }
      }
      WeaviateC {
      	Things {
          Municipality(where: {
            path: ["name"],
            operator: NotEqual,
            valueString: "Rotterdam"
          }) {
            name
          }
          Human {
            birthday
            name
          }
        }
      }
    }
  }
}
```


## Network Fetch
For [fetching](graphql_network#fetch-function) Things and Actions, you need to specify what you are looking for in the filter. You can ask for the beacon and match certainty per node as a result.

In the first query below, we are looking for nodes in the network like the node `Amsterdam`. This is specified by defining that you are looking for a `Thing` named `City`, but you also know (for 90%) that this might be called, or is in the same context as, `Place`. Then `Amsterdam` should be a property of this class defined by the property name `name`, or another keyword like `identifier`.
Beacons and certainty values will then be returned.

```graphql
{
  Network{
    Fetch{
      Things(where: {
        class: {
          name: "City",
          keywords: [{
            value: "Place",
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
          certainty: 0.8,
          operator: Equal,
          valueString: "Amsterdam"
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

A `Fuzzy` `Fetch` requires less information in the filter. Only a property value and a certainty value need to be provided. Note that the property value is always a `string`, containing one word. To get the best results, the value should be present in the Contextionary. If not, the search still works, but searched through the data with a different approach (in the data directly). Moreover, the strings will be matched with data values on "contains" and ["Levenshtein distance"](https://en.wikipedia.org/wiki/Levenshtein_distance) basis. This implies that classes with a property `NotAmsterdam` will be returned if the query contains `Amsterdam`, and that the class with property `Amsterdam` will be returned if the query contains a typo like `Amstedram`. 

```graphql
{
  Network {
    Fetch {
      Fuzzy(value:"Amsterdam", certainty:0.95){ // value is always a string
        className
        beacon
        certainty
      }
    }
  }
}
```


## Network Meta
Just like querying a [local Weaviate](graphql_filters_local#local-get-and-getmeta), meta data about instances in the Network can be retrieved. 

```graphql
{
  Network {
    Meta {
      WeaviateB {
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

## Network Aggregate
Grouping is associated with aggregation. The GraphQL query function is called `Aggregate`, which returns aggregations of data groups. The data can be grouped by a specific property, which can be specified on the class level in the query. The `minimum`, `maximum`, `median`, `sum`, `mode`, and the `mean` of numeric property values can be queried, as well as the number of specific property values by `count`. The returned data is a list of groups, indicated by `groupedBy` `path` (same as the filter), and the actual `value`. 

### Example
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


## Other parameters

### Limit
The limit filter (pagination) allows to request a certain amount of Things or Actions at one query. The argument `limit` can be combined in the query for classes of Things and Actions, where `limit` is an integer with the maximum amount of returned nodes.

``` graphql
{
  Network{
    Get{
      weaviateB{
        Things{
          Animal(limit:5){
            name
          }
        }
      }
    }
  }
}
```
