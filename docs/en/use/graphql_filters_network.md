# GraphQL - Network Filters

> About GraphQL's Network{} filters.

## Audience: technical

## Purpose: Show how filters can be used in network queries

This page explains by an example how the Network fetch works. The design of the Network fetch can be found [here](https://github.com/SeMI-network/weaviate-graphql-prototype/wiki/Website:-GraphQL-Network).

## Index
- [Network Get](#network-get)
- [Network Fetch](#network-fetch)
- [Network Introspect](#network-introspect)
- [Network GetMeta](#network-getmeta)


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
As explained in the design, there are two different query types possible for querying the Network. `Things` and `Actions` can be 'Fetched' from the network or the ontology of nodes in the network can be 'Introspected'. For fetching Things and Actions, you  need to specify what you are looking for in the filter. You can ask for the beacon and match certainty per node as a result.

For this example, we use the prototype's demo dataset, which you can run using the example dataset in this GraphQL prototype. 
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
        beacon
        certainty
      }
    }
  }
}
```

A `Fuzzy` `Fetch` requires less information in the filter. Only a property value and a certainty value need to be provided. Note that the property value is always a `string`, and needs to exists in the Contextionary.

```graphql
{
  Network {
    Fetch {
      Fuzzy(value:"Amsterdam", certainty:0.95){ // value is always a string, because needs to be in contextionary
        beacon
        certainty
      }
    }
  }
}
```

## Network Introspect
Introspection is about exploring what's available in different ontologies in the network. The `Introspect` query is split into two functions: introspecting `Things` and `Actions` schemas, and introspecting a specific `Beacon`. For the former function you need to use a similar filter as in the `Fetch` function, but without specifying a specific property value for a node. For the latter one you need to enter the beacon id as a filter.

### Network Introspect Things and Actions 
The following query is an example when you want to know if there are any classes with properties in the Network which looks like `City` (or `Place`) with the property `name` (or `identifier`). In this case, it will return class names, certainty values and its properties with the name and the certainty value.

```graphql
{
  Network {
    Introspect {
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
          certainty: 0.8
        }]
      }) {
        className
        certainty
        properties {
          propertyName
          certainty
        }
      }
    }
  }
}
```

### Introspect a beacon
If you found a beacon in for example the Fetch query, you can query its ontology schema. This function will return the class name and the certainty of the class names that match with the beacon, and also for the properties. The returned ontology names will always be in terms of your own defined semantic schema (ontology). 

```graphql
{
  Network {
    Introspect {
      Beacon(id:"weaviate://weaviate_2/8569c0aa-3e8a-4de4-86a7-89d010152ad1") {
        className
        properties {
          propertyName
        }
      }
    }
  }
}
```

## Network GetMeta
Just like querying a local Weaviate, meta data about instances in the Network can be retrieved. 

```graphql
{
  Network {
    GetMeta {
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