# GraphQL - Introduction

> An introduction to GraphQL in Weaviate.

## Purpose: Introduction to the use of GraphQL

## Index

- [What is GraphQL](#what-is-graphql)
- [How to use GraphQL](#how-to-use-graphql)
- [How to write queries](#how-to-write-queries)
  - [Query structure and naming](#query-structure-and-naming)
  - [Arguments](#arguments)
  - [Pagination](#pagination)

## What is GraphQL

GraphQL is a query language which allows clients to ask and get exactly what they need. GraphQL is not meant to be a database language, but can be used for many databases by different database languages and connectors. Instead, GraphQL gives you as a user nothing more and nothing less than what you need. This allows the user to control the data, which makes developers' life easy and ensures apps on top of GraphQL to be fast and stable. You can learn more about what GraphQL is [here](https://graphql.org/).

Because GraphQL APIs are organised in schemas with types and fields rather than endpoints, it makes is very suitable for querying data in a knowledge network based on schemas, just like Weaviate.


## How to use GraphQL

To query Weaviate, the most easy way is to use GraphQL. The endpoint for GraphQL is always the same:

```bash
$ curl -X POST -H "X-API-KEY: [[apiKey]]" -H "X-API-TOKEN: [[apiToken]]" -H "Content-Type: application/json" --data '[[DATA]]' "https://weaviate-host/weaviate/v1/graphql"
```

## How to write queries

The body of the request should contain the GraphQL query. The body of the following example will return the name of all animals in your local Weaviate instance.
```graphql
{
  Local {
    Get {
      Things {
        Animal {
          name
        }
      }
    }
  }
}
```

The result will result in the following, which has the same shape as the query. 
```graphql
{
  "data": {
    "Local": {
      "Get": {
        "Things": {
          "Animal": [{
            "name": "Bella"
          }, {
            "name": "Charlie"
          }, {
            "name": "Max"
          }]
        }
      }
    }
  }
}
```


### Query structure and naming

The following high level structure for GraphQL for querying Weaviate applies. At the highest level, the location where the request is sent is specified by `Local` or `Network`. The Local network is split up into a Get and GetMeta function. Similarly, the Network query has the queries Fetch and FetchMeta. Where `Get` implies that you retrieve something you have the sole possession of, the Network's `Fetch` means that an entity needs to go and get something which is remote. Additionally, the Network query has an `Introspect` option, which can be used to discover what is (ontology wise) available in the Network.

``` graphql
{
  Local {
    Get
    GetMeta
    Aggregate
  }
  Network {
    Get
    Fetch
    Introspect
    GetMeta
  }
}
```

If we look deeper, the following structure applies to the Local design.
``` graphql
{
  Local {
    Get {
      Things {
        <Class> {
          <property>
          <property>
        }
        <Class> {
          <property>
        }
      }
      Actions {
        <Class> {
          <property>
        }
      }
    }
  }
}
```
Properties can be nested if a property contains a cross-reference. The following syntax then applies:

``` graphql
{
  Local {
    Get {
      Things {
        <Class> {
          <propertyWithCref> {
            ... on <Class> {
             <property>
             <property>
            }
          }
        }
      }
    }
  }
}
```

Note that the second layer of the query is always a verb. The queries can be formed into natural language sentences if the structure is read. For example, "From my `Local` Weaviate, I want to `Get` all the `name`s of the `Things` in the class `Animal`".


### Arguments

Arguments can be passed to query fields and nested objects, which makes GraphQL a replacement for doing multiple API fetches. Arguments will be handled and data transformations will be implemented server-side, instead of on every separate client. 

How to filter on Things and Actions in Local and Network queries is explained at the [Local](#) and [Network](#) pages.


### Pagination

Pagination allows to request a certain amount of Things or Actions at one query. The argument `limit` can be combined in the query for classes of Things and Actions, where `limit` is an integer with the maximum amount of returned nodes.

``` graphql
{
  Local{
    Get{
      Things{
        Animal(limit:5){
          name
        }
      }
    }
  }
}
```

# GraphQL - Query Structure

> An overview of how Weaviate uses Graphql.

## Audience: technical

## Purpose: Show GraphQL Query structure

The queries in GraphQL for Weaviate are designed to make it easy for users to query the graph. The following high level structure applies. At the highest level, the location where the request is sent is specified by `Local` or `Network`. The Local network is split up into a `Get` and `GetMeta` function. Similarly, the `Network` query has the queries `Get` and `GetMeta`, but additionally has `Fetch`. Where `Get` implies that you retrieve something you have the sole possession of, the `Network`'s `Fetch` means that an entity needs to go and get something which is remote. Additionally, the `Network` query has an `Introspect` option, which can be used to discover what is (ontology wise) available in the `Network`.

_Concrete examples are explained on [this page](https://github.com/creativesoftwarefdn/weaviate/blob/develop/docs/en/use/graphql_query-API.md)._


``` graphql
{
  Local {
    Get
    GetMeta 
    Aggregate
  }
  Network {
    Get
    Fetch
    Introspect
    GetMeta
  }
}
```

If we look deeper, the following structure applies to the Local design.
``` graphql
{
  Local {
    Get {
      Things {
        <Class>{
          <property>
          <property>
        }
        <Class> {
          <property>
        }
      }
      Actions {
        <Class>{
          <property>
          <property>
        }
        <Class> {
          <property>
        }
      }
    }
  }
}
```

Properties can be nested if a property contains a cross-reference. The following syntax then applies:

``` graphql
{
  Local {
    Get {
      Things{
        <Class> {
          <propertyWithCref> {
            ... on <ClassOfWhereCrefGoesTo> {
             <propertyOfClass>
             <propertyOfClass>
            }
          }
        }
      }
    }
  }
}
```

Note that the second layer of the query is always a verb. The queries can be formed into natural language sentences if the structure is read. For example, "From my `Local` Weaviate, I want to `Get` all the `name`s of the `Things` in the class `City`".

``` graphql
{
  Local {
    Get {
      Things {
        City {
          name
        }
      }
    }
  }
}
```



