# GraphQL - Query Structure

> SHORT DESCRIPTION OF THIS DOC

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



