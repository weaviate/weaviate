# GraphQL - Parameters

> About the use of parameters in GraphQL 

## Purpose: Show which parameters can be used in GraphQL and what their function is and how to use them.

## Index
- [Where filter](#Where-filter)
- [Limit](#Limit)
- [OLAP](#OLAP)

## Where filter
For filtering data in the query, specific filters are designed. For `Local` queries, look [here](graphql_filters_local.md). For `Network` queries, look [here](graphql_filters_network.md).

## Limit
This is similar to a pagination option, although the difference is that there are no pages with data returned. `limit`, instead, is the maximum number of data instances to be returned. It can be used on all levels that will be resolved in a list. For example:

```graphql
{
  Local{
    Get{
      Things{
        City(limit:10){
          name
        }
      }
    }
  }
}
```

But also:
```graphql
{
  Local{
    GetMeta{
      Things{
        City(limit:10){
          name{
            topOccurrences(limit:3) {
              value
              occurs
            }
          }
        }
      }
    }
  }
}
```


## OLAP
OLAP queries take a long time (minutes to hours) to complete, so there is a way to send an OLAP query, let it run in the background, and come back later to get the results. The query result will be stored in cache. 

This applies to `GetMeta` and `Aggregate` queries, where large amount of data may be processed. Currently, this is offered for `Local` queries. OLAP queries send to the `Network` can be computationally very expensive, and a design for this is not implemented yet.

The following two parameters can be set in the `<class>` level of GraphQL queries, which both allow boolean values:
- `useAnalyticsEngine`
- `forceRecalculate` 
- Default setting is both `forceRecalculate` and `useAnalyticsEngine` to `false`. When these settings are set to other settings in the configuration file of Weaviate, these settings will be adopted. When these parameters are used in direct GraphQL queries, these settings are used rather than the default and configuration settings. Note that the following settings are not a valid combination and will fail in configuration: `(forceRecalculate: true, useAnalyticsEngine: false)`

When you send a specific OLAP query for the first time for the first time, you will get an error message. This error message will let you know that the analytics engine is running to calculate the query results for you. When you run the same query again while the result is not ready yet, you will see another message to let you know the job is still running, under the same hash code. When you run the query again when the job is done, you will see the results. If you send the same query again without `forceRecalculate` set to `true`, the cached result will be retrieved and no recalculation will to be done.

### Example
``` graphql
{
  Local {
    GetMeta {
      Things {
        City(groupBy: ["isCapital"],
          forceRecalculate: false,
          useAnalyticsEngine: true,
        ) {
          population {
            sum
            maximum
            minimum
            mean
            count
          }
          groupedBy {
            value
          }
        }
      }
    }
  }
}
```
