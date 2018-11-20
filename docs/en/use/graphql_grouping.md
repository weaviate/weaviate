---
publishedOnWebsite: false
title: GraphQL Grouping
subject: OSS
---

# Grouping

A grouping function is under development. Current status can be found on this page and in [this](https://gist.github.com/bobvanluijt/a6f812589095f7435e4e8a99a7f8fef6) gist. 

The groupby filter is implemented on class level. Results can then be grouped on property values. The `groupBy` filter requires a property name as string as `group` argument, and at least one of the aggregation functions.

Shortcomings:
- Unique aggregation functions can only be used once in a filter. This could be solved by allowing multiple properties per aggregation function (as a list for example)
- A property can only be aggregated once, because it can only be presented in the results once in the current design. This means it is not possible to display both the minimum and maximum value of the same property. The only solution for this I could think of are directives, as @moretea proposes (here)[https://gist.github.com/bobvanluijt/a6f812589095f7435e4e8a99a7f8fef6#gistcomment-2713015]. After researching directives, I found out that it is not easy or even impossible to implement this in GraphQL, because it is not the purpose of GraphQL to design directives yourself as it is out of GraphQL's scope (because it is confusing for users, see https://github.com/graphql/graphql-js/issues/41). 
- Currently, there cannot be grouped by crefs. This is because it is unclear on which property of the reference there should be grouped on. This could be solved by making the `group` argument a list (like `path` in other filters), where the exact property of crefs is specified. 
- Only `sum`, `mean`, `maximum`, `minimum` and `count` are implemented in the demo resolver.
- Descriptions are not added yet, because the final design is prone to change.

Example of current implementation:
``` graphql
{
  Local{
    Get{
      Things{
        City(groupBy:{group:"isCapital", mean:"population"}){
          population
          isCapital
        }
      }
    }
  }
}
```

would result in:

``` json
{
  "data": {
    "Local": {
      "Get": {
        "Things": {
          "City": [
            {
              "population": 2635000,
              "isCapital": true
            },
            {
              "population": 1200000,
              "isCapital": false
            }
          ]
        }
      }
    }
  }
}
```

While the query below without groupBy filter

```graphql
{
  Local{
    Get{
      Things{
        City{
          population
          isCapital
        }
      }
    }
  }
}
```

results in 

```json 
{
  "data": {
    "Local": {
      "Get": {
        "Things": {
          "City": [
            {
              "population": 1800000,
              "isCapital": true
            },
            {
              "population": 1800000,
              "isCapital": false
            },
            {
              "population": 3470000,
              "isCapital": true
            },
            {
              "population": 600000,
              "isCapital": false
            }
          ]
        }
      }
    }
  }
}
```

Next steps will be to resolve the shortcomings above. This could be by iterating on these first steps or by taking another approach, like [this](https://github.com/prisma/prisma/issues/1312). But IMO this won't do good to the user experience, it looks quite complex to compose the right queries..

...