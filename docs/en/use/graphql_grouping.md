---
publishedOnWebsite: false
title: GraphQL Grouping
subject: OSS
author: Laura Ham
date first published: 01Dec2018
last updated: 05Dec2018
---

## Audience: technical

## Purpose: Show how grouping can be done using GraphQL queries


# Grouping

The grouping function is under development. The design for Local grouping is implemented in the prototype. Aggregation functions resolve in empty results for now.

Grouping is associated with aggregation. The GraphQL query function is called `Aggregate`, which returns aggregations of data groups. The data can be grouped by a specific property, which can be specified on the class level in the query. The `minimum`, `maximum`, `median`, `sum`, `mode`, and the `mean` of numeric property values can be queried, as well as the number of specific property values by `count`. The returned data is a list of groups, indicated by `groupedBy` `path` (same as the filter), and the actual `value`. 

### Example
The query below groups all the cities in the dataset on the name of the country, and should return the aggregated data values of the specified functions.

``` graphql
{
  Local{
    Aggregate{
      Things{
        City(groupBy:["country", "Country", "name"]){ #has to be path/list because of possible crefs
          minimum{
            population
          }
          maximum{
            population
          }
          median{
            population
          }
          sum{
            population
          }
          mode{
            population
          }
          count{ #number of names found
            name
          }
          groupedBy{ #indicates the groups
            path #the path as shown in the filter
            value #the property value of the path's property key of the group
          }
        }
      }
    }
  }
}
```