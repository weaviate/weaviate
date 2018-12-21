# GraphQL - Grouping

> About Grouping in GraphQL.

## Purpose: Show how grouping can be done using GraphQL queries


# Grouping

The grouping function is under development. The design for Local grouping is implemented in the prototype. Aggregation functions resolve in empty results for now.

Grouping is associated with aggregation. The GraphQL query function is called `Aggregate`, which returns aggregations of data groups. The data can be grouped by a specific property, which can be specified on the class level in the query. The `minimum`, `maximum`, `median`, `sum`, `mode`, and the `mean` of numeric property values can be queried, as well as the number of specific property values by `count`. The returned data is a list of groups, indicated by `groupedBy` `path` (same as the filter), and the actual `value`. 

### Example
The query below groups all the cities in a local Weaviate on the name of the country, and should return the aggregated data values of the specified functions.

``` graphql
{
  Local {
    Aggregate {
      Things {
        City(groupBy:["inCountry", "Country", "name"]) { 
          minimum {
            population
          }
          maximum {
            population
          }
          median {
            population
          }
          sum {
            population
          }
          mode {
            population
          }
          count { #number of names found
            name
          }
          groupedBy { #indicates the groups
            path #the path as shown in the filter, will be ["inCountry", "Country", "name"]
            value #the property value of the path's property key of the group
          }
        }
      }
    }
  }
}
```

The same type of grouping can be done with single Weaviates in the Network:

``` graphql
{
  Network{
    Aggregate{
      WeaviateB {
        Things {
          Airline(groupBy: ["label"]) {
            minimum {
              hasNumberOfPlanes
            }
            maximum {
              hasNumberOfPlanes
            }
            median {
              hasNumberOfPlanes
            }
            sum {
              hasNumberOfPlanes
            }
            mode {
              hasNumberOfPlanes
            }
            count { #number of labels found
              label
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