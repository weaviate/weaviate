# GraphQL - Query API examples

> An overview of Weaviate GraphQL examples.

## Purpose: Show example GraphQL queries

In this section, some example queries will be clarified.

##### Get local Things
Fetch information about all Things from a specified class.
In this case all airports will be returned, with information about properties references to cities and countries nested in these `Things`. Note that when the property refers to a class (like `InCity` and `InCountry`), these fields start with a capital (although this is not the case in the original schema where all property names start with small letters). These fields are filled with an object mentioning the Class as an object like `... on <Class> {<property>}`.

``` graphql
{
  Local {
    Get {
      Things {
        Airport {
          code
          name
          InCity {
            ... on City {
              name
              population
              coordinates
              isCapital
              InCountry {
                ... on Country {
                  name
                  population
                }
              }
            }
          }
        }
      }
    }
  }
}
```

##### Get local Things with filters
The query below returns information about all airports which lie in a city with a population bigger than 1,000,000, AND which also lies in the Netherlands.

``` graphql
{
  Local {
    Get {
      Things {
        Airport(where: {
          operator: And,
          operands: [{
            path: ["inCity", "City", "inCountry", "Country", "name"],
            operator: Equal,
            valueString: "Netherlands"
          },
          {
            path: ["inCity", "City", "population"],
            operator: GreaterThan
            valueInt: 1000000
          }]
        }) {
          code
          InCity {
            ... on City {
              name
              population
              coordinates
              isCapital
              InCountry {
                ... on Country {
                  name
                  population
                }
              }
            }
          }
        }
      }
    }
  }
}
```

##### Get Things on a local Weaviate instance with arbitratry AND an OR filters
For the filter design, look [here](graphql_filters_local.md).
The query below returns all cities where either (at least one of the following conditions should hold):
- The population is between 1,000,000 and 10,000,000
- The city is a capital

``` graphql
{
  Local {
    Get {
      Things {
        City (where: {
          operator: Or,
          operands: [{
            operator: And,
            operands: [{
              path: ["population"],
              operator: LessThan,
              valueInt: 10000000
            },
            {
              path: ["population"],
              operator: GreaterThanEqual
              valueInt: 1000000
            }],
          }, {
            path: ["isCapital"],
            operator: Equal
            valueBoolean: true
        }]
      }) {
        name
        population
        coordinates
        isCapital
        }
      }
    }
  }
}
```

##### Query generic meta data of Thing or Action classes
Generic meta data about classes and its properties can be queried. This does not count for individial nodes.
The query below returns metadata of the nodes in the class `City`.

``` graphql
{
  Local {
    Meta {
      Things {
        City {
          meta {
            count
          }
          InCountry {
            type
            count
            pointingTo
          }
          isCapital {
            type
            count
            totalTrue
            percentageTrue
            totalFalse
            percentageFalse
          }
          population {
            type
            count
            minimum
            maximum
            mean
            sum
          }
          name {
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

```

The same filters as the converted fetch can be used to filter the data. The following query returns the meta data of all cities of the Netherlands. 

``` graphql
{
  Local {
    Meta {
      Things {
        City(where: {
          path: ["inCountry", "name"],
          operator: Equal
          valueString: "Netherlands"
        }) {
          meta {
            count
          }
          InCountry {
            type
            count
            pointingTo
          }
          isCapital {
            type
            count
            totalTrue
            percentageTrue
            totalFalse
            percentageFalse
          }
          population {
            type
            count
            minimum
            maximum
            mean
            sum
          }
          name {
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
```

##### Pagination
Pagination allows to request a certain amount of `Things` or `Actions` at one query. The argument `limit` can be used in the query for classes of `Things` and `Actions`, where `limit` is an integer with the maximum amount of returned nodes.

``` graphql
{
  Local {
    Get {
      Things {
        City(limit:10) {
          name
          population
          coordinates
          isCapital
        }
      }
    }
  }
}
```
