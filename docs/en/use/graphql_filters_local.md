# GraphQL - Local Filters

> About GraphQL's Local{} filters.

## Purpose: Show how filters can be used in local queries

## Index
- [Local Get and GetMeta](#local-get-and-getmeta)
- [Local Fetch](#local-fetch)
- [Local Aggregate](#local-aggregate)
- [Other parameters](#other-parameters)
  - [Limit](#limit)
  - [OLAP](#OLAP)

## Local Get and GetMeta
Without filters, a local query could look like this:

``` graphql 
{
  Local {
    Get {
      Things {
        City {
          name
          population
          InCountry {
            ... on Country {
              name
            }
          }
        }
      }
    }
  }
}
```

In this query, the result will contain the names and population of all the cities, and which country they are in. If you only want to Get all the cities in the Netherlands with a population higher than 100.000, this can be specified in the `where` filter in the class in the `Get` function:

```graphql
{
  Local {
    Get {
      Things {
        City (where: {
          operator: And,
          operands: [{
            path: ["population"],
            operator: GreaterThan
            valueInt: 1000000
          }, {
            path: ["inCountry", "Country", "name"],
            operator: Equal,
            valueString: "Netherlands"
          }]
        }){
          name
          InCountry {
            ... on Country {
              name
            }
          }
        }
      }
    }
  }
}
```

More generally, the `where` filter is an algrebraic object, which takes the following arguments:
- `Operator`: Takes one of the following values: 
  - `And`
  - `Or`
  - `Equal`
  - `NotEqual`
  - `GreaterThan`
  - `GreaterThanEqual`
  - `LessThan`
  - `LessThanEqual`
- `Operands`: Is a list of filter objects of this same structure
- `Path`: Is a list of strings indicating the property name of the class. If the property is a cross reference, the path of that should be followed to the property of the cross reference should be specified as a list of strings.
- `ValueInt`: The integer value where the `Path`'s last property name should be compared to
- `ValueBoolean`: The boolean value that the `Path`'s last property name should be compared to
- `ValueString`: The string value that the `Path`'s last property name should be compared to
- `ValueNumber`: The number (float) value that the `Path`'s last property name should be compared to
- `ValueDate`: The date (ISO 8601 timestamp) value that the `Path`'s last property name should be compared to

The following 'rules' for using the `where` filter apply:
- If the operator is `And` or `Or`, the `Operands` must be filled.
- If one of the other operators is filled, the `Path` and a `value<Type>` must be filled.

```graphql
{
  Local {
    Get {
      Things {
        <className> (where: {
          operator: <operator>,
          operands: [{
            path: [<path>],
            operator: <operator>
            value<Type>: <value>
          }]
        }) {
          <propertyName>
        }
      }
    }
  }
}
```

Without operator 'And' or 'Or' at the highest level:

```graphql
{
  Local {
    Get {
      Things {
        <className> (where: {
          path: [<path>],
          operator: <operator>
          value<Type>: <value>
          }
        }) {
          <propertyName>
        }
      }
    }
  }
}
```

The same holds for GetMeta queries.

## geoCoordinates
Distance ranges of geoCoordinates can be filtered as follows:
```graphql
{
  Local {
    Get {
      Things {
        City(where: {
          operator: WithinGeoRange,
          valueGeoRange: {geoCoordinates: {latitude: 52.4, longitude: 4.9}, distance: {max:2.0}},
          path: ["geolocation"]
        }) {
          name
          geolocation {
            latitude
            longitude 
          }
        }
      }
    }
  }
}
```
The `distance` is always in kilometers. `geoCoordinates` are in DMS format.
This query will result in all cities within a 2 kilometer range of the geoCoordinates `{latitude: 52.4, longitude: 4.9}`.


## Local Fetch
For [fetching](graphql_local#fetch-function) Things and Actions, you need to specify what you are looking for in the filter. You can ask for the beacon and match certainty per node as a result.

In the first query below, we are looking for cities or places in the local Weaviate with a property value `Amsterdam`. This is specified by defining that you are looking for a `Thing` named `City`, but you also know (for 90%) that this might be called, or is in the same context as, `Place`. Then `Amsterdam` should be a property of this class defined by the property name `name`, or another keyword like `identifier`.
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
  Local {
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


## Local Aggregate
Grouping is associated with aggregation. The GraphQL query function is called `Aggregate`, which returns aggregations of data groups. The data can be grouped by a specific property, which can be specified on the class level in the query. The `minimum`, `maximum`, `median`, `sum`, `mode`, and the `mean` of numeric property values can be queried, as well as the number of specific property values by `count`. The returned data is a list of groups, indicated by `groupedBy` `path` (same as the filter), and the actual `value`. 

### Example
The query below groups all the cities in a local Weaviate on the name of the country, and should return the aggregated data values of the specified functions.

``` graphql
{
  Local {
    Aggregate {
      Things {
        City(groupBy:["inCountry", "Country", "name"]) { 
          population {
            minimum
            maximum
            median
            mean
            sum
            mode
            count
          }
          name { # This property has no numeric values, but 'string' values instead. Only 'count' can be queried for non-numeric propertie
            count
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

## Other parameters

### Limit
The limit filter (pagination) allows to request a certain amount of Things or Actions at one query. The argument `limit` can be combined in the query for classes of Things and Actions, where `limit` is an integer with the maximum amount of returned nodes.

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

### OLAP
OLAP queries take a long time (minutes to hours) to complete, so there is a way to send an OLAP query, let it run in the background, and come back later to get the results. The query result will be stored in cache. 

This applies to `GetMeta` and `Aggregate` queries, where large amount of data may be processed. An example of how this can be used is:

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

For more information about this filter, have a look [here](graphql_parameters.md#OLAP).