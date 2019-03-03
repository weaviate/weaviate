# GraphQL - Local Filters

> About GraphQL's Local{} filters.

## Purpose: Show how filters can be used in local queries

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

In this query, the result will contain the names and population of all the cities, and which country they are in. If you only want to Get all the cities in the Netherlands with a population higher than 100,000, this can be specified in the `where` filter in the class in the `Get` function:

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