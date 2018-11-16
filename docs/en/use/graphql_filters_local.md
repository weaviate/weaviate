---
publishedOnWebsite: true
title: Filter design pattern for Local queries
subject: OSS
---

Without filters, a query in the local Weaviate could look like this:

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

In this query, the result will contain the names and population of all the cities, and in which country they are. If you only want to Get all the Cities in the Netherlands with a population higher than 100,000, this can be specified in the `where` filter of the `Get` function:

```graphql
{
  Local {
    Get(where:{
      operator: And,
      operands: [{
        path: ["Things", "City", "population"],
        operator: GreaterThan
        valueInt: 1000000
      }, {
        path: ["Things", "City", "inCountry", "Country", "name"],
        operator:Equal,
        valueString: "Netherlands"
      }]
    }) {
      Things {
        City {
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

More generally, the `where` filter is an algrebraic designed object, which takes the following arguments:
- `Operator`: Takes one of the following values: 
  - `And`
  - `Or`
  - `Not`
  - `Equal`
  - `NotEqual`
  - `GreaterThan`
  - `GreaterThanEqual`
  - `LessThan`
  - `LessThanEqual`
- `Operands`: Is a list of filter objects of this same structure
- `Path`: Is a list of Strings indicating the path from 'Things' or 'Actions' to the specific property name
- `ValueInt`: The integer value where the Path's last property name should be compared to
- `ValueBoolean`: The boolean value where the Path's last property name should be compared to
- `ValueString`: The string value where the Path's last property name should be compared to
- `ValueNumber`: The number (float) value where the Path's last property name should be compared to
- `ValueDate`: The date (ISO 8601 timestamp) value where the path's last property name should be compared to

The following 'rules' for using the 'where' filter apply:
- If the operator is `And`, `Or` or `Not`, the `Operands` must be filled.
- If one of the other operators is filled, the `Path` and a ValueType must be filled.

So, the `Not` operator only works on operands, while `NotEqual` only works on values.

```graphql
{
  Local{
    Get(where:{
      operator: <operator>,
      operands: [{
        path: [<path>],
        operator: <operator>
        value<Type>: <value>
      }]
    })
  }
}
```

Without operator 'And' or 'Or' at the highest level:

```graphql
{
  Local{
    Get(where:{
      path: [<path>],
      operator: <operator>
      value<Type>: <value>
      }
    })
  }
}
```