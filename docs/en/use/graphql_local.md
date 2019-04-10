# GraphQL - Local Queries

> About GraphQL's Local{} queries.

## Purpose: Show how to query a local Weaviate

## Index

- [Query a local Weaviate](#query-a-local-weaviate)
- [Query structure](#query-structure)
- [Get function](#get-function)
- [GetMeta function](#getmeta-function)
- [Aggregation function](#aggregation-function)
- [Parameters](#parameters)
  - [Where filter](#where-filter)
  - [Limit](#limit)
  - [OLAP](#OLAP)


## Query a local Weaviate

Querying a local Weaviate implies that the client is aware of the data schemas in this Weaviate instance. If you don't know the data schema of the local Weaviate, this can be queried via an `Introspection` query. See [this page](graphql_introspection.md) for more details. If you know the data schema or ontology of your local Weaviate, you are able to get specific data and metadata of your instance. 


## Query structure

Data for Weaviate is devided into `Things` and `Actions`, this distinction is also made in the query structure. 

```graphql
{
  Local {
    Get {
      Things
      Actions
    }
    GetMeta {
      Things
      Actions
    }
    Aggregate {
      Things
      Actions
    }
  }
}
```

Where `Things` and `Actions` split further down in the following object structure:
```graphql
{
  <Class>{
    <property>
    <PropertyWithCref>{
      ... on <ClassOfWhereCrefGoesTo>{
        <property>
        <property>
      }
    }
  }
}
```

The deepest layers start with a lowercase letter, and all layer that are split down in deeper layers start with a capital. This means that all class fields start with a capital. All properties start with a lowercase letter, except for properties which data value is a reference to another class, which start with a capital.


## Get function

With the Local Get function, information about specific classes can be queried. Without filters, a `Get` query in the local Weaviate could look like the following. The result will contain the name and age of all the animals, and in which zoo they are. 

``` graphql 
{
  Local {
    Get {
      Things {
        Animal {
          name
          age
          InZoo {
            ... on Zoo {
              name
            }
          }
        }
      }
    }
  }
}
```


## GetMeta function

Generic meta data about classes and its properties can be queried. Property meta information consists of general meta information about classes and their properties, like the data `type` of property. In addition, certain statistics about nodes in classes can be retrieved. Examples are the `count` of nodes in the (potentially filtered) dataset. For different type of properties, different statistical meta information can be queried: 
- `String`: 
  - `type`
  - `count`
  - `topOccurrences {
		value
		occurs
	}`
- `Number or Integer`:
  - `type`
  - `count`
  - `minimum`
  - `maximum`
  - `mean`
  - `sum`
- `Boolean`: 
  - `type`
  - `count`
  - `totalTrue`
  - `percentageTrue`
  - `totalFalse`
  - `percentageFalse`
- `Reference`:
  - `type`
  - `count`
	- `pointingTo`

Notes:
- `date`s are for now treated as `String`s
- The components `latitude` and `longitude` of `geoCoordinates` are treated as `Number`.

Additionally, the number of nodes in a (potentially filtered) class is avaiable. This can be queried by `meta { count }`, at the same level as the properties of the class. 

The query below returns metadata of the nodes in the class `Animal`.  

``` graphql 
{
  Local {
    GetMeta {
      Things {
        Animal {
          meta {
            count
          }
          name {
            type
            count
            topOccurrences {
              value
              occurs
            }
          }
          age {
            type
            count
            mimimum
            maximum
            mean
            sum
          }
          InZoo {
            type
            count
            pointingTo
          }
          hasFeathers {
            type
            count
            totalTrue
            percentageTrue
            totalFalse
            percentageFalse
          }
        }
      }
    }
  }
}
```

## Aggregation function
To aggregate and group results, you can use the `Aggregation` function. 

data can be grouped by a specific property, which can be specified on the class level in the query. The `minimum`, `maximum`, `median`, `sum`, `mode`, and the `mean` of numeric property values can be queried, as well as the number of specific property values by `count`. The returned data is a list of groups, indicated by `groupedBy` `path` (same as the filter), and the actual `value`. 

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

## Filters

## Where filter
For both functions `Get` and `GetMeta` in the Local Query filtering is possible. In the query introducted in the [Get function](#get-function) section, the result will contain the name and age of all the animals, and in which zoo they are. If you only want to `Get` all the Animals younger than 5 years old and living in the London Zoo, this can be specified in the `where` filter of the class in the `Get` function:

```graphql
{
  Local {
    Get{
      Things {
        Animal(where: {
          operator: And,
          operands: [{
            path: ["age"],
            operator: LessThan
            valueInt: 5
          }, {
            path: ["inZoo", "Zoo", "name"],
            operator: Equal,
            valueString: "London Zoo"
          }]
        }) {
          name
          age
          InZoo {
            ... on Zoo {
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
