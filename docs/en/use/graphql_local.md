# GraphQL - Local Queries

> About GraphQL's Local{} queries.

## Purpose: Show how to query a local Weaviate

## Index

- [Query a local Weaviate](#query-a-local-weaviate)
- [Query structure](#query-structure)
- [Get function](#get-function)
- [GetMeta function](#getmeta-function)
- [Aggregation function](#aggregation-function)
- [Filters](#filters)


## Query a local Weaviate

Querying a local Weaviate implies that the client is aware of the data schemas in this Weaviate instance. If you know the data schema or ontology of your local Weaviate, you are able to get specific data and metadata of your instance. 


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

All fields start with a capital. All properties start with a lowercase letter, except for properties which data value is a reference to another class.


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
	- `topOccurrences {
		value
		occurs
	}`
- `Number or Integer`:
	- `minimum`
	- `maximum`
	- `mean`
	- `sum`
- `Boolean`: 
	- `totalTrue`
	- `percentageTrue`
  - `totalFalse`
  - `percentageFalse`
- `Reference`:
	- `pointingTo`

Notes:
- `date`s are for now treated as `String`s
- The components `latitude` and `longitude` of `geoCoordinates` are treated as `Number`

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
To aggregate and group results, you can use the `Aggregation` function. More information of how to use the `Aggregation` function can be found [here](https://github.com/creativesoftwarefdn/weaviate/blob/develop/docs/en/use/graphql_grouping.md).


## Filters

For both functions `Get` and `GetMeta` in the Local Query filtering is possible. In the query introducted in the [Get function](#get-function) section, the result will contain the name and age of all the animals, and in which zoo they are. If you only want to Get all the Animals younger than 5 years old and living in the London Zoo, this can be specified in the `where` filter of the class in the `Get` function:

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