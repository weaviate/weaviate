---
publishedOnWebsite: false
title: GraphQL Local
subject: OSS
---

## Index

- [Query a local Weaviate](#query-a-local-weaviate)
- [Query structure](#query-structure)
- [Get function](#get-function)
- [GetMeta function](#getmeta-function)
- [Filters](#filters)


## Query a local Weaviate

Querying a local Weaviate implies that the client is aware of the data schemas in this Weaviate instance. If you know the data schema or ontology of your local Weaviate, you are able to get specific data and metadata of your instance. 


## Query structure

Data for Weaviate is devided into Things and Actions, this distinction is also made in the query structure. 

```graphql
{
  Local{
    Get{
      Things
      Actions
    }
    GetMeta{
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

Generic meta data about classes and its properties can be queried. Property meta information consists of general meta information about classes and their properties, like the data `type` of property. In addition, certain statistics about nodes in classes can be retrieved. Examples are the `count` of nodes in the (potentially filtered) dataset. For different type of properties, different statistical meta information can be queried (`Date`s are for now treated as `String`s): 
- `String`: 
	- `topOccurrences {
		value
		occurs
	}`
- `Number or Integer`:
	- `lowest`
	- `highest`
	- `average`
	- `sum`
- `Boolean`: 
	- `totalTrue`
	- `percentageTrue`
- Reference:
	- `pointingTo`

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
            lowest
            highest
            average
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
          }
        }
      }
    }
  }
}
```


## Filters

For both functions `Get` and `GetMeta` in the Local Query filtering is possible. In the query introducted in the [Get function](#get-function) section, the result will contain the name and age of all the animals, and in which zoo they are. If you only want to Get all the Animals younger than 5 years old and living in the London Zoo, this can be specified in the `where` filter of the `Get` function:

```graphql
{
  Local {
    Get(where:{
      operator: And,
      operands: [{
        path: ["Things", "Animal", "age"],
        operator: LessThan
        valueInt: 5
      }, {
        path: ["Things", "Animal", "inZoo", "Zoo", "name"],
        operator: Equal,
        valueString: "London Zoo"
      }]
    }) {
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

More generally, the `where` filter is an algrebraic designed object, which takes the following arguments:
- `Operator`: Takes one of the following values: 
  - `And`
  - `Or`
  - `Equal`
  - `Not`
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
- If the operator is `And` or `Or`, the `Operands` must be filled.
- If the operator is `Not` or `NotEqual` either the `Operands` or the `Path` and a ValueType must be filled.
- If one of the other operators is filled, the `Path` and a ValueType must be filled.

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