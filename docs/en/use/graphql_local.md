> We are updating our docs and they will be moved to [www.semi.technology](https://www.semi.technology) soon.
> You can leave your email [here](http://eepurl.com/gye_bX) to get a notification when they are live.

# GraphQL - Local Queries

> About GraphQL's Local{} queries.

## Purpose: Show how to query a local Weaviate

## Index

- [Query a local Weaviate](#query-a-local-weaviate)
- [Query structure](#query-structure)
- [Get function](#get-function)
- [Meta function](#getmeta-function)
- [Fetch function](#fetch-function)
  - [Fetch Things and Actions](#fetch-things-and-actions)
  - [Fuzzy Fetch](#fuzzy-fetch)
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
    Meta {
      Things
      Actions
    }
    Fetch {
      Things
      Actions
      Fuzzy
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


## Meta function

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
    Meta {
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

## Fetch function

The goal of the `Fetch` function is to find a class instance in the Weaviate with a class- or property name of which you are not sure. This function actually fetches data, which is different from exploring ontologies schemas. Returned beacons can be used to get actual data from the network or to search what kind of information is available around this beacon. Beacons have the following format in the Local network: `weaviate://localhost/things/<uuid>` or `weaviate://localhost/actions/<uuid>`. 
With the `Fetch` function Things and Actions can be fetched using a `where` filter. In addition, a `Fuzzy` fetch can be performed to do look for nodes when less information is provided.


### Fetch Things and Actions
Example request:
```graphql
{
  Local {
    Fetch {
      Things(where: {
        class: {
          name: "Animal",
          keywords: [{
            value: "Mammal",
            weight: 0.9
          }],
          certainty: 0.8
        },
        properties: [{
          name: "name",
          keywords: [{
            value: "identifier",
            weight: 0.9
          }],
          certainty: 0.8,
          operator: Equal,
          valueString: "Bella"
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

With this request, class names and beacons that match the queried information are returned. In this case, we are looking for Animals with the name 'Bella', where we give the additional naming context information of that we mean to find classes that may also match the context of `Mammal`, and that the property `name` may also be called something in the context of `identifier` with 0.8 on the scale of 0.0-1.0 certainty. 
The `certainty` of the classes found will also be returned. This value is calculated by a model combining weighted distances of the queried and found information. 


### Fuzzy Fetch
Next to the above introduced `Things` and `Actions` search where at least the class name, property name and property value should be provided to fetch nodes, a `Fuzzy` `Fetch` is provided which gives the option to to a search only based on a property value. A search could look like this:

```graphql
{
  Local{
    Fetch{
      Fuzzy(value:"Amsterdam", certainty:0.95){ // value is always a string
        className
        beacon
        certainty
      }
    }
  }
}
```

Where the property value `Amsterdam` and the minimal certainty of `0.95` are provided in the query. Note that the property value is always a `string`, one word. To get the best results, the value should be present in the Contextionary. If not, the search still works, but searched through the data with a different approach (in the data directly). Moreover, the strings will be matched with data values on "contains" and ["Levenshtein distance"](https://en.wikipedia.org/wiki/Levenshtein_distance) basis. This implies that classes with a property `NotAmsterdam` will be returned if the query contains `Amsterdam`, and that the class with property `Amsterdam` will be returned if the query contains a typo like `Amstedram`. 
The result will be, like the `Fetch` for `Things` and `Actions`, a list of class names, beacons and corresponding certainty levels. With this information, more information could be requested by using the `Introspect` function. To get the data of the result, the beacon can be used in a REST API Get Things or Actions query.


## Aggregation function
To aggregate and group results, you can use the `Aggregation` function. 

Data can be grouped by a specific property, which can be specified on the class level in the query. The `minimum`, `maximum`, `median`, `sum`, `mode`, and the `mean` of numeric property values can be queried, as well as the number of specific property values by `count`. The returned data is a list of groups, indicated by `groupedBy` `path` (same as the filter), and the actual `value`. 

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

## Parameters

## Where filter
For both functions `Get` and `Meta` in the Local Query filtering is possible. In the query introducted in the [Get function](#get-function) section, the result will contain the name and age of all the animals, and in which zoo they are. If you only want to `Get` all the Animals younger than 5 years old and living in the London Zoo, this can be specified in the `where` filter of the class in the `Get` function:

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

This applies to `Meta` and `Aggregate` queries, where large amount of data may be processed. An example of how this can be used is:

``` graphql
{
  Local {
    Meta {
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
