# Batch request processing

> Weaviate adds on option to group specific requests into batches with the goal
> to speed up certain scenarios, such as imports.

## 1. The batch request

A batch request is an array of requests. The goal of this construction is to
reduce network or allow for more efficient database access. Each database
connector can decide for themselves what to do with batches. For example, the
Janusgraph connector will group up to 50 "add class" statements into a single
query, as this has proven to be the most efficient way of importing into
Janusgraph.

This document uses the following terminology: 
- `batch request`: refers to an array of requests
- `batched request`: refers to an individual request in a `batch request`
- `batch response`: refers to an array of responses
- `batched response`: refers to an individual response in a `batch response`

### 1.1 Format

Examples of the formats used by a batch request and a batch response are
detailed below (these were taken from the [Apollo
blog](https://blog.apollographql.com/query-batching-in-apollo-63acfd859862)).

#### 1.1.1 GraphQL batch request

```
[
  {
    query: < query 0 >,
    variables: < variables for query 0 >,
  },
  {
    query: < query 1 >,
    variables: < variables for query 1 >,
  },
  {
    query: < query 2 >,
    variables: < variables for query 2 >,
  }
]
```

#### 1.1.2 GraphQL batch response

```
[
  <result for query 0>,
  <result for query 1>,
  ...
  <result for query n>
]
```

#### 1.1.3 Action/Thing create batch requests

The Action and Thing create batch endpoints use a slightly different format to
allow batch-level parameters to be applied to the batched requests.

In contrast to non-batch endpoints, no `async` option is present on this type
of request. The request is already optimized for maximum performance of the
respective database connector. Performing them async would lead to the user
sending of all batches at once, thus congesting the databse. Instead the
blocking nature of the request should be used to throttle the imports as this
will lead to an optimal import frequency.

Note, that you can still parallelize your import strategy, however each worker
or thread should in itself block for the duration of one batch job. The ideal
number of workers and parallel imports depends on your setup. As a rule of
thumb, the higher the number of horizontal replicas of both weaviate and its
dependencies the more parallel workers you can use.

The `fields` parameter determines which field values of the batched responses
will be returned normally; the values of fields that are not named in this
parameter will be returned as `null`. This parameter defaults to `"ALL"`. The
options are as follows:
+ `"ALL"`
+ `"@class"`
+ `"schema"`
+ `"creationTimeUnix"`
+ `"key"`
+ `"actionId"` or `"thingId"`

##### 1.1.3.1 An Action/Thing create batch request

```
{
	"actions": [
	  {
	    <regular action create request 0>,
	  },
	  {
	    <regular action create request 1>,
	  },
	  {
	    <regular action create request 2>,
	  }
	],
	"fields": [
		"ALL"
	]
}
```

##### 1.1.3.2 An Action/Thing create batch response

```
[
  <response for request 0>,
  <response for request 1>,
  ...
  <response for request n>,
]
```

### 1.1.4 Batch Cross-References

*Warning: Using this endpoint can be dangerous. Make sure you understand the
implications outlined in the [Caveats and Warnings about the Batch References
importer](#1142-caveats-and-warnings-about-the-batch-references-importer)
section below!*

Cross-References in a graph can be circular. Imagine a class `Person` with a
ref-property `Knowns`, which in turn points to a person. Since this scenario is
almost impossible to import in a single setting you can decouple importing the
class instances (i.e. Persons) and their `knows` cross-reference.

To do so, you can first import all Person class instances with only their
primitive properties using the `batch/things` endpoint and then import all the
relations (i.e. `knows` cross-references) using the `batch/references` endpoint. 

#### 1.1.4.1 Batch Reference Request and Response structure
An excerpt from the swagger documentation about the request structure:
```
[{
  from    string($uri)
          example:  weaviate://localhost/things/Zoo/a5d09582-4239-4702-81c9-92a6e0122bb4/hasAnimals
          Long-form beacon-style URI to identify the source of the cross-ref including the property name. Should be in the form of weaviate://localhost////, where must be one of 'actions’, ‘things’ and and must represent the cross-ref property of source class to be used.

  to      string($uri)
          example: weaviate://localhost/things/97525810-a9a5-4eb0-858a-71449aeb007f
          Short-form URI to point to the cross-ref. Should be in the form of weaviate://localhost/things/ for the example of a local cross-ref to a thing
}]
```

Please consult the swagger documentation at
`weaviate.batching.references.create` for the full reference and all return
types. 

#### 1.1.4.2 Caveats and Warnings about the Batch References importer

As outlined in the [introduction](#1-the-batch-request) all batch importers are
designed for maximum speed. This enableds imports of large datasets. However,
this also means that validation cannot be as thorough as on non-batched
requests. In extreme cases this can lead to corrupting your dataset.

##### General limitations
Due to the focus on speed and throughput at large-scale batch importers have to
skip all patterns which would slow imports down, such as "reading before
writing". In general this means that validation is less strict, and you have to
make sure that your data is logically consistent.

Weaviate will be able validate the structure of your request and parse both the `from`
and `to` beacons. For those the general structure is checked, e.g. does the
beacon contain enough path segments and is the segment at the position of the
ID a valid UUID? However, weaviate cannot validatate whether all properties the
user specified are valid in combination.

If you are in doubt, send a representative request of your batching plan to a
non-batched endpoint, such as `PATCH /weaviate/v1/things` first, where
validation is very strict. If validation is passed there it can be considered
safe to be used in a batch setting as well.

##### Connector-specific consequences

The exact consequences of (accidentally) bypassing validation, vary between
connectors:

**Janusgraph (with Cassandra)**

As an example, consider the follwing `from` beacon when using the `Janusgraph`
connector, which is based on NoSQL databases:
```url
weaviate://localhost/things/Zoo/a5d09582-4239-4702-81c9-92a6e0122bb4/hasAnimals
```

In this case weaviate cannot validate:
- whether the uuid exists. However, the import would fail if the uuid cannot be
  found in the db during import.
- whether the specified uuid indeed represents a `Zoo`: Assuming `hasAnimals` is
  a valid property of the `Zoo` class, this import will succeed even if the
  specified class (identified by its uuid) is of a different type. This can
  potentially lead to a corrupted dataset.


### 1.2 Errors

There are two types of error that can occur when Weaviate receives and/or
processes a batched request.

#### 1.2.1 Batch request error

The batch request itself resulted in an error. Its contents are not processed
and an error code is included in the header response.

#### 1.2.2 Batched request error(s)

The batch request itself is successful (status code 200), but one or more of
its batched requests result in an error. The error(s) in a batch result will
use this format for the `GraphQL` endpoint:

```
[
    {
        "errors": [
            "location": "",
            "message": "<error code> : <error message>",
            "path": ""
        ]
    }, 
    <result for query 1>,
    ...
    <result for query n>
]
```

or this format for `non-GraphQL` endpoints: 

```
[
    {
        "result": {
            "errors": {
                "error": [
                    "message": "error message"
                ]    
            }
        }    
    }
    <result for query 1>
    ...
    <result for query n>
```


## 2. Batch request endpoints

Weaviate has a batch endpoint for a number of request types. These batch
endpoints are specified below:
* `POST weaviate/v1/batching/actions`
* `POST weaviate/v1/batching/things`
* `POST weaviate/v1/batching/references`
* `POST weaviate/v1/graphql/batch`

