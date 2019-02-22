# Batch request processing

> Weaviate can process either regular HTTP requests or batch HTTP requests.

## 1. The batch request

A batch request is an array of requests. The goal of this construction is to reduce network traffic. 
This document uses the following terminology: 
- `batch request`: refers to an array of requests
- `batched request`: refers to an individual request in a `batch request`
- `batch response`: refers to an array of responses
- `batched response`: refers to an individual response in a `batch response`

### 1.1 Format

Examples of the formats used by a batch request and a batch response are detailed below (these were taken from the [Apollo blog](https://blog.apollographql.com/query-batching-in-apollo-63acfd859862)).

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

The Action and Thing create batch endpoints use a slightly different format to allow batch-level parameters to be applied to the batched requests.

The `async` parameter enables asynchronous processing for the batched requests. The responses will each contain a 202 with the ID of the created Thing or Action, this reply is generated before persistence of the data is confirmed. This parameter defaults to `false`. 

The `fields` parameter determines which field values of the batched responses will be returned normally; the values of fields that are not named in this parameter will be returned as `null`. This parameter defaults to `"ALL"`. The options are as follows:
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
	"async": true,
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
### 1.2 Errors(section pending [issue](https://github.com/creativesoftwarefdn/weaviate/issues/513) resolution)

There are two types of error that can occur when Weaviate receives and/or processes a batched request.

#### 1.2.1 Batch request error

The batch request itself resulted in an error. Its contents are not processed and an error code is included in the header response.

#### 1.2.2 Batched request error(s)

The batch request itself is successful (status code 200), but one or more of its batched requests result in an error. The error(s) in a batch result will use this format for the `GraphQL` endpoint:

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

Weaviate has a batch endpoint for a number of request types. These batch endpoints are specified below:
* batching/actions
* batching/things
* graphql/batch

