# Batch request processing
Weaviate can process either regular requests or batch requests.
## 1. The batch request
A batch request is an array of requests. The goal of this construction is to reduce network traffic. 
This document uses the following terminology: 
- `batch request`: refers to an array of requests
- `batched request`: refers to an individual request in a `batch request`
- `batch response`: refers to an array of responses
- `batched response`: refers to an individual response in a `batch response`
### 1.1 Format
Examples of the format used by a batch request and a batch response are detailed below (these were taken from the [Apollo blog](https://blog.apollographql.com/query-batching-in-apollo-63acfd859862)).
#### 1.1.1 Request
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
#### 1.1.2 Response

```
[
  <result for query 0>,
  <result for query 1>,
  ...
  <result for query n>
]
```
### 1.2 Errors **TODO** refer to gh-506
There are two types of error that can occur when Weaviate receives and/or processes a batched request.
#### 1.2.1 Batch request error
The batch request itself resulted in an error. Its contents are not processed and an error code is included in the header response.
#### 1.2.2 Batched request error(s)
happens more often, this is the output....

## 2. Batched request endpoints
Weaviate has a regular and a batch endpoint for each HTTP request type it can process. These batch endpoints are specified below:
* graphql/batch (link code)
* bla/batch (link code)
