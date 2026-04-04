# Implementation Notes: Query Vector Global Response

## Summary of Changes
Implemented a solution to globally expose the query vector (`query_vector`) used during searches without modifying every internal business logic interface. The query vector is now included at the uppermost level of the response structure for GraphQL search requests. This implementation securely uses Go's `context.Context` to bridge the gap between `Traverser` (where the vectorization happens) and the REST/GraphQL adapters (where the global response is formatted).

## Modified Files
- `entities/models/graph_q_l_response.go`: Appended the `QueryVector []float32` property natively with the JSON tag `query_vector,omitempty`.
- `entities/dto/dto.go`: Created the standard `QueryVectorKey` to be safely used within `context.WithValue` injections.
- `usecases/traverser/near_params_vector.go`: Intercepted module-inferred query vectors within `vectorFromParams` to aggressively capture it (via context pointer passing) prior to returning to `Traverser`.
- `adapters/handlers/rest/handlers_graphql.go`: Updated both solitary (`GraphqlPostHandlerFunc`) and batched (`GraphqlBatchHandlerFunc`) request handlers. They now dynamically provision an empty array pointer inside the query's Context payload. Once the GraphQL resolution succeeds, Weaviate intercepts this array and securely anchors it down to `models.GraphQLResponse`.
- `usecases/traverser/get_test.go`: Added new testing suite ensuring correctness and avoiding future regressions.

## Avoiding Double-Querying
Double-querying happens when internal engines must reach out to external providers (OpenAI, Cohere) a *second time* simply to retrieve exactly what was just inferred.

By leveraging the pre-established `modulesProvider` workflow housed inside `vectorFromParams` (within the `Traverser`), Weaviate natively triggers module inference (which resolves to standard outputs like `VectorizationResult`). We immediately capture this computed `models.Vector` straight from the primary inference execution. By hooking a standard pointer reference into the `context.Context`, we propagate this memory address globally up the call stack back to the REST API wrapper without ever triggering a secondary external payload. We avoided deep modifications to native provider capabilities (`VectorizeInput`) as the single inference already fulfilled all requirements.

## How to Test
A comprehensive suite is defined inside `usecases/traverser/get_test.go`. We test cases for both `nearVector`-styled inferences (direct injections) alongside abstracted module workflows (simulated text modules mapping directly into vectors).

To execute the test on your environment, run:
```bash
go test ./usecases/traverser/ -run TestGetClass_QueryVectorExtraction
```
```bash
go test ./usecases/traverser/ -run TestGetClass_QueryVectorExtraction_Modules
```
