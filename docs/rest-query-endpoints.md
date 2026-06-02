# REST query & aggregate endpoints

REST endpoints expose search and aggregation over HTTP as the replacement for
GraphQL data queries:

```
POST /v1/{collection}/query/{method}     → gRPC Search, scoped to one method
POST /v1/{collection}/aggregate          → gRPC Aggregate
```

The `{method}` routes mirror the client-library query methods so the surface is
discoverable and self-validating: `fetch`, `bm25`, `hybrid`, `near-vector`,
`near-text`, `near-object`, and `near-media` (the multi-modal searches —
image/audio/video/depth/thermal/IMU — behind a single route). They all parse
into the same gRPC `SearchRequest` and run the same pipeline — see "Per-method
query endpoints" below. (There is no catch-all `/query`; each search type has its
own route.)

GraphQL is being deprecated (it is incompatible with Namespaces — schema
leakage, no per-namespace RBAC — and introspection exposes the schema). Every
other dedicated vector database exposes querying over REST; these endpoints give
HTTP-only integrations a first-class path and converge on that norm. Because they
delegate to the gRPC pipeline, they are **namespace-compatible** (unlike GraphQL).

## Core design: one pipeline, two transports

The request and response bodies are the **canonical proto-JSON encoding of the
gRPC `SearchRequest`/`AggregateRequest` and `SearchReply`/`AggregateReply`
messages**. REST and gRPC therefore share a single `parse → traverse → reply`
implementation — there is no second query parser, no hand-maintained REST query
model, and automatic feature parity (hybrid, bm25, nearVector/nearText, filters,
groupBy, generative, rerank, …).

```
gRPC client ─► Search()/Aggregate() ─┐
                                      ├─► SearchWithPrincipal / AggregateWithPrincipal ─► traverser ─► reply
REST client ─► /v1/{collection}/query ┘        (parse_search_request, prepare_reply, …)
```

The gRPC entrypoints (`adapters/handlers/grpc/v1/service.go`) resolve the
principal from request metadata, then delegate to the exported
`SearchWithPrincipal` / `AggregateWithPrincipal`. The REST handler authenticates
itself and calls the same methods. The split is a pure extraction — gRPC behavior
is unchanged and is guarded by the existing parser test suite.

A REST-vs-gRPC **parity acceptance test** (`test/acceptance/rest_query/`) asserts
the REST results are `proto.Equal` to gRPC for the same request.

## Routing (custom middleware, not go-swagger)

The endpoints are registered as a custom route in the global middleware chain
(`adapters/handlers/rest/handlers_query.go`, wired from `middlewares.go`'s
`makeSetupGlobalMiddleware`), mirroring how module routes
(`makeAddModuleHandlers`) are attached — **not** as go-swagger operations. The
body is opaque proto-JSON that doesn't belong in the OpenAPI model surface, and
this avoids generating/committing a parallel REST model layer. Non-matching
paths/methods fall through to the swagger router. `matchRESTQueryPath` matches
exactly `/v1/{collection}/query|aggregate`; no existing route has that shape, so
nothing is shadowed.

Consequences of being outside go-swagger:
- **Auth** is performed in the handler, mirroring the gRPC auth handler: a Bearer
  token is validated via the shared `composer`; a missing/invalid token falls
  back to anonymous access when enabled.
- **Errors** from the pipeline are mapped to HTTP status by
  `httpStatusForQueryError` (mirroring the gRPC typed-error translation):
  Unauthenticated→401, Forbidden→403, usage-limit→429, restriction/other→422.
- **Operational mode**: these POSTs are classified as reads (`isRESTQueryReadPath`
  in `middlewares.go`) — allowed in read-only/scale-out, blocked in write-only —
  consistent with `IsGRPCRead` for the equivalent RPCs.

## Per-method query endpoints

`/v1/{collection}/query/{method}` mirrors the client-library query methods
(`fetch`, `bm25`, `hybrid`, `near-vector`, `near-text`, `near-object`, and the
multi-modal `near-media`). They are **not** a second API — each one parses into
the same `pb.SearchRequest` and calls the same `SearchWithPrincipal`. The path
only adds a per-route assertion (`validateSearchForKind`):

- `/query/{method}` requires exactly its own search field set (e.g.
  `/query/near-vector` ⇒ `nearVector`), and rejects any other search method with
  a 422.
- `/query/fetch` requires *no* search method (filter / sort / paginate only).
- `/query/near-media` requires *exactly one* of the six media search fields
  (`nearImage`/`nearAudio`/`nearVideo`/`nearDepth`/`nearThermal`/`nearImu`), and
  rejects a non-media method or more than one. The six collapse into one route
  because they are structurally identical (a media blob + the usual
  certainty/distance/target-vector knobs) and differ only in which proto field
  carries the payload.

This keeps the runtime body identical to the gRPC `SearchRequest`, so the
predictability invariant holds: any per-method body is also a valid gRPC `Search`
— the path just adds an additive constraint and clearer errors. Routing lives in
`matchRESTQueryPath` (`querySubKinds`), validation in `validateSearchForKind`.
`/aggregate` is left single (search method in the body) but could be split the
same way.

In the OpenAPI spec each route advertises only its relevant fields via a flat
per-route request schema that inlines the common query parameters plus the one
search field — e.g. `NearVectorQueryRequest` lists the common params + `nearVector`.
This limits the *documented* body per endpoint without changing the runtime
(still one `SearchRequest`). `allOf` composition would avoid duplicating the
common fields, but the docs renderer (Scalar 1.49) does not render properties
across an `allOf` boundary — neither merged members nor a sibling field next to a
`$ref` show up in the body list — so the fields are inlined instead. The
duplication is generated from a single source (the `SearchRequest` fields), not
hand-maintained, and the schemas are `x-doc-only`, so they don't reach go-swagger
codegen.

## Request conveniences

The handler pre-processes the JSON body (`preprocessQueryBody`) before the strict
proto-JSON parse to smooth over the parts that are awkward to hand-write over raw
HTTP:

- **`consistencyLevel`** — accepts the short form (`ONE`/`QUORUM`/`ALL`,
  case-insensitive) and rewrites it to the protobuf enum name; unknown values
  fall through to the strict parser.
- Plain **`vector`** float arrays (nearVector/hybrid) and a simple **`alpha`**
  (hybrid) are accepted directly by the underlying parser — no base64 or
  `useAlphaParam` needed. These are documented but require no special handling.

The collection always comes from the path and overrides any `collection` in the
body; an empty body runs a default query.

## Gating

Enabled by default; set `DISABLE_REST_QUERY=true` to turn the endpoints off (they
then return 422). In the Namespaces world this is the supported query surface, so
its default is the opposite of GraphQL's.

## OpenAPI documentation

The endpoints and their request/response schemas are documented in
`openapi-specs/schema.json` for the docs site, with the body schemas derived from
the proto messages. Because that file is also go-swagger's code-generation source
(and CI regenerates + diffs it), these documentation entries are marked
`"x-doc-only": true` and stripped from the spec fed to `swagger generate` by
`tools/swagger_strip_doc_only` (wired into `tools/gen-code-from-swagger.sh`). The
docs keep the full surface; the generated/committed Go is unchanged.

## Not included

`Explore` (cross-class search) has no gRPC RPC and is intentionally not exposed.
`nearText` and other vectorizer-dependent searches work only when the collection
has a vectorizer module configured (the endpoint accepts the request and returns
the module's error otherwise).

## Key files

| Area | File |
|---|---|
| Shared pipeline entrypoints | `adapters/handlers/grpc/v1/service.go` |
| REST handler (routing, auth, consistency shorthand, errors) | `adapters/handlers/rest/handlers_query.go` |
| Middleware wiring + operational-mode read classification | `adapters/handlers/rest/middlewares.go` |
| `Querier` interface | `adapters/handlers/rest/state/state.go` |
| Gating flag | `usecases/config/config_handler.go`, `usecases/config/environment.go` |
| OpenAPI doc-only strip for codegen | `tools/swagger_strip_doc_only/main.go` |
| Unit tests | `adapters/handlers/rest/handlers_query_test.go` |
| REST-vs-gRPC parity acceptance test | `test/acceptance/rest_query/parity_test.go` |
