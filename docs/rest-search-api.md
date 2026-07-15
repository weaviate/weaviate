# REST Search API — design notes & decision log

`POST /v1/search/{collection}/near-text` — semantic (near-text) search over
REST, with server-side embedding of the query text. First endpoint of the
REST Search API family proposed in the RFC *"REST Search API over
QUERY/POST"* (Ivan Despot, 2026-07-06); hybrid, bm25 and near-object will
follow the same `/v1/search/{collection}/…` shape, and aggregate the
`/v1/aggregate/{collection}` shape, when built.

> **Status:** draft, POST-only, part of the swagger surface
> (`openapi-specs/schema.json`, operation `search.nearText`). Enabled by
> default; opt out with `DISABLE_REST_SEARCH=true`.

---

## 1. Decision log

### 2026-07-06 — first draft: QUERY+POST, custom-mounted

The endpoint was first implemented **outside** the swagger surface as a
custom `http.Handler` in the global middleware chain (the `/v1/modules/*`
pattern), because Swagger 2.0 cannot express an HTTP QUERY operation. QUERY
was the canonical method with POST as a byte-identical alias. That version
carried its own bearer-token authentication (composer), manual 405/`Allow`,
manual CORS headers, manual Content-Type 415, a 10 MiB body cap, a
reserved-roots deny-list to avoid shadowing existing `/v1` routes, and
`Cache-Control: private`/`no-store` handling. It passed two review rounds
(code review + fix round) and a 62-assertion live smoke.

### 2026-07-07 — rescope: POST-only through the swagger spec

Directive (Ivan): the first version is **POST-only — QUERY is dropped for
now** — and the endpoint goes **through `openapi-specs/schema.json` like the
existing endpoints** (spec + go-swagger codegen + `configure_api.go`
wiring). The QUERY+POST custom-mount tree is preserved on the tag
`rest-search-querypost-snapshot`.

**Routing feasibility was gated first**: go-swagger's denco router had to
prove it can route a path parameter as the *first* segment after the base
path (`/{collection}/search/near-text`) alongside the static roots
(`/objects`, `/schema`, ...) without shadowing in either direction.
Verified live against the generated (unimplemented) operation:

- `POST /v1/Movie/search/near-text` → routed to the search operation.
- `GET /v1/schema`, `GET /v1/meta`, `POST /v1/objects`,
  `POST /v1/batch/objects` → unchanged (static roots win where they match).
- `GET /v1/backups/search/near-text` → still the backups status route
  (`/backups/{backend}/{id}` with `backend=search`, `id=near-text`).
- `POST /v1/backups/search/near-text` (and `objects`, `schema`, ...) → the
  search operation: denco backtracks static→param **per method**, so even
  collection names whose lowercase spelling equals a reserved API root
  route correctly when no static route claims the exact method+shape.

**What the swagger move bought** (previously hand-rolled): request routing;
the authenticated principal via the swagger security layer (no manual
TokenFunc extraction); 405 + `Allow: POST` for other methods; 415 via
per-operation `consumes` (including for *absent* Content-Type — sniffed as
`application/octet-stream` and rejected, which fully closes the prior
review's "absent-CT CORS simple request" residual); CORS via the standard
middlewares; a spec-documented endpoint that client generators can see; and
the reserved-roots deny-list plus its swagger-parity test became
unnecessary (the real router disambiguates).

**What changed semantically vs. the custom mount** (documented, deliberate):

| Behavior | custom mount (2026-07-06) | swagger surface (2026-07-07) |
|---|---|---|
| Other methods (GET, PUT, QUERY, ...) | 405, `Allow: POST, QUERY`, `ErrorResponse` body | 405, `Allow: POST`, swagger body `{"code":405,"message":"method GET is not allowed, but [POST] are"}` |
| Wrong Content-Type | 415, `ErrorResponse` body | 415, swagger body `{"code":415,"message":"unsupported media type ..."}` |
| Absent Content-Type | tolerated (known residual) | 415 (sniffed as octet-stream) — residual closed |
| Malformed JSON body | 400, `ErrorResponse` body | 400, swagger body `{"code":400,"message":"parsing body body from \"\" failed, ..."}` |
| Missing body | 400 "query is required" | 422, swagger body `{"code":602,"message":"body in body is required"}` |
| Body size cap | 10 MiB → 413 | none (parity with the other swagger endpoints, which are uncapped; the prior review's DoS concern is a **known residual** — a cap would have to be server-wide to be meaningful) |
| `Cache-Control` | `private` on 200, `no-store` on errors | none (parity with the other endpoints; the RFC's caching rule was motivated by QUERY auto-retries, which are gone with QUERY) |
| `DISABLE_REST_SEARCH` | route not mounted → 404 | operation stays registered → **422** `{"error":[{"message":"rest search api is disabled"}]}` — the exact `DISABLE_GRAPHQL` mechanism |

Handler-level behavior (status table, parsing, reply shape, authz order,
namespace stripping) is unchanged from the reviewed draft.

### 2026-07-08 — option 2: explicitly typed request body

Directive (Ivan): after a trade-off analysis of three ways to model the
body, **option 2** — a fully, explicitly typed `SearchNearTextRequest` in
`schema.json` — was chosen over option 1 (`x-go-type`, which would have kept
strict unknown-field-400 + string-or-array `query`) and option 3 (the
free-form `{"type":"object"}` model, all gates passed, snapshotted on tag
`rest-search-untyped-snapshot`). The goal: a self-documenting spec and
generated clients with real types. **Request-side only** —
`SearchNearTextResponse`/`SearchResultObject` stay free-form (each hit is
inherently variable-shaped).

**This is the one variant whose WIRE CONTRACT changes** versus the prior
gated version. Two deliberate, accepted changes:

1. **Unknown/typo'd body fields are now silently ignored** (was strict-400).
   Platform parity with every other Weaviate endpoint — the typed model
   drops unknown keys at unmarshal. `DisallowUnknownFields` is gone.
2. **`query` is array-only** (`["x"]`, `["x","y"]`). Swagger 2.0 has no
   `oneOf`, so the string form `"query":"x"` is dropped; it now fails the
   JSON decode → 400. A single concept is a one-element array.

Reserved fields are **declared** in the schema (each `x-nullable: true` so
it generates as a pointer), so their 422 "not yet supported" demand signal
survives — presence is a non-nil pointer. `certainty`/`distance`/`limit`/
`offset`/`auto_limit` are likewise `x-nullable` to keep the absent-vs-zero
pointer semantics the handler relies on. `consistency_level` and
`return_metadata` items carry enums (they now validate at bind time).

Swagger-native behaviors are unchanged from 2026-07-07 (missing body → 422
code 602; wrong/absent Content-Type → 415; other methods → 405 + `Allow:
POST`).

Two consequences of the typed model, both from swagger validating at bind
time before the handler runs:

- **Missing `query` → 422** (swagger `required` validation), where the
  free-form version returned the handler's 400. An explicit empty array
  `{"query":[]}` still reaches the handler → 400.
- **`where` is now the typed `WhereFilter` model**. A bad `operator` enum
  value is caught by swagger's enum validation → **422** at bind time, but a
  wrong *value type* (e.g. a string for `valueInt`, or `path` sent as a string
  instead of an array) fails the JSON decode → **400**, not 422. A
  schema-valid but semantically-invalid filter (e.g. an unknown property path)
  reaches the handler's `filterext.Parse` / `ValidateFilters` → **400**.
  Similarly, bad `consistency_level` / `return_metadata` enum values → 422 at
  bind (the handler keeps its own tolerant checks as a defensive fallback for
  the direct-call path).

**Migration note:** restoring string-or-array `query` later is a **safe
widening**. But turning on `additionalProperties: false` (strict unknown-
field rejection) during a future OpenAPI 3.x migration would be a
**breaking** change (ignored → rejected).

**Migration note (canonical values):** the enums make two inputs stricter on
the wire than the untyped version accepted — clients that relied on lenient
casing or lenient metadata keys must now send canonical values. (1)
`consistency_level` is **uppercase-only**: the untyped handler silently
upper-cased a lowercase `"quorum"` via `ToUpper`, but the enum
`[ONE, QUORUM, ALL]` now returns 422 for it at bind time. (2)
`return_metadata` values are enum-validated: an unsupported key (e.g.
`"vector"`) now returns 422 at bind, where the untyped version reached the
handler and returned 400.

### 2026-07-08 — endpoint path reordered to /v1/search/{collection}/near-text

Directive (Ivan, from Dirk's RFC review): the path was reordered from
`/v1/{collection}/search/near-text` to
**`/v1/search/{collection}/near-text`**. Rationale:

- **Removes the leading-wildcard/static-root collision class permanently.**
  In the previous shape `{collection}` was the first segment after `/v1`,
  sharing that slot with the static roots (`/objects`, `/schema`, ...); the
  denco router resolved it by static→param backtracking, which worked but
  was fragile and needed reserved-root awareness. With `search` static and
  first, no collision is possible **by construction** — `{collection}` is
  now the 2nd segment, disambiguated by the static `search` prefix.
- **Matches Weaviate's own `/v1/{resource-type}/…` convention.**
- **Gives a clean static `/v1/search/` prefix** for the operational-mode
  middleware (which keys on the segment after `/v1`), metrics route labels,
  and any future gating — no dynamic first segment to special-case.

The RFC (both Notion pages) was updated to match; **aggregate correspondingly
moves to `/v1/aggregate/{collection}`** in the RFC (not yet built). When the
other search types land they follow the same `/v1/search/{collection}/…`
shape.

This supersedes the 2026-07-07 routing-feasibility analysis below: the
denco static→param backtracking narrative is now historical — the static
`search` prefix means there is nothing to backtrack.

### 2026-07-09 — handler refactored for multi-endpoint reuse

Behavior-preserving internal restructuring so that adding hybrid / bm25 /
near-object becomes *spec + a small param-builder*, not copy-paste. No
observable change — same wire shapes, status codes and live smoke
(65/65 + disabled 2/2 + writeonly 1/1). What changed:

- **Shared response model.** `SearchNearTextResponse` → `SearchResponse`
  (`{results: []SearchResultObject, took_ms}`); the near-text 200 repoints
  at it. One model serves all four endpoints. `SearchResultObject`
  unchanged.
- **Shared request base via `allOf`.** A new `SearchCommon` definition holds
  every field common to all search types (`where`, `limit`/`offset`/
  `auto_limit`, `return_properties`, `return_metadata`, `tenant`,
  `consistency_level`, and the seven search-reserved fields).
  `SearchNearTextRequest = allOf[SearchCommon, {query (required), certainty,
  distance, target_vector}]` — `target_vector` stays near-text-specific (bm25
  won't have it). go-swagger generates `SearchNearTextRequest` with an
  **embedded** `SearchCommon` (fields promoted), so the handler reads shared
  fields via promotion (`body.Where`, `body.Limit`, …) and the reserved base
  via `&body.SearchCommon`. Validation is preserved: the generated
  `Validate()` calls `SearchCommon.Validate` (enums + `where`) *and*
  `validateQuery` (required).
- **Generic orchestrator.** `Handler.execute(ctx, principal, collection,
  tenant, *SearchCommon, buildParams)` carries the fixed flow —
  disabled→422, reserved-fields→422 **before any schema access**, namespace/
  alias resolve, **authz before schema**, params build, traverser, reply —
  and delegates only the search-type-specific `dto.GetParams` construction.
  The reserved-before-authz and authz-before-schema ordering is load-bearing
  and unchanged. `NearText` is now a thin wrapper that supplies
  `buildNearTextParams`.
- **Decoupled shared parsers.** `checkReservedFields` and `parsePagination`
  take `*models.SearchCommon`; `buildGetParams` → `buildNearTextParams`
  (reads shared fields off `SearchCommon`, near-text fields off the concrete
  body). `parseWhere`/`parseReturnMetadata`/`parseReturnProperties`/
  `parseConsistencyLevel`/`resolveTargetVectors`/`parseNearText`/`parseQuery`
  logic is untouched — only their call sites moved.
- **`IsSearchRoute` generalized** to any `/v1/search/{collection}/{type}`, so
  the op-mode read classification already covers hybrid/bm25/near-object.

Also on 2026-07-10 (Copilot review, two rounds): a denied request against
an alias no longer names the alias target in the 403 (deny on the
caller-supplied name); certainty outside [0,1] → 400; `limit`/`offset` and
their sum are capped at `QUERY_MAXIMUM_RESULTS` pre-db (400; also closes an
int-overflow path into the negative special limit flags); 429 declared in
the spec with a typed responder; and swagger-layer errors on search routes
(bind validation, 405, 415, and the swagger security layer's 401) are
re-shaped into the standard `ErrorResponse` body by a search-scoped
`api.ServeError` wrapper, so the error contract has one body shape. The one
exception is a request with **no/malformed credentials**: the anonymous-access
middleware answers that 401 above the swagger layer (before `api.ServeError`
runs), so it keeps the legacy `{"code","message"}` body — the same shape every
existing endpoint returns for that case.

Also on 2026-07-10 (review): error classification moved from message
substrings to typed errors. The blocker was that the explorer wrapped
errors with pkg/errors `Errorf("...: %v", err)`, which drops `Unwrap` — the
tenant sentinels attached in `adapters/repos/db/multitenancy` never reached
the handler, so its `errors.Is` checks were dead and only the string
fallbacks worked. Those wraps are now chain-preserving
(`errors.Wrapf`/`%w`, byte-identical messages), and the producers attach
typed errors from `entities/errors` (`ErrNoVectorizerModule`,
`ErrQueryVectorization`, `ErrCertaintyIncompatible`). Drift guards live in
the producing packages (`usecases/modules`, `usecases/traverser`,
`entities/schema/configvalidation`): if a producer stops attaching its type
or a wrap goes back to `%v`, a test fails next to the code that broke it.

### 2026-07-13 — review (tsmith023): response envelope `{id, properties, references, metadata}`

Each hit in `results` is a **gRPC-proto-like envelope**, the typed
`SearchResultObject`:

- `id` — the object UUID, **always returned** (the handler always requests
  it internally).
- `properties` — the selected non-reference properties (nested objects
  pruned to the selected nested fields). Always present, `{}` when the
  request selects no properties.
- `references` — the selected cross-references (reference name → array of
  objects with the selected one-hop properties). Omitted when the request
  selects no references.
- `metadata` — a typed `SearchResultMetadata` (`distance`, `certainty`,
  `score`, `explain_score`, `creation_time`, `last_update_time`; all
  optional pointer fields, so absent ≠ zero). Omitted unless non-id
  metadata was requested and is present.

Properties of this contract:

- **The `metadata` name is collision-safe by construction** — user
  properties live under `properties`, so a collection property named
  `metadata` sits at `properties.metadata`, disjoint from the envelope's
  `metadata` (regression tests in `reply_test.go` and the acceptance
  suite). `return_properties` has no reserved names: any name that is not
  a schema property is the generic unknown-property 400.
- **`id` is always returned**; `return_metadata: []` (or omitted) means
  "no metadata block", but every hit carries its `id` anyway.

Also from the same review round:

- `buildParamsFunc`/`classGetterWithAuthz` now use a named
  `classGetterFunc` type, and `NearText` hoists its params-builder closure
  into a named variable; the unreachable nil-body check in `NearText` was
  deleted (the generated `BindRequest` 422s a missing body before the
  handler runs).
- `interface{}` → `any` in the PR's handwritten files
  (`adapters/handlers/rest/search/`, `handlers_search.go`).
- The duplicated "all non-ref non-blob properties" selection helpers (REST
  `request.go` vs gRPC `parse_search_request.go`) were extracted into one
  canonical implementation: `entities/search/select_properties.go`
  (`search.AllNonRefNonBlobProperties` +
  `search.AllNonRefNonBlobNestedProperties`); the gRPC side keeps a thin
  wrapper that does its `authorizedGetClass` call and delegates.
- The PR-introduced pkg/errors `Wrapf` sites in `usecases/traverser`
  (`explorer.go`, `near_params_vector.go`) now use stdlib
  `fmt.Errorf("…: %w", err)` — messages byte-identical, chain preserved;
  the `ErrQueryVectorization` attachments are unchanged.

### 2026-07-14 — `return_metadata` accepts metadata keys only (Ivan)

`return_metadata` selects only metadata keys: `distance`, `certainty`,
`score`, `explain_score`, `creation_time`, `last_update_time`. The object
`id` is not a metadata key — it is always returned as each result's
top-level `id` field, whatever `return_metadata` contains. Any value
outside the enum (including `id`) is rejected by swagger validation at
bind time → 422; the handler's own parser keeps a matching 400 fallback
for the direct-call path. Strict-now-widen-later: accepting `id` again
would be a safe widening, so rejecting it pre-ship is the reversible
choice.

---

## 2. Implementation

### Files

- `openapi-specs/schema.json` — path `/search/{collection}/near-text`
  (POST, tag `search`, operationId `search.nearText`, per-op
  `consumes: [application/json]`), definitions `SearchCommon` (shared base),
  `SearchNearTextRequest` (= `allOf[SearchCommon, near-text-specific]`),
  `SearchResponse`, `SearchResultObject` (the typed
  `{id, properties, references, metadata}` envelope; `properties`/
  `references` are free-form maps) and `SearchResultMetadata` (all-optional
  typed metadata). The spec declares the always-present parts required:
  `results` + `took_ms` on `SearchResponse`, `id` + `properties` on
  `SearchResultObject` (`properties` may be `{}`). Every declared error
  status carries the `ErrorResponse` schema — matching the search-scoped
  `ServeError` wrapper, so generated clients decode the message on every
  status. The sole wire exception is the no/malformed-credentials 401, which
  the anonymous-access middleware answers above the swagger layer in the legacy
  `{"code","message"}` shape (see the decision log). `SearchResponse`/`SearchResultObject`/
  `SearchResultMetadata` are shared by all search endpoints.
- generated (never hand-edited; `tools/gen-code-from-swagger.sh`, pinned
  go-swagger v0.30.4): `adapters/handlers/rest/operations/search/*`,
  `entities/models/search_common.go`, `search_near_text_request.go`,
  `search_response.go`, `search_result_object.go`,
  `search_result_metadata.go`, `client/search/*`,
  `adapters/handlers/rest/embedded_spec.go`.
- `adapters/handlers/rest/handlers_search.go` — wires the generated
  operation to the handler; maps `APIError` statuses onto the generated
  responders (unlisted statuses, e.g. 429, use `middleware.Error`).
- `adapters/handlers/rest/search/handler.go` — the generic `execute`
  orchestrator (search-type-agnostic): disabled-check, `checkReservedFields`
  (typed nil-pointer 422 scan on `SearchCommon`, before authz), alias/
  namespace resolution (`namespacing.Resolve`), authorization **before** any
  schema access (`authorization.READ` on `CollectionsData`/`ShardsData`, the
  gRPC handler's classGetter pattern), `traverser.GetClass`, error→status
  mapping, namespace stripping (`namespacing.StripErrForPrincipal`). It
  delegates the `dto.GetParams` build to a per-type `buildParamsFunc`.
  `NearText` is a thin wrapper passing `&body.SearchCommon` +
  `buildNearTextParams`. `IsSearchRoute` classifies any
  `/v1/search/{collection}/{type}` as a read.
- `adapters/handlers/rest/search/request.go` — the shared parsers
  (`checkReservedFields`/`parsePagination` on `*SearchCommon`, plus
  `parseConsistencyLevel`/`parseWhere`/`parseReturnMetadata`/
  `parseReturnProperties`/`resolveTargetVectors`) and the near-text-specific
  `buildNearTextParams` (+ `parseNearText`/`parseQuery`) that assemble
  `dto.GetParams`: nearText module params (gRPC `extractNearText` shape),
  where via `filterext.Parse` + `filters.ValidateFilters` (same parser as
  GraphQL/batch-delete), pagination (`auto_limit` → autocut, `*int64` →
  `int`), target-vector resolution (gRPC `extractTargetVectors` parity),
  deterministic no-vectorizer 422 pre-check, `return_metadata` →
  `additional.Properties`, `return_properties` incl. one-hop dot-path refs.
- `adapters/handlers/rest/search/reply.go` — traverser output →
  `models.SearchResponse` (shared): per hit the `{id, properties,
  references, metadata}` envelope — `id` always present, the selected
  non-reference properties under `properties` with nested-object pruning to
  the selected nested properties (blobs never leak), reference selections
  under `references` as arrays of objects, typed retrieval metadata under
  `metadata` (omitted when only the id was requested), vectors never
  returned, no `count` field, `took_ms` in integer milliseconds.
- `adapters/handlers/rest/middlewares.go` — operational-mode
  classification only (see below); no custom mount remains.
- `usecases/config/config_handler.go` + `environment.go` —
  `DisableRESTSearch` / `DISABLE_REST_SEARCH`.

(An end-to-end smoke harness, `rest_search_neartext_smoke.sh`, is used for
local development but is not committed to the repository — see section 4.)

### The typed body model (spec decision — option 2)

`SearchNearTextRequest` is a fully typed schema definition. The typed model
means unknown fields are ignored (platform parity) and `query` is array-only
(Swagger 2.0 has no `oneOf` for the string-or-array union) — see the
2026-07-08 decision entry for the accepted trade-offs. `x-nullable: true`
generates a **pointer** for every field where absent-vs-zero matters
(`certainty`, `distance`, `limit`, `offset`, `auto_limit`, and every reserved
scalar), preserving the handler's presence semantics. Slices
(`return_properties`, `return_metadata`) are naturally nil-vs-`[]`
distinguishable. `query` is `required`.

### Request body fields (as built)

Types are the generated Go types. "ptr" fields are `x-nullable` (nil =
absent). Statuses in the Behavior column that read "422 (swagger)" are
enforced by the generated model at bind time; the rest are the handler's.

| Field | Type | Behavior |
|---|---|---|
| `query` | `[]string` (required) | the near-text concepts, embedded server-side; array-only (single concept = one-element array); absent → 422 (swagger required); `[]` or empty concept → 400 |
| `certainty` | `*float64` (ptr) | cosine-only (else 422); mutually exclusive with `distance` (else 400); outside [0,1] → 400 |
| `distance` | `*float64` (ptr) | max vector distance |
| `target_vector` | `string` | required when the collection has >1 named vector (else 422); unknown name → 400; sole named vector selected implicitly |
| `where` | `*models.WhereFilter` | reuses the existing definition; bad `operator` enum → 422 (swagger); wrong value type (e.g. a string for `valueInt`) → 400 (JSON decode); schema-valid but unknown property → 400 (handler `filterext.Parse`) |
| `limit` / `offset` | `*int64` (ptr) | negative → 400; `limit` 0/omitted → `QUERY_DEFAULTS_LIMIT`; each and their sum capped at `QUERY_MAXIMUM_RESULTS` (else 400, checked pre-db so int overflow cannot reach the negative special limit flags) |
| `auto_limit` | `*int64` (ptr) | maps to autocut |
| `return_properties` | `[]string` | omitted → all non-ref, non-blob props; `[]` → no props; dot-path = one reference hop (`hasAuthor.name`); bare ref name = all non-ref, non-blob props of the target; ≥2 hops or multi-target refs → 422 "not yet supported"; unknown name → 400 |
| `return_metadata` | `[]string` (enum) | `distance`, `certainty`, `score`, `explain_score`, `creation_time`, `last_update_time` — metadata keys only; the object `id` is **always returned** as the envelope's `id` field and is not a valid entry; omitted or `[]` → no `metadata` block; value outside the enum (incl. `id`) → 422 (swagger enum); `certainty` silently dropped on non-cosine (gRPC parity) |
| `tenant` | `string` | tenant-scoped authz (`ShardsData`) |
| `consistency_level` | `string` (enum) | ONE / QUORUM / ALL; other value → 422 (swagger enum) |
| reserved | `*string` / `*int64` (ptr) | `single_prompt`, `grouped_task` (RAG, deferred), `group_by`, `number_of_groups`, `objects_per_group`, `rerank_property`, `rerank_query` — declared but return 422 "not yet supported" when present (non-nil).
### Error-status table (as built)

| Condition | Status | Body shape |
|---|---|---|
| malformed JSON body; `query` string form (array-only); wrong field type (incl. a `where` value of the wrong JSON type, e.g. a string for `valueInt`, or `path` as a string) | 400 | `ErrorResponse` |
| missing body; absent `query`; bad `consistency_level`/`return_metadata` enum; bad `where` `operator` enum | 422 | `ErrorResponse` |
| empty `query` array / empty concept; unknown `target_vector`; negative paging; paging beyond `QUERY_MAXIMUM_RESULTS`; certainty outside [0,1]; both certainty+distance; semantically-invalid `where` (unknown property); unknown property in `return_properties` | 400 | `ErrorResponse` |
| invalid credentials (bad key/token, via the swagger security layer) | 401 | `ErrorResponse` |
| no/malformed credentials (anonymous-access middleware, above the swagger layer) | 401 | legacy `{"code","message"}` (parity with existing endpoints) |
| not authorized for collection/tenant data (checked **before** schema access) | 403 | `ErrorResponse` |
| unknown collection; unknown tenant | 404 | `ErrorResponse` |
| no vectorizer / missing `target_vector` on multi-vector collection / certainty on non-cosine / reserved param present / tenant-vs-MT-config mismatch / tenant not active / `where` on a property with its inverted index disabled / API disabled | 422 | `ErrorResponse` |
| embedding provider failure | 502 | `ErrorResponse` |
| rate limited (traverser) | 429 | `ErrorResponse` — only the traverser's own typed `ErrRateLimit` maps here; an embedding provider's rate-limit error is an ordinary vectorization failure and maps to 502 |
| other method on the route | 405 + `Allow: POST` | `ErrorResponse` |
| non-JSON or absent Content-Type | 415 | `ErrorResponse` |

Every search error now has the same `{"error":[{"message":"…"}]}` body:
the generated model still validates the request **schema** at bind time
(required `query`, enums, `where` structure, field types) before the
handler runs, but a search-route-scoped `api.ServeError` wrapper
(`search.ServeError`) re-shapes the swagger layer's `{"code","message"}`
bodies into `ErrorResponse` — statuses and headers (e.g. `Allow` on 405)
are exactly what the default renderer computes. The handler tier applies
**semantic** validation (unknown property, no vectorizer, tenant/MT
mismatch, ...). The `search` unit tests exercise the handler tier in
isolation (they bypass the router), so a few of them assert the handler's
400 for inputs that live would reject with a swagger 422 first — this is
called out in `TestParseWhere` / `TestParseConsistencyLevel`.

`statusFromError` matches typed errors (`errors.Is`/`errors.As`) attached at
the producers: `ErrNoVectorizerModule` (usecases/modules),
`ErrQueryVectorization` (vectorize wrap sites in usecases/traverser +
usecases/modules), `ErrCertaintyIncompatible`
(entities/schema/configvalidation), the tenant sentinels
(adapters/repos/db/multitenancy), `objects.ErrMultiTenancy`, and the
handler's own collection-not-found sentinel. **Case order is still
load-bearing** in one place: a no-vectorizer config error surfaces wrapped
inside an `ErrQueryVectorization`, so the 422 check precedes the 502 check —
see the comment on the function. The single remaining substring fallback is
the upstream "could not find class ... in schema" (many producers, no
sentinel yet; reachable when a collection is deleted mid-request).

### DISABLE_REST_SEARCH

Mirrors `DISABLE_GRAPHQL` exactly: the operation stays registered and every
request is rejected with 422 `rest search api is disabled`. (The custom
mount used to 404; the change is deliberate mechanism-parity.)

### Operational modes

Search requests are POSTs (an HTTP "write" method) but semantically reads.
`addOperationalMode` classifies them by the static `/v1/search/` prefix
(`search.IsSearchRoute`, a pure shape check — routing itself is the
router's job):

- **READ_ONLY / SCALE_OUT**: allowed (parity with `/v1/graphql`).
- **WRITE_ONLY**: blocked with 503 — a deliberate divergence from
  `POST /v1/graphql`, which slips through the method-based check (legacy
  hole we chose not to replicate).
- With `DISABLE_REST_SEARCH=true` there is no carve-out (503 in read-only
  modes before the 422 would be reached).

### Metrics

The route is part of the swagger router, so `staticRoute` resolves it to
the low-cardinality pattern `/search/{collection}/near-text` natively — no
special-casing needed (the custom mount had required a rewrite to avoid
per-collection Prometheus label series).

---

## 3. RFC feedback (from the implementation rounds)

Items the RFC should settle, discovered while building all three variants.

**Resolved by decision (option 2, 2026-07-08):**

- **Strict unknown-field 400 → dropped.** Unknown/typo'd fields are now
  silently ignored (platform parity). Not open; it is the chosen contract.
- **`query` string-or-array → array-only.** The string form is dropped;
  restoring it later is a safe widening (documented in the decision log).

**Still for the RFC:**

1. **"Byte-identical QUERY/POST responses" is unsatisfiable** with
   `took_ms` in the body. Moot while QUERY is dropped, but the RFC text
   should say "identical modulo `took_ms`" or move timing to a header
   before QUERY returns.
2. **`[]` vs omitted** for `return_properties`/`return_metadata` is
   unspecified. As built: `return_properties` omitted → all non-ref props,
   `[]` → no properties; `return_metadata` omitted or `[]` → no `metadata`
   block — but the object `id` is **always returned** on the envelope
   either way (2026-07-13).
3. **Unknown `target_vector`** is absent from the error table. As built:
   400 (bad value); only the *missing*-on-multi-vector case is the
   RFC-specified 422.
4. **Tenant misuse** (tenant on a non-MT collection; missing tenant on an
   MT collection) is absent from the table. As built: 422; unknown tenant
   is the RFC-specified 404.
5. **`certainty` in `return_metadata` on non-cosine** is silently dropped
   (gRPC parity), while the certainty *threshold* is a 422. The RFC should
   pick one behavior for both.
6. **Reserved-root collision rule — moot by construction (2026-07-08).**
   Earlier variants placed `{collection}` as the first segment after `/v1`,
   which shared the slot with the static roots; the reorder to
   `/v1/search/{collection}/near-text` makes `search` a static first
   segment, so a collection can be named anything (even `objects` or
   `backups`) without any routing ambiguity. Nothing for the RFC to settle
   here anymore.
7. **Default-ON gating** (`DISABLE_REST_SEARCH` opt-out) inverts the
   Weaviate preview convention (previews gate OFF, cf.
   `WEAVIATE_PREVIEW_NESTED_FILTERING`). Implemented as specified;
   flagged for the eng review.
8. **Absent-Content-Type residual — closed** by the swagger surface (415).
   The **body-size cap** is the remaining open residual: the swagger
   surface is uncapped (parity), so the pre-authentication allocation
   concern from the review now applies to every REST endpoint equally and
   should be solved server-wide if at all.
9. **Missing body is a 422** (go-swagger required-parameter validation),
   not the RFC's 400 for "malformed body". Swagger-owned; the RFC error
   table should carve out the swagger-native behaviors (405/415/602-422)
   or accept their shapes.

---

## 4. Local testing

Bring up a local instance with a vectorizer module (contextionary) and exercise
the endpoint by hand. Development uses a local fast-test harness
(`rest_search_neartext_smoke.sh`) that is intentionally **not committed** to
the repository; the steps below reproduce it without the script.

```bash
# contextionary (pick free host ports)
docker run -d --name rest-search-c11y -p 9998:9999 \
  -e EXTENSIONS_STORAGE_MODE=weaviate \
  -e EXTENSIONS_STORAGE_ORIGIN=http://host.docker.internal:8091 \
  semitechnologies/contextionary:en0.16.0-v1.2.1

# dev server
go build -o /tmp/wv ./cmd/weaviate-server
PERSISTENCE_DATA_PATH=/tmp/wv-data CLUSTER_IN_LOCALHOST=true \
CLUSTER_GOSSIP_BIND_PORT=7300 CLUSTER_DATA_BIND_PORT=7301 \
RAFT_BOOTSTRAP_EXPECT=1 GRPC_PORT=50071 CONTEXTIONARY_URL=localhost:9998 \
AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
DEFAULT_VECTORIZER_MODULE=text2vec-contextionary \
ENABLE_MODULES=text2vec-contextionary DISABLE_TELEMETRY=true \
PROMETHEUS_MONITORING_ENABLED=false \
/tmp/wv --scheme http --host 127.0.0.1 --port 8091 &
```

Create a collection with the vectorizer, insert a few objects (via
`POST /v1/schema` and `POST /v1/batch/objects`), then search:

```bash
curl -s -X POST -H 'Content-Type: application/json' \
  -d '{"query":["spaceship galaxy"],"limit":3,
       "return_properties":["title"],"return_metadata":["distance"]}' \
  http://127.0.0.1:8091/v1/search/Movie/near-text
# -> {"results":[{"id":"...","properties":{"title":"..."},"metadata":{"distance":0.12}}],"took_ms":8}
```

If you have the local harness, run it against the running instance
(`WEAVIATE_URL=http://127.0.0.1:8091 rest_search_neartext_smoke.sh`) for the
full assertion suite; otherwise the `curl` above plus the error cases from the
status table exercise the endpoint.

To check the two gated modes, restart the server with the relevant env var and
re-issue the same request:

- `DISABLE_REST_SEARCH=true` — the search request is rejected with 422.
- `OPERATIONAL_MODE=WriteOnly` — the search request is blocked with 503
  (search is a read).

---

## 5. Future work

- **QUERY re-introduction**: QUERY cannot live in the Swagger 2.0 spec, so
  it would return as a thin custom mount that *reuses* the same
  `search.Handler` (the 2026-07-06 draft on tag
  `rest-search-querypost-snapshot` is the reference implementation:
  middleware placement, CORS/415/405 handling, op-mode carve-outs).
- **hybrid / bm25 / near-object** endpoints per the RFC. After the
  2026-07-09 refactor each one is: a new request definition
  (`allOf[SearchCommon, {type-specific fields}]`) in `schema.json` + regen, a
  small `buildXParams` (the shared parsers on `SearchCommon` are already
  reusable), and a thin `Handler.X` wrapper over the generic `execute()` that
  returns the shared `SearchResponse` — no copy-paste of the auth/resolve/
  reply flow. Aggregate follows the same shape under `/v1/aggregate/
  {collection}`.
- **RAG params** (`single_prompt`/`grouped_task`): currently 422; needs
  `Cache-Control: no-store` (or equivalent) when implemented, since
  generation re-invokes paid LLM calls.
- **Dot-paths beyond one hop**, multi-target reference selection.
- **OpenAPI 3.x migration** would allow restoring string-or-array `query`
  (`oneOf`) and, if desired, opt-in strict rejection of unknown fields
  (`additionalProperties: false` — a breaking change, see the decision log).
- **Dedicated `requests_total` metric** (the graphql sibling handler has
  one) — deferred to GA.
