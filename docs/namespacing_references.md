# Namespacing — Cross-References (WS9)

Codebase-internal design note for WS9 of the namespaces workstream. Covers cross-reference write and read paths on namespace-enabled clusters.

> Source of truth for the workstream: [Implementation Log](https://www.notion.so/weaviate/Implementation-Log-34970562ccd6803cb7a0e4934cad5429), section WS9. This file describes *what we implement* and *where the hooks live*. The Notion log has the full rationale across the WS series.

## Goal

Keep cross-reference behavior namespace-local and portable on namespace-enabled clusters, without regressing non-namespace clusters.

**Invariants:**

1. **Beacons stored on disk carry the *short* target class name.** A cross-reference is an object property like `weaviate://localhost/Animal/<uuid>` regardless of which namespace the source object lives in. This is what makes per-namespace export/import (e.g. moving `customer1`'s data into a new cluster as `customer2`) work without rewriting object bytes.
2. **Internal operations on cross-refs use the *qualified* target class.** Authorization, schema lookup, target-object existence checks, and the multi-get of resolved targets all key on the qualified form (`customer1:Animal`).
3. **The namespace used for read-time enrichment is the *source object's* namespace, not the caller's.** A global admin retrieving a `customer1:Zoo` row gets its `hasAnimals` beacons resolved against `customer1:`, even though the admin has no namespace of their own.
4. **No cross-namespace references.** A namespaced caller cannot construct a beacon that points to a target in another namespace. A global admin reading rows from namespace A sees beacons that only ever resolve within A.
5. **Non-namespace clusters are unaffected.** The resolver is a no-op when `Namespaces.Enabled` is false.

Invariant 1 is the load-bearing storage-format decision; invariants 2–4 follow from it.

## Current state (post-WS4, pre-WS9)

- Source class names on objects (`models.Object.Class`) are qualified on disk — WS4 wired `m.resolveNS` into the regular object write paths. So a `Zoo` row created by a `customer1` principal is stored as `customer1:Zoo`.
- The four reference handlers (`AddObjectReference`, `UpdateObjectReferences`, `DeleteObjectReference`, `BatchManager.AddReferences`) use the namespace-unaware `m.resolveAlias`. The gRPC `BatchReferences` handler does no resolution of its own — it delegates straight to `BatchManager.AddReferences`. On namespace-enabled clusters all five paths therefore write whatever class name the user supplied — short for namespaced users, qualified for global admins — directly into the stored beacon. Authorization, schema lookup, and existence checks against that unqualified class then fail to find the qualified storage.
- The gRPC search parser at `parse_search_request.go:767-779` reads the `linkedClassName` for cross-ref properties from `schemaProp.DataType[0]` (always short) or `prop.TargetCollection` (always short on the wire), then calls `authorizedGetClass(linkedClassName)` directly. On namespace-enabled clusters the schema stores `customer1:Animal`, so the lookup returns nil and the parse fails before refcache runs. WS4 fixed the top-level `req.Collection` at `service.go:288` but missed the nested ref tree.
- REST `GET /objects/{class}/{id}` returns the stored beacon as-is, no target fetching. No enrichment is needed on this path.
- GraphQL endpoints are gated off on namespace-enabled clusters by an earlier WS, so they don't surface this break.

The pre-WS9 namespace-enabled state is therefore broken on both writes and reads for cross-refs.

## Write path — reference handlers

**Files (all under `usecases/objects/`):** `references_add.go`, `references_update.go`, `references_delete.go`, `batch_references_add.go`. The gRPC `BatchReferences` handler at `adapters/handlers/grpc/v1/batch/handler.go` needs no change — it delegates straight to `BatchManager.AddReferences`.

### Source class

For each handler, replace the alias-only `m.resolveAlias` with `m.resolveNS` (which wraps `namespacing.Resolve`) for the source class. The qualified source class flows through authz, schema fetch, and source-object existence checks — same pattern WS4 established for the regular object write paths.

For batch: `validateReferencesConcurrently` parses each beacon's `From` URI; we then resolve `From.Class` (on `*crossref.RefSource`) through `b.resolveNS` so the qualified form reaches `GetCachedClass` and the source-side authz batch. The repo internally uses `From.Class` for shard routing; the source class never gets serialized into a beacon URI (only the *target* class does), so there is no storage-format concern on the source side.

### Target class — two views

The handlers must derive both:

- **Operational (qualified):** `customer1:Animal`. Used for `authorizer.Authorize(READ, ShardsData(target.Class, tenant))`, `validator.ValidateExistence` (existence-checks the target object), and multi-tenancy class lookups.
- **Storage (short):** `Animal`. Used to construct the `crossref.Ref` / `crossref.NewLocalhost` that the repo writes into the source object's property bytes. The beacon URI on disk uses this short form.

Computing both views, given a parsed user-supplied target class from `crossref.Parse(input.Ref.Beacon)`:

```go
qualifiedTarget, _, err := m.resolveNS(principal, targetRef.Class)
if err != nil {
    return &Error{err.Error(), StatusUnprocessableEntity, err}
}
shortTarget := namespacing.StripQualification(qualifiedTarget)
```

`StripQualification` is a new helper sibling to `namespacing.NamespaceFromQualified` (~5 lines using `strings.Cut`). It returns the substring after the namespace separator, or the input unchanged if there's no separator. On non-namespace clusters `qualifiedTarget` and `shortTarget` are identical, so this stays a no-op.

`shortTarget` then drives the beacon rewrite:

```go
input.Ref.Class  = strfmt.URI(shortTarget)
input.Ref.Beacon = strfmt.URI(crossref.NewLocalhost(shortTarget, targetRef.TargetID).String())
targetRef.Class  = qualifiedTarget   // for downstream authz / existence
```

The split matters because the two values flow to different layers:

- `targetRef.Class` (qualified) is the in-memory copy used by the immediately-following `authorizer.Authorize` and `validator.ValidateExistence` calls.
- `input.Ref.Beacon` (short) is what `crossref.ParseSingleRef(&input.Ref)` reparses into `target` later in the handler, and `target` is what `m.vectorRepo.AddReference(ctx, source, target, ...)` writes into the source object's property bytes.

The `input.Ref.Beacon` rewrite is what makes the global-admin write case correct: an admin submitting `customer1:Animal` in the beacon ends up with `Animal` on disk, identical to what a namespaced user produces, so beacons stay namespace-portable regardless of writer.

**Note on global-admin short-class submissions on NS-enabled clusters.** A global admin submitting a *short* class name (e.g. `Animal` directly) on a namespace-enabled cluster fails downstream because the schema has no plain `Animal` entry — this is the WS4-inherited behavior, not WS9. Global admins must address classes by their qualified storage names. WS9 does not change this contract.

### Autodetect path

When the user submits a beacon without a class (`weaviate://localhost/<uuid>` only) and `targetRef.Class == ""`, `m.autodetectToClass` reads `prop.DataType[0]` from the source class's schema. `schema.ValidateClassName` rejects `:`, so DataType entries are always short — autodetect yields a short class directly. We then run `m.resolveNS` on it (now serves only to qualify for the ops view) and apply the same `StripQualification` step for the storage view. The non-autodetect and autodetect paths converge on the same final state.

### Batch target

`To.Class` on `*crossref.Ref` gets the same two-view treatment. To avoid mutating the storage struct mid-flight, hold the qualified form in a parallel `[]string` keyed by ref index (used for `validateReferenceMultiTenancy`, target-side authz, and the cached-class fetch). The `refs[i].To.Class` field that travels to `AddBatchReferences` stays short. The qualified array lives in the function scope of `BatchManager.AddReferences` and never leaves it.

### Cross-namespace deny

Falls out of `namespacing.ValidateNamespacePrefix` inside `m.resolveNS`. A namespaced caller sending a qualified beacon (e.g. `customer2:Animal`) is rejected with **422 Unprocessable Entity** ("is not a valid class name") at the prefix validator, before any storage work. Same error code applies to a malformed `:` prefix from any caller. The handlers wrap the resolver error in `&Error{..., StatusUnprocessableEntity, err}`; the REST layer maps both `StatusBadRequest` and `StatusUnprocessableEntity` to 422 for the references endpoints, so the error code on the wire is consistent.

### Delete

Short stored beacon + short submitted beacon (namespaced user) means `ref.Beacon == remove.Beacon` in `removeReference` matches directly. **Delete therefore calls `m.resolveNS` only on the source class** (for authz on `ShardsData(input.Class, tenant)` and for source-object lookup). The target side does **not** need qualification — no `resolveNS` on the beacon class, no beacon URI rewrite — but it does need a **prefix check**:

```go
if beacon.Class != "" {
    if err := namespacing.ValidateNamespacePrefix(principal, nsEnabled, beacon.Class, "class"); err != nil {
        return &Error{err.Error(), StatusUnprocessableEntity, err}
    }
}
```

This makes the cross-namespace deny consistent across all three single-ref endpoints: a namespaced caller submitting `customer2:Animal` in a delete beacon gets the same 422 they'd get on add or update, instead of a silent 204 no-content from a string-mismatched comparison.

The autodetect-replacement branch (`input.Class != "" && beacon.Class == ""`) still fills in the missing class from schema, which produces the short form directly — no further work needed.

For the global-admin delete case (admin submits `weaviate://localhost/customer1:Animal/<uuid>`), `ValidateNamespacePrefix` accepts a qualified prefix from a global principal, then the submitted beacon does not match the stored short beacon as a string and the delete is a no-op. This is consistent with the storage invariant: admins must speak the storage form for raw operations. If we wanted admins to be able to delete refs via qualified beacons, we'd need to normalize-then-match — out of scope for WS9 and arguably a WS12 concern.

### Deprecated REST endpoints

The `input.Class == ""` paths in `AddObjectReference` / `UpdateObjectReferences` / `DeleteObjectReference` (deprecated cross-class endpoints that infer the source class via `ObjectByID`) are out of scope for WS9. On namespace-enabled clusters they remain functionally untouched: my `m.resolveNS` call guards on `input.Class != ""`, so the inferred-class path falls through to the existing logic. WS13 owns the question of disabling these endpoints on NS clusters entirely.

## Read path — gRPC search parser

**File:** `adapters/handlers/grpc/v1/parse_search_request.go`. No refcache changes.

**Why the parser, not the cacher:** refcache builds its `multi.Identifier` from `selectPropRef.ClassName` (the request filter), not from the parsed beacon — see [cacher.go:173-176](adapters/repos/db/refcache/cacher.go#L173-L176) and [resolver.go:206-209](adapters/repos/db/refcache/resolver.go#L206-L209). The class of the parsed beacon is informational; only `TargetID` flows into the multi-get. So once the parser hands the refcache a qualified `selectPropRef.ClassName`, every downstream lookup hits the right shard. Beacons can stay short on disk without the refcache caring.

**Current state.** WS4 already qualifies the top-level `req.Collection` at [service.go:288](adapters/handlers/grpc/v1/service.go#L288). The nested ref class names in the select-properties tree were missed:

```go
// parse_search_request.go:767-779
var linkedClassName string
if len(schemaProp.DataType) == 1 {
    linkedClassName = schemaProp.DataType[0]   // short — schema rejects ":"
} else {
    linkedClassName = prop.TargetCollection    // from request, also short
}
linkedClass, err := authorizedGetClass(linkedClassName)  // fails on NS clusters
```

On a namespace-enabled cluster the schema stores `customer1:Animal`, so `authorizedGetClass("Animal")` returns nil. The parser breaks before refcache ever runs.

**Change.** Extract the parent class's namespace from `className` (which arrives qualified at this layer — it's either the top-level qualified `req.Collection` or, in the recursive call below, the previously-qualified linked class) and stitch it onto `linkedClassName` via `namespacing.QualifiedName`:

```go
parentNS := namespacing.NamespaceFromQualified(className)
qualifiedLinked := namespacing.QualifiedName(parentNS, linkedClassName)
linkedClass, err := authorizedGetClass(qualifiedLinked)
// ... and pass qualifiedLinked into the recursive call at line 787 and the
// SelectClass.ClassName at line 816
```

The recursive call at [line 787](adapters/handlers/grpc/v1/parse_search_request.go#L787) passes `linkedClassName` as the new `className` for nested refs, so the namespace propagates down the chain naturally — no separate plumbing needed for multi-level refs.

Non-namespace clusters: `NamespaceFromQualified("Zoo")` returns `""`, `QualifiedName("", "Animal")` returns `"Animal"` unchanged. No-op.

**Multi-target refs (`prop.TargetCollection` from the request):** the user submits short and the same `QualifiedName(parentNS, target)` stitching applies. Before stitching, call `namespacing.ValidateNamespacePrefix(principal, namespacesEnabled, prop.TargetCollection, "class")` so a namespaced caller cannot smuggle a foreign namespace prefix in (rejected with 422, same wording as the write-path deny). After validation `parentNS` is fixed by the parent class, so any successfully-stitched name resolves inside the source's namespace.

**REST GET object:** unaffected — returns the stored beacon as-is, no target fetching. No change needed.

**GraphQL:** disabled on namespace-enabled clusters by an earlier WS (`Disable GQL endpoints for namespaces`); not a concern here.

## Schema-create cross-ref existence check

The RAFT `AddClass` handler at [cluster/schema/manager.go:182-200](cluster/schema/manager.go#L182-L200) validates that every cross-ref `Property.DataType` entry points to an existing class — but it does the existence check against a short class name (`Animal`) while storage is qualified (`customer1:Animal`). On namespace-enabled clusters the check always fails with "reference property to nonexistent class", blocking creation of any cross-ref schema.

Same fix as the gRPC parser: extract the parent class's namespace from `req.Class.Class` (which arrives qualified) and stitch it onto each DataType entry before the `s.schema.classExists` lookup. Self-reference comparison also has to use the qualified form so `dt == req.Class.Class` still works on NS clusters.

```go
parentNS := namespacing.NamespaceFromQualified(req.Class.Class)
for _, prop := range req.Class.Properties {
    for _, dt := range prop.DataType {
        qualifiedDT := namespacing.QualifiedName(parentNS, dt)
        if qualifiedDT == req.Class.Class { continue }       // self-ref
        if s.schema.classExists(qualifiedDT) { continue }    // exists
        return ErrRefToNonexistentClass
    }
}
```

`parentNS` is `""` on non-namespace clusters, so the qualification is a no-op there and the existing behavior is preserved.

## Non-goals (split out to other workstreams)

- **Response stripping** of qualified class names from REST/gRPC output is **WS12**. WS9 may leave qualified internal forms in some response shapes (e.g. the resolved target's `Class` field in a gRPC ref-resolved response); that's intentional.
- **Schema-response handling for reference DataTypes**. The schema constrains `Property.DataType` to short forms (`:` rejected by `ValidateClassName`), and the schema-read side returns short already. Schema-write also needs **one targeted change** for cross-ref data types — see "Schema-create cross-ref existence check" below.
- **OIDC principal classification** and **operator-only enforcement** are owned by WS6 and WS13.
- **Backfill / migration** of existing data on namespace-enabled clusters. The RFC scopes WS9 to new-cluster-only rollout; no upgrade story for clusters that already wrote pre-WS9 (mixed-form) beacons. If such clusters exist, the operator path is to recreate them.
- **Admin operations via qualified beacons** (e.g. global admin deleting a ref by submitting a qualified beacon). Operations match the storage form. Admins working at the raw beacon layer must use the short form. Higher-level admin tooling that normalizes is out of scope.

## Test plan

**Unit (in `usecases/objects/`):**

- `Test_References_NamespaceResolution_Add` — namespaced principal: source class qualifies for authz; target class qualifies for authz/existence; **stored beacon URI on `AddReference` carries the short target class**; cross-namespace qualified target rejected with 422; global admin submitting qualified target: writes short on disk; global on non-namespace cluster: pass-through.
- Same shape for Update, Batch.
- Delete: source qualifies for authz; **no target resolveNS call**; submitted short beacon matches stored short beacon byte-for-byte; namespaced user submitting `customer2:Animal` in delete beacon → 422 (consistent with add/update); admin submitting qualified beacon → 204 no-op (string mismatch).
- Regression: the `*crossref.Ref` (single) and `[]*crossref.Ref` (batch) passed to the repo always have `Class` in short form after WS9, regardless of caller type.

**Namespacing package (`usecases/schema/namespacing/`):**

- Table-driven test for the new `StripQualification` helper: `customer1:Foo` → `Foo`, `Foo` → `Foo`, empty → empty, `customer1:` → `""` (degenerate), `customer1:customer2:Foo` → `customer2:Foo` (single split).

**Parser (in `adapters/handlers/grpc/v1/`):**

- Subtest where top-level class is qualified (`customer1:Zoo`), schema's `hasAnimals.DataType` is short (`Animal`), and assert produced `SelectClass.ClassName` is `customer1:Animal`.
- Recursion test for two-level refs: top-level `customer1:Zoo`, nested `Animal.hasHabitat: Habitat`, assert both levels stitch the qualified namespace through.
- Non-namespace cluster regression: top-level `Zoo`, assert `linkedClassName` stays `Animal` unchanged.
- Multi-target ref: `prop.TargetCollection = "customer2:Animal"` from a `customer1` principal → 422 at the prefix validator.

**Acceptance (`test/acceptance/namespace/references_test.go`):**

- Single-ref add/update/delete inside one namespace via REST + gRPC.
- Batch refs inside one namespace, via REST and gRPC.
- Cross-namespace beacon (`customer2:Animal` from a `customer1` user) rejected with 422 on add and on batch add (per-ref `Err`).
- Stored-shape invariant: after add, fetch the source object via admin and assert the `hasAnimals` property contains a beacon with the short target class (`Animal`, no `:`).
- **Read-side end-to-end:** gRPC search with `return_references` against a namespaced row resolves the target object inline; assert the resolved target's `class` field is the qualified form and matches the source's namespace.
- Global admin reading a namespaced row resolves refs via the row's namespace, not the admin's empty namespace.
- Two namespaces with identical short class names + identical UUIDs — user1 reading user1's `Zoo` only sees user1's `Animal` rows.

## Implementation order

Each step is independently reviewable and the package builds after every step.

1. **`namespacing.StripQualification` helper + table-driven test.** ~10 lines of code + ~20 lines of test. No callers yet.
2. **Write path:** revert the beacon-URI mutation introduced in the in-progress branch; add the two-view computation in `references_add.go`, `references_update.go`, `references_delete.go`, `batch_references_add.go`. Existing unit-test scaffolding in `usecases/objects/references_namespace_test.go` (from the in-progress PR) gets updated to assert *short* on the stored beacon. ~150 lines of code change, ~50 lines of test updates.
3. **Read path:** qualify `linkedClassName` in `adapters/handlers/grpc/v1/parse_search_request.go`. Add validation for `prop.TargetCollection`. Add parser tests covering single-target, multi-target, recursion, cross-namespace deny, non-namespace pass-through. ~20 lines of code, ~80 lines of tests.
4. **Schema-create cross-ref existence check:** qualify each `Property.DataType` entry in `cluster/schema/manager.go` before the `classExists` lookup. ~5 lines.
5. **Acceptance:** update `test/acceptance/namespace/references_test.go` (also in the in-progress branch) — add the read-side gRPC test, the stored-short assertion, and the two-namespaces-same-uuid isolation test.
6. **Lint + go test sweep** on the touched packages.
