# Rebase notes — hfresh-muvera-decouple-routing-rescore over rob/hfresh-muvera

Audience: whoever rebases `hfresh-muvera-decouple-routing-rescore` onto
`rob/hfresh-muvera` after the issue #275/#276/#277/#278/#280/#281 fixes landed
(`5dd13ea43b..80f17dda5c`). Merge-base at the time of writing: `3384d2b2ff`.

Files BOTH branches modify (guaranteed conflicts):
`adapters/repos/db/vector/hfresh/{search.go, insert.go, hfresh.go, helper_for_test.go}`.
The decouple branch additionally touches `multivector/muvera.go`,
`index_metadata.go`, `reassign.go`, `posting_expansion.go` (new), and
`shard_init_vector.go` — no conflicts expected there, but see the semantic
notes below.

## ⚠️ 1. #277 — do not reintroduce the rescoreLimit cap (search.go)

Commit `6aa73af4e0` ("decouple routing and rerank budgets...") rewrites the
`SearchByMultiVector` call site as:

```go
routingBudget := max(searchProbe, rescoreLimit)
rerankBudget  := rescoreLimit
candidateIDs, err := h.searchByFDE(ctx, queryFDE, routingBudget, rerankBudget, allow)
```

`rerankBudget = rescoreLimit` **reintroduces issue #277**: with
`k > rescoreLimit` (default 350) the candidate pool — and therefore the
number of returned results — silently caps at `rescoreLimit`. On
`rob/hfresh-muvera` the candidate pool is `max(k, rescoreLimit)` (commented
`max()` in `SearchByVector`, `k` passed through in `SearchByMultiVector`).
Carry the semantics over:

```go
rerankBudget := max(k, rescoreLimit)
```

Must-keep-passing test: `TestSearchLimitNotCappedByRescoreLimit` — limits
{10, 350, 400, 500} over 500 docs on both paths (the limit==nDocs case has a
small tolerance for approximate centroid retrieval; don't tighten it).

## ⚠️ 2. #276 — cosine normalization + folded rescore (search.go, insert.go)

- `insert.go` `AddMulti` on the base branch normalizes tokens before
  `EncodeDoc` (`normalizeMultiVec`). The decouple branch's `AddMulti`
  predates this — when resolving, KEEP the normalization or cosine
  self-recall collapses to ~44-62% (e2e TC-006/TC-018).
- `search.go` `SearchByMultiVector` / `QueryMultiVectorDistancer` normalize
  query tokens once per query; `maxSimScore` dispatches cosine to
  `maxSimScoreCosine`, which FOLDS the document token inverse norms into the
  dot product instead of normalizing (perf: a normalization pass per
  candidate costs ~+40% p50). If `searchByFDE`/posting-expansion re-plumb the
  rescore, keep `maxSimScoreCosine` as the cosine scorer.
- `ResultSet.Insert`/`searchByDistance` tie-break by `(distance, id)`
  ascending. `searchByFDE` duplicates the posting-scan loop over `ResultSet`
  — it inherits the tie-break automatically unless the copy forked the type.

Must-keep-passing tests: `TestSearchByMultiVectorSelfRecallCosine`,
`TestSearchByMultiVectorMaxSimOrderingCosine`, `TestResultSetTieBreak`.

## ⚠️ 3. #275 — empty-collection guard (search.go, hfresh.go)

`SearchByMultiVector` returns `ErrMuveraNotInitialized` (defined in
hfresh.go) when `dims == 0`, BEFORE calling `EncodeQuery` — the encoder's
projection matrices are nil until the first `AddMulti` and `EncodeQuery`
panics on them. `searchByFDE` receives the already-encoded FDE, so the guard
must stay in `SearchByMultiVector` ahead of the encode step.

Must-keep-passing test: `TestSearchByMultiVectorOnEmptyCollection`.

## ⚠️ 4. #278 — Add/add split (insert.go)

`Add` rejects single vectors when muvera is enabled (they would initialize
`dims` with token dimensionality and corrupt the index) and delegates to the
private `add`; `AddMulti` inserts the encoded FDE via `add`, NOT via `Add`.
The decouple branch's `AddMulti` calls `h.Add(ctx, docID, encoded)` — after
the rebase that would reject every muvera insert. Use `h.add(...)`.
`ValidateBeforeInsert` carries the same muvera guard, and
`ValidateMultiBeforeInsert` also rejects empty tokens (`[[]]`).

Must-keep-passing tests: `TestSingleVectorRejectedOnMuveraIndex`,
`TestValidateMultiBeforeInsertEmpty`.

## 5. #281 — bounds are create/update-only (no conflict, semantic note)

Muvera upper bounds (ksim ≤ 10, dprojections ≤ 1024, repetitions ≤ 256,
combined FDE ≤ 2^20) live in `enthfresh.ValidateMuveraUpperBounds`, called
from `usecases/schema/class.go:validateCreateUpdateOnlyBounds` — NOT from
`ParseAndValidateConfig`, which also runs on startup/RAFT-replay where a
persisted out-of-range class must not block boot (`hfresh.New` warns
instead). If the decouple branch adds config fields, keep that split.

## 6. helper_for_test.go

Both branches extend it. Base branch adds variadic `testIndexOption` +
`withDistanceProvider` (cosine must be settable — the L2 default masked the
#276 bug). Decouple adds its own helpers (~98 lines). Union-merge; keep both.

## 7. Misc

- `6aa73af4e0`'s `searchByFDE` swallows the `EnqueueMerge` error (`_ =`);
  the base branch propagates it — keep the base behavior.
- The base branch also changed, outside hfresh (no decouple overlap, listed
  for awareness): `shard_read.go` (single-vector query on multi-vector index
  → clear error), `usecases/objects/validation/vector_validation.go`
  (schema-aware shape/emptiness validation), `entities/models/vectors.go` +
  `tools/swagger_custom_code/main.go` (empty JSON arrays survive unmarshal;
  regenerating swagger code must run the injector), and
  `usecases/schema/class.go` (create/update-only bounds hook).
