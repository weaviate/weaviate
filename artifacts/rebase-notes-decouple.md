# Rebase notes — hfresh-muvera-decouple-routing-rescore over rob/hfresh-muvera

Audience: whoever rebases `hfresh-muvera-decouple-routing-rescore` onto
`rob/hfresh-muvera` after the issue #276/#277/#278/#280 fixes landed.

## ⚠️ #277 — do not reintroduce the rescoreLimit cap (WILL conflict)

Commit `6aa73af4e0` ("decouple routing and rerank budgets...") rewrites the
`SearchByMultiVector` call site in `adapters/repos/db/vector/hfresh/search.go` as:

```go
routingBudget := max(searchProbe, rescoreLimit)
rerankBudget  := rescoreLimit
candidateIDs, err := h.searchByFDE(ctx, queryFDE, routingBudget, rerankBudget, allow)
```

`rerankBudget = rescoreLimit` **reintroduces issue #277**: with `k > rescoreLimit`
(default 350) the candidate pool — and therefore the number of returned results —
silently caps at `rescoreLimit`. The fix on `rob/hfresh-muvera` makes the candidate
pool `max(k, rescoreLimit)` (see the commented `max()` in `SearchByVector` and the
`k` passed through in `SearchByMultiVector`). When resolving the conflict, carry the
semantics over:

```go
rerankBudget := max(k, rescoreLimit)
```

Regression test that must keep passing: `TestSearchLimitNotCappedByRescoreLimit`
(hfresh package) — limits {10, 350, 400, 500} over 500 docs on both the single- and
multi-vector paths must return `min(limit, nDocs)` results.

Upstream guard for context: `Explorer.CalculateTotalLimit`
(usecases/traverser/explorer.go) caps `offset+limit` at `QueryMaximumResults`
(default 10 000), so `max(k, rescoreLimit)` is bounded.

## #276 — normalization + deterministic ties (context, semantic conflicts)

The same file now:

- normalizes query tokens in `SearchByMultiVector`/`QueryMultiVectorDistancer` and
  doc tokens in `maxSimScore` (cosine only, via `normalizeMultiVec` in hfresh.go);
  `insert.go`'s `AddMulti` normalizes tokens before `EncodeDoc`. Any rework of the
  routing/rescore split must keep tokens normalized on BOTH the encode and the
  rescore side for cosine, or self-recall collapses (~44-62% observed) — regression
  tests: `TestSearchByMultiVectorSelfRecallCosine`, `TestSearchByMultiVectorMaxSimOrderingCosine`.
- `ResultSet.Insert`/`searchByDistance` tie-break by `(distance, id)` ascending —
  `searchByFDE`'s copy of the posting-scan loop (it duplicates `ResultSet` usage)
  inherits this automatically, but if the decouple branch forked `ResultSet`, port the
  tie-break (test: `TestResultSetTieBreak`).

Also note: `6aa73af4e0`'s `searchByFDE` swallows the `EnqueueMerge` error (`_ =`);
the base branch propagates it — keep the base branch behavior.
