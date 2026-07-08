# Posting expansion (exploratory) — status

This branch preserves the exploratory posting-expansion work that was
extracted from the `hfresh-muvera-decouple-routing-rescore` PR (it is not
part of the decoupling deliverable).

## What it is

A recall-recovery step for HFresh+MUVERA search: after the RQ1 candidate
scan, the top candidates' OTHER postings (documents live in up to
`replicas` postings) are scanned too, discovering candidates the initial
centroid routing missed. Implemented as:

- `posting_expansion.go` — `DocToPostings`, a lazily-built in-memory
  reverse map from document ID to posting IDs, plus the
  `topKDocsForExpansion` / `maxAdditionalPostings` budget constants.
- `search.go` — `expandPostings`, called from `searchByFDE` when
  `h.docToPostings != nil`; `scannedCentroids` tracks postings already
  visited so expansion never re-scans.
- `hfresh.go` — the `docToPostings` field, initialized unconditionally
  whenever muvera is enabled (i.e. the feature is ON by default here).

## State as of extraction (2026-07-08)

- Compiles; `posting_expansion_test.go` and
  `TestPostingExpansionIntegration` (search_fde_test.go) green; full hfresh
  race suite green on this exact commit's parent.
- With routing budgets that already cover all postings (small corpora, or
  searchProbe >= posting count), expansion finds nothing and early-returns;
  its measurable effect only appears with narrow probes on large corpora.
- Known costs not yet addressed: the reverse map is built lazily on the
  FIRST search (latency spike) and lives fully in memory; no invalidation
  strategy is implemented for splits/merges/reassigns beyond rebuilds, and
  the interaction with `TestSearchProbeChangesResults`-style probe
  semantics (expansion deliberately masks narrow probes) needs a product
  decision: a probe knob and an always-on recall-recovery step pull in
  opposite directions.

## Open questions before productionizing

1. Should expansion be gated behind a config flag (off by default) instead
   of always-on with muvera?
2. Reverse-map maintenance under splits/merges/reassigns — incremental
   updates vs rebuild; memory budget per shard.
3. Interaction with the searchProbe contract: expansion converts a low
   probe's latency win back into scan work; decide which knob wins.
