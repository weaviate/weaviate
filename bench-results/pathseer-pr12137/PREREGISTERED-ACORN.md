# Sparse-NEG at 10M — pre-registered protocol (the ACORN decision)

Committed **before any measurement run** (dataset construction is not a
measurement; smoke runs excluded from analysis). Follow-up to the closed
evaluation (`PREREGISTERED.md`) and ablation (`PREREGISTERED-ABLATION.md`).
Date: 2026-08-20. Timebox: one day.

## Question and decision rule (verbatim from the brief)

ACORN's only win across both closed rounds is BEIR neg10 (500k, 10% NEG).
The 10M grid never contained sparse/mid NEG cells. This round fills that
hole. The decision is made only in the **decision band**: members above the
scan threshold (~306k at ef128 by m\* = √(73.4·ef·N)) and ratio below the
0.4 gate — i.e. ~300k to 4M on 10M. Verdicts (exactly one will be written):

- **"ACORN survives"** — engaged ACORN beats PathSeer materially at ≥1
  cardinality inside the decision band.
- **"deprecation supported"** — PathSeer ties or beats ACORN (ablation
  margins) at every point inside the band.
- **"mixed"** — name the flip point and the margins on each side.

Materiality = the ablation's thresholds: >5% QPS at matched recall 0.90, or
reaching a recall the other cannot within the ef grid.

## Fairness rules

As before: no tuning, no dropped runs outside the pre-registered drop order,
PR evaluated as submitted (head `1c144e4c73`), single-threaded in-process,
warmup + 3 timed passes, medians, counters on with burned-map off. The only
tooling change is a `-acornratio` harness flag (cmd/pathseerbench, eval
branch) so the acorn arm can be engaged at every ratio; no changes to the
code under test in `adapters/`.

## The neg-sparse family (new, mirrors anticorr methodology)

Queries: the first 50 anticorr pivot rows (41, 59, 64, 73, 77, 80, 85, 95,
117, 126, …, 506) — same instrument as the anticorr family. For query q and
cardinality m: members = uniform random sample of size m from vectors with
similarity rank to q > K, K = 100,000 (the anticorr family's near-field
exclusion depth). Seed 42, one bitmap per (query, m), exact GT (top-100 by
dot over the allowlist; corpus is L2-normalized). 50 filters per point,
aggregated in reporting like the anticorr rows.

This family is simultaneously the perfect-bubble probe: the nearest member
is past a 100k-vector excluded near field; entry-crossing cost is read from
the existing counters (nodes popped, pop index at results-full, seed
distance computations for acorn) — ns-per-crossed-node = mean latency /
nodes popped.

Cardinality points: **50k** (0.5%, BEIR-cardinality repro, outside the
band), **300k** (3%, band bottom), **1M** (10%, ratio repro), **4M** (40%,
gate edge); optional if the day allows: **150k, 2M** (constructed and
scheduled; covered by the drop order).

Gate-edge documentation (from code, `search.go:214`): the production gate
disables ACORN only when ratio is **strictly greater** than 0.4, so at
exactly 4M/10M ACORN stays engaged; the per-query `strategy_used` column
records what production would have run at every cell.

## Arms

1. **acorn, engaged** — `-acornratio 1.0`; production-gate behavior recorded
   per cell (ratio vs 0.4, plus RRE routing counts).
2. **pathseer** — the PR as submitted.
3. **sweeping** — reference; bounded at sparse cells exactly as before
   (ef {64,128}, 200-query cap — moot here, cells have 50 queries).
4. **scan** — `cmd/filteredscan` (feat/filtered-scan, the shipping scan with
   its centered-rq1 configuration, same as all prior scan rows) at 50k,
   300k, 1M only, B1 ∈ {4096, 32768}, B2 = 700. One pass: recall is
   deterministic and is the PN4 criterion; latency indicative.

Graph arms: ef ∈ {64, 128, 256, 512}. Cells where a graph arm never reaches
0.90 keep their full ef grid rows (evidence for the ACORN-collapse issue).

## Predictions (verbatim)

- **PN1 (ratio repro):** at 1M/10%, engaged ACORN's BEIR-neg10 advantage
  reappears at least in sign (ACORN ≥ PathSeer at matched 0.90), because
  induced degree is preserved and H5 says degree is the axis. Named
  outcomes: reappears / shrinks below materiality / inverts.
- **PN2 (scale stress):** at 50k/0.5%, ACORN collapses (recall < 0.7 at
  every ef) — induced degree ~0.3 is far below the navigability floor the
  10M POS families located.
- **PN3 (the flip):** the transition sits between 300k (3%) and 1M (10%).
  Where it sits relative to 306k decides the verdict: if ACORN only wins
  above ~1M, its surviving territory is 1M-4M NEG — real but narrow; if it
  already wins at 300k, it keeps the whole band.
- **PN4 (scan under NEG):** with B1=32768 the scan holds ≥0.99 at 50k-1M
  on this family at unchanged latency (the τ=0.01 mechanism and fix,
  transplanted). If B1=4096 shows the retention gap again, report both.
- **PN5 (bubble crossing):** entry cost ranks acorn (seeding, filter-check
  priced) < pathseer ≈ sweeping (distance priced) at every point; report
  ns-per-crossed-node implied by the counters.

## Timebox drop order

Optional points first (2M, then 150k), then filters/point 50 → 25, then
ef512 rows. Never drop 300k, 1M, or the paired pathseer rows.

## Deliverables

`data/neg-sparse.csv` (one row per filter × arm × ef|B1 × pass, standard
counters, engaged-vs-production-gate flag derivable from ratio +
strategy_used); one frontier plot per cardinality point, four arms;
`FINDINGS-ACORN.md` with the verdict word, PN1–PN5 with deciding numbers,
and three ready-to-paste paragraphs (John decision-2 close; the internal
ACORN-collapse issue with its ef curves; the routing-doc paragraph naming
the NEG-sparse band's owner with the scan's B1 verdict folded in).
