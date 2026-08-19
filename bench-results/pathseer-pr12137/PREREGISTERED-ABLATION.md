# PathSeer prefilter ablation — pre-registered protocol

Committed **before any measurement run** (smoke runs at toy corpus sizes are
excluded from analysis and never cited). Follow-up to the closed PR #12137
evaluation (`PREREGISTERED.md`, `FINDINGS.md`). Date: 2026-08-19. Timebox:
one day; results feed a routing decision the next morning.

Question: does the PR's compound prefilter condition (skip only when the
popped parent fails the filter AND `results.Len() >= ef`, plus the two-hop
burst) buy anything over a single-condition variant? Verdict vocabulary
(exactly one will be written in FINDINGS-ABLATION.md): **"replaceable"** /
**"condition earns its keep"** / **"mixed"**.

## Fairness rules

Same as the closed round: PR evaluated as submitted (head `1c144e4c73`),
variants live on `pathseer-eval` and are labeled `ours-*`; no tuning of any
arm; no dropped runs outside the pre-registered drop order; single-threaded,
in-process, warmup pass + 3 timed passes, medians; counters on with the
burned-map OFF (`-trackburned=false`); no H4 reorder in any arm; no seeding
variant; no centered RQ. The paired comparison uses only rows measured in
this session — closed-round timings are not reused for arm-vs-arm deltas.

## Arms

1. **sweeping** — reference; bounded exactly as the closed round below 400k
   members (ef {64,128}, first 200 queries).
2. **pathseer** — the PR as submitted, rerun in-session.
3. **ours-no-twohop** — PR with the two-hop expansion block disabled;
   compound prefilter condition unchanged. Isolates the two-hop burst
   causally.
4. **ours-guard** — one-hop prefilter gated ONLY by `results.Len() >= ef`;
   no parent-fails check, no two-hop. Skipped neighbors stay visited-marked
   (correct under this design: once results are full a non-member is never
   wanted again).
5. **ours-puredos** — unconditional prefilter, no guard; runs ONLY at
   random_10k and BEIR neg40 (200 queries each) to document its failure
   mode.

acorn joins only at BEIR neg10 (its winning cell in the closed round), as
the paired reference.

## Cells

wiki-dpr 10M, plain rq1, same filter IDs as the closed round: random_10k;
conj_topical1m_a_idmid (238k); conj_topical1m_a_idlo (531k); topical_2m_b
(2M); topical_5m_b (5M); anticorr τ=0.01 × 10 and τ=0.03 × 10 — the 10
lowest-numbered pivots per τ: q0041, q0059, q0064, q0073, q0077, q0080,
q0085, q0095, q0117, q0126. τ=0.01 and τ=0.03 judged at matched recall; any
τ=0 by regret.

BEIR 500k under f32 AND rq1: neg10, neg40, pos40 (all 22 category filters
per family, all 1000 queries, exact GT — the closed round's sidecar).

Degeneracy probe: subef_32, ours-guard only, 5 queries, ef 64, 1 timed pass
— documents that the guard inherits H3's degeneracy by construction; the
counter-proposal needs the same `min(ef, |allowlist|)` gate we asked of the
PR.

Grid: ef ∈ {64, 128, 256, 512} except the sweeping bound and the probe.
Run order (timebox-aware): BEIR f32 → BEIR rq1 → wiki 10M; within each run,
meta order. Sequential runs only (timing fidelity).

**Drop order if the day runs out:** (1) the 5M cell, (2) pos40, (3) τ=0.03
anticorr, (4) ef512 rows. Never dropped: random_10k, neg10, neg40, the
paired in-session pathseer rerun.

## Tie rule (decides the verdict)

Per cell: `ours-guard` TIES the PR if QPS-at-recall-0.90 (ef-interpolated)
is within ±3% AND recall@10 at matched ef is within −0.005. The PR **wins
materially** if it beats guard by >5% QPS at matched recall or >0.01 recall
at matched ef. Overall: "replaceable" = guard ties everywhere and loses
materially nowhere; "condition earns its keep" = PR wins materially in ≥1
non-degenerate cell; otherwise "mixed", with the exact cells and deltas.

## Predictions (verbatim from the brief)

- **PA1:** ours-no-twohop ties the PR (±3%) in every cell. The two-hop
  burst is vestigial causally, not just correlationally.
- **PA2:** ours-guard ties the PR in the dispersed band. Mechanism: at 0.1%
  the PR's distance count is ef/s almost exactly (65.6k at ef64, 130k at
  ef128 = ef × 1/s), i.e. its work IS the starvation phase, which guard
  shares by construction. Check: guard's distance count at random_10k within
  ±10% of the PR's.
- **PA3 (the open one):** guard vs PR at NEG (neg10, neg40, anticorr). If
  guard ties, the parent-fails condition buys nothing and the verdict is
  "replaceable". If the PR wins materially in NEG, the condition earns its
  keep — the mechanism would be bridges from passing parents mattering
  after results fill.
- **PA4:** ours-puredos collapses at random_10k (recall < 0.3 or empty
  results — it cannot cross non-member territory at all) and lands
  materially below guard at neg40 (depleted entry region). If it does NOT
  collapse, say so loudly — that would mean even the guard is superfluous.
- **PA5 (formula check, analysis only):** in the dispersed band, graph cost
  ≈ (ef/s) × per-node overhead for both the PR and guard. Fit it across
  the dispersed cells (closed-round per-filter data + this round's guard
  rows) and report the per-node constant — it becomes the graph side of the
  routing-threshold formula against the scan's members × 64B.

## Deliverables

`data/ablation.csv` (one row per corpus × filter × arm × ef × pass, standard
counters); one frontier plot per cell with the five arms overlaid;
`FINDINGS-ABLATION.md` with each PA confirmed/refuted plus deciding number,
the single verdict word, and the two ready-to-paste paragraphs (John
message, PR follow-up comment).
