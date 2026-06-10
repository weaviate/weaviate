# Common Indexing Mistakes - Demo UI

A visual demo of five typical Weaviate indexing misconfigurations against a
1,000,000-object product catalog. Each card runs a pre-canned query and shows
wall-clock latency, total match count (where applicable), and the top result
rows so the failure mode is visible at a glance.

## 1. Import the data

```bash
# Local Weaviate on :8080 / grpc :50051
.venv/bin/python tools/dev/bench/demo_indexing_mistakes_import.py

# Or against a Weaviate Cloud cluster
WCD_URL=https://cluster.weaviate.cloud WCD_API_KEY=... \
  .venv/bin/python tools/dev/bench/demo_indexing_mistakes_import.py
```

The script drops and recreates the `IndexingMistakesDemo` collection and
imports 1,000,000 deterministic synthetic product objects (seed 42). Each
object carries a ~3 KB unindexed `review_summary` payload so the catalog has
the per-object weight of a real product database - that weight is what the
aggregation cards (4, 5) make visible.

## 2. Open the UI

`start.py` runs a tiny same-origin proxy: static files at `/`, everything
under `/api/*` is forwarded to the dev cluster with `WCD_API_KEY` injected
as a Bearer token server-side. The browser never sees the API key, and the
cluster's CORS policy (which only allows the console origin) is irrelevant.

```bash
cd tools/dev/bench/demo_indexing_mistakes_ui
WCD_API_KEY=$(wcs --dev token) ./start.py
# then open http://localhost:8089
```

If `WCD_API_KEY` is not set, `start.py` exits with a clear error.

## 3. Run each card

| # | Card | Misconfigured property | Query | What you should see |
|---|---|---|---|---|
| 1 | Range query | `price_cents` (int, filterable=true, rangeable=false) | `price_cents BETWEEN 30000 AND 40000` | The query works but walks the filterable bucket value by value - hundreds of ms instead of single-digit ms. |
| 2 | Path search | `spec_sheet_path` (text, tokenization=word) | Any-of filter on five GoPro spec-sheet paths | Top results include paths from other categories that happen to share a token like `gopro`. |
| 3 | Equality filter | `category` (text, filterable=false) | `category == "Cameras > Action"` | Weaviate refuses the query with `requires inverted index`. The category facet is broken. |
| 4 | Filtered aggregation (selective) | `price_cents` (int, columnar=false) | `brand == "GoPro"` (~18k matches), aggregate mean/min/max/sum of `price_cents` | The filter is fast, but the aggregation fetches + unmarshals every matching object to extract one integer. |
| 5 | Filtered aggregation (broad) | `price_cents` (int, columnar=false) | `in_stock == true` (~700k matches), same aggregation | The object-scan path scales linearly with match count - this lands in the seconds range. |

The displayed code snippets use the v4 Python client; the UI itself happens to
use GraphQL over the browser fetch API for transport convenience, but the
Python form is what you should use in real code.

## 4. Fix each mistake from the Weaviate console

For each card, the fix is a property reconfiguration that runtime reindex picks
up. After the migration runs, come back here and press *Run query* again - the
UI never caches, so the new state shows immediately.

| # | Property | Fix |
|---|---|---|
| 1 | `price_cents` | Enable `indexRangeFilters: true`. |
| 2 | `spec_sheet_path` | Change tokenization to `field` so the path is an opaque token. |
| 3 | `category` | Enable `indexFilterable: true` so equality has an inverted bucket. |
| 4 | `price_cents` | Enable `indexColumnar: true` - aggregations switch to 8-byte point lookups from the column bucket. |
| 5 | `price_cents` | Same fix as card 4 - one block scan over the column bucket, near-constant cost regardless of selectivity. |

## Notes

- The wall-clock latency is measured client-side via `performance.now()`. On a
  local Weaviate that's effectively the server latency.
- The Aggregate `meta.count` queries hit the same code path the filter does, so
  the slow cards (1, 3) also surface their cost via the count query.
- `fetch` uses `cache: "no-store"` everywhere - re-running after a reindex
  reflects the new state immediately.
