# Common Indexing Mistakes - Demo UI

A visual demo of three typical Weaviate indexing misconfigurations against a
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
imports 1,000,000 deterministic synthetic product objects (seed 42).

## 2. Open the UI

Either open `index.html` directly, or serve the directory:

```bash
cd tools/dev/bench/demo_indexing_mistakes_ui
python -m http.server 8089
# then open http://localhost:8089
```

Set the **Weaviate URL** at the top to your target. For a Weaviate Cloud
cluster, also fill in the **API key**. Both persist to localStorage.

## 3. Run each card

| # | Card | Misconfigured property | Query | What you should see |
|---|---|---|---|---|
| 1 | Range query | `price_cents` (int, filterable=false, rangeable=false) | `price_cents BETWEEN 30000 AND 40000` | Weaviate refuses the query with `requires inverted index`. The storefront price filter is broken. |
| 2 | Path search | `spec_sheet_path` (text, tokenization=word) | BM25 `"/products/cameras/gopro"` | Top results include paths from other categories that happen to contain `gopro` as a token. |
| 3 | Equality filter | `category` (text, filterable=false) | `category == "Cameras > Action"` | Weaviate refuses the query with `requires inverted index`. The category facet is broken. |

The displayed code snippets use the v4 Python client; the UI itself happens to
use GraphQL over the browser fetch API for transport convenience, but the
Python form is what you should use in real code.

## 4. Fix each mistake from the Weaviate console

For each card, the fix is a property reconfiguration that runtime reindex picks
up. After the migration runs, come back here and press *Run query* again - the
UI never caches, so the new state shows immediately.

| # | Property | Fix |
|---|---|---|
| 1 | `price_cents` | Enable `indexRangeFilters: true` (and `indexFilterable: true`). |
| 2 | `spec_sheet_path` | Change tokenization to `field` so the path is an opaque token. |
| 3 | `category` | Enable `indexFilterable: true` so equality has an inverted bucket. |

## Notes

- The wall-clock latency is measured client-side via `performance.now()`. On a
  local Weaviate that's effectively the server latency.
- The Aggregate `meta.count` queries hit the same code path the filter does, so
  the slow cards (1, 3) also surface their cost via the count query.
- `fetch` uses `cache: "no-store"` everywhere - re-running after a reindex
  reflects the new state immediately.
