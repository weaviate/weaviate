# Key/doc column

A resident in-memory index that answers batched `ContainsAny` and `ContainsNone`
on a filterable property whose values are near-unique — a SKU, a UUID, an
external id — without the per-key LSM work the standard fold does.

Opted into per property, per node, with `INDEX_FILTERABLE_KEY_DOC_COLUMN`.
Nothing is built unless it is configured.

The package godocs are the per-symbol source of truth
(`adapters/repos/db/roaringset/keydoccolumn`, and the lsmkv and inverted files
named below). This document is the overview: what the structure is, when it
declines, what it costs, and what an operator sees. Where this disagrees with a
godoc, the godoc wins and this document has a bug.

## 1. What it is

The property's key→docID mapping is held as two positionally matched columns: a
sorted key column, and a parallel docID column where a key's entry is **one
document** rather than a posting list. A batch of query values resolves with a
single merge-scan (dense queries) or a binary-search sweep (sparse ones),
emitting docIDs into a slice and building exactly one bitmap at the end — in
place of N per-key roaringset lookups, each of which is a segment-index search, a
bloom check and a bitmap-container decode.

Three key backings are selected from the corpus: one arena plus offsets for
variable-width keys, contiguous fixed-width keys where every key is the same
size, and a prefix-elided form where they also share a leading run of bytes.

A document may sit under any number of keys — an array-valued or tokenized
property puts it there. The reverse is what the structure is for: a key holding
several documents keeps the rest in a per-segment overflow, and past
`overflowLimit` the build refuses so the property stays on the standard fold.

## 2. Where it lives and how it stays current

The column belongs to the bucket's `SegmentGroup`, built at open from a merged
cursor over the disk segments alone. Whatever is unflushed is layered over it at
query time instead, so a base that included the memtables would hold those
documents twice.

- **A flush** is merged in as a *layer* — adds and deletes as two small segments
  — before the segment swap and outside `flushLock`
  (`lsmkv/segment_group_key_doc_column.go`, with the ordering argument at the
  call site in `Bucket.FlushAndSwitch`). Running after the swap would leave an
  instant where the flushed documents are in neither the column nor a visible
  memtable.
- **Past 8 layers** a background rebuild folds base plus layers into a fresh
  base, one at a time per column, so a query never walks an unbounded list. A
  rebuild the node has no memory or no slot for is retried on a backoff and, past
  that, left to the next flush.
- **A query** reads the column's state pinned in the same view as the memtables,
  so a flush landing mid-query cannot leave the two disagreeing.

Nothing is persisted. There is no file format, no migration, and no cluster API:
the column is rebuilt from the segments at every open, so an upgrade and a
downgrade each read what the other wrote.

## 3. When it declines

Three different layers can decline, and they decline for different reasons.

**The schema**, through `inverted.WhyNoKeyDocColumn` — one predicate shared by
the query path and the startup report, so the two cannot disagree about whether
a property is eligible:

| reason | why |
| --- | --- |
| `no-filterable-index` | there is no roaringset bucket to build from |
| `nested-object-property`, `reference-property`, `geo-property` | not filtered this way |
| `tokenization-not-field` | any other tokenization can turn one value into zero or several keys, so one value is not one key |

**The data**, at build or at a flush: values that are not unique are kept, with a
warning, until the number of keys holding more than one document passes
`overflowLimit`, at which point the build refuses or the flush detaches the
column. Results stay correct either way — a detached column means the standard
fold answers.

**The query**, in `servedByKeyDocColumn`: only `ContainsAny` and `ContainsNone`,
and only carrying two or more values. `ContainsAll` is excluded because its fold
stops as soon as the running intersection empties, which on a near-unique
property is after about two keys. A single value resolves as a plain `Equal`,
which applies the query limit while it reads.

`QUERY_BATCHED_CONTAINS_ENABLED=false` holds the column back too: the gate covers
everything the batched path serves rather than having a hole where the newest
code runs. It stops reads, not builds — see §5.

## 4. What it costs

`lsm_key_doc_column_size` is the node's total resident bytes across every column
(see `docs/metrics.md`). It counts three things, and the two that grow without a
rebuild are the ones worth watching:

- the columns themselves, which change only at a rebuild;
- the **overflow** — the documents beyond the first under a key — which is also
  where a per-query cost lives: a value shared by very many documents is cheap to
  hold and expensive to resolve, since each of those documents becomes an entry
  in the result before the bitmap is built;
- the **layers**, which grow with every flush and fall back at a rebuild.

A rebuild holds a whole second base while it runs, so a column costs about twice
its settled size for that time. How many run at once is bounded process-wide
(`keyDocColumnFlattenSlots`), because the memory check they each consult reads a
heap figure refreshed on a timer and cannot see what the others just took.

## 5. Operating it

- **Changing which properties carry a column takes a restart.** The setting is
  read once at startup.
- **The live gate does not free memory.** `QUERY_BATCHED_CONTAINS_ENABLED=false`
  stops the column being read; it goes on absorbing flushes and rebuilding.
- **Namespaced collections cannot be named**, because `:` already separates a
  collection from its properties.
- **Property names are matched as the schema stores them**, which lowercases the
  first letter: `Sku` reaches `sku`, and `SKU` is held as `sKU` and is a
  different property.
- **At startup the node says what the configuration got it**: a collection it
  does not have, a property it does not have, a property no column can answer,
  and a collection where none of the above left anything to build.

## 6. Where the tests are

| what | where |
| --- | --- |
| the structure: layers, flushes, rebuilds, deletions, widths | `roaringset/keydoccolumn/*_test.go` |
| the bucket's side: the view, the reader, detaching, shutdown | `lsmkv/segment_group_key_doc_column_test.go` |
| the query path: routing, the fold comparison, annotations, the gate | `inverted/keydoccolumn_wiring_e2e_test.go` |
| what the configuration means | `db/key_doc_column_config_test.go`, `db/shard_key_doc_column_test.go`, `usecases/config` |

The column-versus-fold equality is the central claim and is asserted by running
both over the same corpus; `requireColumnAnswers` is what makes those comparisons
meaningful, since an attached column is not the same fact as a column that
answered.
