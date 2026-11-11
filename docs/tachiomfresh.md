## TachiomFresh vector index

TachiomFresh is a token-level inverted-file (IVF) index for multi-vectors (ColBERT-style late interaction). Every token of a document is placed in the posting of a centroid; a query scores documents by MaxSim. The index follows hfresh: the centroids live in an HNSW graph, postings are split, merged and reassigned in the background, and most of the lifecycle code is copied from hfresh. It differs from hfresh in what a posting holds, and in where a token goes. An hfresh posting holds a 1-bit code of each vector: hfresh scans postings and makes the checks after a split and the merge checks on those codes, and reads the full-precision vector from the object store to cluster a split, to reassign a vector and for the exact scoring. A tachiomfresh posting holds only (doc id, position, version) entries and no vector, so every distance that involves a token (clustering a split, the checks after it, a merge check, a reassign, the exact scoring) reads the token's float32 values from the object store. Distances to centroids use the centroid graph's codes in both indexes. A token goes only among the centroids of its token id.

The code is in `adapters/repos/db/vector/tachiomfresh`; the user configuration is in `entities/vectorindex/tachiomfresh`.

### What a client must send

- The vector index type `tachiomfresh` on a named multi-vector, with vectorizer `none`. A vectorizer module is refused at schema time: no module returns token ids as of now.
- With every multi-vector, its token ids: `Vectors.token_ids` in gRPC, `tokenIds` in REST, one `uint32` per token, the ids the model's tokenizer gave the tokens. An object whose tachiomfresh multi-vector has another number of ids is refused before it is stored, and so is a create or batch object with token ids under a name that is not one of its multi-vectors. A PATCH may change the token ids alone; they are checked against the stored multi-vector.
- The distance is `dot`, the only one accepted, and the multi-vector aggregation `maxSim`.

The token ids are stored with the object (an optional section after the multi-vectors in the object's binary) and the index reads them from there at insert time. A put, batch or patch that changes only the token ids gives the object a new doc id, so the index places its tokens again.

### Search

1. Stage 1: each query token finds its `kCentroids` nearest centroids (dot product) and scans their postings. A document found there gets, for that query token, the similarity of the nearest of these centroids that holds one of its tokens. Its stage-1 score is the sum of these similarities over the query tokens.
2. Stage 2: the `kDocsToScore` documents with the highest sums are kept.
3. Stage 3: they are scored with the exact MaxSim on their float32 tokens, and the top k are returned.

Entries older than their document's version, and those of deleted documents, are skipped: a document reached only through them is not a candidate. A filter is applied to the scanned documents. A filter that allows at most 2 x `kDocsToScore` documents skips the stages and scores every allowed document exactly. Above that, when stage 1 reached fewer allowed documents than `kDocsToScore`, the probe is doubled, at most 4 times.

### Lifecycle

- **Staging.** Under 10,000 document tokens the index holds no centroid and no posting. Every document goes, with its token ids, into the staging bucket, and a search scores every staged document exactly.
- **Build.** When the staged tokens reach 10,000, the index takes the mean of every staged token as its centering mean, places every staged document, and empties the staging bucket. From then on every document token is centered before it meets a centroid; query tokens are not. The phase (staging or built) is persisted. A document deleted while the build runs is not placed. A build that stops part way is started over at the next insert or restart.
- **Placement.** A token goes to the posting of the centroid of its token id that is nearest in L2. The first token of a token id creates that id's first posting. Splits, merges and reassigns stay within the token id, so every posting holds one token id.
- **Splits and merges.** A posting above its size limit is split in two; a posting below its minimum is merged into a neighbour of its token id; an empty posting loses its centroid. Up to 256 entries a token id uses `maxPostingSize`; above, its limit is the square root of its entry count divided by its spread relative to the mean spread (capped at 1), and never below `maxPostingSize`. A tight token id gets larger postings. A split or merge that fails part way is undone, so that every token stays in one posting: a new posting whose centroid is already searchable is kept and its tokens leave the old posting.
- **Deletes and re-inserts.** A delete marks the document deleted in the version map; its entries are dropped by the next split, merge or cleanup of their posting. A re-insert of a doc id raises its version, and entries of an older version are ignored. Entries store their version only in postings that hold a re-inserted document.

### What is persisted where

All under the shard; `<id>` is the index's physical id.

| what | where |
|---|---|
| postings: one key per posting, one 6-byte entry per token (7 when the posting holds a re-inserted document) | LSM bucket `tachiomfresh_postings_<id>` |
| the token id of each posting, the spread sums per token id, the centering mean, the phase, the posting format | LSM bucket `tachiomfresh_types_<id>` |
| the token count of each document | LSM bucket `tachiomfresh_tokens_<id>` |
| staged documents, before the build | LSM bucket `tachiomfresh_staging_<id>` |
| version map, index metadata (dimensions), the posting id sequence | LSM bucket `hfresh_shared_<id>` (hfresh's shared bucket) |
| the centroid HNSW's commit log; the split, merge and reassign queues | directory `<id>.tachiomfresh.d` |

The centroid graph is Weaviate's hnsw index, which keeps its state in commit-log files, and the task queues are Weaviate disk queues, also files; hfresh keeps both in `<id>.hfresh.d`, and this index copies that layout. Everything the index adds of its own is in LSM buckets.

**Write order.** The dimensions, phase and posting format are written to their commit logs before the first document is staged. Before a split or merge changes the centroid graph, the postings, types, token-count and shared buckets write their commit logs, so a crash never leaves a centroid without its posting or its token id. A reassign appends the token to its new posting before it removes it from the old one. After a crash, restore keeps one copy of a token found in two postings (the one whose centroid is nearer), counts each document's live entries against its stored token count, and treats a document as indexed only when every token is present. The order holds for a crash of the process; like the rest of the write path, nothing is fsynced, so a power loss can lose the last writes.

The reverse order is not kept, as in hfresh, which has the same gap: the centroid graph's commit log is written out at Flush, so a centroid change can reach its file after the posting writes that depend on it. A crash in between can lose the tokens of a split posting, or a token moved into a new centroid. Writing the commit log at every centroid change would close the gap, at the cost of one more write per split and merge.

### Backup and drop

- `PrepareForBackup` writes the spread sums (they cannot be recounted from the postings), switches the centroid commit log and pauses the queues. `ListFiles` lists the centroid commit log and the queues; the shard lists the LSM buckets. Empty files and the commit log just opened are left out, as hnsw does.
- Dropping the vector index removes its buckets, its directory and its indexing queue, and the token ids stored with the objects under that vector.
- The collection export writes the token ids in the optional Parquet column `token_ids` (JSON, vector name to ids), next to `multi_vectors`.

### Configuration

| field | default | limits | change after creation |
|---|---|---|---|
| `maxPostingSize` | 64 | 4 to 100,000 | refused |
| `kCentroids` | 32 | 1 to 4,096 | allowed |
| `kDocsToScore` | 500 | 1 to 20,000 | allowed |
| `efConstruction` | 64 | 1 to 1,024 | refused |
| `distance` | `dot` | `dot` only | refused |
| `multivector.enabled` | must be true | | refused |
| `multivector.aggregation` | `maxSim` | `maxSim` only | refused |
| `multivector.muvera.enabled` | must be false | | refused |

`tachiomfresh` cannot be the `DEFAULT_VECTOR_INDEX`: it holds only multi-vectors.

### Limits

- Doc ids up to 4,294,967,295 and documents of at most 65,536 tokens: a posting entry stores the doc id in 4 bytes and the token's position in its document in 2. An insert beyond either is refused.
- A token id (the tokenizer's vocabulary id) is a `uint32`: some vocabularies have more than 65,536 entries. It is not stored per entry, only once per posting, since every posting holds one token id.
- Every document needs token ids; there is no mode without them. The method could be made to work without token ids, by clustering all tokens together; that has been neither tested nor tuned, and is not in this code.
