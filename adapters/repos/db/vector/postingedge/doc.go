//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

// Package postingedge implements the posting+edges vector index: an
// inverted-file index without replicas, where the boundary problem that
// replicas solve in HFresh is solved by a small neighbor graph between vectors
// of different postings instead. The package name is provisional.
//
// # Structure
//
//   - Centroids. The corpus is partitioned into postings of a target mean
//     size. A graph (the hnsw package, used as a component) over the centroids
//     finds the entry posting of a query.
//   - Postings. Every vector lives in exactly one posting: the one of its
//     exact nearest centroid. A posting holds, per member, the doc id and the
//     centered 1-bit RQ code, plus the edges of its members.
//   - Edges. Every vector keeps up to Edges links to its nearest neighbors
//     that live in other postings. An edge is 4 bytes: the target's slot.
//   - Code sidecar. The codes of all vectors, addressable by slot, resident in
//     memory. It is what lets a search rank an edge by the position of its
//     endpoint without loading the endpoint's posting.
//
// # Search
//
// A query loads the posting of its nearest centroids, brute-forces the codes
// of its members into the candidate set, and scores the endpoint of every edge
// leaving the posting with the asymmetric estimator over the endpoint's
// sidecar code. A posting that has not been loaded is ranked by its best known
// endpoint. Each round loads the LoadBatch best-ranked postings; the search
// stops when the best ranked posting is no closer than the ef-th candidate
// found so far. The best RescoreLimit candidates are then rescored against the
// full-precision vectors.
//
// # Code budget
//
// UserConfig.CodeBits keeps only the first sign bits of every code. The
// default is the full code, one bit per dimension. What a shorter code costs
// was measured on dbpedia-openai 1M at 1536 dimensions (bench-report.md, v1.2
// phase B), against the full code:
//
//   - 768 bits is the memory mode: 34% less index heap, a recall@10 of 0.99
//     within reach, 0.74x of the QPS at a recall of 0.95 and 0.90x at 0.99.
//   - 512 bits is valid and not recommended: 46% less heap, 0.67x and 0.75x,
//     and its 0.99 takes a RescoreLimit of 800.
//   - 256 bits is not recommended in memory: 57% less heap, 0.28x at a recall
//     of 0.95, and no recall above 0.97 with a RescoreLimit of 800.
//
// CodeBits alone does not give those numbers. A capped code is noisier, and
// three other settings have defaults made for the full code:
//
//   - RescueLimit 512 (1024 at 512 bits, 4096 at 256). With the default of
//     64 the edge builder loses neighbors: its edge recall falls from 0.963
//     to 0.910 at 768 bits.
//   - TargetPostingSize 236, what the full code gets out of the posting
//     budget at 1536 dimensions, and the size the capped codes were measured
//     at. Left at 0, shorter codes make the budget hold more vectors (438 at
//     768 bits); with the full code, postings of 512 were slower below a
//     recall of 0.95 (phase C).
//   - RescoreLimit 200, or 400 where the target is a recall of 0.99. With the
//     default of 64 the recall tops out at 0.975.
//
// # Two indexes
//
// Index is the in-memory version: it is built once from the vectors added
// through AddBatch (see Index.Build) and then serves concurrent searches. It
// is what the experiments build, and what builds the first postings of the
// other one. Persistence and restore, deletes, updates, incremental inserts,
// filtered search and multi-vectors are not implemented in it; the affected
// methods return an error rather than silently doing nothing. The posting
// store and the sidecar are separate types over one shared code array.
// UserConfig.ScanCodeBits splits that array in two, a short code in the
// postings for the scan and the code of CodeBits in the sidecar, at the price
// of two codes per vector in memory.
//
// Persistent is the index a shard opens (persistent.go), under the index type
// "postingedge": the postings in the LSM store of the shard, the routing
// codes in memory, the exact vectors in a file of its own. It starts empty,
// takes vectors one by one, marks the ones that are deleted, and comes back
// as it was after a restart. It is the index that grows on disk of the
// experiments (grow.go, over disk.go) behind the interface of a shard. What
// it leaves out it refuses with an error: filtered search, search by
// distance, backups, multi-vectors, and every distance but cosine. Nothing
// takes a deleted vector out of it yet, and nothing of it runs in the
// background.
//
// The configuration of a class is in entities/vectorindex/postingedge. The
// UserConfig of this package is the internal one, with the knobs of the
// experiments.
package postingedge
