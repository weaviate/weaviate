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
// # Scope of this version
//
// This is the in-memory version: it is built once from the vectors added
// through AddBatch (see Index.Build) and then serves concurrent searches.
// Persistence and restore, deletes, updates, incremental inserts, filtered
// search and multi-vectors are not implemented; the affected methods return an
// error rather than silently doing nothing. The posting store and the sidecar
// are separate types over one shared code array so that a disk-resident
// version can keep the postings on disk and only the sidecar in memory.
// UserConfig.ScanCodeBits splits that array in two, a short code in the
// postings for the scan and the code of CodeBits in the sidecar: the layout
// such a version would have, here at the price of two codes per vector in
// memory.
package postingedge
