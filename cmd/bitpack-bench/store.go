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

package main

import (
	"fmt"
	"math/bits"
	"os"
	"sort"
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/vectorindex/compression"
)

// bitStore is a column-major, bit-packed store. One bit per retained
// dimension, 64 dimensions per block. Block b of every vector is contiguous:
// codes[b*n + id]. A scan over one block touches one word per vector
// sequentially instead of pulling the vector's full code line.
type bitStore struct {
	n        int
	blocks   int              // retained dims / 64
	codes    []uint64         // len blocks*n, column-major
	rotation *compression.FastRotation // nil when rotation is disabled
	retained int
}

// buildBitStore encodes all vectors: (optional) rotation, truncation to
// retained dims, sign-bit packing. Reuses the FastRotation from RQ and the
// sign-bit packing of compressionhelpers.BinaryQuantizer.
func buildBitStore(vectors []float32, dims, n, retained int, rotation *compression.FastRotation) *bitStore {
	blocks := retained / 64
	s := &bitStore{
		n:        n,
		blocks:   blocks,
		codes:    make([]uint64, blocks*n),
		rotation: rotation,
		retained: retained,
	}
	bq := compressionhelpers.NewBinaryQuantizer(nil)
	var rotScratch []float32
	if rotation != nil {
		rotScratch = make([]float32, rotation.OutputDim)
	}
	start := time.Now()
	for id := 0; id < n; id++ {
		v := vectors[id*dims : (id+1)*dims]
		if rotation != nil {
			v = rotation.RotateInto(v, rotScratch)
		}
		enc := bq.Encode(v[:retained])
		for b := 0; b < blocks; b++ {
			s.codes[b*n+id] = enc[b]
		}
		if (id+1)%100_000 == 0 {
			fmt.Fprintf(os.Stderr, "  encoded %d/%d vectors (%.1fs)\n", id+1, n, time.Since(start).Seconds())
		}
	}
	return s
}

// byHamming sorts candidate ids by their accumulated Hamming distance.
// dist is indexed by id, so only ids move.
type byHamming struct {
	ids  []uint32
	dist []uint16
}

func (s *byHamming) Len() int           { return len(s.ids) }
func (s *byHamming) Less(i, j int) bool { return s.dist[s.ids[i]] < s.dist[s.ids[j]] }
func (s *byHamming) Swap(i, j int)      { s.ids[i], s.ids[j] = s.ids[j], s.ids[i] }

// byExact sorts the rescore window by exact distance.
type byExact struct {
	ids []uint32
	d   []float32
}

func (s *byExact) Len() int           { return len(s.ids) }
func (s *byExact) Less(i, j int) bool { return s.d[i] < s.d[j] }
func (s *byExact) Swap(i, j int) {
	s.ids[i], s.ids[j] = s.ids[j], s.ids[i]
	s.d[i], s.d[j] = s.d[j], s.d[i]
}

// scanner holds all per-query scratch state so the hot path allocates
// nothing. Candidate selection is deliberately the simplest thing that
// works: full sort of the survivors by distance, then truncate. This is the
// baseline a later selection optimisation is compared against.
type scanner struct {
	store    *bitStore
	vectors  []float32 // normalized base vectors, flat row-major
	dims     int
	budgets  []int
	rescoreN int // rescore window; 0 = all final survivors
	k        int
	dist     distancer.Provider

	qRot     []float32 // rotation output scratch
	qWords   []uint64  // packed query, one word per block
	hamming  []uint16  // accumulated hamming per id
	cand     []uint32  // surviving candidate ids
	exactIDs []uint32  // rescore window ids
	exactD   []float32 // rescore window exact distances
	hSorter  byHamming
	eSorter  byExact
}

func newScanner(store *bitStore, vectors []float32, dims int, budgets []int, rescoreN, k int) *scanner {
	maxWindow := store.n
	sc := &scanner{
		store:    store,
		vectors:  vectors,
		dims:     dims,
		budgets:  budgets,
		rescoreN: rescoreN,
		k:        k,
		dist:     distancer.NewCosineDistanceProvider(),
		qWords:   make([]uint64, store.blocks),
		hamming:  make([]uint16, store.n),
		cand:     make([]uint32, store.n),
		exactIDs: make([]uint32, 0, maxWindow),
		exactD:   make([]float32, 0, maxWindow),
	}
	if store.rotation != nil {
		sc.qRot = make([]float32, store.rotation.OutputDim)
	}
	sc.hSorter.dist = sc.hamming
	return sc
}

func (sc *scanner) budget(block int) int {
	if block < len(sc.budgets) {
		return sc.budgets[block]
	}
	return sc.budgets[len(sc.budgets)-1]
}

// pruneTo sorts the current candidates by accumulated Hamming distance and
// keeps the best `budget`.
func (sc *scanner) pruneTo(budget int) {
	sc.hSorter.ids = sc.cand
	sort.Sort(&sc.hSorter)
	if len(sc.cand) > budget {
		sc.cand = sc.cand[:budget]
	}
}

type queryResult struct {
	topK      []uint32
	survivors []int // len == blocks, candidate count after each block's prune
	block0    time.Duration
	restBlocks time.Duration
	rescore   time.Duration
}

// search runs the progressive elimination scan for one query. survivors must
// have room for store.blocks entries; topK for k entries. Both are filled in
// place so the hot path stays allocation free.
func (sc *scanner) search(q []float32, topK []uint32, survivors []int) queryResult {
	s := sc.store
	n := s.n

	t0 := time.Now()

	// Encode the query exactly like the stored vectors: rotate, truncate,
	// sign-pack.
	qv := q
	if s.rotation != nil {
		qv = s.rotation.RotateInto(q, sc.qRot)
	}
	qv = qv[:s.retained]
	for b := range sc.qWords {
		sc.qWords[b] = 0
	}
	for i, x := range qv {
		if x < 0 {
			sc.qWords[i>>6] |= 1 << (uint(i) & 63)
		}
	}

	// Block 0: one sequential pass over the whole column.
	col := s.codes[:n]
	q0 := sc.qWords[0]
	dist := sc.hamming
	for id := 0; id < n; id++ {
		dist[id] = uint16(bits.OnesCount64(col[id] ^ q0))
	}
	sc.cand = sc.cand[:n]
	for id := range sc.cand {
		sc.cand[id] = uint32(id)
	}
	sc.pruneTo(sc.budget(0))
	survivors[0] = len(sc.cand)

	t1 := time.Now()

	// Remaining blocks: accumulate over survivors only, then prune.
	for b := 1; b < s.blocks; b++ {
		col := s.codes[b*n : (b+1)*n]
		qb := sc.qWords[b]
		for _, id := range sc.cand {
			dist[id] += uint16(bits.OnesCount64(col[id] ^ qb))
		}
		sc.pruneTo(sc.budget(b))
		survivors[b] = len(sc.cand)
	}

	t2 := time.Now()

	// Exact rescore of the final survivors against the float vectors.
	window := len(sc.cand)
	if sc.rescoreN > 0 && sc.rescoreN < window {
		window = sc.rescoreN
	}
	sc.exactIDs = sc.exactIDs[:0]
	sc.exactD = sc.exactD[:0]
	for _, id := range sc.cand[:window] {
		v := sc.vectors[int(id)*sc.dims : (int(id)+1)*sc.dims]
		d, err := sc.dist.SingleDist(q, v)
		if err != nil {
			panic(err)
		}
		sc.exactIDs = append(sc.exactIDs, id)
		sc.exactD = append(sc.exactD, d)
	}
	sc.eSorter.ids = sc.exactIDs
	sc.eSorter.d = sc.exactD
	sort.Sort(&sc.eSorter)
	k := sc.k
	if k > len(sc.exactIDs) {
		k = len(sc.exactIDs)
	}
	copy(topK, sc.exactIDs[:k])

	t3 := time.Now()

	return queryResult{
		topK:       topK[:k],
		survivors:  survivors,
		block0:     t1.Sub(t0),
		restBlocks: t2.Sub(t1),
		rescore:    t3.Sub(t2),
	}
}
