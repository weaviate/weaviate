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

package postingedge

import (
	"github.com/pkg/errors"
)

// The degree cap of v1.5 (bench-report.md): one build timed under several caps
// on the number of edges per vector, each keeping a vector's nearest edges.
// It did not win at 10M (as fast at recall 0.95, about 30 % slower at 0.99),
// so it is not a parameter of the index. It stays here, with the tests,
// because the benchmark that measured it and its tests use it.

// capDegree keeps at most k edges per source, its k nearest. The rows of
// buildEdges are nearest first, so a capped row is a prefix of the full one
// and no distance is computed again. With k <= 0 every edge stays, and the
// arrays come back as they were given. Otherwise the result is new arrays in
// the same CSR form, and the given ones are left as they are.
func capDegree(edgeStart, edges []uint32, k int) (cappedStart, capped []uint32) {
	if k <= 0 || len(edgeStart) == 0 {
		return edgeStart, edges
	}
	n := len(edgeStart) - 1
	cappedStart = make([]uint32, n+1)
	for slot := 0; slot < n; slot++ {
		cappedStart[slot+1] = cappedStart[slot] + min(edgeStart[slot+1]-edgeStart[slot], uint32(k))
	}
	capped = make([]uint32, cappedStart[n])
	for slot := 0; slot < n; slot++ {
		copy(capped[cappedStart[slot]:cappedStart[slot+1]], edges[edgeStart[slot]:edgeStart[slot+1]])
	}
	return cappedStart, capped
}

// replaceEdges publishes the built index again with other edges over the same
// postings. Searches that started before keep the edges they started with.
// The edges are in the form buildEdges returns: one offset per slot and one
// that closes the last, endpoints that are slots of the index. It is how the
// benchmark times one build under several caps on the degree (capDegree)
// without building it again.
func (ix *Index) replaceEdges(edgeStart, edges []uint32) error {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if ix.closed {
		return errors.New("posting-edge index is closed")
	}
	st := ix.state.Load()
	if st == nil {
		return errors.New("posting-edge index is not built")
	}
	n := st.postings.vectors()
	if len(edgeStart) != n+1 {
		return errors.Errorf("%d edge offsets for %d vectors, expected %d", len(edgeStart), n, n+1)
	}
	if edgeStart[0] != 0 || int(edgeStart[n]) != len(edges) {
		return errors.Errorf("edge offsets span [%d, %d), the edges [0, %d)", edgeStart[0], edgeStart[n], len(edges))
	}
	for slot := 0; slot < n; slot++ {
		if edgeStart[slot+1] < edgeStart[slot] {
			return errors.Errorf("edge offsets decrease at slot %d", slot)
		}
	}
	for i, endpoint := range edges {
		if int(endpoint) >= n {
			return errors.Errorf("edge %d ends at slot %d, the index has %d", i, endpoint, n)
		}
	}

	postings := *st.postings
	postings.edgeStart, postings.edges = edgeStart, edges
	next := &state{
		dims:          st.dims,
		quantizer:     st.quantizer,
		scanQuantizer: st.scanQuantizer,
		centroids:     st.centroids,
		postings:      &postings,
		side:          st.side,
		visited:       st.visited,
		build:         st.build,
	}
	next.scratch.New = st.scratch.New
	ix.state.Store(next)
	return nil
}
