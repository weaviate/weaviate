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
	"context"
	"math"
	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// edgeInput is what the edge builder reads. Nothing in it is modified.
type edgeInput struct {
	vectors   [][]float32 // by position in the build input
	layout    slotLayout
	assign    *assignment
	side      *sidecar
	quantizer codec
	provider  distancer.Provider
	probe     int // nearest centroids whose postings supply the candidates, at most assign.width
	neighbors int // nearest neighbors considered per vector
	rescue    int // code-ranked candidates re-ranked exactly per vector
}

// edgeScratch is the working memory of the neighbor search of one vector.
type edgeScratch struct {
	candidates topK
	nearest    topK
}

// nearestNeighbors finds the nearest neighbors of the vector in a slot among
// the members of its probe postings: the candidates are ranked by code, the
// best rescue of them re-ranked with exact distances, and the neighbors
// nearest of those returned, nearest first. The result aliases the scratch.
// ranked is the number of candidates ranked by code.
func (in *edgeInput) nearestNeighbors(slot uint32, sc *edgeScratch) (nearest []scored, ranked int64, err error) {
	input := in.layout.order[slot]
	vector := in.vectors[input]
	estimator := in.quantizer.NewEstimator(vector)

	sc.candidates.reset(in.rescue)
	for _, c := range in.assign.probesOf(int(input))[:min(in.probe, in.assign.width)] {
		if c == noCentroid {
			continue
		}
		from, to := in.layout.start[c], in.layout.start[c+1]
		for t := from; t < to; t++ {
			if t == slot {
				continue
			}
			dist, err := estimator.Distance(in.side.code(t))
			if err != nil {
				return nil, 0, errors.Wrapf(err, "estimate distance from slot %d to slot %d", slot, t)
			}
			sc.candidates.push(uint64(t), dist)
		}
		ranked += int64(to - from)
	}

	sc.nearest.reset(in.neighbors)
	for _, cand := range sc.candidates.items {
		dist, err := in.provider.SingleDist(vector, in.vectors[in.layout.order[cand.id]])
		if err != nil {
			return nil, 0, errors.Wrapf(err, "distance from slot %d to slot %d", slot, cand.id)
		}
		sc.nearest.push(cand.id, dist)
	}
	return sc.nearest.sorted(), ranked, nil
}

// buildEdges links every vector to its nearest neighbors in other postings.
//
// For a vector, the candidates are the members of the postings of its probe
// nearest centroids. They are ranked with the vector's own full-precision
// values against the candidates' codes (the estimator a search uses), the best
// rescue of them are re-ranked with exact distances, and of the neighbors
// nearest exact ones those outside the vector's posting become its edges,
// nearest first. Neighbors inside the posting need no edge: a search that
// loads the posting scans them anyway.
//
// The edges come back in CSR form by source slot. scored is the number of
// candidates ranked, the dominant cost of the build.
func buildEdges(ctx context.Context, logger logrus.FieldLogger, in edgeInput,
) (edgeStart, edges []uint32, scored int64, err error) {
	n := len(in.vectors)
	if uint64(n)*uint64(in.neighbors) > math.MaxUint32 {
		return nil, nil, 0, errors.Errorf("%d vectors with %d edges each exceed the edge offset range",
			n, in.neighbors)
	}

	// Fixed-width rows first, compacted once every row is known.
	rows := make([]uint32, n*in.neighbors)
	edgeStart = make([]uint32, n+1)
	var totalScored atomic.Int64
	scratches := make([]edgeScratch, parallelWorkers())

	// Slot order keeps consecutive vectors in the same posting, whose probe
	// sets overlap almost entirely, so the candidate codes stay cached.
	err = parallelChunks(ctx, logger, n, 64, func(worker, start, end int) error {
		sc := &scratches[worker]
		var chunkScored int64
		for slot := uint32(start); slot < uint32(end); slot++ {
			nearest, ranked, err := in.nearestNeighbors(slot, sc)
			if err != nil {
				return err
			}
			chunkScored += ranked

			own := in.side.postingOf[slot]
			row := rows[int(slot)*in.neighbors:]
			count := uint32(0)
			for _, nb := range nearest {
				if in.side.postingOf[nb.id] != own {
					row[count] = uint32(nb.id)
					count++
				}
			}
			edgeStart[slot+1] = count
		}
		totalScored.Add(chunkScored)
		return nil
	})
	if err != nil {
		return nil, nil, 0, err
	}

	for slot := 0; slot < n; slot++ {
		edgeStart[slot+1] += edgeStart[slot]
	}
	edges = make([]uint32, edgeStart[n])
	for slot := 0; slot < n; slot++ {
		count := edgeStart[slot+1] - edgeStart[slot]
		copy(edges[edgeStart[slot]:], rows[slot*in.neighbors:slot*in.neighbors+int(count)])
	}
	return edgeStart, edges, totalScored.Load(), nil
}
