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

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// noCentroid marks a probe entry whose centroid was dropped for being empty.
const noCentroid = math.MaxUint32

// assignment maps every vector of the build input to its nearest centroid,
// and to the wider set of near centroids the edge builder draws neighbor
// candidates from.
type assignment struct {
	primary []uint32
	probes  []uint32 // len(primary) * width entries, nearest centroid first per vector
	width   int
}

func (a *assignment) probesOf(i int) []uint32 {
	return a.probes[i*a.width : (i+1)*a.width]
}

// assign computes the exact nearest centroid of every vector, and its probe
// nearest centroids, by brute force in l2 space. The primary assignment has to
// be exact: without replicas a vector lives in one posting only, and there is
// no second copy to absorb an approximate choice.
//
// TODO: the cost is n*k distance computations, quadratic in n at a fixed
// posting size. Large builds need a blocked or graph-assisted exact pass.
func assign(ctx context.Context, logger logrus.FieldLogger, vectors, centroids [][]float32,
	probe int,
) (*assignment, error) {
	n := len(vectors)
	width := min(probe, len(centroids))
	if width < 1 {
		return nil, errors.New("cannot assign vectors without centroids")
	}

	a := &assignment{
		primary: make([]uint32, n),
		probes:  make([]uint32, n*width),
		width:   width,
	}
	l2 := distancer.NewL2SquaredProvider()
	heaps := make([]topK, parallelWorkers())

	err := parallelChunks(ctx, logger, n, 64, func(worker, start, end int) error {
		nearest := &heaps[worker]
		for i := start; i < end; i++ {
			nearest.reset(width)
			best, bestDist := uint32(0), float32(math.Inf(1))
			for c, centroid := range centroids {
				dist, err := l2.SingleDist(vectors[i], centroid)
				if err != nil {
					return errors.Wrapf(err, "distance of vector %d to centroid %d", i, c)
				}
				if dist < bestDist {
					best, bestDist = uint32(c), dist
				}
				nearest.push(uint64(c), dist)
			}
			if math.IsInf(float64(bestDist), 1) {
				return errors.Errorf("vector %d has no finite distance to any centroid", i)
			}
			a.primary[i] = best
			probes := a.probesOf(i)
			for j := range probes {
				probes[j] = noCentroid
			}
			// Nearest first, so that any prefix of the probes of a vector
			// is its nearest centroids too.
			for j, e := range nearest.sorted() {
				probes[j] = uint32(e.id)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return a, nil
}

// dropEmpty removes the centroids no vector was assigned to and renumbers the
// assignment accordingly. An empty posting would be a dead end for a search
// entering through it. Dropping a centroid nobody chose leaves every vector's
// nearest centroid unchanged, so the assignment stays exact.
func dropEmpty(centroids [][]float32, a *assignment) [][]float32 {
	sizes := make([]uint32, len(centroids))
	for _, p := range a.primary {
		sizes[p]++
	}

	remap := make([]uint32, len(centroids))
	kept := centroids[:0:0]
	for c, size := range sizes {
		if size == 0 {
			remap[c] = noCentroid
			continue
		}
		remap[c] = uint32(len(kept))
		kept = append(kept, centroids[c])
	}
	if len(kept) == len(centroids) {
		return centroids
	}

	for i, p := range a.primary {
		a.primary[i] = remap[p]
	}
	for i, p := range a.probes {
		if p != noCentroid {
			a.probes[i] = remap[p]
		}
	}
	return kept
}
