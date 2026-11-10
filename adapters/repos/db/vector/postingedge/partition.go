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

	"github.com/weaviate/weaviate/adapters/repos/db/vector/kmeans"
)

// cluster is a node of the split tree while the corpus is being partitioned.
type cluster struct {
	members []uint32 // positions in the build input
	leaves  int      // how many final clusters this node still stands for
	path    uint64   // position in the split tree: 1 for the root, 2p and 2p+1 below p
}

// partition splits the vectors into k clusters and returns the mean of each.
//
// It bisects top-down with the balanced 2-means split HFresh applies to an
// oversized posting (kmeans.FitBalanced), handing each side a share of the
// remaining k proportional to its size. That costs O(n log k) distance
// computations instead of the O(n k) per iteration of a flat k-means, and
// keeps the cluster sizes close to n/k. The clusters only seed the centroids:
// membership is decided afterwards by exact nearest-centroid assignment.
func partition(ctx context.Context, logger logrus.FieldLogger, vectors [][]float32,
	dims, k int, seed uint64,
) ([][]float32, error) {
	clusters, err := partitionClusters(ctx, logger, vectors, dims, k, seed)
	if err != nil {
		return nil, err
	}

	centroids := make([][]float32, len(clusters))
	sums := make([][]float64, parallelWorkers())
	err = parallelChunks(ctx, logger, len(clusters), 16, func(worker, start, end int) error {
		if sums[worker] == nil {
			sums[worker] = make([]float64, dims)
		}
		for i := start; i < end; i++ {
			centroids[i] = mean(vectors, clusters[i].members, sums[worker])
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return centroids, nil
}

// partitionClusters returns the min(k, n) clusters partition takes the means
// of. Every vector is in exactly one of them and none is empty.
func partitionClusters(ctx context.Context, logger logrus.FieldLogger, vectors [][]float32,
	dims, k int, seed uint64,
) ([]cluster, error) {
	n := len(vectors)
	if n == 0 {
		return nil, errors.New("cannot partition an empty set of vectors")
	}
	k = min(max(k, 1), n)

	all := make([]uint32, n)
	for i := range all {
		all[i] = uint32(i)
	}

	// Level by level: the splits of one level are independent, so they run
	// in parallel, and collecting them in order keeps the result
	// deterministic for a given seed.
	level := []cluster{{members: all, leaves: k, path: 1}}
	done := make([]cluster, 0, k)
	for len(level) > 0 {
		children := make([][2]cluster, len(level))
		err := parallelChunks(ctx, logger, len(level), 1, func(_, start, end int) error {
			for i := start; i < end; i++ {
				if level[i].leaves == 1 {
					continue
				}
				left, right, err := bisect(vectors, dims, level[i], seed)
				if err != nil {
					return err
				}
				children[i] = [2]cluster{left, right}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}

		next := make([]cluster, 0, 2*len(level))
		for i, c := range level {
			if c.leaves == 1 {
				done = append(done, c)
				continue
			}
			next = append(next, children[i][0], children[i][1])
		}
		level = next
	}
	return done, nil
}

// bisect splits a cluster in two. Both sides are non-empty and neither holds
// more than three quarters of the members (the FitBalanced floor), so the
// recursion always terminates, duplicates included.
func bisect(vectors [][]float32, dims int, c cluster, seed uint64) (cluster, cluster, error) {
	data := make([][]float32, len(c.members))
	for i, m := range c.members {
		data[i] = vectors[m]
	}

	km := kmeans.New(2, dims, 0)
	km.Seed = seed ^ (c.path * 0x9E3779B97F4A7C15)
	labels, err := km.FitBalanced(data)
	if err != nil {
		return cluster{}, cluster{}, errors.Wrapf(err, "split cluster of %d vectors", len(data))
	}

	nLeft := 0
	for _, label := range labels {
		if label == 0 {
			nLeft++
		}
	}
	left := make([]uint32, 0, nLeft)
	right := make([]uint32, 0, len(labels)-nLeft)
	for i, label := range labels {
		if label == 0 {
			left = append(left, c.members[i])
		} else {
			right = append(right, c.members[i])
		}
	}
	if len(left) == 0 || len(right) == 0 {
		return cluster{}, cluster{}, errors.Errorf(
			"split of %d vectors left one side empty", len(data))
	}

	kLeft := splitLeaves(c.leaves, len(left), len(right))
	return cluster{members: left, leaves: kLeft, path: 2 * c.path},
		cluster{members: right, leaves: c.leaves - kLeft, path: 2*c.path + 1}, nil
}

// splitLeaves decides how many of a node's leaves go to its left child:
// proportional to the child's size, at least one per side and never more than
// a side has members. Requires 2 <= leaves <= nLeft+nRight and both sides
// non-empty.
func splitLeaves(leaves, nLeft, nRight int) int {
	kLeft := int(math.Round(float64(leaves) * float64(nLeft) / float64(nLeft+nRight)))
	kLeft = min(max(kLeft, 1), leaves-1)
	kLeft = min(kLeft, nLeft)
	kLeft = max(kLeft, leaves-nRight)
	return kLeft
}

// mean returns the mean of the given members. sum is scratch of the vector
// dimensionality; accumulating in float64 keeps large clusters exact.
func mean(vectors [][]float32, members []uint32, sum []float64) []float32 {
	clear(sum)
	for _, m := range members {
		for j, x := range vectors[m] {
			sum[j] += float64(x)
		}
	}
	out := make([]float32, len(sum))
	inv := 1 / float64(len(members))
	for j, s := range sum {
		out[j] = float32(s * inv)
	}
	return out
}
