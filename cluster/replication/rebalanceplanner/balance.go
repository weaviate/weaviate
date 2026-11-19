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

package rebalanceplanner

import (
	"math"

	"github.com/weaviate/weaviate/cluster/replication/types"
)

// balanceTolerance is the largest standard deviation of disk utilisation and
// of count ratio for which the baseline counts as balanced.
const balanceTolerance = 0.05

// Balanced reports whether the standard deviations of disk utilisation and of
// count ratio (replicas over the mean replica count, 0 when that mean is 0)
// are both within balanceTolerance.
func Balanced(rows []types.RebalanceNodeBalance) bool {
	disk := make([]float64, len(rows))
	counts := make([]float64, len(rows))
	for i, r := range rows {
		disk[i] = r.DiskUtilisation
		counts[i] = float64(r.ReplicaCount)
	}
	ratios := make([]float64, len(rows))
	if m := mean(counts); m != 0 {
		for i, c := range counts {
			ratios[i] = c / m
		}
	}
	return stddev(disk) <= balanceTolerance && stddev(ratios) <= balanceTolerance
}

func mean(xs []float64) float64 {
	sum := 0.0
	for _, x := range xs {
		sum += x
	}
	return sum / float64(len(xs))
}

// stddev is the population standard deviation of xs.
func stddev(xs []float64) float64 {
	m := mean(xs)
	sum := 0.0
	for _, x := range xs {
		sum += (x - m) * (x - m)
	}
	return math.Sqrt(sum / float64(len(xs)))
}
