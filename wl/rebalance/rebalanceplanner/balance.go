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

// balanceTolerance is the largest standard deviation of disk utilisation for
// which the baseline counts as balanced.
const balanceTolerance = 0.05

// Balanced reports whether the standard deviation of disk utilisation is
// within balanceTolerance. Replica counts do not affect the result. A NaN or
// infinite disk utilisation makes it false.
func Balanced(rows []types.RebalanceNodeBalance) bool {
	disk := make([]float64, len(rows))
	for i, r := range rows {
		disk[i] = r.DiskUtilisation
	}
	return stddev(disk) <= balanceTolerance
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
