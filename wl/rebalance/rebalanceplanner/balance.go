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
	"slices"

	"github.com/weaviate/weaviate/cluster/replication/types"
)

// BalanceTolerance is the largest amount by which a source's disk utilisation
// may exceed a target's for the nodes to count as balanced.
//
// TODO(calibration): BalanceTolerance is a placeholder until tests on large
// clusters set it.
const BalanceTolerance = 0.10

// Balanced reports whether no source's disk utilisation exceeds any target's
// by more than BalanceTolerance. A row whose node is in sources is a source;
// every other row is a target. If the rows hold no source or no target,
// Balanced returns true.
// Replica counts do not affect the result. A NaN or infinite disk utilisation
// in any row makes it false.
func Balanced(rows []types.RebalanceNodeBalance, sources []string) bool {
	maxSource, minTarget := math.Inf(-1), math.Inf(1)
	for _, r := range rows {
		d := r.DiskUtilisation
		if math.IsNaN(d) || math.IsInf(d, 0) {
			return false
		}
		if slices.Contains(sources, r.Node) {
			maxSource = max(maxSource, d)
		} else {
			minTarget = min(minTarget, d)
		}
	}
	return maxSource-minTarget <= BalanceTolerance
}
