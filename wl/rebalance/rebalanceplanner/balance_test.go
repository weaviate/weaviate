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

package rebalanceplanner_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/wl/rebalance/rebalanceplanner"
)

func TestBalanced(t *testing.T) {
	rows := func(disk [2]float64, counts [2]int) []types.RebalanceNodeBalance {
		return []types.RebalanceNodeBalance{
			{Node: "node-1", DiskUtilisation: disk[0], ReplicaCount: counts[0]},
			{Node: "node-2", DiskUtilisation: disk[1], ReplicaCount: counts[1]},
		}
	}
	// The tolerance is 0.05. For two nodes, the disk stddev is half the spread.
	tests := []struct {
		name string
		rows []types.RebalanceNodeBalance
		want bool
	}{
		{
			// disk stddev 0.02.
			name: "disk deviation within tolerance",
			rows: rows([2]float64{0.50, 0.54}, [2]int{10, 10}),
			want: true,
		},
		{
			// disk stddev 0.1.
			name: "disk deviation over tolerance",
			rows: rows([2]float64{0.40, 0.60}, [2]int{10, 10}),
			want: false,
		},
		{
			// disk stddev 0; counts 3, 1.
			name: "skewed counts with level disk are balanced",
			rows: rows([2]float64{0.5, 0.5}, [2]int{3, 1}),
			want: true,
		},
		{
			// disk 0 and 0.1: mean 0.05, stddev exactly 0.05 in float64.
			name: "deviation exactly at tolerance counts as balanced",
			rows: rows([2]float64{0, 0.1}, [2]int{10, 10}),
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, rebalanceplanner.Balanced(tt.rows))
		})
	}
}
