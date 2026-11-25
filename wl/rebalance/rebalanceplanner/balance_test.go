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
	"fmt"
	"math"
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
	// many returns n source rows at disk utilisation source, then m target
	// rows at disk utilisation target. It also returns the source names.
	many := func(n int, source float64, m int, target float64) ([]types.RebalanceNodeBalance, []string) {
		var out []types.RebalanceNodeBalance
		var sources []string
		for i := 0; i < n; i++ {
			name := fmt.Sprintf("s%d", i)
			out = append(out, types.RebalanceNodeBalance{Node: name, DiskUtilisation: source})
			sources = append(sources, name)
		}
		for i := 0; i < m; i++ {
			out = append(out, types.RebalanceNodeBalance{Node: fmt.Sprintf("t%d", i), DiskUtilisation: target})
		}
		return out, sources
	}
	fullSource, fullSourceNames := many(1, 0.95, 99, 0.5)
	emptyTarget, emptyTargetNames := many(99, 0.5, 1, 0)
	// The tolerance is 0.10. node-2 is the source in these rows.
	node2 := []string{"node-2"}
	tests := []struct {
		name    string
		rows    []types.RebalanceNodeBalance
		sources []string
		want    bool
	}{
		{
			name:    "source within tolerance above the target",
			rows:    rows([2]float64{0.50, 0.54}, [2]int{10, 10}),
			sources: node2,
			want:    true,
		},
		{
			name:    "source over tolerance above the target",
			rows:    rows([2]float64{0.40, 0.60}, [2]int{10, 10}),
			sources: node2,
			want:    false,
		},
		{
			name:    "skewed counts with level disk are balanced",
			rows:    rows([2]float64{0.5, 0.5}, [2]int{3, 1}),
			sources: node2,
			want:    true,
		},
		{
			// 0.1 − 0 is exactly 0.1 in float64.
			name:    "source exactly at tolerance above the target counts as balanced",
			rows:    rows([2]float64{0, 0.1}, [2]int{10, 10}),
			sources: node2,
			want:    true,
		},
		{
			name:    "one full source among 99 targets",
			rows:    fullSource,
			sources: fullSourceNames,
			want:    false,
		},
		{
			name:    "one empty target among 99 sources",
			rows:    emptyTarget,
			sources: emptyTargetNames,
			want:    false,
		},
		{
			name:    "source emptier than every target",
			rows:    rows([2]float64{0.75, 0.25}, [2]int{10, 10}),
			sources: node2,
			want:    true,
		},
		{
			name: "sources far apart, none more than the tolerance above the target",
			rows: []types.RebalanceNodeBalance{
				{Node: "s0", DiskUtilisation: 0.9},
				{Node: "s1", DiskUtilisation: 0.5},
				{Node: "t0", DiskUtilisation: 0.85},
			},
			sources: []string{"s0", "s1"},
			want:    true,
		},
		{
			name: "sources far apart, the fuller one over the tolerance above the target",
			rows: []types.RebalanceNodeBalance{
				{Node: "s0", DiskUtilisation: 0.9},
				{Node: "s1", DiskUtilisation: 0.5},
				{Node: "t0", DiskUtilisation: 0.5},
			},
			sources: []string{"s0", "s1"},
			want:    false,
		},
		{
			name: "targets far apart, the source over the tolerance above the emptier one",
			rows: []types.RebalanceNodeBalance{
				{Node: "s0", DiskUtilisation: 0.6},
				{Node: "t0", DiskUtilisation: 0.55},
				{Node: "t1", DiskUtilisation: 0.4},
			},
			sources: []string{"s0"},
			want:    false,
		},
		{
			name:    "infinite utilisation on a target",
			rows:    rows([2]float64{math.Inf(1), 0.5}, [2]int{10, 10}),
			sources: node2,
			want:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, rebalanceplanner.Balanced(tt.rows, tt.sources))
		})
	}
}
