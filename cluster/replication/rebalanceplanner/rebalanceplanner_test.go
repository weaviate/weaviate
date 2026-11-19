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
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	rp "github.com/weaviate/weaviate/cluster/replication/rebalanceplanner"
	"github.com/weaviate/weaviate/cluster/replication/types"
)

// Unless a case says otherwise, every node has 16 usable bytes and one
// worker. With two nodes, each variance is (half the spread)².

func load(role rp.Role, used int64, replicas int) rp.NodeLoad {
	return rp.NodeLoad{Role: role, UsedBytes: used, UsableBytes: 16, Workers: 1, Replicas: replicas}
}

func TestCost(t *testing.T) {
	tests := []struct {
		name string
		p    rp.Placement
		want float64
	}{
		{
			// disk 0.75, 0.25: variance 0.25² = 0.0625. counts 1, 1: 0.
			name: "disk variance term",
			p: rp.Placement{
				Nodes:           []rp.NodeLoad{load(rp.Source, 12, 1), load(rp.Target, 4, 1)},
				SourceUsedBytes: 12,
			},
			want: 0.0625,
		},
		{
			// counts 3, 1: mean 2, ratios 1.5, 0.5, variance 0.5² = 0.25.
			name: "count-ratio term",
			p: rp.Placement{
				Nodes:           []rp.NodeLoad{load(rp.Source, 8, 3), load(rp.Target, 8, 1)},
				SourceUsedBytes: 8,
			},
			want: 0.25,
		},
		{
			name: "count-ratio term with a zero mean",
			p: rp.Placement{
				Nodes:           []rp.NodeLoad{load(rp.Source, 8, 0), load(rp.Target, 8, 0)},
				SourceUsedBytes: 8,
			},
			want: 0,
		},
		{
			// 0.1 × 4 / 10.
			name: "movement term",
			p: rp.Placement{
				Nodes:           []rp.NodeLoad{load(rp.Source, 8, 2), load(rp.Target, 8, 2)},
				MovedBytes:      4,
				SourceUsedBytes: 10,
			},
			want: 0.04,
		},
		{
			name: "zero source used bytes",
			p: rp.Placement{
				Nodes:      []rp.NodeLoad{load(rp.Source, 0, 0), load(rp.Target, 0, 0)},
				MovedBytes: 4,
			},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.InDelta(t, tt.want, rp.Cost(tt.p), 1e-12)
		})
	}
}

func TestAcceptable(t *testing.T) {
	// Baseline: disk 0.75, 0.25 (variance 0.0625); counts 3, 1 (variance
	// 0.25); cost 0.3125.
	baseline := rp.Placement{
		Nodes:           []rp.NodeLoad{load(rp.Source, 12, 3), load(rp.Target, 4, 1)},
		SourceUsedBytes: 12,
	}
	// One 4-byte move: disk 0.5, 0.5 and counts 2, 2, so both variances are 0.
	moved := func(movedBytes int64, targetWorkers int) rp.Placement {
		target := load(rp.Target, 8, 2)
		target.Workers, target.LargestIncoming = targetWorkers, 4
		return rp.Placement{
			Nodes:           []rp.NodeLoad{load(rp.Source, 8, 2), target},
			MovedBytes:      movedBytes,
			SourceUsedBytes: 12,
		}
	}
	incoming := func(l rp.NodeLoad, largest int64) rp.NodeLoad {
		l.LargestIncoming = largest
		return l
	}

	tests := []struct {
		name     string
		baseline rp.Placement
		plan     rp.Placement
		want     bool
	}{
		{
			// cost 0.1 × 4 / 12 = 0.0333 < 0.3125; disk variance falls 0.0625;
			// target 8 + 1 × 4 = 12 ≤ 16.
			name:     "all three rules pass",
			baseline: baseline,
			plan:     moved(4, 1),
			want:     true,
		},
		{
			// cost 0.1 × 40 / 12 = 0.3333 ≥ 0.3125. Rules 2 and 3 pass as above.
			name:     "cost not lower",
			baseline: baseline,
			plan:     moved(40, 1),
			want:     false,
		},
		{
			// target 8 + 3 × 4 = 20 > 16; 8 alone fits. Rules 1 and 2 pass as above.
			name:     "target over usable bytes with headroom",
			baseline: baseline,
			plan:     moved(4, 3),
			want:     false,
		},
		{
			// target 8 + 2 × 4 = 16, exactly the usable bytes. Rules 1 and 2 pass as above.
			name:     "target exactly at usable bytes with headroom",
			baseline: baseline,
			plan:     moved(4, 2),
			want:     true,
		},
		{
			// Baseline: disk 1.25, 0.25 (variance 0.25); counts 2, 2 (0); cost 0.25.
			// Plan: disk 1.125, 0.375 (0.140625); movement 0.1 × 2 / 20 = 0.01;
			// cost 0.150625. Disk variance falls 0.109375; target 6 + 2 = 8 ≤ 16.
			// The source's 18 bytes exceed its 16 usable bytes.
			name: "source over usable bytes",
			baseline: rp.Placement{
				Nodes:           []rp.NodeLoad{load(rp.Source, 20, 2), load(rp.Target, 4, 2)},
				SourceUsedBytes: 20,
			},
			plan: rp.Placement{
				Nodes:           []rp.NodeLoad{load(rp.Source, 18, 2), incoming(load(rp.Target, 6, 2), 2)},
				MovedBytes:      2,
				SourceUsedBytes: 20,
			},
			want: true,
		},
		{
			// Baseline: disk 0.75, 0.25 (0.0625); counts 2, 2 (0); cost 0.0625.
			// Plan: disk 0.5, 0.5 (0); counts 2, 2 (0); movement 0.1 × 4 / 12 =
			// 0.0333. Disk variance falls 0.0625; count-ratio variance does not
			// fall. Target 8 + 4 = 12 ≤ 16.
			name: "disk gain alone passes rule 2",
			baseline: rp.Placement{
				Nodes:           []rp.NodeLoad{load(rp.Source, 12, 2), load(rp.Target, 4, 2)},
				SourceUsedBytes: 12,
			},
			plan: rp.Placement{
				Nodes:           []rp.NodeLoad{load(rp.Source, 8, 2), incoming(load(rp.Target, 8, 2), 4)},
				MovedBytes:      4,
				SourceUsedBytes: 12,
			},
			want: true,
		},
		{
			// Baseline: disk 0.5, 0.5 (0); counts 3, 1 (0.25); cost 0.25.
			// Plan: disk 0.5, 0.5 (0); counts 2, 2 (0); movement 0.1 × 1 / 12 =
			// 0.0083. Disk variance does not fall; count-ratio variance falls
			// 0.25. Target 8 + 1 = 9 ≤ 16. Acceptable takes MovedBytes and
			// SourceUsedBytes as given. This row sets them independently of the
			// node loads to isolate rule 2.
			name: "count gain alone passes rule 2",
			baseline: rp.Placement{
				Nodes:           []rp.NodeLoad{load(rp.Source, 8, 3), load(rp.Target, 8, 1)},
				SourceUsedBytes: 12,
			},
			plan: rp.Placement{
				Nodes:           []rp.NodeLoad{load(rp.Source, 8, 2), incoming(load(rp.Target, 8, 2), 1)},
				MovedBytes:      1,
				SourceUsedBytes: 12,
			},
			want: true,
		},
		{
			// Baseline: disk 0.75, 0.25 (0.0625); counts 2, 2 (0); cost 0.0625.
			// Plan: disk 0.5, 0.5 (0); counts 2, 2 (0); movement 0.1 × 10 / 16 =
			// 0.0625. Both costs are exactly 2⁻⁴ in float64, so rule 1 fails.
			// Rules 2 and 3 pass: disk variance falls 0.0625; target 8 + 4 = 12 ≤ 16.
			name: "cost equal to the baseline",
			baseline: rp.Placement{
				Nodes:           []rp.NodeLoad{load(rp.Source, 12, 2), load(rp.Target, 4, 2)},
				SourceUsedBytes: 16,
			},
			plan: rp.Placement{
				Nodes:           []rp.NodeLoad{load(rp.Source, 8, 2), incoming(load(rp.Target, 8, 2), 4)},
				MovedBytes:      10,
				SourceUsedBytes: 16,
			},
			want: false,
		},
		{
			// Usable 10,000. Baseline: disk 0.52, 0.48 (variance 0.0004); counts
			// 101, 99 (ratios 1.01, 0.99, variance 0.0001); cost 0.0005.
			// Plan, one 1-byte move: disk 0.5199, 0.4801 (variance 0.00039601);
			// counts 100, 100 (variance 0); movement 0.1 × 1 / 5,200 = 0.0000192;
			// cost 0.0004152 < 0.0005. The variances fall by 0.00000399 and
			// 0.0001, both under 0.001. Target 4,801 + 1 ≤ 10,000.
			name: "minGain missed on both terms",
			baseline: rp.Placement{
				Nodes: []rp.NodeLoad{
					{Role: rp.Source, UsedBytes: 5200, UsableBytes: 10000, Workers: 1, Replicas: 101},
					{Role: rp.Target, UsedBytes: 4800, UsableBytes: 10000, Workers: 1, Replicas: 99},
				},
				SourceUsedBytes: 5200,
			},
			plan: rp.Placement{
				Nodes: []rp.NodeLoad{
					{Role: rp.Source, UsedBytes: 5199, UsableBytes: 10000, Workers: 1, Replicas: 100},
					{Role: rp.Target, UsedBytes: 4801, UsableBytes: 10000, Workers: 1, Replicas: 100, LargestIncoming: 1},
				},
				MovedBytes:      1,
				SourceUsedBytes: 5200,
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, rp.Acceptable(tt.baseline, tt.plan))
		})
	}
}

func TestBalanceRule(t *testing.T) {
	// disk 0.75, 0.25 and counts 3, 1: 0.0625 + 0.25 = 0.3125.
	skewed := []rp.NodeLoad{load(rp.Source, 12, 3), load(rp.Target, 4, 1)}
	// disk 0.5, 0.5 and counts 2, 2: 0.
	even := []rp.NodeLoad{load(rp.Source, 8, 2), load(rp.Target, 8, 2)}

	tests := []struct {
		name          string
		before, after []rp.NodeLoad
		want          bool
	}{
		{name: "lower balance term passes", before: skewed, after: even, want: true},
		{name: "higher balance term fails", before: even, after: skewed, want: false},
		{name: "unchanged balance term fails", before: skewed, after: skewed, want: false},
		{
			// before: disk 0.75, 0.25 (0.0625); counts 10, 10 (0); 0.0625.
			// after: disk 0.5, 0.5 (0); counts 9, 11 (ratios 0.9, 1.1, 0.01); 0.01.
			name:   "more replicas on the target passes on disk gain",
			before: []rp.NodeLoad{load(rp.Source, 12, 10), load(rp.Target, 4, 10)},
			after:  []rp.NodeLoad{load(rp.Source, 8, 9), load(rp.Target, 8, 11)},
			want:   true,
		},
		{
			// before: disk 0.75, 0.25 (0.0625); counts 2, 2 (0); 0.0625.
			// after: disk 0.5, 0.5 (0); counts 3, 1 (0.25); 0.25.
			name:   "disk gain outweighed by count-ratio loss fails",
			before: []rp.NodeLoad{load(rp.Source, 12, 2), load(rp.Target, 4, 2)},
			after:  []rp.NodeLoad{load(rp.Source, 8, 3), load(rp.Target, 8, 1)},
			want:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, rp.PassesBalanceRule(tt.before, tt.after))
		})
	}
}

func source(name string, used int64) rp.Node {
	return rp.Node{Name: name, Role: rp.Source, UsedBytes: used, UsableBytes: 16, Workers: 1}
}

func target(name string, used int64, workers int) rp.Node {
	return rp.Node{Name: name, Role: rp.Target, UsedBytes: used, UsableBytes: 16, Workers: workers}
}

func replica(node, shard string, bytes int64) rp.Replica {
	return rp.Replica{Collection: "c", Shard: shard, Node: node, Bytes: bytes, Eligible: true}
}

func ineligible(node, shard string, bytes int64) rp.Replica {
	r := replica(node, shard, bytes)
	r.Eligible = false
	return r
}

func move(shard, from, to string, bytes int64) rp.Move {
	return rp.Move{Collection: "c", Shard: shard, SourceNode: from, TargetNode: to, Bytes: bytes}
}

func row(node string, disk float64, count int) types.RebalanceNodeBalance {
	return types.RebalanceNodeBalance{Node: node, DiskUtilisation: disk, ReplicaCount: count}
}

func TestSearch(t *testing.T) {
	// Source S holds a, b, c of 4 bytes each; targets T1 and T2 are empty.
	// Baseline: disk 0.75, 0, 0 (variance 0.125); counts 3, 0, 0 (ratios
	// 3, 0, 0, variance 2); cost 2.125.
	//   1. Every move ties: disk 0.5, 0.25, 0 (0.0417); counts 2, 1, 0 (0.6667);
	//      movement 0.0333; cost 0.7417. a → T1 is first.
	//   2. b → T2: disk 0.25 × 3 (0); counts 1, 1, 1 (0); movement 0.0667;
	//      cost 0.0667. b → T1 costs 0.775.
	//   3. c to either target: disk 0, 0.5, 0.25 (0.0417); counts 0, 2, 1
	//      (0.6667); movement 0.1; cost 0.808. Search stops.
	threeNodes := []rp.Node{source("S", 12), target("T1", 0, 1), target("T2", 0, 1)}
	threeReplicas := []rp.Replica{replica("S", "a", 4), replica("S", "b", 4), replica("S", "c", 4)}
	threeMoves := []rp.Move{move("a", "S", "T1", 4), move("b", "S", "T2", 4)}

	// S holds s01 to s10 of 1 byte each; T is empty. After k moves the cost
	// is ((10−2k)/32)² + ((10−2k)/10)² + 0.01k: for k = 0 to 6, 1.0977,
	// 0.7125, 0.4152, 0.2056, 0.0839, 0.05, 0.1039. The plan needs 5 moves.
	tenNodes := []rp.Node{source("S", 10), target("T", 0, 1)}
	var tenReplicas []rp.Replica
	var fiveMoves []rp.Move
	for i := 1; i <= 10; i++ {
		shard := fmt.Sprintf("s%02d", i)
		tenReplicas = append(tenReplicas, replica("S", shard, 1))
		if i <= 5 {
			fiveMoves = append(fiveMoves, move(shard, "S", "T", 1))
		}
	}

	// S uses 10 (6, 4) and T 8 (5, 3). Baseline: disk 0.625, 0.5 (0.0039);
	// counts 2, 2 (0); cost 0.0039. Every move leaves counts 1, 3 (0.25):
	// moving the 4 costs 0.0352 + 0.25 + 0.04 = 0.325, the 6 costs
	// 0.0977 + 0.25 + 0.06 = 0.408. Both raise cost, so Search takes no move.
	noMove := rp.Input{
		Nodes: []rp.Node{source("S", 10), target("T", 8, 1)},
		Replicas: []rp.Replica{
			replica("S", "s6", 6), replica("S", "s4", 4), replica("T", "t5", 5), replica("T", "t3", 3),
		},
		MaxMoves: 10,
	}

	// S uses 16 bytes: a (4) and b (2) are eligible; x, y and z (10 in all)
	// are not. T holds only t, of targetUsed bytes. Counts 5, 1 (ratios 5/3,
	// 1/3, variance 0.4444); after one move 4, 2 (0.1111); after two, 3, 3 (0).
	twoIncoming := func(targetUsed int64) rp.Input {
		return rp.Input{
			Nodes: []rp.Node{source("S", 16), target("T", targetUsed, 1)},
			Replicas: []rp.Replica{
				replica("S", "a", 4), replica("S", "b", 2),
				ineligible("S", "x", 4), ineligible("S", "y", 3), ineligible("S", "z", 3),
				replica("T", "t", targetUsed),
			},
			MaxMoves: 10,
		}
	}

	tests := []struct {
		name string
		in   rp.Input
		want rp.Result
	}{
		{
			// twoIncoming with T using 5. Baseline: disk 1, 0.3125 (0.1182); cost
			// 0.5626. Each candidate below is disk (variance) + count-ratio
			// variance + movement = cost.
			//   1. a: disk 0.75, 0.5625 (0.0088) + 0.1111 + 0.1 × 4 / 16 = 0.1449.
			//      b: disk 0.875, 0.4375 (0.0479) + 0.1111 + 0.0125 = 0.1715.
			//   2. b: disk 0.625, 0.6875 (0.0010) + 0 + 0.0375 = 0.0385.
			// After move 2, T needs 11 + 1 × 4 = 15 ≤ 16. With the sum of the
			// incoming bytes it would need 11 + 6 = 17 > 16.
			name: "headroom uses the largest incoming replica, not the sum",
			in:   twoIncoming(5),
			want: rp.Result{
				Verdict: rp.MovesFound,
				Moves:   []rp.Move{move("a", "S", "T", 4), move("b", "S", "T", 2)},
				Goal:    []types.RebalanceNodeBalance{row("S", 0.625, 3), row("T", 0.6875, 3)},
			},
		},
		{
			// twoIncoming with T using 7. Baseline: disk 1, 0.4375 (0.0791); cost
			// 0.5235.
			//   1. a: disk 0.75, 0.6875 (0.0010) + 0.1111 + 0.025 = 0.1371.
			//      b: disk 0.875, 0.5625 (0.0244) + 0.1111 + 0.0125 = 0.1480.
			//   2. b: disk 0.625, 0.8125 (0.0088) + 0 + 0.0375 = 0.0463.
			// After move 1, T needs 11 + 4 = 15 ≤ 16. After move 2 it needs
			// 13 + 4 = 17 > 16; with the last incoming replica, 13 + 2 = 15 would fit.
			name: "headroom uses the largest incoming replica, not the last",
			in:   twoIncoming(7),
			want: rp.Result{
				Verdict: rp.MovesFound,
				Moves:   []rp.Move{move("a", "S", "T", 4)},
				Goal:    []types.RebalanceNodeBalance{row("S", 0.75, 4), row("T", 0.6875, 2)},
			},
		},
		{
			// S uses 11: a (7) is eligible, b (4) is not. T uses 1 (t). Counts go
			// from 2, 1 to 1, 2; count-ratio variance is 0.1111 both times.
			// Baseline: disk 0.6875, 0.0625 (0.0977); cost 0.2088. Moving a: disk
			// 0.25, 0.5 (0.0156); movement 0.1 × 7 / 11 = 0.0636; cost 0.1904.
			// T needs 8 + 7 = 15 ≤ 16. Divided by the 4 bytes left on S instead,
			// movement would be 0.1 × 7 / 4 = 0.175 and cost 0.3017, above the
			// baseline.
			name: "movement is relative to the sources' used bytes before any move",
			in: rp.Input{
				Nodes:    []rp.Node{source("S", 11), target("T", 1, 1)},
				Replicas: []rp.Replica{replica("S", "a", 7), ineligible("S", "b", 4), replica("T", "t", 1)},
				MaxMoves: 10,
			},
			want: rp.Result{
				Verdict: rp.MovesFound,
				Moves:   []rp.Move{move("a", "S", "T", 7)},
				Goal:    []types.RebalanceNodeBalance{row("S", 0.25, 1), row("T", 0.5, 2)},
			},
		},
		{
			// Baseline: disk 0.75, 0.25 (0.0625); counts 3, 1 (0.25); cost 0.3125.
			// Each move leaves counts 2, 2 (0). Movement is 0.1 × bytes / 12.
			//   s1 (6): disk 0.375, 0.625 (0.015625) + 0.05   = 0.0656
			//   s2 (4): disk 0.5, 0.5 (0)            + 0.0333 = 0.0333
			//   s3 (2): disk 0.625, 0.375 (0.015625) + 0.0167 = 0.0323
			// Next, s1 costs 0.379 and s2 0.3156, so Search stops.
			name: "two-node layout",
			in: rp.Input{
				Nodes: []rp.Node{source("S", 12), target("T", 4, 1)},
				Replicas: []rp.Replica{
					replica("S", "s1", 6), replica("S", "s2", 4), replica("S", "s3", 2), replica("T", "s4", 4),
				},
				MaxMoves: 10,
			},
			want: rp.Result{
				Verdict: rp.MovesFound,
				Moves:   []rp.Move{move("s3", "S", "T", 2)},
				Goal:    []types.RebalanceNodeBalance{row("S", 0.625, 2), row("T", 0.375, 2)},
			},
		},
		{
			name: "three-node layout",
			in:   rp.Input{Nodes: threeNodes, Replicas: threeReplicas, MaxMoves: 10},
			want: rp.Result{
				Verdict: rp.MovesFound,
				Moves:   threeMoves,
				Goal:    []types.RebalanceNodeBalance{row("S", 0.25, 1), row("T1", 0.25, 1), row("T2", 0.25, 1)},
			},
		},
		{
			// The three-node layout in reverse order. Without sorting, c → T2
			// would win the first tie. Goal rows follow the input order.
			name: "shuffled input gives identical moves",
			in: rp.Input{
				Nodes:    []rp.Node{target("T2", 0, 1), target("T1", 0, 1), source("S", 12)},
				Replicas: []rp.Replica{replica("S", "c", 4), replica("S", "b", 4), replica("S", "a", 4)},
				MaxMoves: 10,
			},
			want: rp.Result{
				Verdict: rp.MovesFound,
				Moves:   threeMoves,
				Goal:    []types.RebalanceNodeBalance{row("T2", 0.25, 1), row("T1", 0.25, 1), row("S", 0.25, 1)},
			},
		},
		{
			// S1 holds a, b, c and S2 holds a, d, e, 4 bytes each; T is empty.
			// Baseline: disk 0.75, 0.75, 0 (0.125); counts 3, 3, 0 (0.5); cost 0.625.
			//   1. Every move ties: disk 0.5, 0.75, 0.25 (0.0417); counts 2, 3, 1
			//      (0.1667); movement 0.1 × 4 / 24 = 0.0167; cost 0.225.
			//      a: S1 → T is first.
			//   2. A move from S2: disk 0.5 × 3 (0); counts 2, 2, 2 (0); cost
			//      0.0333. S2's a would be first, but T holds a, so d goes.
			//   3. Any move costs 0.258. Search stops.
			name: "a shard on two sources reaches a target once",
			in: rp.Input{
				Nodes: []rp.Node{source("S1", 12), source("S2", 12), target("T", 0, 1)},
				Replicas: []rp.Replica{
					replica("S1", "a", 4), replica("S1", "b", 4), replica("S1", "c", 4),
					replica("S2", "a", 4), replica("S2", "d", 4), replica("S2", "e", 4),
				},
				MaxMoves: 10,
			},
			want: rp.Result{
				Verdict: rp.MovesFound,
				Moves:   []rp.Move{move("a", "S1", "T", 4), move("d", "S2", "T", 4)},
				Goal:    []types.RebalanceNodeBalance{row("S1", 0.5, 2), row("S2", 0.5, 2), row("T", 0.5, 2)},
			},
		},
		{
			// Usable 100,000. S uses 100,000 (r1 1,000 bytes, r2 99,000 bytes
			// ineligible) and T 88,995 (t1). Disk 1, 0.88995 (variance 0.0030277506).
			// Moving r1: disk 0.99, 0.89995 (0.0020272506), a fall of 0.0010005;
			// counts 2, 1 become 1, 2 at the same variance; movement 0.1 × 1,000 /
			// 100,000 = 0.001. Cost falls by 0.0000005, under epsilon 0.000001.
			// The move alone would pass acceptance.
			name: "epsilon stop",
			in: rp.Input{
				Nodes: []rp.Node{
					{Name: "S", Role: rp.Source, UsedBytes: 100_000, UsableBytes: 100_000, Workers: 1},
					{Name: "T", Role: rp.Target, UsedBytes: 88_995, UsableBytes: 100_000, Workers: 1},
				},
				Replicas: []rp.Replica{
					replica("S", "r1", 1_000), ineligible("S", "r2", 99_000), replica("T", "t1", 88_995),
				},
				MaxMoves: 10,
			},
			want: rp.Result{Verdict: rp.NoAcceptablePlan},
		},
		{
			// The three-node layout with a marked ineligible. The figures are the
			// same; b and c move, and S keeps a in its count.
			name: "ineligible replicas are counted but never moved",
			in: rp.Input{
				Nodes:    threeNodes,
				Replicas: []rp.Replica{ineligible("S", "a", 4), replica("S", "b", 4), replica("S", "c", 4)},
				MaxMoves: 10,
			},
			want: rp.Result{
				Verdict: rp.MovesFound,
				Moves:   []rp.Move{move("b", "S", "T1", 4), move("c", "S", "T2", 4)},
				Goal:    []types.RebalanceNodeBalance{row("S", 0.25, 1), row("T1", 0.25, 1), row("T2", 0.25, 1)},
			},
		},
		{
			// The three-node layout with 4 workers on T2. After move 2, T2 needs
			// 4 + 4 × 4 = 20 > 16 bytes, so only move 1 passes acceptance.
			name: "last accepted prefix is returned",
			in: rp.Input{
				Nodes:    []rp.Node{source("S", 12), target("T1", 0, 1), target("T2", 0, 4)},
				Replicas: threeReplicas,
				MaxMoves: 10,
			},
			want: rp.Result{
				Verdict: rp.MovesFound,
				Moves:   threeMoves[:1],
				Goal:    []types.RebalanceNodeBalance{row("S", 0.5, 2), row("T1", 0.25, 1), row("T2", 0, 0)},
			},
		},
		{
			// After 3 moves, move 4 still lowers cost from 0.2056 to 0.0839.
			name: "limit exceeded",
			in:   rp.Input{Nodes: tenNodes, Replicas: tenReplicas, MaxMoves: 3},
			want: rp.Result{Verdict: rp.LimitExceeded, MinMoveCount: 4},
		},
		{
			// After 5 moves, move 6 raises cost from 0.05 to 0.1039.
			name: "MaxMoves equal to the moves needed",
			in:   rp.Input{Nodes: tenNodes, Replicas: tenReplicas, MaxMoves: 5},
			want: rp.Result{
				Verdict: rp.MovesFound,
				Moves:   fiveMoves,
				Goal:    []types.RebalanceNodeBalance{row("S", 0.3125, 5), row("T", 0.3125, 5)},
			},
		},
		{
			name: "[6,4]/[5,3] has no acceptable plan",
			in:   noMove,
			want: rp.Result{Verdict: rp.NoAcceptablePlan},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := rp.Search(context.Background(), tt.in)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}

	cancelTests := []struct {
		name string
		in   rp.Input
		live int // checks that see a live context
	}{
		{
			// noMove has no move, so Search makes one check: the one before the first step.
			name: "context cancelled before the first step",
			in:   noMove,
			live: 0,
		},
		{
			// Five live checks allow the ten-replica layout's 5 moves. The sixth
			// check fails before the scan that would find no further move.
			name: "context that ends after the last move",
			in:   rp.Input{Nodes: tenNodes, Replicas: tenReplicas, MaxMoves: 10},
			live: 5,
		},
	}
	for _, tt := range cancelTests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := &cancelAfter{Context: context.Background(), live: tt.live}
			got, err := rp.Search(ctx, tt.in)
			require.ErrorIs(t, err, context.Canceled)
			require.Equal(t, rp.Result{}, got)
		})
	}
}

// cancelAfter counts each Err or Done call as one check. The first checks, as
// many as the live field holds, see a live context: Err returns nil and Done
// returns a nil channel, which never receives. Every later check sees a
// cancelled context: Err returns context.Canceled and Done returns a closed
// channel.
type cancelAfter struct {
	context.Context
	live int
}

func (c *cancelAfter) cancelled() bool {
	if c.live > 0 {
		c.live--
		return false
	}
	return true
}

func (c *cancelAfter) Err() error {
	if c.cancelled() {
		return context.Canceled
	}
	return nil
}

func (c *cancelAfter) Done() <-chan struct{} {
	if !c.cancelled() {
		return nil
	}
	done := make(chan struct{})
	close(done)
	return done
}
