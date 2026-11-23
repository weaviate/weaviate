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
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	rp "github.com/weaviate/weaviate/cluster/replication/rebalanceplanner"
	"github.com/weaviate/weaviate/cluster/replication/types"
)

// Unless a case says otherwise, every node has 16 usable bytes and one
// worker. With two nodes, the disk variance is (half the spread)².

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

// In TestSearch comments, a figure in parentheses after a list of disk
// utilisations is their variance.
func TestSearch(t *testing.T) {
	// Source S holds a, b, c of 4 bytes each; targets T1 and T2 are empty.
	// Baseline: disk 0.75, 0, 0 (0.125).
	//   1. Every move ties: disk 0.5, 0.25, 0 (0.0417). a → T1 is first.
	//   2. b → T2: disk 0.25 × 3 (0). b → T1 gives disk 0.25, 0.5, 0
	//      (0.0417), equal to the current value, so it does not gain.
	//   3. The state passes Balanced, so Search stops.
	threeNodes := []rp.Node{source("S", 12), target("T1", 0, 1), target("T2", 0, 1)}
	threeReplicas := []rp.Replica{replica("S", "a", 4), replica("S", "b", 4), replica("S", "c", 4)}
	threeMoves := []rp.Move{move("a", "S", "T1", 4), move("b", "S", "T2", 4)}
	threeResult := rp.Result{
		Verdict: rp.MovesFound,
		Moves:   threeMoves,
		Goal:    []types.RebalanceNodeBalance{row("S", 0.25, 1), row("T1", 0.25, 1), row("T2", 0.25, 1)},
	}

	// S holds s01 to s10 of 1 byte each; T is empty. After k moves the disk
	// stddev is (10−2k)/32. Its square, the variance, is 0.0977, 0.0625,
	// 0.0352, 0.0156, 0.0039, 0 for k = 0 to 5. The stddev is within 0.05 only
	// at k = 5, so the plan needs 5 moves.
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

	// S uses 10 (6, 4) and T 8 (5, 3). Baseline: disk 0.625, 0.5 (0.0039),
	// stddev 0.0625, so it is unbalanced. Moving the 4 gives disk 0.375, 0.75
	// (0.0352); moving the 6 gives 0.25, 0.875 (0.0977). Either move widens
	// the spread, so Search takes none.
	noMove := rp.Input{
		Nodes: []rp.Node{source("S", 10), target("T", 8, 1)},
		Replicas: []rp.Replica{
			replica("S", "s6", 6), replica("S", "s4", 4), replica("T", "t5", 5), replica("T", "t3", 3),
		},
		MaxMoves: 10,
	}

	// S uses 12 + xBytes: a (4) and b (2) are eligible; x (xBytes), y (3) and
	// z (3) are not. T holds only t, of targetUsed bytes.
	twoIncoming := func(targetUsed, xBytes int64) rp.Input {
		return rp.Input{
			Nodes: []rp.Node{source("S", 12+xBytes), target("T", targetUsed, 1)},
			Replicas: []rp.Replica{
				replica("S", "a", 4), replica("S", "b", 2),
				ineligible("S", "x", xBytes), ineligible("S", "y", 3), ineligible("S", "z", 3),
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
			// twoIncoming with T using 5 and x of 4. Baseline: disk 1, 0.3125
			// (0.1182).
			//   1. a: disk 0.75, 0.5625 (0.0088). b: disk 0.875, 0.4375 (0.0479).
			//      a wins; stddev 0.0938 fails Balanced.
			//   2. b: disk 0.625, 0.6875 (0.0010). The state passes Balanced.
			// Before move 2, T needs 9 + 2 + 1 × 4 = 15 ≤ 16. With the sum of the
			// incoming bytes it would need 9 + 2 + 6 = 17 > 16.
			name: "headroom uses the largest incoming replica, not the sum",
			in:   twoIncoming(5, 4),
			want: rp.Result{
				Verdict: rp.MovesFound,
				Moves:   []rp.Move{move("a", "S", "T", 4), move("b", "S", "T", 2)},
				Goal:    []types.RebalanceNodeBalance{row("S", 0.625, 3), row("T", 0.6875, 3)},
			},
		},
		{
			// twoIncoming with T using 7 and x of 8. Baseline: disk 1.25, 0.4375
			// (0.1650).
			//   1. a: T needs 7 + 4 + 4 = 15 ≤ 16; disk 1, 0.6875 (0.0244). b:
			//      disk 1.125, 0.5625 (0.0791). a wins; stddev 0.1563 fails
			//      Balanced.
			//   2. b: T needs 11 + 2 + 4 = 17 > 16, so no move is allowed. With the
			//      last incoming replica, 11 + 2 + 2 = 15 would fit, and disk
			//      0.875, 0.8125 (0.0010) would gain.
			name: "headroom uses the largest incoming replica, not the last",
			in:   twoIncoming(7, 8),
			want: rp.Result{
				Verdict: rp.MovesFound,
				Moves:   []rp.Move{move("a", "S", "T", 4)},
				Goal:    []types.RebalanceNodeBalance{row("S", 1, 4), row("T", 0.6875, 2)},
			},
		},
		{
			// S uses 11: a (7) is eligible, b (4) is not. T uses 1 (t). Moving a
			// takes disk from 0.6875, 0.0625 (0.0977) to 0.25, 0.5 (0.0156). T
			// needs 1 + 7 + 7 = 15 ≤ 16. S then holds no eligible replica.
			name: "a move that only narrows the disk spread is taken",
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
			// Baseline: disk 0.75, 0.25 (0.0625).
			//   s1 (6): disk 0.375, 0.625 (0.015625)
			//   s2 (4): disk 0.5, 0.5 (0)
			//   s3 (2): disk 0.625, 0.375 (0.015625)
			// T needs 4 + 4 + 4 = 12 ≤ 16 for s2. The state then passes Balanced.
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
				Moves:   []rp.Move{move("s2", "S", "T", 4)},
				Goal:    []types.RebalanceNodeBalance{row("S", 0.5, 2), row("T", 0.5, 2)},
			},
		},
		{
			name: "three-node layout",
			in:   rp.Input{Nodes: threeNodes, Replicas: threeReplicas, MaxMoves: 10},
			want: threeResult,
		},
		{
			name: "a replica with negative bytes on an unlisted node is ignored",
			in: rp.Input{
				Nodes:    threeNodes,
				Replicas: append(slices.Clone(threeReplicas), replica("X", "x", -1)),
				MaxMoves: 10,
			},
			want: threeResult,
		},
		{
			name: "a repeated replica on an unlisted node is ignored",
			in: rp.Input{
				Nodes:    threeNodes,
				Replicas: append(slices.Clone(threeReplicas), replica("X", "x", 4), replica("X", "x", 4)),
				MaxMoves: 10,
			},
			want: threeResult,
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
			// Baseline: disk 0.75, 0.75, 0 (0.125).
			//   1. Every move ties: disk 0.5, 0.75, 0.25 (0.0417). a: S1 → T is
			//      first.
			//   2. A move from S2: disk 0.5 × 3 (0). S2's a would be first, but T
			//      holds a, so d goes.
			//   3. The state passes Balanced, so Search stops.
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
			// The three-node layout with 4 workers on T2. Any move onto T2 needs
			// 0 + 4 + 4 × 4 = 20 > 16 bytes, so a → T1 is the only first step.
			// Next, b → T1 gives disk 0.25, 0.5, 0 (0.0417), equal to the current
			// value, so it does not gain and Search stops.
			name: "a target without headroom for any move receives none",
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
			// After 3 moves, disk stddev 0.125 fails Balanced and move 4 lowers
			// the variance from 0.0156 to 0.0039.
			name: "limit exceeded",
			in:   rp.Input{Nodes: tenNodes, Replicas: tenReplicas, MaxMoves: 3},
			want: rp.Result{Verdict: rp.LimitExceeded, MinMoveCount: 4},
		},
		{
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
			// check fails before the Balanced stop that would end the search.
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
