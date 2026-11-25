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
	"math"
	"math/rand"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/replication/types"
	rp "github.com/weaviate/weaviate/wl/rebalance/rebalanceplanner"
)

// The tests in this file state what Search must achieve. They call only
// Search and Balanced. Each expected state comes from replaying the returned
// moves on the input, and Balanced decides whether a state is balanced.

type replicaKey struct{ collection, shard, node string }

func mustSearch(t *testing.T, in rp.Input) rp.Result {
	t.Helper()
	res, err := rp.Search(context.Background(), in)
	require.NoError(t, err)
	return res
}

// baselineRows returns the input's balance rows, in input node order.
func baselineRows(in rp.Input) []types.RebalanceNodeBalance {
	counts := make(map[string]int, len(in.Nodes))
	for _, r := range in.Replicas {
		counts[r.Node]++
	}
	rows := make([]types.RebalanceNodeBalance, len(in.Nodes))
	for i, n := range in.Nodes {
		rows[i] = types.RebalanceNodeBalance{
			Node:            n.Name,
			DiskUtilisation: float64(n.UsedBytes) / n.UsableBytes,
			ReplicaCount:    counts[n.Name],
		}
	}
	return rows
}

// sources returns the names of the input's sources, in input node order.
func sources(in rp.Input) []string {
	var names []string
	for _, n := range in.Nodes {
		if n.Role == rp.Source {
			names = append(names, n.Name)
		}
	}
	return names
}

// replay applies moves to in and returns the resulting rows, in input
// node order. It fails the test on a move that is not legal: the replica is
// an eligible replica on a source, the target is a target that does not hold
// the shard, the bytes match, and no replica moves twice.
func replay(t *testing.T, in rp.Input, moves []rp.Move) []types.RebalanceNodeBalance {
	t.Helper()
	nodes := make(map[string]rp.Node, len(in.Nodes))
	used := make(map[string]int64, len(in.Nodes))
	counts := make(map[string]int, len(in.Nodes))
	for _, n := range in.Nodes {
		nodes[n.Name] = n
		used[n.Name] = n.UsedBytes
	}
	movable := make(map[replicaKey]rp.Replica, len(in.Replicas))
	holds := make(map[replicaKey]struct{}, len(in.Replicas))
	for _, r := range in.Replicas {
		k := replicaKey{r.Collection, r.Shard, r.Node}
		holds[k] = struct{}{}
		counts[r.Node]++
		if r.Eligible {
			movable[k] = r
		}
	}
	received := make(map[string]bool)
	for i, m := range moves {
		src, ok := nodes[m.SourceNode]
		require.Truef(t, ok, "move %d: unknown source node %q", i, m.SourceNode)
		require.Equalf(t, rp.Source, src.Role, "move %d: %q is not a source", i, m.SourceNode)
		tgt, ok := nodes[m.TargetNode]
		require.Truef(t, ok, "move %d: unknown target node %q", i, m.TargetNode)
		require.Equalf(t, rp.Target, tgt.Role, "move %d: %q is not a target", i, m.TargetNode)

		from := replicaKey{m.Collection, m.Shard, m.SourceNode}
		to := replicaKey{m.Collection, m.Shard, m.TargetNode}
		r, ok := movable[from]
		require.Truef(t, ok, "move %d: no eligible unmoved replica %+v", i, from)
		require.Equalf(t, r.Bytes, m.Bytes, "move %d: bytes differ from the replica's", i)
		_, held := holds[to]
		require.Falsef(t, held, "move %d: target already holds %+v", i, to)

		delete(movable, from)
		delete(holds, from)
		holds[to] = struct{}{}
		used[m.SourceNode] -= m.Bytes
		used[m.TargetNode] += m.Bytes
		counts[m.SourceNode]--
		counts[m.TargetNode]++
		received[m.TargetNode] = true
	}
	rows := make([]types.RebalanceNodeBalance, len(in.Nodes))
	for i, n := range in.Nodes {
		if received[n.Name] {
			require.LessOrEqualf(t, float64(used[n.Name]), n.UsableBytes, "target %q ends above its usable bytes", n.Name)
		}
		rows[i] = types.RebalanceNodeBalance{
			Node:            n.Name,
			DiskUtilisation: float64(used[n.Name]) / n.UsableBytes,
			ReplicaCount:    counts[n.Name],
		}
	}
	return rows
}

func stddev(xs []float64) float64 {
	mean := 0.0
	for _, x := range xs {
		mean += x
	}
	mean /= float64(len(xs))
	sum := 0.0
	for _, x := range xs {
		sum += (x - mean) * (x - mean)
	}
	return math.Sqrt(sum / float64(len(xs)))
}

// diskSpread returns the standard deviation of disk utilisation.
func diskSpread(rows []types.RebalanceNodeBalance) float64 {
	utils := make([]float64, len(rows))
	for i, r := range rows {
		utils[i] = r.DiskUtilisation
	}
	return stddev(utils)
}

// checkResult asserts what holds for every result on valid input, and returns
// the rows after the moves. A baseline that Balanced accepts gets
// NoAcceptablePlan. For MovesFound: the moves are legal, no target that
// receives a replica ends above its usable bytes, Goal equals the replayed
// rows, and the disk spread is lower than the baseline's.
func checkResult(t *testing.T, in rp.Input, res rp.Result) []types.RebalanceNodeBalance {
	t.Helper()
	base := baselineRows(in)
	if rp.Balanced(base, sources(in)) {
		require.Equal(t, rp.NoAcceptablePlan, res.Verdict, "the baseline is balanced")
	}
	switch res.Verdict {
	case rp.MovesFound:
		require.NotEmpty(t, res.Moves)
		after := replay(t, in, res.Moves)
		require.Len(t, res.Goal, len(in.Nodes))
		for i, want := range after {
			require.Equal(t, want.Node, res.Goal[i].Node)
			require.Equal(t, want.ReplicaCount, res.Goal[i].ReplicaCount, "goal count of %s", want.Node)
			require.InDelta(t, want.DiskUtilisation, res.Goal[i].DiskUtilisation, 1e-9, "goal disk of %s", want.Node)
		}
		require.Less(t, diskSpread(after), diskSpread(base), "the disk spread did not fall: %+v", after)
		return after
	case rp.NoAcceptablePlan:
		require.Empty(t, res.Moves)
		require.Empty(t, res.Goal)
	default:
		t.Fatalf("unknown verdict %d", res.Verdict)
	}
	return base
}

func rowsOf(rows []types.RebalanceNodeBalance, names ...string) []types.RebalanceNodeBalance {
	var out []types.RebalanceNodeBalance
	for _, r := range rows {
		for _, n := range names {
			if r.Node == n {
				out = append(out, r)
			}
		}
	}
	return out
}

func shards(node, prefix string, n int, bytes int64) []rp.Replica {
	out := make([]rp.Replica, n)
	for i := range out {
		out[i] = rp.Replica{Collection: "c", Shard: prefix + strconv.Itoa(i), Node: node, Bytes: bytes, Eligible: true}
	}
	return out
}

// relocated builds a layout that direct moves can balance exactly. Every
// node owns perNode shards whose sizes cycle through sizes, plus equal
// non-shard bytes. The shards the targets own sit on the sources, spread
// round-robin. Moving them back gives every node the same bytes.
func relocated(sources, targets, perNode int, sizes []int64) rp.Input {
	var owned int64
	for j := 0; j < perNode; j++ {
		owned += sizes[j%len(sizes)]
	}
	other := owned / 2
	in := rp.Input{}
	used := make(map[string]int64)
	sourceName := func(i int) string { return "s" + strconv.Itoa(i) }
	for i := 0; i < sources+targets; i++ {
		owner := sourceName(i)
		if i >= sources {
			owner = "t" + strconv.Itoa(i-sources)
		}
		for j := 0; j < perNode; j++ {
			on := owner
			if i >= sources {
				on = sourceName((i + j) % sources)
			}
			b := sizes[j%len(sizes)]
			in.Replicas = append(in.Replicas, rp.Replica{
				Collection: "c", Shard: fmt.Sprintf("%s-%04d", owner, j), Node: on, Bytes: b, Eligible: true,
			})
			used[on] += b
		}
	}
	for i := 0; i < sources+targets; i++ {
		n := rp.Node{Name: sourceName(i), Role: rp.Source, UsableBytes: float64(3 * owned), Workers: 1}
		if i >= sources {
			n.Name, n.Role = "t"+strconv.Itoa(i-sources), rp.Target
		}
		n.UsedBytes = other + used[n.Name]
		in.Nodes = append(in.Nodes, n)
	}
	return in
}

// A layout that direct moves can balance exactly must end balanced.
func TestSearchBalancesWhatDirectMovesCanBalance(t *testing.T) {
	equal := []int64{1000}
	mixed := []int64{1000, 2000, 4000, 8000}
	tests := []struct {
		name                      string
		sources, targets, perNode int
		sizes                     []int64
	}{
		{"one source, one target, equal shards", 1, 1, 40, equal},
		{"two sources, one target, equal shards", 2, 1, 40, equal},
		{"three sources, two targets, equal shards", 3, 2, 80, equal},
		{"four sources, two targets, equal shards", 4, 2, 120, equal},
		{"one source, one target, mixed shards", 1, 1, 40, mixed},
		{"two sources, one target, mixed shards", 2, 1, 40, mixed},
		{"three sources, two targets, mixed shards", 3, 2, 80, mixed},
		{"four sources, two targets, mixed shards", 4, 2, 120, mixed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := relocated(tt.sources, tt.targets, tt.perNode, tt.sizes)
			require.False(t, rp.Balanced(baselineRows(in), sources(in)), "the baseline must start unbalanced")
			res := mustSearch(t, in)
			require.Equal(t, rp.MovesFound, res.Verdict)
			after := checkResult(t, in, res)
			require.True(t, rp.Balanced(after, sources(in)), "goal is not balanced: %+v", after)
			// Moving back every relocated shard balances the layout exactly.
			require.LessOrEqual(t, len(res.Moves), tt.targets*tt.perNode)
		})
	}
}

// drained puts equal 1,000-byte shards on one source and several targets,
// with no other bytes. The source holds onSource shards and each target
// onTarget, and every node's usable bytes make the mean disk utilisation
// percentFull. Moving shards off the source can give every node the same
// bytes.
func drained(targets, onSource, onTarget, percentFull int) rp.Input {
	const b = int64(1_000)
	nodes := targets + 1
	usable := float64(int64(onSource+targets*onTarget)*b) / float64(nodes) * 100 / float64(percentFull)
	in := rp.Input{
		Nodes:    []rp.Node{{Name: "S", Role: rp.Source, UsedBytes: int64(onSource) * b, UsableBytes: usable, Workers: 1}},
		Replicas: shards("S", "s", onSource, b),
	}
	for i := 0; i < targets; i++ {
		name := fmt.Sprintf("T%02d", i)
		in.Nodes = append(in.Nodes, rp.Node{Name: name, Role: rp.Target, UsedBytes: int64(onTarget) * b, UsableBytes: usable, Workers: 1})
		in.Replicas = append(in.Replicas, shards(name, name+"-", onTarget, b)...)
	}
	return in
}

// One source that is fuller than every target, among targets that can level
// it, must end balanced.
func TestSearchBalancesOneSourceAmongManyTargets(t *testing.T) {
	tests := []struct {
		targets, onSource, onTarget int
	}{
		{2, 450, 225},
		{4, 400, 150},
		{9, 235, 85},
		{19, 126, 46},
	}
	for _, tt := range tests {
		for _, percentFull := range []int{20, 35} {
			name := fmt.Sprintf("%d targets, %d against %d shards, %d%% full", tt.targets, tt.onSource, tt.onTarget, percentFull)
			t.Run(name, func(t *testing.T) {
				in := drained(tt.targets, tt.onSource, tt.onTarget, percentFull)
				require.False(t, rp.Balanced(baselineRows(in), sources(in)), "the baseline must start unbalanced")
				res := mustSearch(t, in)
				require.Equal(t, rp.MovesFound, res.Verdict)
				after := checkResult(t, in, res)
				require.True(t, rp.Balanced(after, sources(in)), "goal is not balanced: %+v", after)
				// Shards are equal, so equal counts is level disk. The plan stops
				// at or before it.
				mean := (tt.onSource + tt.targets*tt.onTarget) / (tt.targets + 1)
				require.GreaterOrEqual(t, after[0].ReplicaCount, mean)
			})
		}
	}
}

// skewed puts total equal shards on one source and one target. The source
// holds permille/1000 of the total more than the target, so the disk spread
// is permille/2000, whatever the total. Shard bytes shrink as total grows, so
// used bytes do not change.
func skewed(total, permille int) rp.Input {
	const allBytes = int64(1_000_000_000_000)
	b := allBytes / int64(total)
	onS := total/2 + total*permille/2000
	onT := total - onS
	return rp.Input{
		Nodes: []rp.Node{
			{Name: "S", Role: rp.Source, UsedBytes: int64(onS) * b, UsableBytes: float64(allBytes), Workers: 1},
			{Name: "T", Role: rp.Target, UsedBytes: int64(onT) * b, UsableBytes: float64(allBytes), Workers: 1},
		},
		Replicas: append(shards("S", "s", onS, b), shards("T", "t", onT, b)...),
	}
}

// rejectedSkew is a skew, in permille, that Balanced rejects at 2,000 replicas:
// one permille past the smallest it rejects, to stay clear of a float tie.
func rejectedSkew(t *testing.T) int {
	t.Helper()
	for p := 1; p <= 800; p++ {
		if in := skewed(2_000, p); !rp.Balanced(baselineRows(in), sources(in)) {
			return p + 1
		}
	}
	t.Fatal("Balanced accepts a skew of 800 permille")
	return 0
}

// The same relative skew gets the same verdict at every replica total, and
// every skew that Balanced rejects gets a plan that Balanced accepts.
func TestSearchVerdictIgnoresReplicaTotal(t *testing.T) {
	edge := rejectedSkew(t)

	// firstStep asks whether Search takes a first move. Only one source
	// replica is eligible, so Search stops after at most one move. skewed
	// gives every source replica the same bytes, so it does not matter which
	// one is eligible. Bytes and replica counts stay as skewed sets them, so
	// the disk spread is unchanged.
	firstStep := func(t *testing.T, total, permille int) rp.Verdict {
		in := skewed(total, permille)
		if permille >= edge {
			require.False(t, rp.Balanced(baselineRows(in), sources(in)), "the baseline must start unbalanced")
		}
		require.Equal(t, "S", in.Replicas[0].Node)
		for i := range in.Replicas {
			in.Replicas[i].Eligible = i == 0
		}
		res := mustSearch(t, in)
		checkResult(t, in, res)
		require.LessOrEqual(t, len(res.Moves), 1)
		return res.Verdict
	}
	for _, permille := range []int{0, edge / 2, edge * 9 / 10, edge, (edge + 1000) / 2, 900} {
		small := firstStep(t, 2_000, permille)
		switch {
		case permille == 0:
			require.Equal(t, rp.NoAcceptablePlan, small, "skew 0 at 2000 replicas")
		case permille >= edge:
			require.Equal(t, rp.MovesFound, small, "skew %d permille at 2000 replicas", permille)
		}
		for _, total := range []int{20_000, 200_000, 1_000_000} {
			if total > 200_000 && testing.Short() {
				continue
			}
			t.Run(fmt.Sprintf("first step, %d replicas, skew %d permille", total, permille), func(t *testing.T) {
				require.Equal(t, small, firstStep(t, total, permille), "the verdict differs from 2000 replicas")
			})
		}
	}

	runs := []struct{ total, permille int }{
		{2_000, edge},
		{2_000, (edge + 1000) / 2},
		{2_000, 900},
		{20_000, edge},
		{20_000, edge + 50},
	}
	for _, tt := range runs {
		t.Run(fmt.Sprintf("full plan, %d replicas, skew %d permille", tt.total, tt.permille), func(t *testing.T) {
			in := skewed(tt.total, tt.permille)
			require.False(t, rp.Balanced(baselineRows(in), sources(in)), "the baseline must start unbalanced")
			res := mustSearch(t, in)
			require.Equal(t, rp.MovesFound, res.Verdict)
			after := checkResult(t, in, res)
			require.True(t, rp.Balanced(after, sources(in)), "goal is not balanced: %+v", after)
			// Shards are equal, so equal counts is level disk. The plan stops
			// at or before it.
			require.GreaterOrEqual(t, after[0].ReplicaCount, after[1].ReplicaCount)
		})
	}
}

// two puts sN shards of sB bytes on a source and tN shards of tB bytes on a
// target. Both nodes have 1,000,000 usable bytes.
func two(sUsed, tUsed int64, sN int, sB int64, tN int, tB int64) rp.Input {
	return rp.Input{
		Nodes: []rp.Node{
			{Name: "S", Role: rp.Source, UsedBytes: sUsed, UsableBytes: 1_000_000, Workers: 1},
			{Name: "T", Role: rp.Target, UsedBytes: tUsed, UsableBytes: 1_000_000, Workers: 1},
		},
		Replicas: append(shards("S", "s", sN, sB), shards("T", "t", tN, tB)...),
	}
}

// Replica count alone never earns a move. In every layout the source holds
// more replicas than the target, and no move narrows the disk spread.
func TestSearchTakesNoMoveForReplicaCount(t *testing.T) {
	// S holds 18 small shards that are not eligible and two eligible shards of
	// 250,000 bytes.
	fuller := two(600_000, 400_000, 2, 250_000, 2, 200_000)
	small := shards("S", "small", 18, 5_000)
	for i := range small {
		small[i].Eligible = false
	}
	fuller.Replicas = append(fuller.Replicas, small...)

	tests := []struct {
		name     string
		in       rp.Input
		balanced bool
	}{
		{name: "level disk, 1,000 replicas against 10", in: two(500_000, 500_000, 1_000, 500, 10, 50_000), balanced: true},
		{name: "level disk, 10 replicas against 1", in: two(400_000, 400_000, 10, 40_000, 1, 400_000), balanced: true},
		// S is 0.08 above T, inside the tolerance.
		{name: "source 8 points fuller, 100 replicas against 10", in: two(480_000, 400_000, 100, 4_800, 10, 40_000), balanced: true},
		// S sits at 40% disk and T at 80%, so no source is fuller than a target.
		{name: "source emptier than the target, 20 replicas against 2", in: two(400_000, 800_000, 20, 20_000, 2, 400_000), balanced: true},
		// S sits at 60% disk and T at 40%. Moving either eligible shard leaves
		// S at 35% and T at 65%.
		{name: "source 20 points fuller, 20 replicas against 2", in: fuller, balanced: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := baselineRows(tt.in)
			require.Greater(t, base[0].ReplicaCount, base[1].ReplicaCount, "the source must hold more replicas")
			require.Equal(t, tt.balanced, rp.Balanced(base, sources(tt.in)), "the baseline's balance")
			res := mustSearch(t, tt.in)
			require.Equal(t, rp.NoAcceptablePlan, res.Verdict)
			checkResult(t, tt.in, res)
		})
	}
}

// Where the fuller node holds no more replicas than the emptier one, every
// move that narrows the disk spread widens the replica-count spread. The plan
// must still end balanced.
func TestSearchBalancesDiskAgainstReplicaCount(t *testing.T) {
	// s0 and s1 hold 60 large shards, each with a third replica on one target.
	// The three targets hold 900 small shards.
	five := rp.Input{}
	used := map[string]int64{}
	place := func(shard string, bytes int64, on ...string) {
		for _, n := range on {
			five.Replicas = append(five.Replicas, rp.Replica{Collection: "c", Shard: shard, Node: n, Bytes: bytes, Eligible: true})
			used[n] += bytes
		}
	}
	targets := []string{"t0", "t1", "t2"}
	for i := 0; i < 60; i++ {
		place(fmt.Sprintf("big%04d", i), 8_000, "s0", "s1", targets[i%3])
	}
	for i := 0; i < 900; i++ {
		place(fmt.Sprintf("small%04d", i), 200, targets...)
	}
	for _, n := range []string{"s0", "s1", "t0", "t1", "t2"} {
		role := rp.Target
		if n[0] == 's' {
			role = rp.Source
		}
		five.Nodes = append(five.Nodes, rp.Node{Name: n, Role: role, UsedBytes: used[n], UsableBytes: 1_000_000, Workers: 1})
	}

	tests := []struct {
		name string
		in   rp.Input
	}{
		// S sits at 80% disk and T at 10%.
		{name: "equal counts, 100 against 100", in: two(800_000, 100_000, 100, 8_000, 100, 1_000)},
		// S sits at 60% disk and T at 40%.
		{name: "the fuller node holds fewer replicas, 100 against 1,000", in: two(600_000, 400_000, 100, 6_000, 1_000, 400)},
		// The sources sit at 48% disk with 60 replicas each. The targets sit
		// at 34% with 920 each.
		{name: "two sources and three targets, 60 against 920", in: five},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := baselineRows(tt.in)
			require.False(t, rp.Balanced(base, sources(tt.in)), "the baseline must start unbalanced")
			for _, r := range base[1:] {
				if r.DiskUtilisation < base[0].DiskUtilisation {
					require.LessOrEqual(t, base[0].ReplicaCount, r.ReplicaCount, "the fuller node must hold no more replicas")
				}
			}
			res := mustSearch(t, tt.in)
			require.Equal(t, rp.MovesFound, res.Verdict)
			after := checkResult(t, tt.in, res)
			require.True(t, rp.Balanced(after, sources(tt.in)), "goal is not balanced: %+v", after)
		})
	}
}

// scaleOut puts n shards on three sources, three replicas each, beside two
// empty targets. Sizes cycle through sizes. Every node has 1,000,000 usable
// bytes and 50,000 bytes that are not shards.
func scaleOut(sizes []int64, n int) rp.Input {
	in := rp.Input{}
	var perSource int64 = 50_000
	for i := 0; i < n; i++ {
		b := sizes[i%len(sizes)]
		perSource += b
		for _, node := range []string{"s0", "s1", "s2"} {
			in.Replicas = append(in.Replicas, rp.Replica{Collection: "c", Shard: fmt.Sprintf("%04d", i), Node: node, Bytes: b, Eligible: true})
		}
	}
	for _, node := range []string{"s0", "s1", "s2"} {
		in.Nodes = append(in.Nodes, rp.Node{Name: node, Role: rp.Source, UsedBytes: perSource, UsableBytes: 1_000_000, Workers: 1})
	}
	for _, node := range []string{"t0", "t1"} {
		in.Nodes = append(in.Nodes, rp.Node{Name: node, Role: rp.Target, UsedBytes: 50_000, UsableBytes: 1_000_000, Workers: 1})
	}
	return in
}

// A cluster grown from three nodes to five, with three replicas per shard,
// must end balanced.
func TestSearchBalancesScaleOut(t *testing.T) {
	// One shard in 100 holds 30,000 bytes and the rest 100 bytes.
	fewLarge := make([]int64, 100)
	for i := range fewLarge {
		fewLarge[i] = 100
	}
	fewLarge[0] = 30_000

	tests := []struct {
		name  string
		sizes []int64
		n     int
	}{
		{name: "300 equal shards", sizes: []int64{1_000}, n: 300},
		{name: "150 mixed shards", sizes: []int64{500, 1_000, 2_500, 10_000}, n: 150},
		{name: "990 small shards and 10 large", sizes: fewLarge, n: 1_000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := scaleOut(tt.sizes, tt.n)
			require.False(t, rp.Balanced(baselineRows(in), sources(in)), "the baseline must start unbalanced")
			res := mustSearch(t, in)
			require.Equal(t, rp.MovesFound, res.Verdict)
			after := checkResult(t, in, res)
			require.True(t, rp.Balanced(after, sources(in)), "goal is not balanced: %+v", after)
		})
	}
}

// A target that cannot take a replica does not end the plan for the others.
func TestSearchPlansAroundFullTarget(t *testing.T) {
	tests := []struct {
		name    string
		blocked rp.Node
	}{
		// 950 of 1000 used by non-shard data: room for five shards at most.
		{name: "one target is nearly full", blocked: rp.Node{Name: "T1", Role: rp.Target, UsedBytes: 950_000, UsableBytes: 1_000_000, Workers: 1}},
		{name: "one target is above its usable bytes", blocked: rp.Node{Name: "T1", Role: rp.Target, UsedBytes: 1_001_000, UsableBytes: 1_000_000, Workers: 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := rp.Input{
				Nodes: []rp.Node{
					{Name: "S", Role: rp.Source, UsedBytes: 600_000, UsableBytes: 1_000_000, Workers: 1},
					tt.blocked,
					{Name: "T2", Role: rp.Target, UsedBytes: 0, UsableBytes: 1_000_000, Workers: 1},
				},
				Replicas: shards("S", "s", 60, 10_000),
			}
			res := mustSearch(t, in)
			require.Equal(t, rp.MovesFound, res.Verdict)
			after := checkResult(t, in, res)
			// S and T2 can split evenly every shard that T1 does not take.
			pair := rowsOf(after, "S", "T2")
			require.True(t, rp.Balanced(pair, []string{"S"}), "S and T2 are not balanced: %+v", after)
		})
	}

	// The source holds one 20-byte shard and thirty 2-byte shards. Each target
	// has 4 workers and 100 usable bytes.
	t.Run("a target filled by one large shard", func(t *testing.T) {
		in := rp.Input{
			Nodes: []rp.Node{
				{Name: "S", Role: rp.Source, UsedBytes: 80, UsableBytes: 100, Workers: 4},
				{Name: "T1", Role: rp.Target, UsedBytes: 0, UsableBytes: 100, Workers: 4},
				{Name: "T2", Role: rp.Target, UsedBytes: 0, UsableBytes: 100, Workers: 4},
			},
			Replicas: append(shards("S", "s", 30, 2),
				rp.Replica{Collection: "c", Shard: "huge", Node: "S", Bytes: 20, Eligible: true}),
		}
		res := mustSearch(t, in)
		require.Equal(t, rp.MovesFound, res.Verdict)
		checkResult(t, in, res)
	})
}

// A target that is fuller than the source receives no move and does not end
// the plan for the others. S sits at 80% disk, T1 at 90% and T2 at 40%.
// Moving 2-byte shards onto T2 brings S within the tolerance of T2.
func TestSearchPlansAroundFullerTarget(t *testing.T) {
	in := rp.Input{
		Nodes: []rp.Node{
			{Name: "S", Role: rp.Source, UsedBytes: 80, UsableBytes: 100, Workers: 1},
			{Name: "T1", Role: rp.Target, UsedBytes: 90, UsableBytes: 100, Workers: 1},
			{Name: "T2", Role: rp.Target, UsedBytes: 40, UsableBytes: 100, Workers: 1},
		},
		Replicas: shards("S", "s", 20, 2),
	}
	res := mustSearch(t, in)
	require.Equal(t, rp.MovesFound, res.Verdict)
	after := checkResult(t, in, res)
	for i, m := range res.Moves {
		require.Equal(t, "T2", m.TargetNode, "move %d", i)
	}
	pair := rowsOf(after, "S", "T2")
	require.True(t, rp.Balanced(pair, []string{"S"}), "S and T2 are not balanced: %+v", after)
}

// A layout that only a swap can improve has no acceptable plan. S holds 6 and
// 4 bytes and T holds 5 and 3, of 16 usable bytes each: disk 62.5% and 50%.
// Either move widens the disk spread.
func TestSearchSwapOnlyLayoutHasNoPlan(t *testing.T) {
	in := rp.Input{
		Nodes: []rp.Node{
			{Name: "S", Role: rp.Source, UsedBytes: 10, UsableBytes: 16, Workers: 1},
			{Name: "T", Role: rp.Target, UsedBytes: 8, UsableBytes: 16, Workers: 1},
		},
		Replicas: []rp.Replica{
			{Collection: "c", Shard: "s6", Node: "S", Bytes: 6, Eligible: true},
			{Collection: "c", Shard: "s4", Node: "S", Bytes: 4, Eligible: true},
			{Collection: "c", Shard: "t5", Node: "T", Bytes: 5, Eligible: true},
			{Collection: "c", Shard: "t3", Node: "T", Bytes: 3, Eligible: true},
		},
	}
	res := mustSearch(t, in)
	require.Equal(t, rp.NoAcceptablePlan, res.Verdict)
	checkResult(t, in, res)
}

// A layout whose only move mirrors it has no acceptable plan. S and T have
// equal disks and equal shards, and S holds one shard more than T. Moving one
// shard swaps the two nodes' disk utilisations, so the disk spread does not
// change. Every shard is over a tenth of the disk, so the baseline is
// unbalanced.
func TestSearchMirrorOnlyLayoutHasNoPlan(t *testing.T) {
	for _, usable := range []int64{100, 1_000, 1_000_000_007} {
		for _, percent := range []int64{11, 13, 17, 23} {
			shardBytes := usable * percent / 100
			for _, otherBytes := range []int64{0, usable / 20, usable / 7} {
				for onT := 0; onT <= 8; onT++ {
					usedS := int64(onT+1)*shardBytes + otherBytes
					if usedS+shardBytes > usable {
						// T lacks room for the move and its headroom.
						break
					}
					name := fmt.Sprintf("%d against %d shards of %d bytes, %d other bytes, %d usable", onT+1, onT, shardBytes, otherBytes, usable)
					t.Run(name, func(t *testing.T) {
						in := rp.Input{
							Nodes: []rp.Node{
								{Name: "S", Role: rp.Source, UsedBytes: usedS, UsableBytes: float64(usable), Workers: 1},
								{Name: "T", Role: rp.Target, UsedBytes: usedS - shardBytes, UsableBytes: float64(usable), Workers: 1},
							},
							Replicas: append(shards("S", "s", onT+1, shardBytes), shards("T", "t", onT, shardBytes)...),
						}
						require.False(t, rp.Balanced(baselineRows(in), sources(in)), "the baseline must start unbalanced")
						res := mustSearch(t, in)
						require.Equal(t, rp.NoAcceptablePlan, res.Verdict)
						checkResult(t, in, res)
					})
				}
			}
		}
	}
}

// A context cancelled before the call returns its error and no result.
func TestSearchCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	res, err := rp.Search(ctx, skewed(2_000, 500))
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, rp.Result{}, res)
}

// Invalid input never yields a plan.
func TestSearchRejectsInvalidInput(t *testing.T) {
	valid := func() rp.Input {
		return rp.Input{
			Nodes: []rp.Node{
				{Name: "S", Role: rp.Source, UsedBytes: 300_000, UsableBytes: 1_000_000, Workers: 1},
				{Name: "T", Role: rp.Target, UsedBytes: 0, UsableBytes: 1_000_000, Workers: 1},
			},
			Replicas: shards("S", "s", 10, 10_000),
		}
	}
	require.Equal(t, rp.MovesFound, mustSearch(t, valid()).Verdict, "the unchanged input must plan")

	tests := []struct {
		name   string
		mutate func(in *rp.Input)
	}{
		{"zero usable bytes on a source", func(in *rp.Input) { in.Nodes[0].UsableBytes = 0 }},
		{"zero usable bytes on a target", func(in *rp.Input) { in.Nodes[1].UsableBytes = 0 }},
		{"negative usable bytes", func(in *rp.Input) { in.Nodes[1].UsableBytes = -1_000_000 }},
		{"NaN usable bytes", func(in *rp.Input) { in.Nodes[0].UsableBytes = math.NaN() }},
		{"infinite usable bytes", func(in *rp.Input) { in.Nodes[1].UsableBytes = math.Inf(1) }},
		{"negative used bytes", func(in *rp.Input) { in.Nodes[1].UsedBytes = -1 }},
		{"negative replica bytes", func(in *rp.Input) { in.Replicas[0].Bytes = -10_000 }},
		{"zero workers on a source", func(in *rp.Input) { in.Nodes[0].Workers = 0 }},
		{"zero workers on a target", func(in *rp.Input) { in.Nodes[1].Workers = 0 }},
		{"negative workers", func(in *rp.Input) { in.Nodes[1].Workers = -1 }},
		{"duplicate node name", func(in *rp.Input) { in.Nodes = append(in.Nodes, in.Nodes[1]) }},
		{"duplicate replica", func(in *rp.Input) { in.Replicas = append(in.Replicas, in.Replicas[0]) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := valid()
			tt.mutate(&in)
			res, err := rp.Search(context.Background(), in)
			if err != nil {
				require.Equal(t, rp.Result{}, res)
				return
			}
			require.NotEqual(t, rp.MovesFound, res.Verdict)
			require.Empty(t, res.Moves)
			require.Empty(t, res.Goal)
		})
	}

	// S uses 30 of 100 bytes, and its shards are drafted at 40 and 10 bytes. T
	// is empty with 1,000 usable bytes. Moving the 40-byte shard narrows the
	// disk spread most and would leave S at −10 bytes. Moving the 10-byte shard
	// leaves S at 20% disk and T at 1%.
	t.Run("shard sizes above the node's used bytes give no negative goal", func(t *testing.T) {
		in := rp.Input{
			Nodes: []rp.Node{
				{Name: "S", Role: rp.Source, UsedBytes: 30, UsableBytes: 100, Workers: 1},
				{Name: "T", Role: rp.Target, UsedBytes: 0, UsableBytes: 1_000, Workers: 1},
			},
			Replicas: []rp.Replica{
				{Collection: "c", Shard: "large", Node: "S", Bytes: 40, Eligible: true},
				{Collection: "c", Shard: "small", Node: "S", Bytes: 10, Eligible: true},
			},
		}
		require.False(t, rp.Balanced(baselineRows(in), sources(in)), "the baseline must start unbalanced")
		res := mustSearch(t, in)
		require.Equal(t, rp.MovesFound, res.Verdict)
		checkResult(t, in, res)
		for _, row := range res.Goal {
			require.GreaterOrEqual(t, row.DiskUtilisation, 0.0, "node %s", row.Node)
		}
	})

	t.Run("Balanced rejects rows that are not numbers", func(t *testing.T) {
		for _, bad := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
			rows := []types.RebalanceNodeBalance{
				{Node: "a", DiskUtilisation: bad, ReplicaCount: 1},
				{Node: "b", DiskUtilisation: 0.5, ReplicaCount: 1},
			}
			require.False(t, rp.Balanced(rows, []string{"a"}), "disk utilisation %v", bad)
		}
	})
}

// randomLayout builds a valid layout of 2 to 6 nodes with equal usable bytes,
// mixed shard sizes, one or two replicas per shard and some ineligible ones.
func randomLayout(r *rand.Rand) rp.Input {
	const usable = 1_000_000
	sizes := []int64{500, 1_000, 2_500, 10_000}
	in := rp.Input{}
	n := 2 + r.Intn(5)
	for i := 0; i < n; i++ {
		role := rp.Source
		if i == n-1 || (i > 0 && r.Intn(2) == 0) {
			role = rp.Target
		}
		in.Nodes = append(in.Nodes, rp.Node{
			Name: "n" + strconv.Itoa(i), Role: role, UsedBytes: int64(r.Intn(200_000)), UsableBytes: usable, Workers: 1 + r.Intn(3),
		})
	}
	for s, total := 0, 5+r.Intn(76); s < total; s++ {
		b := sizes[r.Intn(len(sizes))]
		collection := "c" + strconv.Itoa(r.Intn(3))
		first := r.Intn(n)
		on := []int{first}
		if second := r.Intn(n); second != first && r.Intn(2) == 0 {
			on = append(on, second)
		}
		for _, i := range on {
			in.Replicas = append(in.Replicas, rp.Replica{
				Collection: collection, Shard: fmt.Sprintf("s%03d", s), Node: in.Nodes[i].Name, Bytes: b, Eligible: r.Intn(5) > 0,
			})
			in.Nodes[i].UsedBytes += b
		}
	}
	return in
}

// Every result on a valid layout is legal, matches its moves and does not
// depend on input order.
func TestSearchResultInvariants(t *testing.T) {
	for seed := int64(1); seed <= 300; seed++ {
		t.Run(fmt.Sprintf("seed %d", seed), func(t *testing.T) {
			r := rand.New(rand.NewSource(seed))
			in := randomLayout(r)
			res := mustSearch(t, in)
			checkResult(t, in, res)

			shuffled := rp.Input{
				Nodes:    append([]rp.Node(nil), in.Nodes...),
				Replicas: append([]rp.Replica(nil), in.Replicas...),
			}
			r.Shuffle(len(shuffled.Nodes), func(i, j int) {
				shuffled.Nodes[i], shuffled.Nodes[j] = shuffled.Nodes[j], shuffled.Nodes[i]
			})
			r.Shuffle(len(shuffled.Replicas), func(i, j int) {
				shuffled.Replicas[i], shuffled.Replicas[j] = shuffled.Replicas[j], shuffled.Replicas[i]
			})
			again := mustSearch(t, shuffled)
			require.Equal(t, res.Verdict, again.Verdict)
			require.Equal(t, res.Moves, again.Moves)
			require.ElementsMatch(t, res.Goal, again.Goal)
		})
	}
}
