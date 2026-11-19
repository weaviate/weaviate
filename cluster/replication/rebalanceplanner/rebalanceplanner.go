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

// Package rebalanceplanner scores rebalance placements and searches for the
// direct moves of a rebalance plan.
package rebalanceplanner

import (
	"cmp"
	"context"
	"slices"

	"github.com/weaviate/weaviate/cluster/replication/types"
)

// TODO(calibration): the weights, epsilon, minGain and headroomMargin are
// placeholders until tests on large clusters set them.
const (
	// epsilon is the smallest cost drop for which Search takes a move.
	epsilon = 1e-6
	// minGain is the smallest drop that an acceptable plan achieves in disk
	// variance or in count-ratio variance. The drop is absolute, not relative.
	minGain        = 1e-3
	diskWeight     = 1.0
	countWeight    = 1.0
	movementWeight = 0.1
	headroomMargin = 1.0
)

// Role is a listed node's role in a plan.
type Role int

const (
	Source Role = iota + 1
	Target
)

// Node is one listed node's measured state.
type Node struct {
	Name      string
	Role      Role
	UsedBytes int64
	// UsableBytes is the disk total × the disk warning percentage / 100. It is
	// greater than 0.
	UsableBytes float64
	Workers     int
}

// Replica is one shard replica on a listed node. Every replica counts towards
// its node's replica count; Search moves only eligible replicas on sources.
type Replica struct {
	Collection string
	Shard      string
	Node       string
	Bytes      int64
	Eligible   bool
}

// Input is what Search plans on. Replicas on a node not in Nodes are ignored.
type Input struct {
	Nodes    []Node
	Replicas []Replica
	// MaxMoves is the largest number of moves Search returns. It is at least 0.
	MaxMoves int
}

// Move moves one replica from a source to a target.
type Move struct {
	Collection string
	Shard      string
	SourceNode string
	TargetNode string
	Bytes      int64
}

// Verdict is the outcome of a Search.
type Verdict int

const (
	MovesFound Verdict = iota + 1
	NoAcceptablePlan
	LimitExceeded
)

// Result is what Search returns.
type Result struct {
	Verdict Verdict
	// Moves holds the accepted moves in greedy order. Goal holds one row per
	// input node, in input node order. Both are set only for MovesFound.
	Moves []Move
	Goal  []types.RebalanceNodeBalance
	// MinMoveCount is MaxMoves + 1 for LimitExceeded and 0 otherwise.
	MinMoveCount int
}

// NodeLoad is one node's state in a placement.
type NodeLoad struct {
	Role        Role
	UsedBytes   int64
	UsableBytes float64
	Workers     int
	Replicas    int
	// LargestIncoming is the size in bytes of the largest replica the plan
	// moves onto the node. It is 0 when the node receives none.
	LargestIncoming int64
}

// Placement is the state of every listed node after a list of moves.
type Placement struct {
	Nodes []NodeLoad
	// MovedBytes is the sum of the bytes of the moves.
	MovedBytes int64
	// SourceUsedBytes is the sum of the sources' used bytes before any move.
	SourceUsedBytes int64
}

// Cost is diskWeight × variance(disk utilisation) + countWeight ×
// variance(count ratio) + movementWeight × MovedBytes / SourceUsedBytes. The
// movement term is 0 when SourceUsedBytes is 0.
func Cost(p Placement) float64 {
	return sumsOf(p.Nodes).balance() + movement(p.MovedBytes, p.SourceUsedBytes)
}

// Acceptable reports whether plan passes three rules against baseline:
//  1. Its cost is lower.
//  2. Its disk variance or its count-ratio variance is lower by at least minGain.
//  3. On every target, used bytes plus headroom fit within usable bytes.
//     Headroom is workers × LargestIncoming × headroomMargin.
func Acceptable(baseline, plan Placement) bool {
	if Cost(plan) >= Cost(baseline) {
		return false
	}
	b, p := sumsOf(baseline.Nodes), sumsOf(plan.Nodes)
	if b.diskVariance()-p.diskVariance() < minGain && b.countVariance()-p.countVariance() < minGain {
		return false
	}
	for _, n := range plan.Nodes {
		if n.Role != Target {
			continue
		}
		headroom := float64(n.Workers) * float64(n.LargestIncoming) * headroomMargin
		if float64(n.UsedBytes)+headroom > n.UsableBytes {
			return false
		}
	}
	return true
}

// PassesBalanceRule reports whether diskWeight × variance(disk utilisation) +
// countWeight × variance(count ratio) is strictly lower for after than for
// before.
func PassesBalanceRule(before, after []NodeLoad) bool {
	return sumsOf(after).balance() < sumsOf(before).balance()
}

func movement(movedBytes, sourceUsedBytes int64) float64 {
	if sourceUsedBytes == 0 {
		return 0
	}
	return movementWeight * float64(movedBytes) / float64(sourceUsedBytes)
}

// sums holds the running sums behind both variances. A move changes the terms
// of only two nodes, so costAfter scores a candidate in constant time.
type sums struct {
	n            float64
	disk, diskSq float64
	// count, the replica total, is the same for every placement of one input,
	// so costAfter updates only countSq.
	count, countSq int64
}

func sumsOf(nodes []NodeLoad) sums {
	s := sums{n: float64(len(nodes))}
	for _, n := range nodes {
		d := utilisation(n)
		s.disk += d
		s.diskSq += d * d
		s.count += int64(n.Replicas)
		s.countSq += int64(n.Replicas) * int64(n.Replicas)
	}
	return s
}

func utilisation(n NodeLoad) float64 {
	return float64(n.UsedBytes) / n.UsableBytes
}

func (s sums) diskVariance() float64 {
	m := s.disk / s.n
	return s.diskSq/s.n - m*m
}

// countVariance is the variance of replicas / mean(replicas), which equals
// (n × Σc² − (Σc)²) / (Σc)². It is 0 when no node holds a replica.
func (s sums) countVariance() float64 {
	if s.count == 0 {
		return 0
	}
	total := float64(s.count)
	return (s.n*float64(s.countSq) - total*total) / (total * total)
}

func (s sums) balance() float64 {
	return diskWeight*s.diskVariance() + countWeight*s.countVariance()
}

// Search plans direct moves from the input's sources to its targets. Each step
// takes the candidate move that lowers cost the most, and Search stops when
// none lowers it by at least epsilon. If Search would take a step beyond
// MaxMoves, it returns LimitExceeded. Otherwise it returns the longest prefix
// of the moves that passes Acceptable, or NoAcceptablePlan when none does.
// The order of nodes and replicas in the input does not change the result.
// Search checks ctx before each step and returns ctx.Err() once ctx is done.
func Search(ctx context.Context, in Input) (Result, error) {
	s := newSearch(in)
	baseline := s.p
	baseline.Nodes = slices.Clone(s.p.Nodes)

	var moves []Move
	var goal []types.RebalanceNodeBalance
	accepted := 0
	for {
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}
		c, ok := s.best()
		if !ok {
			break
		}
		if len(moves) >= in.MaxMoves {
			return Result{Verdict: LimitExceeded, MinMoveCount: in.MaxMoves + 1}, nil
		}
		moves = append(moves, s.apply(c))
		if Acceptable(baseline, s.p) {
			accepted = len(moves)
			goal = s.goal(in.Nodes)
		}
	}
	if accepted == 0 {
		return Result{Verdict: NoAcceptablePlan}, nil
	}
	return Result{Verdict: MovesFound, Moves: moves[:accepted:accepted], Goal: goal}, nil
}

type replica struct {
	Replica
	moved bool
}

type holding struct {
	collection, shard string
	node              int
}

type candidate struct {
	source, target, replica int
}

// search holds the projected placement. Nodes, sources, targets and each
// source's replicas are sorted. This makes every float sum run in the same
// order for equal input.
type search struct {
	names    []string
	p        Placement
	sums     sums
	sources  []int
	targets  []int
	replicas []replica
	// onSource maps a source node to the positions in replicas of its
	// eligible replicas. Nodes that are not sources have no entry.
	onSource map[int][]int
	holds    map[holding]struct{}
}

func newSearch(in Input) *search {
	nodes := slices.SortedFunc(slices.Values(in.Nodes), func(a, b Node) int { return cmp.Compare(a.Name, b.Name) })
	s := &search{
		names:    make([]string, len(nodes)),
		p:        Placement{Nodes: make([]NodeLoad, len(nodes))},
		onSource: make(map[int][]int),
		holds:    make(map[holding]struct{}, len(in.Replicas)),
	}
	index := make(map[string]int, len(nodes))
	for i, n := range nodes {
		index[n.Name] = i
		s.names[i] = n.Name
		s.p.Nodes[i] = NodeLoad{Role: n.Role, UsedBytes: n.UsedBytes, UsableBytes: n.UsableBytes, Workers: n.Workers}
		switch n.Role {
		case Source:
			s.sources = append(s.sources, i)
			s.p.SourceUsedBytes += n.UsedBytes
		case Target:
			s.targets = append(s.targets, i)
		}
	}

	replicas := slices.SortedFunc(slices.Values(in.Replicas), func(a, b Replica) int {
		return cmp.Or(cmp.Compare(a.Collection, b.Collection), cmp.Compare(a.Shard, b.Shard), cmp.Compare(a.Node, b.Node))
	})
	for _, r := range replicas {
		i, listed := index[r.Node]
		if !listed {
			continue
		}
		s.p.Nodes[i].Replicas++
		s.holds[holding{r.Collection, r.Shard, i}] = struct{}{}
		if r.Eligible && s.p.Nodes[i].Role == Source {
			s.onSource[i] = append(s.onSource[i], len(s.replicas))
			s.replicas = append(s.replicas, replica{Replica: r})
		}
	}
	s.sums = sumsOf(s.p.Nodes)
	return s
}

// best returns the candidate move with the lowest resulting cost. It reports
// false when that move lowers cost by less than epsilon. On a tie, the first
// candidate in source, replica, target order wins.
func (s *search) best() (candidate, bool) {
	current := s.sums.balance() + movement(s.p.MovedBytes, s.p.SourceUsedBytes)
	var best candidate
	bestCost, found := 0.0, false
	for _, src := range s.sources {
		for _, ri := range s.onSource[src] {
			r := &s.replicas[ri]
			if r.moved {
				continue
			}
			for _, tgt := range s.targets {
				if _, held := s.holds[holding{r.Collection, r.Shard, tgt}]; held {
					continue
				}
				if c := s.costAfter(src, tgt, r.Bytes); !found || c < bestCost {
					best, bestCost, found = candidate{src, tgt, ri}, c, true
				}
			}
		}
	}
	return best, found && current-bestCost >= epsilon
}

// costAfter is the cost after moving bytes from src to tgt.
func (s *search) costAfter(src, tgt int, bytes int64) float64 {
	from, to := s.p.Nodes[src], s.p.Nodes[tgt]
	fromBefore, toBefore := utilisation(from), utilisation(to)
	from.UsedBytes -= bytes
	to.UsedBytes += bytes
	fromAfter, toAfter := utilisation(from), utilisation(to)

	next := s.sums
	next.disk += fromAfter + toAfter - fromBefore - toBefore
	next.diskSq += fromAfter*fromAfter + toAfter*toAfter - fromBefore*fromBefore - toBefore*toBefore
	// (from−1)² + (to+1)² − from² − to² = 2(to − from) + 2
	next.countSq += 2*int64(to.Replicas-from.Replicas) + 2
	return next.balance() + movement(s.p.MovedBytes+bytes, s.p.SourceUsedBytes)
}

func (s *search) apply(c candidate) Move {
	r := &s.replicas[c.replica]
	r.moved = true
	from, to := &s.p.Nodes[c.source], &s.p.Nodes[c.target]
	from.UsedBytes -= r.Bytes
	from.Replicas--
	to.UsedBytes += r.Bytes
	to.Replicas++
	to.LargestIncoming = max(to.LargestIncoming, r.Bytes)
	s.p.MovedBytes += r.Bytes
	delete(s.holds, holding{r.Collection, r.Shard, c.source})
	s.holds[holding{r.Collection, r.Shard, c.target}] = struct{}{}
	s.sums = sumsOf(s.p.Nodes)
	return Move{
		Collection: r.Collection,
		Shard:      r.Shard,
		SourceNode: s.names[c.source],
		TargetNode: s.names[c.target],
		Bytes:      r.Bytes,
	}
}

// goal returns one row per node of nodes, in that order.
func (s *search) goal(nodes []Node) []types.RebalanceNodeBalance {
	rows := make([]types.RebalanceNodeBalance, len(nodes))
	for i, n := range nodes {
		j, _ := slices.BinarySearch(s.names, n.Name)
		rows[i] = types.RebalanceNodeBalance{
			Node:            n.Name,
			DiskUtilisation: utilisation(s.p.Nodes[j]),
			ReplicaCount:    s.p.Nodes[j].Replicas,
		}
	}
	return rows
}
