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

// Package rebalanceplanner searches for the direct moves of a rebalance plan
// that balance disk utilisation across nodes.
package rebalanceplanner

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"slices"

	"github.com/weaviate/weaviate/cluster/replication/types"
)

// TODO(calibration): headroomMargin is a placeholder until tests on large
// clusters set it.
const headroomMargin = 1.0

// minRelativeGain guards against rounding error. It is not a value awaiting
// calibration. Search computes a candidate's balance value by updating running
// sums. So a move that changes nothing in exact arithmetic can come out one
// rounding step below the current value. Search takes a move only when it
// lowers the current value by more than minRelativeGain × the current value.
const minRelativeGain = 1e-9

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
	// finite and greater than 0.
	UsableBytes float64
	// Workers is at least 1. Search keeps Workers × headroomMargin × the
	// largest incoming replica free on a target.
	Workers int
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
	// MaxMoves is the largest number of moves Search returns. It is at least 0;
	// Search returns an error for a negative value.
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
	// Moves holds every move Search took, in the order taken. Goal holds one
	// row per input node, in input node order. Both are set only for
	// MovesFound.
	Moves []Move
	Goal  []types.RebalanceNodeBalance
	// MinMoveCount is MaxMoves + 1 for LimitExceeded and 0 otherwise.
	MinMoveCount int
}

// NodeLoad is one node's projected state during a search.
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

// sums holds the running sums behind the disk variance. A move changes the
// terms of only two nodes, so balanceAfter scores a candidate in constant time.
type sums struct {
	n            float64
	disk, diskSq float64
}

func sumsOf(nodes []NodeLoad) sums {
	s := sums{n: float64(len(nodes))}
	for _, n := range nodes {
		d := utilisation(n)
		s.disk += d
		s.diskSq += d * d
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

// Search plans direct moves from the input's sources to its targets.
//
// Search returns an error and Result{} when any of these holds:
//   - MaxMoves is negative;
//   - two nodes share a name;
//   - a node's UsableBytes is not finite or not above 0;
//   - a node's UsedBytes is negative;
//   - a node's Workers is below 1;
//   - a replica on a listed node has negative Bytes;
//   - two replicas on a listed node share a (collection, shard, node).
//
// The balance value of a state is the variance of disk utilisation across the
// listed nodes. Let current be the balance value before a move. The move gains
// when the balance value after it is below current − current × minRelativeGain.
// Each step runs in this order:
//  1. If ctx is done, Search returns ctx.Err() and Result{}.
//  2. If Balanced accepts the current state, Search stops.
//  3. Among allowed moves that gain, Search takes the one with the lowest
//     balance value after it. It stops when no allowed move gains.
//
// A move is allowed when:
//   - it leaves its source's used bytes at 0 or above;
//   - its target's used bytes after the move, plus Workers × headroomMargin ×
//     the largest replica moved onto the target, fit within the target's
//     usable bytes.
//
// If Search would take a step beyond MaxMoves, it returns LimitExceeded.
// Otherwise it returns MovesFound with every move taken, or NoAcceptablePlan
// when it takes none. The order of nodes and replicas in the input does not
// change the result.
func Search(ctx context.Context, in Input) (Result, error) {
	if err := validate(in); err != nil {
		return Result{}, err
	}
	s := newSearch(in)

	var moves []Move
	for {
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}
		if Balanced(s.rows()) {
			break
		}
		c, ok := s.best()
		if !ok {
			break
		}
		if len(moves) >= in.MaxMoves {
			return Result{Verdict: LimitExceeded, MinMoveCount: in.MaxMoves + 1}, nil
		}
		moves = append(moves, s.apply(c))
	}
	if len(moves) == 0 {
		return Result{Verdict: NoAcceptablePlan}, nil
	}
	return Result{Verdict: MovesFound, Moves: moves, Goal: s.goal(in.Nodes)}, nil
}

// validate returns an error for the first error case from Search's doc comment
// that it finds. It checks MaxMoves, then nodes in input order, then replicas
// in input order.
func validate(in Input) error {
	if in.MaxMoves < 0 {
		return fmt.Errorf("max moves %d is negative", in.MaxMoves)
	}
	names := make(map[string]struct{}, len(in.Nodes))
	for _, n := range in.Nodes {
		if _, dup := names[n.Name]; dup {
			return fmt.Errorf("node %q is listed twice", n.Name)
		}
		names[n.Name] = struct{}{}
		if !(n.UsableBytes > 0) || math.IsInf(n.UsableBytes, 1) {
			return fmt.Errorf("node %q: usable bytes %v is not finite and above 0", n.Name, n.UsableBytes)
		}
		if n.UsedBytes < 0 {
			return fmt.Errorf("node %q: used bytes %d is negative", n.Name, n.UsedBytes)
		}
		if n.Workers <= 0 {
			return fmt.Errorf("node %q: workers %d is below 1", n.Name, n.Workers)
		}
	}
	type key struct{ collection, shard, node string }
	replicas := make(map[key]struct{}, len(in.Replicas))
	for _, r := range in.Replicas {
		if _, listed := names[r.Node]; !listed {
			continue
		}
		k := key{r.Collection, r.Shard, r.Node}
		if r.Bytes < 0 {
			return fmt.Errorf("replica %s/%s on node %q: bytes %d is negative", r.Collection, r.Shard, r.Node, r.Bytes)
		}
		if _, dup := replicas[k]; dup {
			return fmt.Errorf("replica %s/%s on node %q is listed twice", r.Collection, r.Shard, r.Node)
		}
		replicas[k] = struct{}{}
	}
	return nil
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
	names []string
	nodes []NodeLoad
	// disk[i] is utilisation(nodes[i]).
	disk     []float64
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
		nodes:    make([]NodeLoad, len(nodes)),
		disk:     make([]float64, len(nodes)),
		onSource: make(map[int][]int),
		holds:    make(map[holding]struct{}, len(in.Replicas)),
	}
	index := make(map[string]int, len(nodes))
	for i, n := range nodes {
		index[n.Name] = i
		s.names[i] = n.Name
		s.nodes[i] = NodeLoad{Role: n.Role, UsedBytes: n.UsedBytes, UsableBytes: n.UsableBytes, Workers: n.Workers}
		s.disk[i] = utilisation(s.nodes[i])
		switch n.Role {
		case Source:
			s.sources = append(s.sources, i)
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
		s.nodes[i].Replicas++
		s.holds[holding{r.Collection, r.Shard, i}] = struct{}{}
		if r.Eligible && s.nodes[i].Role == Source {
			s.onSource[i] = append(s.onSource[i], len(s.replicas))
			s.replicas = append(s.replicas, replica{Replica: r})
		}
	}
	s.sums = sumsOf(s.nodes)
	return s
}

// Among allowed candidate moves that gain, best returns the one with the lowest
// balance value after it. Search's doc comment defines which moves are allowed
// and which gain. best reports false when no allowed move gains. On a tie, the
// first candidate in source, replica, target order wins.
func (s *search) best() (candidate, bool) {
	var best candidate
	current := s.sums.diskVariance()
	bestValue, found := current-current*minRelativeGain, false
	for _, src := range s.sources {
		from := s.nodes[src]
		for _, ri := range s.onSource[src] {
			r := &s.replicas[ri]
			if r.moved || from.UsedBytes-r.Bytes < 0 {
				continue
			}
			for _, tgt := range s.targets {
				if _, held := s.holds[holding{r.Collection, r.Shard, tgt}]; held {
					continue
				}
				to := s.nodes[tgt]
				headroom := float64(to.Workers) * float64(max(to.LargestIncoming, r.Bytes)) * headroomMargin
				if float64(to.UsedBytes)+float64(r.Bytes)+headroom > to.UsableBytes {
					continue
				}
				value := s.balanceAfter(src, tgt, r.Bytes)
				// bestValue starts at the gain threshold, so this check also
				// rejects every move that does not gain.
				if value >= bestValue {
					continue
				}
				best, bestValue, found = candidate{src, tgt, ri}, value, true
			}
		}
	}
	return best, found
}

// balanceAfter returns the balance value after moving bytes from src to tgt.
func (s *search) balanceAfter(src, tgt int, bytes int64) float64 {
	from, to := s.nodes[src], s.nodes[tgt]
	from.UsedBytes -= bytes
	to.UsedBytes += bytes
	fromBefore, toBefore := s.disk[src], s.disk[tgt]
	fromAfter, toAfter := utilisation(from), utilisation(to)

	next := s.sums
	next.disk += fromAfter + toAfter - fromBefore - toBefore
	next.diskSq += fromAfter*fromAfter + toAfter*toAfter - fromBefore*fromBefore - toBefore*toBefore
	return next.diskVariance()
}

func (s *search) apply(c candidate) Move {
	r := &s.replicas[c.replica]
	r.moved = true
	from, to := &s.nodes[c.source], &s.nodes[c.target]
	from.UsedBytes -= r.Bytes
	from.Replicas--
	to.UsedBytes += r.Bytes
	to.Replicas++
	to.LargestIncoming = max(to.LargestIncoming, r.Bytes)
	s.disk[c.source], s.disk[c.target] = utilisation(*from), utilisation(*to)
	delete(s.holds, holding{r.Collection, r.Shard, c.source})
	s.holds[holding{r.Collection, r.Shard, c.target}] = struct{}{}
	s.sums = sumsOf(s.nodes)
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
		rows[i] = s.row(j)
	}
	return rows
}

// rows returns one row per node, in sorted node order.
func (s *search) rows() []types.RebalanceNodeBalance {
	rows := make([]types.RebalanceNodeBalance, len(s.nodes))
	for i := range s.nodes {
		rows[i] = s.row(i)
	}
	return rows
}

func (s *search) row(i int) types.RebalanceNodeBalance {
	return types.RebalanceNodeBalance{Node: s.names[i], DiskUtilisation: s.disk[i], ReplicaCount: s.nodes[i].Replicas}
}
