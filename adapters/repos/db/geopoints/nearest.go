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

package geopoints

import (
	"container/heap"
	"fmt"
	"slices"
)

// Neighbor is a point of a nearest search and its distance to the origin.
type Neighbor struct {
	DocID    uint64
	Distance float32
}

// closer orders neighbors by distance, a tie by doc ID, so the result of a
// search does not depend on the order its layers were read in.
func (a Neighbor) closer(b Neighbor) bool {
	if a.Distance != b.Distance {
		return a.Distance < b.Distance
	}
	return a.DocID < b.DocID
}

// Nearest gathers the points closest to an origin from the layers of a bucket,
// each of which offers its candidates in turn. Once it holds limit points, the
// circle through the farthest of them bounds the rest of the search: a tree
// node outside that circle's bounding boxes cannot improve the result. A
// Nearest serves one query on one goroutine.
type Nearest struct {
	lat, lon float32 // canonical
	limit    int
	// keep decides whether a doc ID may be part of the result; nil keeps all.
	// It is asked only for points close enough to enter the result.
	keep func(docID uint64) bool

	kept neighborHeap // the farthest kept neighbor on top

	bound         *Circle
	boundDistance float32
}

func NewNearest(lat, lon float32, limit int, keep func(docID uint64) bool) (*Nearest, error) {
	if err := ValidateCoordinates(lat, lon); err != nil {
		return nil, err
	}
	if limit < 0 {
		return nil, fmt.Errorf("limit %d must not be negative", limit)
	}
	lat, lon = Canonical(lat, lon)
	return &Nearest{lat: lat, lon: lon, limit: limit, keep: keep}, nil
}

func (n *Nearest) full() bool {
	return len(n.kept) >= n.limit
}

// offer considers one point.
func (n *Nearest) offer(docID uint64, lat, lon float32) {
	if n.limit == 0 {
		return
	}
	candidate := Neighbor{DocID: docID, Distance: Distance(n.lat, n.lon, lat, lon)}
	if n.full() && !candidate.closer(n.kept[0]) {
		return
	}
	if n.keep != nil && !n.keep(docID) {
		return
	}
	if n.full() {
		n.kept[0] = candidate
		heap.Fix(&n.kept, 0)
		return
	}
	heap.Push(&n.kept, candidate)
}

// prunes reports whether no point of b can enter the result.
func (n *Nearest) prunes(b Box) bool {
	if n.limit == 0 {
		return true
	}
	if !n.full() {
		return false
	}
	if worst := n.kept[0].Distance; n.bound == nil || worst != n.boundDistance {
		bound, err := NewCircle(n.lat, n.lon, worst)
		if err != nil {
			return false // a NaN distance from a corrupt coordinate bounds nothing
		}
		n.bound, n.boundDistance = bound, worst
	}
	return !n.bound.IntersectsBox(b)
}

// approach estimates how close b comes to the origin, as the distance to the
// origin's coordinates clamped into the box. It only orders the descent; what
// is pruned is decided by prunes.
func (n *Nearest) approach(b Box) float32 {
	lat := min(max(n.lat, b.MinLat), b.MaxLat)
	lon := n.lon
	if lon < b.MinLon || lon > b.MaxLon {
		lon = b.MinLon
		if wrapOffset(float64(b.MaxLon)-float64(n.lon)) < wrapOffset(float64(b.MinLon)-float64(n.lon)) {
			lon = b.MaxLon
		}
	}
	return Distance(n.lat, n.lon, lat, lon)
}

// Results are the kept neighbors, nearest first.
func (n *Nearest) Results() []Neighbor {
	out := slices.Clone([]Neighbor(n.kept))
	slices.SortFunc(out, func(a, b Neighbor) int {
		switch {
		case a.closer(b):
			return -1
		case b.closer(a):
			return 1
		default:
			return 0
		}
	})
	return out
}

type neighborHeap []Neighbor

func (h neighborHeap) Len() int           { return len(h) }
func (h neighborHeap) Less(i, j int) bool { return h[j].closer(h[i]) }
func (h neighborHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *neighborHeap) Push(x any)        { *h = append(*h, x.(Neighbor)) }

func (h *neighborHeap) Pop() any {
	old := *h
	last := old[len(old)-1]
	*h = old[:len(old)-1]
	return last
}
