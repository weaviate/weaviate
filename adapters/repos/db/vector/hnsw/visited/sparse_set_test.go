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

package visited

import "testing"

func TestNewSparseSet_InitialState(t *testing.T) {
	s := NewSparseSet(1024, 64)

	if s == nil {
		t.Fatal("expected non-nil SparseSet")
	}

	if s.collisionRate != 64 {
		t.Fatalf("expected collisionRate=64, got %d", s.collisionRate)
	}

	if s.collisionShift != 6 {
		t.Fatalf("expected collisionShift=6, got %d", s.collisionShift)
	}

	if len(s.collidingBitSet) == 0 {
		t.Fatal("expected non-empty collidingBitSet")
	}

	if len(s.segmentedBitSets.segments) == 0 {
		t.Fatal("expected non-empty segments")
	}

	if s.maxNodeExclusive == 0 {
		t.Fatal("expected maxNodeExclusive > 0")
	}
}

func TestSparseSet_VisitedInitiallyFalse(t *testing.T) {
	s := NewSparseSet(1024, 64)

	nodes := []uint64{0, 1, 63, 64, 127, 255, 1023}
	for _, node := range nodes {
		if s.Visited(node) {
			t.Fatalf("expected node %d to be initially unvisited", node)
		}
	}
}

func TestSparseSet_VisitAndVisited(t *testing.T) {
	s := NewSparseSet(1024, 64)

	nodes := []uint64{0, 1, 63, 64, 65, 127, 128, 255, 511}

	for _, node := range nodes {
		s.Visit(node)
		if !s.Visited(node) {
			t.Fatalf("expected node %d to be visited after Visit()", node)
		}
	}
}

func TestSparseSet_VisitIfNotVisited(t *testing.T) {
	s := NewSparseSet(1024, 64)

	node := uint64(123)

	if already := s.CheckAndVisit(node); already {
		t.Fatalf("expected first VisitIfNotVisited(%d) to return false", node)
	}

	if !s.Visited(node) {
		t.Fatalf("expected node %d to be marked visited", node)
	}

	if already := s.CheckAndVisit(node); !already {
		t.Fatalf("expected second VisitIfNotVisited(%d) to return true", node)
	}
}

func TestSparseSet_ResetClearsVisitedState(t *testing.T) {
	s := NewSparseSet(1024, 64)

	nodes := []uint64{0, 10, 63, 64, 100, 127, 255}

	for _, node := range nodes {
		s.Visit(node)
	}

	for _, node := range nodes {
		if !s.Visited(node) {
			t.Fatalf("expected node %d to be visited before Reset()", node)
		}
	}

	s.Reset()

	for _, node := range nodes {
		if s.Visited(node) {
			t.Fatalf("expected node %d to be unvisited after Reset()", node)
		}
	}

	if len(s.touchedSegs) != 0 {
		t.Fatalf("expected touchedSegs to be empty after Reset(), got len=%d", len(s.touchedSegs))
	}

	if len(s.touchedCB) != 0 {
		t.Fatalf("expected touchedCB to be empty after Reset(), got len=%d", len(s.touchedCB))
	}
}

func TestSparseSet_ResetDoesNotAffectFutureUse(t *testing.T) {
	s := NewSparseSet(1024, 64)

	firstRound := []uint64{1, 64, 130}
	secondRound := []uint64{2, 65, 131}

	for _, node := range firstRound {
		s.Visit(node)
	}
	s.Reset()

	for _, node := range firstRound {
		if s.Visited(node) {
			t.Fatalf("expected node %d to be cleared after Reset()", node)
		}
	}

	for _, node := range secondRound {
		if already := s.CheckAndVisit(node); already {
			t.Fatalf("expected node %d to be unvisited in second round", node)
		}
		if !s.Visited(node) {
			t.Fatalf("expected node %d to be visited in second round", node)
		}
	}
}

func TestSparseSet_GrowBeyondInitialCapacity(t *testing.T) {
	s := NewSparseSet(64, 64)

	initialMax := s.maxNodeExclusive
	node := initialMax + 123

	if s.Visited(node) {
		t.Fatalf("expected node %d to be unvisited before Visit()", node)
	}

	if already := s.CheckAndVisit(node); already {
		t.Fatalf("expected first VisitIfNotVisited(%d) to return false", node)
	}

	if !s.Visited(node) {
		t.Fatalf("expected node %d to be visited after growth", node)
	}

	if s.maxNodeExclusive <= initialMax {
		t.Fatalf("expected maxNodeExclusive to grow, old=%d new=%d", initialMax, s.maxNodeExclusive)
	}
}

func TestSparseSet_MultipleNodesSameSegment(t *testing.T) {
	s := NewSparseSet(1024, 64)

	// All in same segment when collisionRate = 64
	nodes := []uint64{0, 1, 2, 10, 63}

	for _, node := range nodes {
		if already := s.CheckAndVisit(node); already {
			t.Fatalf("expected node %d first visit to return false", node)
		}
	}

	for _, node := range nodes {
		if !s.Visited(node) {
			t.Fatalf("expected node %d to be visited", node)
		}
	}

	if already := s.CheckAndVisit(10); !already {
		t.Fatalf("expected repeated visit in same segment to return true")
	}
}

func TestSparseSet_MultipleSegmentsSameCBWord(t *testing.T) {
	s := NewSparseSet(4096, 64)

	// Different segments, but likely same cb word for small indexes.
	nodes := []uint64{
		0,       // segment 0
		64,      // segment 1
		64 * 2,  // segment 2
		64 * 10, // segment 10
		64 * 20, // segment 20
		64 * 63, // segment 63
	}

	for _, node := range nodes {
		s.Visit(node)
	}

	for _, node := range nodes {
		if !s.Visited(node) {
			t.Fatalf("expected node %d to be visited", node)
		}
	}

	s.Reset()

	for _, node := range nodes {
		if s.Visited(node) {
			t.Fatalf("expected node %d to be cleared after Reset()", node)
		}
	}
}

func TestSparseSet_VisitedOutOfRangeReturnsFalse(t *testing.T) {
	s := NewSparseSet(64, 64)

	node := s.maxNodeExclusive + 1000
	if s.Visited(node) {
		t.Fatalf("expected out-of-range node %d to be unvisited", node)
	}
}

func TestGrowToUint64SliceLen(t *testing.T) {
	tests := []struct {
		name     string
		initial  []uint64
		need     uint64
		wantMin  uint64
		wantSame bool
	}{
		{
			name:     "no growth needed",
			initial:  make([]uint64, 8),
			need:     8,
			wantMin:  8,
			wantSame: true,
		},
		{
			name:     "grow from non-zero",
			initial:  make([]uint64, 3),
			need:     5,
			wantMin:  5,
			wantSame: false,
		},
		{
			name:     "grow from zero",
			initial:  nil,
			need:     1,
			wantMin:  1,
			wantSame: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			beforeLen := len(tt.initial)
			got := growToUint64SliceLen(tt.initial, tt.need)

			if uint64(len(got)) < tt.wantMin {
				t.Fatalf("expected len >= %d, got %d", tt.wantMin, len(got))
			}

			if tt.wantSame && len(got) != beforeLen {
				t.Fatalf("expected same len=%d, got %d", beforeLen, len(got))
			}
		})
	}
}

func TestGrowToSegmentSliceLen(t *testing.T) {
	tests := []struct {
		name     string
		initial  []segment
		need     uint64
		wantMin  uint64
		wantSame bool
	}{
		{
			name:     "no growth needed",
			initial:  make([]segment, 8),
			need:     8,
			wantMin:  8,
			wantSame: true,
		},
		{
			name:     "grow from non-zero",
			initial:  make([]segment, 2),
			need:     7,
			wantMin:  7,
			wantSame: false,
		},
		{
			name:     "grow from zero",
			initial:  nil,
			need:     1,
			wantMin:  1,
			wantSame: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			beforeLen := len(tt.initial)
			got := growToSegmentSliceLen(tt.initial, tt.need)

			if uint64(len(got)) < tt.wantMin {
				t.Fatalf("expected len >= %d, got %d", tt.wantMin, len(got))
			}

			if tt.wantSame && len(got) != beforeLen {
				t.Fatalf("expected same len=%d, got %d", beforeLen, len(got))
			}
		})
	}
}
