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

package lsmkv

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSegmentRefCountAtomic verifies the lock-free atomic refcount: concurrent
// incRef/decRef neither lose updates nor race. Run with -race to catch the
// latter; the count assertions catch the former (a plain int would under-count).
func TestSegmentRefCountAtomic(t *testing.T) {
	t.Parallel()

	s := &segment{}
	const goroutines = 64
	const perGoroutine = 20000

	// wave 1: everyone incRefs — total must be exact (no lost increments)
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				s.incRef()
			}
		}()
	}
	wg.Wait()
	require.Equal(t, goroutines*perGoroutine, s.getRefs())

	// wave 2: everyone decRefs — must land back exactly at zero
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				s.decRef()
			}
		}()
	}
	wg.Wait()
	require.Equal(t, 0, s.getRefs())
}

func TestSegmentDecRefBelowZeroPanics(t *testing.T) {
	t.Parallel()
	s := &segment{}
	require.Panics(t, func() { s.decRef() })
	require.Equal(t, 0, s.getRefs()) // restored to 0, not pinned negative
}

// TestSegmentGroupConsistentViewConcurrent hammers the now lock-free
// getConsistentViewOfSegments + release from many goroutines. -race proves the
// refcount access is data-race-free without segmentRefCounterLock; the final
// assertion proves every acquire is balanced by its release (refs return to 0).
func TestSegmentGroupConsistentViewConcurrent(t *testing.T) {
	t.Parallel()

	segs := make([]Segment, 0, 4)
	for i := 0; i < 4; i++ {
		segs = append(segs, newFakeReplaceSegment(map[string][]byte{"k": []byte("v")}))
	}
	sg := &SegmentGroup{strategy: StrategyReplace, segments: segs}

	const readers = 32
	const iters = 10000
	var wg sync.WaitGroup
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				_, release := mustSegmentView(t, sg)
				release()
			}
		}()
	}
	wg.Wait()

	for i, s := range sg.segments {
		require.Equalf(t, 0, s.getRefs(), "segment %d should have no outstanding refs", i)
	}
}
