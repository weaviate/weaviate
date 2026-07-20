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

package roaringset

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPendingMergesAdd(t *testing.T) {
	tests := []struct {
		name      string
		items     []int
		wantFlags []bool
	}{
		{name: "single item starts drain", items: []int{1}, wantFlags: []bool{true}},
		{name: "only first item starts drain", items: []int{1, 2, 3}, wantFlags: []bool{true, false, false}},
		{name: "empty queue stays idle", items: nil, wantFlags: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewPendingMerges[int]()
			for i, item := range tt.items {
				assert.Equal(t, tt.wantFlags[i], p.Add(item))
			}
			assert.Equal(t, len(tt.items), p.Len())
		})
	}
}

func TestPendingMergesAddAfterDrainStartsAgain(t *testing.T) {
	p := NewPendingMerges[int]()
	require.True(t, p.Add(1))
	p.Drain(func(int) {})
	require.Equal(t, 0, p.Len())

	// queue was drained empty: the next add must report startDrain again
	require.True(t, p.Add(2))
}

func TestPendingMergesDrainOrderAndMidDrainAdds(t *testing.T) {
	p := NewPendingMerges[int]()
	p.Add(1)
	p.Add(2)

	var merged []int
	p.Drain(func(item int) {
		merged = append(merged, item)
		if item == 2 {
			// added while the drain is running: must still be picked up
			require.False(t, p.Add(3))
		}
	})

	assert.Equal(t, []int{1, 2, 3}, merged)
	assert.Equal(t, 0, p.Len())
	assert.Nil(t, p.Snapshot())
}

func TestPendingMergesSnapshotIndependentCopy(t *testing.T) {
	p := NewPendingMerges[int]()
	p.Add(1)
	p.Add(2)

	snap := p.Snapshot()
	require.Equal(t, []int{1, 2}, snap)

	// draining must not mutate the already-taken snapshot
	p.Drain(func(int) {})
	assert.Equal(t, []int{1, 2}, snap)
	assert.Nil(t, p.Snapshot())
}

func TestPendingMergesConcurrentSmoke(t *testing.T) {
	p := NewPendingMerges[int]()

	var wg, drainWg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				if p.Add(i) {
					drainWg.Add(1)
					go func() {
						defer drainWg.Done()
						p.Drain(func(int) {})
					}()
				}
				_ = p.Snapshot()
				_ = p.Len()
			}
		}()
	}
	wg.Wait()
	// producers are done, so no new drains can start; wait for the in-flight
	// ones so the final drain does not overlap them (one drain at a time)
	drainWg.Wait()

	p.Drain(func(int) {})
	assert.Equal(t, 0, p.Len())
}
