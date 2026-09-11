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

package hnsw

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw/packedconn"
)

// TestCleanupTombstones_PanicSurfacesAsError pins the error contract of the
// cleanup cycle's recover: a panic anywhere in the cycle must surface as an
// error, not as a successful run. A swallowed panic is how the original
// nil-entrypoint crash stayed invisible while tombstones accumulated.
func TestCleanupTombstones_PanicSurfacesAsError(t *testing.T) {
	index := newTombstonedNilEntrypointIndex(t, true)

	err := index.CleanUpTombstonedNodes(func() bool { panic("injected panic") })
	require.ErrorContains(t, err, "tombstone cleanup panicked")
}

// TestCleanupTombstones_KeepsTombstoneWhenEntrypointNotReplaceable: when the
// entrypoint is tombstoned but every replacement candidate is under
// maintenance by a concurrent insert, the cycle must keep the entrypoint's
// tombstone for a later retry. Removing it would leave a dangling,
// untombstoned entrypoint that no later cycle revisits.
func TestCleanupTombstones_KeepsTombstoneWhenEntrypointNotReplaceable(t *testing.T) {
	index := newTombstonedNilEntrypointIndex(t, false)

	conns, err := packedconn.NewWithElements([][]uint64{{}})
	require.Nil(t, err)
	index.Lock()
	index.nodes[1] = &vertex{id: 1, level: 0, connections: conns}
	index.Unlock()
	index.nodes[1].markAsMaintenance()

	// cleanup's reassignment unmarks maintenance, so re-mark on every
	// shouldAbort invocation to keep simulating an in-flight insert
	keepInMaintenance := func() bool {
		if node := index.nodeByID(1); node != nil {
			node.markAsMaintenance()
		}
		return false
	}

	require.Nil(t, index.CleanUpTombstonedNodes(keepInMaintenance))
	assert.Equal(t, uint64(0), index.getEntrypoint(),
		"entrypoint cannot move while the only candidate is under maintenance")
	assert.Equal(t, 1, tombstoneCountOf(index),
		"tombstone must be kept so a later cycle retries")

	index.nodes[1].unmarkAsMaintenance()

	require.Nil(t, index.CleanUpTombstonedNodes(neverStop))
	assert.Equal(t, uint64(1), index.getEntrypoint(),
		"entrypoint must move once the candidate leaves maintenance")
	assert.Equal(t, 0, tombstoneCountOf(index),
		"tombstone must drain once the entrypoint has been replaced")
}

// promotingDenyList simulates a concurrent insert promoting a new, higher
// entrypoint while findNewGlobalEntrypoint is scanning for a replacement:
// the promotion fires during the deny-list check of the scan's final
// candidate, i.e. after the scan's last look at the current entrypoint.
type promotingDenyList struct {
	helpers.AllowList
	index        *hnsw
	triggerID    uint64
	promoteTo    uint64
	promoteLevel int
	once         sync.Once
}

func (p *promotingDenyList) Contains(id uint64) bool {
	if id == p.triggerID {
		p.once.Do(func() {
			p.index.Lock()
			p.index.entryPointID = p.promoteTo
			p.index.currentMaximumLayer = p.promoteLevel
			p.index.Unlock()
		})
	}
	return p.AllowList.Contains(id)
}

// TestDeleteEntrypoint_DoesNotClobberConcurrentPromotion: deleteEntrypoint's
// callers do not exclude concurrent inserts, so a promotion that lands
// between the candidate scan and the entrypoint write must win — an
// unconditional write would demote the entrypoint and lower
// currentMaximumLayer, cutting off the promoted node's upper layers.
func TestDeleteEntrypoint_DoesNotClobberConcurrentPromotion(t *testing.T) {
	index := newTombstonedNilEntrypointIndex(t, false)

	conns3, err := packedconn.NewWithElements([][]uint64{{}, {}, {}, {}, {}, {}})
	require.Nil(t, err)
	conns9, err := packedconn.NewWithElements([][]uint64{{}})
	require.Nil(t, err)
	index.Lock()
	index.nodes[3] = &vertex{id: 3, level: 5, connections: conns3}
	index.nodes[9] = &vertex{id: 9, level: 0, connections: conns9}
	index.Unlock()
	// node 3 is mid-insert: under maintenance, so the scan must not pick
	// it — but the insert promotes it to entrypoint mid-scan
	index.nodes[3].markAsMaintenance()

	deny := &promotingDenyList{
		AllowList:    helpers.NewAllowList(0),
		index:        index,
		triggerID:    9,
		promoteTo:    3,
		promoteLevel: 5,
	}

	require.Nil(t, index.deleteEntrypoint(0, deny))

	assert.Equal(t, uint64(3), index.getEntrypoint(),
		"the concurrent promotion must not be clobbered")
	index.RLock()
	maxLayer := index.currentMaximumLayer
	index.RUnlock()
	assert.Equal(t, 5, maxLayer,
		"currentMaximumLayer must keep the promoted node's level")
}

// TestSetDimensions_FallsBackWhenEntrypointObjectGone: after a torn crash
// recovery the entrypoint's object may be gone from the object store. dims
// must then come from another live node — leaving it at 0 disables dimension
// validation for every insert, and the first wrong-length insert would
// poison the recorded dimensionality.
func TestSetDimensions_FallsBackWhenEntrypointObjectGone(t *testing.T) {
	index := newTombstonedNilEntrypointIndex(t, true)
	index.VectorForIDThunk = func(ctx context.Context, id uint64) ([]float32, error) {
		if id == 0 {
			return nil, fmt.Errorf("object %d deleted", id)
		}
		return []float32{0.1, 0.2, 0.3}, nil
	}

	index.setDimensionsFromEntrypoint()

	assert.Equal(t, int32(3), index.dims.Load(),
		"dims must fall back to a live node when the entrypoint's object is gone")
}

// TestIterate_ConcurrentResetIsRaceFree: resetUnlocked swaps h.resetCtx while
// iterate reads it per iteration; run under -race this pins that the read is
// synchronized. The iterator must still terminate after the reset.
func TestIterate_ConcurrentResetIsRaceFree(t *testing.T) {
	index := newTombstonedNilEntrypointIndex(t, false)
	conns, err := packedconn.NewWithElements([][]uint64{{}})
	require.Nil(t, err)
	index.Lock()
	// a wide, almost-empty nodes slice keeps the iterator busy reading the
	// reset context long enough to overlap the concurrent reset below
	index.nodes = make([]*vertex, 1_000_000)
	index.nodes[0] = &vertex{id: 0, connections: conns}
	index.Unlock()
	index.tombstoneLock.Lock()
	index.tombstones = map[uint64]struct{}{}
	index.tombstoneLock.Unlock()

	entered := make(chan struct{})
	done := make(chan struct{})

	go func() {
		defer close(done)
		index.iterate(func(docID uint64) bool {
			close(entered) // only node 0 is live, so this fires exactly once
			return true
		})
	}()

	<-entered
	// deleting the only node resets the graph, which swaps h.resetCtx
	// while the iterator is still scanning
	require.Nil(t, index.Delete(0))
	<-done
}
