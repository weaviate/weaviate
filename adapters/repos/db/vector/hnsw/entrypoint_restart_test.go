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
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The entrypoint is deleted, the index is reopened from the commit log right
// away and searched before any cleanup ran. In the crash variants the
// delete's entrypoint change and tombstone never reached the log, so the
// reopened index still points at the deleted node.
func TestEntrypoint_DeleteRestartSearch(t *testing.T) {
	type testCase struct {
		name       string
		objectGone bool // the object left the store before the index delete
		changeLost bool // the delete was not durable
		nodes      int
	}
	cases := []testCase{
		{name: "delete persisted", nodes: 12},
		{name: "delete persisted, object gone", objectGone: true, nodes: 12},
		{name: "delete lost in crash, object gone", objectGone: true, changeLost: true, nodes: 12},
		{name: "delete lost in crash, object still there", changeLost: true, nodes: 12},
		{name: "only node deleted", nodes: 1},
	}

	for _, compressed := range []bool{false, true} {
		for _, tc := range cases {
			t.Run(fmt.Sprintf("compressed=%v/%s", compressed, tc.name), func(t *testing.T) {
				f := newBootstrapFixtureWith(t, compressed, 8, 0)
				// upper levels, so the entrypoint is a real top-layer node
				f.index.randFunc = rand.New(rand.NewSource(1)).Float64
				for i := range uint64(tc.nodes) {
					require.NoError(t, f.index.Add(f.ctx, i, f.vectors[i]))
				}
				require.Equal(t, compressed, f.index.Compressed())
				ep := f.index.getEntrypoint()
				if tc.nodes > 1 {
					require.Positive(t, f.index.currentMaximumLayer, "the entrypoint must sit on an upper layer")
				}
				live := without(idRange(0, uint64(tc.nodes)), []uint64{ep})

				if tc.objectGone {
					f.deleteFromObjectStore(ep)
				}
				f.commitLog.dropWrites.Store(tc.changeLost)
				require.NoError(t, f.index.Delete(ep))
				f.commitLog.dropWrites.Store(false)

				f.restart(t)
				if tc.nodes == 1 {
					found, _, err := f.index.SearchByVector(f.ctx, f.vectors[ep], 1, nil)
					require.NoError(t, err)
					require.Empty(t, found, "the reset index must be empty")
					f.addWithin(t, 1, 10*time.Second)
					f.requireReachable(t, 1)
					return
				}
				if tc.changeLost {
					require.Equal(t, ep, f.index.getEntrypoint(), "the stale entrypoint replays from the log")
					require.False(t, f.index.hasTombstone(ep))
				} else {
					require.NotEqual(t, ep, f.index.getEntrypoint())
					require.True(t, f.index.hasTombstone(ep))
				}

				// search right away, before any cleanup
				for _, id := range live {
					found, _, err := f.index.SearchByVector(f.ctx, f.vectors[id], tc.nodes, nil)
					require.NoError(t, err)
					require.Subset(t, found, live, "search from %d", id)
					if !tc.changeLost {
						require.NotContains(t, found, ep)
					}
				}
				if tc.changeLost {
					// the delete is replayed by the caller after the crash
					require.NoError(t, f.index.Delete(ep))
				}
				f.requireReachable(t, live...)
				newEP := f.index.getEntrypoint()
				require.NotEqual(t, ep, newEP)
				require.NotNil(t, f.index.nodeByID(newEP))
				require.False(t, f.index.hasTombstone(newEP))

				next := uint64(tc.nodes)
				f.addWithin(t, next, 10*time.Second)
				live = append(live, next)
				f.requireReachable(t, live...)
				require.NoError(t, f.index.CleanUpTombstonedNodes(neverStop))
				require.Nil(t, f.index.nodeByID(ep))
				f.restart(t)
				f.requireReachable(t, live...)
				f.requireLayersWellFormed(t)
			})
		}
	}
}
