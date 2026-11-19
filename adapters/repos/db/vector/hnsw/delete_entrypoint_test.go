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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
)

// onContainsAllowList runs hook once, when trigger is looked up
type onContainsAllowList struct {
	helpers.AllowList
	trigger uint64
	hook    func()
}

func (l *onContainsAllowList) Contains(id uint64) bool {
	if id == l.trigger && l.hook != nil {
		l.hook()
		l.hook = nil
	}
	return l.AllowList.Contains(id)
}

// deleteEntrypoint must not overwrite an entrypoint a concurrent insert
// promoted after its scan finished
func TestDeleteEntrypoint_KeepsConcurrentPromotion(t *testing.T) {
	f := newBootstrapFixture(t, false)
	ep := f.index.getEntrypoint()
	require.NoError(t, f.index.addTombstone(ep))
	f.index.RLock()
	lastSlot := uint64(len(f.index.nodes) - 1)
	f.index.RUnlock()

	deny := &onContainsAllowList{AllowList: helpers.NewAllowList(ep), trigger: lastSlot, hook: func() {
		node := &vertex{level: 2}
		node.connections.GrowLayersTo(2)
		require.NoError(t, f.index.publishNode(4, node, f.vectors[4]))
		f.index.Lock()
		f.index.entryPointID = 4
		f.index.currentMaximumLayer = 2
		f.index.Unlock()
	}}
	persisted := f.commitLog.setEntrypointCalls.Load()

	require.NoError(t, f.index.deleteEntrypoint(ep, deny))
	require.Nil(t, deny.hook, "the scan must have reached the last slot")
	require.Equal(t, uint64(4), f.index.getEntrypoint(), "the promoted entrypoint was overwritten")
	require.Equal(t, 2, f.index.currentMaximumLayer)
	require.Equal(t, persisted, f.commitLog.setEntrypointCalls.Load(), "nothing to persist")
}

func TestFindNewGlobalEntrypoint_PrefersNodesNotUnderMaintenance(t *testing.T) {
	f := newBootstrapFixture(t, false)
	ep := f.index.getEntrypoint()
	a, b := f.nonEntrypointNodes()[0], f.nonEntrypointNodes()[1]
	deny := helpers.NewAllowList(ep)

	f.index.nodeByID(a).markAsMaintenance()
	got, _, ok := f.index.findNewGlobalEntrypoint(deny, ep)
	require.True(t, ok)
	require.Equal(t, b, got, "the node not under maintenance must win")

	f.index.nodeByID(b).markAsMaintenance()
	got, _, ok = f.index.findNewGlobalEntrypoint(deny, ep)
	require.True(t, ok, "a node under maintenance is the last resort")
	require.Contains(t, []uint64{a, b}, got)

	require.NoError(t, f.index.addTombstone(b))
	got, _, ok = f.index.findNewGlobalEntrypoint(deny, ep)
	require.True(t, ok)
	require.Equal(t, a, got, "tombstoned nodes are never chosen")
}

// A node being inserted has no links at the levels it has not reached yet, so
// it is not used as the entrypoint there; one that has links is, maintenance
// or not, and so is a link-less node that is not under maintenance.
func TestPickEntrypoint_SkipsUnlinkedNodeUnderMaintenance(t *testing.T) {
	tests := []struct {
		name        string
		maintenance bool
		linksAt     []int
		level       int
		wantSkipped bool
	}{
		{name: "under maintenance, no links at the level", maintenance: true, linksAt: []int{1}, level: 0, wantSkipped: true},
		{name: "under maintenance, links at the level", maintenance: true, linksAt: []int{1}, level: 1},
		{name: "under maintenance, links at every level", maintenance: true, linksAt: []int{0, 1}, level: 0},
		{name: "not under maintenance, no links", maintenance: false, level: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newBootstrapFixture(t, false)
			ep := f.index.getEntrypoint()
			node := &vertex{level: 1}
			node.connections.GrowLayersTo(1)
			require.NoError(t, f.index.publishNode(4, node, f.vectors[4]))
			for _, level := range tc.linksAt {
				f.appendLinks(t, 4, level, []uint64{ep})
			}
			if tc.maintenance {
				node.markAsMaintenance()
			}

			n := newNeighborFinderConnector(f.index, 5, &vertex{}, 4, f.vectors[5], nil, 1, 1,
				helpers.NewAllowList(), false, nil)
			require.NoError(t, n.pickEntrypoint(f.ctx, tc.level))
			if tc.wantSkipped {
				require.Equal(t, ep, n.entryPointID, "the global entrypoint must be used instead")
			} else {
				require.Equal(t, uint64(4), n.entryPointID)
			}
			require.Equal(t, ep, f.index.getEntrypoint(), "the global entrypoint is not touched")
		})
	}
}
