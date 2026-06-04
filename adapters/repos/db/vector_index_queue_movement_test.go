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

package db

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/schema"
)

// fakeFSMReader satisfies types.ReplicationFSMReader via the embedded interface (nil —
// its routing methods are never called here), reports a fixed movement state, and
// records the (collection, shard) it was queried with.
type fakeFSMReader struct {
	types.ReplicationFSMReader
	active    bool
	gotColl   string
	gotShard  string
	callCount int
}

func (f *fakeFSMReader) HasActiveReplicationForShard(collection, shard string) bool {
	f.callCount++
	f.gotColl, f.gotShard = collection, shard
	return f.active
}

// movementFakeUpgradable embeds VectorIndex (nil) to satisfy the field type and overrides
// the upgradableIndexer subset checkCompressionSettings consults. Upgrade only records that
// it was called, so the test can assert whether the upgrade was triggered or deferred.
type movementFakeUpgradable struct {
	VectorIndex
	shouldUpgrade bool
	upgradeAt     int
	indexed       uint64
	upgraded      bool
	upgradeCalled bool
}

func (f *movementFakeUpgradable) ShouldUpgrade() (bool, int) { return f.shouldUpgrade, f.upgradeAt }
func (f *movementFakeUpgradable) Upgraded() bool             { return f.upgraded }
func (f *movementFakeUpgradable) AlreadyIndexed() uint64     { return f.indexed }

func (f *movementFakeUpgradable) Upgrade(callback func()) error {
	f.upgradeCalled = true
	if callback != nil {
		callback()
	}
	return nil
}

func newMovementTestQueue(fsm types.ReplicationFSMReader, vi VectorIndex) *VectorIndexQueue {
	idx := &Index{db: &DB{replicationFSM: fsm}, Config: IndexConfig{ClassName: schema.ClassName("C")}}
	return &VectorIndexQueue{
		// Zero-value scheduler + DiskQueue: PauseQueue/ResumeQueue early-return on a nil ctx
		// and ID() reads an empty string, so the upgrade-trigger path runs without a live
		// queue — letting the "no movement → upgrades" case actually fire ci.Upgrade.
		DiskQueue:   &queue.DiskQueue{},
		scheduler:   &queue.Scheduler{},
		shard:       &Shard{index: idx, name: "S"},
		vectorIndex: vi,
	}
}

// imminentUpgrade returns a fake whose config makes an upgrade due on the next schedule
// (indexed past the threshold) — so the movement state is the only thing deciding the
// outcome between the two sub-cases below.
func imminentUpgrade() *movementFakeUpgradable {
	return &movementFakeUpgradable{shouldUpgrade: true, upgradeAt: 0, indexed: 1}
}

// With the upgrade due, the active-movement state is the sole differing input and flips the
// outcome: no movement triggers ci.Upgrade (skip=true); an active movement defers it
// (skip=false, keep indexing into flat) — the on-disk restructure a movement's snapshot+CCL
// can't reconcile. Both cases assert the gate queried this shard's own collection+name.
func TestVectorIndexQueue_MovementGatesUpgrade(t *testing.T) {
	t.Run("no movement upgrades", func(t *testing.T) {
		fsm := &fakeFSMReader{active: false}
		fake := imminentUpgrade()
		iq := newMovementTestQueue(fsm, fake)

		require.True(t, iq.BeforeSchedule(), "skip=true once the upgrade is triggered")
		require.True(t, fake.upgradeCalled, "upgrade must fire when no movement is active")
		require.Equal(t, "C", fsm.gotColl)
		require.Equal(t, "S", fsm.gotShard)
	})

	t.Run("active movement defers", func(t *testing.T) {
		fsm := &fakeFSMReader{active: true}
		fake := imminentUpgrade()
		iq := newMovementTestQueue(fsm, fake)

		require.False(t, iq.BeforeSchedule(), "skip=false: keep indexing into flat")
		require.False(t, fake.upgradeCalled, "upgrade must be deferred while a movement is active")
		require.Equal(t, "C", fsm.gotColl)
		require.Equal(t, "S", fsm.gotShard)
	})
}

// Below the upgrade threshold the FSM is not consulted at all — the gate must not hit the
// cluster-wide, locked replication FSM on every schedule tick.
func TestVectorIndexQueue_DoesNotConsultFSMBelowThreshold(t *testing.T) {
	fsm := &fakeFSMReader{active: true}
	fake := &movementFakeUpgradable{shouldUpgrade: true, upgradeAt: 5, indexed: 0}
	iq := newMovementTestQueue(fsm, fake)

	require.False(t, iq.BeforeSchedule())
	require.Zero(t, fsm.callCount, "FSM must only be consulted when an upgrade is imminent")
	require.False(t, fake.upgradeCalled)
}
