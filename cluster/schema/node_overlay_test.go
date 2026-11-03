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

package schema

import (
	"encoding/json"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/usecases/fakes"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestNodeOverlayReadOnly(t *testing.T) {
	o := &NodeOverlay{}
	assert.False(t, o.ReadOnly("C", "s1"))

	o.SetReadOnly("C", "s1", true)
	assert.True(t, o.ReadOnly("C", "s1"))
	assert.False(t, o.ReadOnly("C", "s2"))
	assert.False(t, o.ReadOnly("D", "s1"))

	o.SetReadOnly("C", "s1", false)
	assert.False(t, o.ReadOnly("C", "s1"))

	var missing *NodeOverlay
	assert.False(t, missing.ReadOnly("C", "s1"))
}

func TestNodeOverlayClears(t *testing.T) {
	o := &NodeOverlay{}
	o.SetReadOnly("C", "s1", true)
	o.SetReadOnly("C", "s2", true)
	o.SetReadOnly("C", "s3", true)
	o.SetReadOnly("D", "s1", true)

	o.clearShards("C", []string{"s1", "unknown"})
	assert.False(t, o.ReadOnly("C", "s1"))
	assert.True(t, o.ReadOnly("C", "s2"))

	o.clearClass("C")
	assert.False(t, o.ReadOnly("C", "s2"))
	assert.False(t, o.ReadOnly("C", "s3"))
	assert.True(t, o.ReadOnly("D", "s1"))
}

func TestNodeOverlayReadAllocatesNothing(t *testing.T) {
	o := &NodeOverlay{}
	o.SetReadOnly("C", "s1", true)

	allocs := testing.AllocsPerRun(1000, func() {
		_ = o.ReadOnly("C", "s1")
		_ = o.ReadOnly("C", "s2")
	})

	assert.Zero(t, allocs, "this runs on every write")
}

func TestNodeOverlayHandleReadsAllocateNothing(t *testing.T) {
	o := &NodeOverlay{}
	phase := o.NewShardPhase("C", "s1", storagestate.StatusReady)

	assert.Zero(t, testing.AllocsPerRun(100, func() {
		_ = o.ResourcePressure()
		_ = phase.Status()
		_ = phase.Reindexing()
	}))
}

func TestNodeOverlayNilReadsAreSafe(t *testing.T) {
	var o *NodeOverlay

	assert.False(t, o.ResourcePressure())
	assert.Nil(t, o.PhaseOf("C", "s1"))

	phase := o.NewShardPhase("C", "s1", storagestate.StatusLoading)
	assert.Equal(t, storagestate.StatusLoading, phase.Status())
	o.DropShardPhase("C", "s1", phase)
}

func TestNodeOverlayShardPhases(t *testing.T) {
	tests := []struct {
		name string
		act  func(o *NodeOverlay, first *ShardPhase)
		want storagestate.Status
		// wantGone expects no phase for the shard at all.
		wantGone bool
	}{
		{
			name: "registered object",
			act:  func(*NodeOverlay, *ShardPhase) {},
			want: storagestate.StatusLoading,
		},
		{
			name: "phase moves through the handle",
			act: func(_ *NodeOverlay, first *ShardPhase) {
				first.Set(storagestate.StatusReady)
			},
			want: storagestate.StatusReady,
		},
		{
			name: "dropped object",
			act: func(o *NodeOverlay, first *ShardPhase) {
				o.DropShardPhase("C", "s1", first)
			},
			wantGone: true,
		},
		{
			name: "a replaced object cannot report for its successor",
			act: func(o *NodeOverlay, first *ShardPhase) {
				o.NewShardPhase("C", "s1", storagestate.StatusReady)
				first.Set(storagestate.StatusShutdown)
			},
			want: storagestate.StatusReady,
		},
		{
			name: "a replaced object cannot drop its successor",
			act: func(o *NodeOverlay, first *ShardPhase) {
				o.NewShardPhase("C", "s1", storagestate.StatusReady)
				o.DropShardPhase("C", "s1", first)
			},
			want: storagestate.StatusReady,
		},
		{
			// The store a reindex paused went with the object it ran on.
			name: "a replaced object's reindex does not hold its successor",
			act: func(o *NodeOverlay, first *ShardPhase) {
				first.SetReindexing(true)
				o.NewShardPhase("C", "s1", storagestate.StatusReady)
			},
			want: storagestate.StatusReady,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := &NodeOverlay{}
			first := o.NewShardPhase("C", "s1", storagestate.StatusLoading)

			tt.act(o, first)

			got := o.PhaseOf("C", "s1")
			if tt.wantGone {
				assert.Nil(t, got)
			} else {
				require.NotNil(t, got)
				assert.Equal(t, tt.want, got.Status())
				assert.False(t, got.Reindexing())
			}
			assert.Nil(t, o.PhaseOf("C", "s2"), "phases are per shard")
		})
	}
}

// Writers copy the set, so one outside the lock would drop another's flag.
func TestNodeOverlayConcurrentAccess(t *testing.T) {
	o := &NodeOverlay{}
	shards := []string{"s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7"}
	var wg sync.WaitGroup
	for _, shard := range shards {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for range 200 {
				o.SetReadOnly("C", shard, false)
				o.SetReadOnly("C", shard, true)
			}
		}()
		go func() {
			defer wg.Done()
			for range 200 {
				_ = o.ReadOnly("C", shard)
			}
		}()
	}
	wg.Wait()

	for _, shard := range shards {
		assert.True(t, o.ReadOnly("C", shard), shard)
	}
}

func overlayManager(t *testing.T, owners []string) *SchemaManager {
	t.Helper()
	logger, _ := test.NewNullLogger()
	s := NewSchema("n1", prometheus.NewPedanticRegistry())
	cls := &models.Class{Class: "C", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true}}
	require.NoError(t, s.addClass(cls, &sharding.State{
		PartitioningEnabled: true,
		Physical: map[string]sharding.Physical{
			"t": {Name: "t", Status: hot, BelongsToNodes: owners},
		},
	}, 1))
	return &SchemaManager{schema: s, log: logger, overlay: &NodeOverlay{}}
}

func shardStatusCmd(t *testing.T, class, shard, status string) *command.ApplyRequest {
	t.Helper()
	sub, err := json.Marshal(command.UpdateShardStatusRequest{Class: class, Shard: shard, Status: status})
	require.NoError(t, err)
	return &command.ApplyRequest{Type: command.ApplyRequest_TYPE_UPDATE_SHARD_STATUS, Class: class, SubCommand: sub}
}

func TestUpdateShardStatusWritesTheOverlay(t *testing.T) {
	cases := []struct {
		name    string
		preset  bool
		status  string
		want    bool
		wantErr bool
	}{
		{name: "READONLY sets it", status: "READONLY", want: true},
		{name: "any case is accepted", status: "readonly", want: true},
		{name: "READY clears it", preset: true, status: "READY"},
		{name: "SHUTDOWN is ignored", preset: true, status: "SHUTDOWN", want: true},
		{name: "INDEXING is ignored", preset: true, status: "INDEXING", want: true},
		{name: "an unknown status fails", preset: true, status: "NOPE", want: true, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := overlayManager(t, []string{"n2"}) // this node is not an owner
			m.overlay.SetReadOnly("C", "t", tc.preset)

			err := m.UpdateShardStatus(shardStatusCmd(t, "C", "t", tc.status), false)

			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tc.want, m.overlay.ReadOnly("C", "t"))
		})
	}
}

func TestUpdateShardStatusIsLostOnReplay(t *testing.T) {
	m := overlayManager(t, []string{"n1"})

	require.NoError(t, m.UpdateShardStatus(shardStatusCmd(t, "C", "t", "READONLY"), true))

	assert.False(t, m.overlay.ReadOnly("C", "t"))
}

func TestUpdateShardStatusNeedsAShard(t *testing.T) {
	m := overlayManager(t, []string{"n1"})

	assert.ErrorIs(t, m.UpdateShardStatus(shardStatusCmd(t, "C", "unknown", "READONLY"), false), ErrShardNotFound)
	assert.ErrorIs(t, m.UpdateShardStatus(shardStatusCmd(t, "unknown", "t", "READONLY"), false), ErrClassNotFound)
	assert.False(t, m.overlay.ReadOnly("C", "unknown"))
}

func TestDeleteClearsReadOnly(t *testing.T) {
	t.Run("tenant delete", func(t *testing.T) {
		m := overlayManager(t, []string{"n1"})
		m.overlay.SetReadOnly("C", "t", true)
		sub, err := proto.Marshal(&command.DeleteTenantsRequest{Tenants: []string{"t"}})
		require.NoError(t, err)

		require.NoError(t, m.DeleteTenants(&command.ApplyRequest{Class: "C", SubCommand: sub}, true))

		assert.False(t, m.overlay.ReadOnly("C", "t"), "a re-created tenant must not inherit it")
	})

	t.Run("class delete", func(t *testing.T) {
		m := overlayManager(t, []string{"n1"})
		m.overlay.SetReadOnly("C", "t", true)

		require.NoError(t, m.DeleteClass(&command.ApplyRequest{Class: "C"}, true, false))

		assert.False(t, m.overlay.ReadOnly("C", "t"))
	})
}

func TestOverlayIsNeverSnapshotted(t *testing.T) {
	m := overlayManager(t, []string{"n1"})
	before, err := m.SchemaSnapshot()
	require.NoError(t, err)

	m.overlay.SetReadOnly("C", "t", true)

	after, err := m.SchemaSnapshot()
	require.NoError(t, err)
	assert.Equal(t, before, after)
}

// A follower that lags past the trailing logs catches up by snapshot, so it
// never applies the READY or delete that cleared a flag.
func TestSnapshotInstallDropsUserReadOnly(t *testing.T) {
	cases := map[string]func(t *testing.T, sm *SchemaManager) error{
		"snapshot": func(t *testing.T, sm *SchemaManager) error { return sm.Restore(snapshotWithout(t, sm, ""), sm.parser) },
		"legacy snapshot": func(t *testing.T, sm *SchemaManager) error {
			return sm.RestoreLegacy(legacySnapshotWithout(t, sm, ""), sm.parser)
		},
	}
	for name, restore := range cases {
		t.Run(name, func(t *testing.T) {
			parser := fakes.NewMockParser()
			parser.On("ParseClass", mock.Anything).Return(nil)
			sm := NewSchemaManager("node1", &recordingIndexer{}, parser, prometheus.NewPedanticRegistry(), logrus.New())
			addClass(t, sm, "C")
			sm.overlay.SetReadOnly("C", "shard1", true)
			var changed []string
			sm.overlay.OnReadOnlyChange(func(class, shard string) { changed = append(changed, class+"/"+shard) })

			require.NoError(t, restore(t, sm))

			assert.False(t, sm.overlay.ReadOnly("C", "shard1"), "a restart drops the flag too")
			assert.Equal(t, []string{"C/shard1"}, changed, "the shard reads its flag again")
		})
	}
}
