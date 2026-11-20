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
	"errors"
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
	var changed []string
	o.OnReadOnlyChange(func(class, shard string) { changed = append(changed, class+"/"+shard) })

	o.SetReadOnly("C", "s1", true)
	o.SetReadOnly("C", "s1", true)
	assert.True(t, o.ReadOnly("C", "s1"))
	assert.False(t, o.ReadOnly("C", "s2"))
	assert.False(t, o.ReadOnly("D", "s1"))

	o.SetReadOnly("C", "s1", false)
	o.SetReadOnly("C", "s1", false)
	assert.False(t, o.ReadOnly("C", "s1"))
	assert.Equal(t, []string{"C/s1", "C/s1"}, changed, "only a change notifies")

	var missing *NodeOverlay
	assert.False(t, missing.ReadOnly("C", "s1"))
	assert.False(t, missing.ResourcePressure())
	assert.NoError(t, missing.RefuseWrite(nil))
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

func TestNodeOverlayDecides(t *testing.T) {
	readOnly := func(reason string) string { return storagestate.ErrStatusReadOnlyWithReason(reason).Error() }
	tests := []struct {
		name       string
		user       bool
		pressure   bool
		conditions PhaseCondition
		wantWrite  string // "" takes the write
		wantClient string
		wantStore  bool
		wantTemp   bool
		wantStatus storagestate.Status
	}{
		{name: "writable", wantStatus: storagestate.StatusReady},
		{
			name: "user read-only refuses only client writes", user: true,
			wantClient: readOnly(storagestate.ReasonUserReadOnly), wantStatus: storagestate.StatusReadOnly,
		},
		{
			name: "pressure pauses everything", pressure: true,
			wantWrite: readOnly(storagestate.ReasonResourcePressure), wantStore: true, wantTemp: true,
			wantStatus: storagestate.StatusReadOnly,
		},
		{
			name: "a reindex keeps its temp buckets writable", conditions: PhaseReindexing,
			wantWrite: storagestate.ErrStatusReadOnly.Error(), wantStore: true, wantStatus: storagestate.StatusReadOnly,
		},
		{
			name: "a reindex swapping pauses its temp buckets", conditions: PhaseReindexing | PhaseReindexSwapping,
			wantWrite: storagestate.ErrStatusReadOnly.Error(), wantStore: true, wantTemp: true,
			wantStatus: storagestate.StatusReadOnly,
		},
		{
			name: "a held migration wins over pressure", pressure: true, conditions: PhaseReindexing | PhaseMigrationHeld,
			wantWrite: readOnly(storagestate.ReasonMirrorUnarmed), wantStore: true, wantTemp: true,
			wantStatus: storagestate.StatusReadOnly,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := &NodeOverlay{}
			o.SetReadOnly("C", "s1", tt.user)
			o.SetResourcePressure(tt.pressure)
			phase := NewShardPhase(storagestate.StatusReady)
			phase.SetCondition(tt.conditions, true)

			if err := o.RefuseWrite(phase); tt.wantWrite == "" {
				assert.NoError(t, err)
			} else {
				assert.EqualError(t, err, tt.wantWrite)
			}
			if err := o.RefuseClientWrite("C", "s1"); tt.wantClient == "" {
				assert.NoError(t, err)
			} else {
				assert.EqualError(t, err, tt.wantClient)
			}
			store, temp := o.StorePaused(phase)
			assert.Equal(t, tt.wantStore, store, "store")
			assert.Equal(t, tt.wantTemp, temp, "temp buckets")
			assert.Equal(t, tt.wantStatus.String(), o.Report("C", "s1", phase, storagestate.StatusReady).Status)
		})
	}
}

func TestShardPhaseConditionsAreIndependent(t *testing.T) {
	phase := NewShardPhase(storagestate.StatusReady)
	phase.SetCondition(PhaseReindexing|PhaseReindexSwapping, true)
	phase.SetCondition(PhaseMigrationHeld, true)

	phase.SetCondition(PhaseReindexing|PhaseReindexSwapping, false)

	assert.False(t, phase.Has(PhaseReindexing|PhaseReindexSwapping))
	assert.True(t, phase.Has(PhaseMigrationHeld))
}

func TestNodeOverlayWritePathAllocatesNothing(t *testing.T) {
	o := &NodeOverlay{}
	o.SetReadOnly("C", "s1", true)
	phase := NewShardPhase(storagestate.StatusReady)

	assert.Zero(t, testing.AllocsPerRun(1000, func() {
		_ = o.ReadOnly("C", "s1")
		_ = o.RefuseClientWrite("C", "s2")
		_ = o.RefuseWrite(phase)
		_, _ = o.StorePaused(phase)
		_ = phase.Status()
	}))
}

func overlayManager(t *testing.T, owners []string) *SchemaManager {
	t.Helper()
	logger, _ := test.NewNullLogger()
	s := NewSchema("n1", prometheus.NewPedanticRegistry())
	cls := &models.Class{Class: "C", MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true}}
	require.NoError(t, s.addClass(cls, &sharding.State{
		PartitioningEnabled: true,
		Physical: map[string]sharding.Physical{
			"t": {Name: "t", Status: models.TenantActivityStatusHOT, BelongsToNodes: owners},
		},
	}, 1))
	return &SchemaManager{schema: s, db: &shardStatusIndexer{}, log: logger, overlay: &NodeOverlay{}}
}

type shardStatusIndexer struct {
	Indexer
	err error
}

func (f *shardStatusIndexer) UpdateShardStatus(*command.UpdateShardStatusRequest) error { return f.err }

func shardStatusCmd(t *testing.T, class, shard, status string) *command.ApplyRequest {
	t.Helper()
	sub, err := json.Marshal(command.UpdateShardStatusRequest{Class: class, Shard: shard, Status: status})
	require.NoError(t, err)
	return &command.ApplyRequest{Type: command.ApplyRequest_TYPE_UPDATE_SHARD_STATUS, Class: class, SubCommand: sub}
}

func TestUpdateShardStatusWritesTheOverlay(t *testing.T) {
	cases := []struct {
		name     string
		preset   bool
		status   string
		storeErr error
		want     bool
		wantErr  bool
	}{
		{name: "READONLY sets it", status: "READONLY", want: true},
		{name: "READY clears it when the store refuses", preset: true, status: "READY", storeErr: errors.New("held"), wantErr: true},
		{name: "any case is accepted", status: "readonly", want: true},
		{name: "READY clears it", preset: true, status: "READY"},
		{name: "SHUTDOWN is ignored", preset: true, status: "SHUTDOWN", want: true},
		{name: "an unknown status fails", preset: true, status: "NOPE", want: true, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := overlayManager(t, []string{"n2"}) // this node is not an owner
			m.overlay.SetReadOnly("C", "t", tc.preset)
			m.db = &shardStatusIndexer{err: tc.storeErr}

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

	assert.ErrorIs(t, m.PreApplyFilter(shardStatusCmd(t, "C", "unknown", "READONLY")), ErrShardNotFound, "refused before the append")
	assert.ErrorIs(t, m.PreApplyFilter(shardStatusCmd(t, "unknown", "t", "READONLY")), ErrClassNotFound)
	assert.NoError(t, m.PreApplyFilter(shardStatusCmd(t, "C", "t", "READONLY")))
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

func TestSnapshotInstallDropsUserReadOnly(t *testing.T) {
	cases := map[string]func(t *testing.T, sm *SchemaManager) error{
		"snapshot": func(t *testing.T, sm *SchemaManager) error {
			return sm.Restore(snapshotWithout(t, sm, ""), sm.parser)
		},
		"legacy snapshot": func(t *testing.T, sm *SchemaManager) error {
			return sm.RestoreLegacy(legacySnapshotWithout(t, sm, ""), sm.parser)
		},
	}
	for name, restore := range cases {
		t.Run(name, func(t *testing.T) {
			parser := fakes.NewMockParser()
			parser.On("ParseClass", mock.Anything).Return(nil)
			sm := NewSchemaManager("node1", &recordingIndexer{}, parser, nil, prometheus.NewPedanticRegistry(), logrus.New())
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
