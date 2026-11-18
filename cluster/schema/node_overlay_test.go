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
	"sync"
	"testing"
	"time"

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

func userReadOnly(o *NodeOverlay, class, shard string) bool {
	return o.Shard(class, shard)&ConditionUserReadOnly != 0
}

type pause struct{ store, temp bool }

// fakeStore records how the overlay drives it.
type fakeStore struct {
	mu       sync.Mutex
	pauses   []pause
	statuses []storagestate.Status
	err      error
	hold     chan struct{} // when set, a pause waits for it
	held     chan struct{} // when set, a pause reports that it waits
}

func (f *fakeStore) PauseStore(store, temp bool) error {
	if f.held != nil {
		f.held <- struct{}{}
	}
	if f.hold != nil {
		<-f.hold
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.pauses = append(f.pauses, pause{store, temp})
	return nil
}

func (f *fakeStore) StatusChanged(status storagestate.Status) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statuses = append(f.statuses, status)
}

func (f *fakeStore) drives() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.pauses)
}

func (f *fakeStore) last() (pause, storagestate.Status) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.pauses) == 0 {
		return pause{}, ""
	}
	return f.pauses[len(f.pauses)-1], f.statuses[len(f.statuses)-1]
}

func readyShard(t *testing.T, o *NodeOverlay, shard string) (*ShardHandle, *fakeStore) {
	t.Helper()
	store := &fakeStore{}
	h := o.Attach("C", shard, store)
	require.NoError(t, h.ReportPhase(PhaseReady))
	return h, store
}

func TestNodeOverlayShardConditions(t *testing.T) {
	o := &NodeOverlay{}
	_, store := readyShard(t, o, "s1")

	o.Set("C", "s1", ConditionUserReadOnly, false)
	o.Set("C", "s1", ConditionUserReadOnly, true)
	o.Set("C", "s1", ConditionUserReadOnly, true)
	o.Set("C", "s1", ConditionReindexing, true)
	assert.Equal(t, ConditionUserReadOnly|ConditionReindexing, o.Shard("C", "s1"))
	assert.Zero(t, o.Shard("C", "s2"))
	assert.Zero(t, o.Shard("D", "s1"))

	o.Set("C", "s1", ConditionUserReadOnly, false)
	o.Set("C", "s1", ConditionUserReadOnly, false)
	assert.Equal(t, ConditionReindexing, o.Shard("C", "s1"), "clearing one keeps another")
	assert.Eventually(t, func() bool { return store.drives() == 4 }, time.Second, time.Millisecond,
		"the ready report and each change drive the store, a repeat does not")
	assert.Never(t, func() bool { return store.drives() > 4 }, 50*time.Millisecond, time.Millisecond)

	o.SetNode(ConditionResourcePressure, true)
	assert.Equal(t, ConditionResourcePressure, o.Node())
	assert.Equal(t, ConditionReindexing, o.Shard("C", "s1"), "node conditions are not a shard's")
	o.SetNode(ConditionResourcePressure, false)
	assert.Zero(t, o.Node())
}

func TestNilNodeOverlayHasNoConditions(t *testing.T) {
	var o *NodeOverlay
	assert.Zero(t, o.Shard("C", "s1"))
	assert.Zero(t, o.Node())
	assert.NoError(t, o.RefuseWrite())
	assert.NoError(t, o.RefuseClientWrite("C", "s1"))
	assert.Equal(t, storagestate.StatusLazyLoading, o.Status("C", "s1"))

	h, store := readyShard(t, o, "s1")
	assert.NoError(t, h.RefuseWrite())
	assert.Equal(t, storagestate.StatusReady, h.Status())
	p, status := store.last()
	assert.Equal(t, pause{}, p, "nothing pauses")
	assert.Equal(t, storagestate.StatusReady, status)
}

func TestNodeOverlayClears(t *testing.T) {
	o := &NodeOverlay{}
	for _, key := range [][2]string{{"C", "s1"}, {"C", "s2"}, {"C", "s3"}, {"D", "s1"}} {
		o.Set(key[0], key[1], ConditionUserReadOnly, true)
	}

	o.clearShards("C", []string{"s1", "unknown"})
	assert.False(t, userReadOnly(o, "C", "s1"))
	assert.True(t, userReadOnly(o, "C", "s2"))

	o.clearClass("C")
	assert.False(t, userReadOnly(o, "C", "s2"))
	assert.False(t, userReadOnly(o, "C", "s3"))
	assert.True(t, userReadOnly(o, "D", "s1"))
}

func TestNodeOverlayDecides(t *testing.T) {
	readOnly := func(reason string) string { return storagestate.ErrStatusReadOnlyWithReason(reason).Error() }
	tests := []struct {
		name       string
		unloaded   bool
		phase      Phase
		user       bool
		pressure   bool
		conditions Condition
		wantWrite  string // "" takes the write
		wantClient string
		wantPause  pause
		wantStatus storagestate.Status
	}{
		{name: "writable", phase: PhaseReady, wantStatus: storagestate.StatusReady},
		{
			name: "user read-only refuses only client writes and pauses nothing", phase: PhaseReady, user: true,
			wantClient: readOnly(storagestate.ReasonUserReadOnly), wantStatus: storagestate.StatusReadOnly,
		},
		{
			name: "pressure pauses everything", phase: PhaseReady, pressure: true,
			wantWrite: readOnly(storagestate.ReasonResourcePressure), wantPause: pause{true, true},
			wantStatus: storagestate.StatusReadOnly,
		},
		{
			name: "a reindex keeps its temp buckets writable", phase: PhaseReady, conditions: ConditionReindexing,
			wantWrite: storagestate.ErrStatusReadOnly.Error(), wantPause: pause{store: true},
			wantStatus: storagestate.StatusReadOnly,
		},
		{
			name: "a reindex swapping pauses its temp buckets", phase: PhaseReady, conditions: ConditionReindexing | ConditionReindexSwapping,
			wantWrite: storagestate.ErrStatusReadOnly.Error(), wantPause: pause{true, true},
			wantStatus: storagestate.StatusReadOnly,
		},
		{
			name: "a held migration wins over pressure", phase: PhaseReady, pressure: true, conditions: ConditionReindexing | ConditionMigrationHeld,
			wantWrite: readOnly(storagestate.ReasonMirrorUnarmed), wantPause: pause{true, true},
			wantStatus: storagestate.StatusReadOnly,
		},
		{
			name: "a loading shard reports its phase and is not driven", phase: PhaseLoading, user: true, pressure: true,
			wantWrite: readOnly(storagestate.ReasonResourcePressure), wantClient: readOnly(storagestate.ReasonUserReadOnly),
			wantStatus: storagestate.StatusLoading,
		},
		{name: "a stopping shard reports its phase", phase: PhaseShutdown, wantStatus: storagestate.StatusShutdown},
		{name: "an unloaded shard", unloaded: true, wantStatus: storagestate.StatusLazyLoading},
		{
			name: "an unloaded shard under user read-only", unloaded: true, user: true,
			wantClient: readOnly(storagestate.ReasonUserReadOnly), wantStatus: storagestate.StatusReadOnly,
		},
		{
			name: "an unloaded shard under pressure", unloaded: true, pressure: true,
			wantWrite: readOnly(storagestate.ReasonResourcePressure), wantStatus: storagestate.StatusReadOnly,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := &NodeOverlay{}
			o.Set("C", "s1", ConditionUserReadOnly, tt.user)
			o.SetNode(ConditionResourcePressure, tt.pressure)

			if err := o.RefuseClientWrite("C", "s1"); tt.wantClient == "" {
				assert.NoError(t, err)
			} else {
				assert.EqualError(t, err, tt.wantClient)
			}
			if tt.unloaded {
				if err := o.RefuseWrite(); tt.wantWrite == "" {
					assert.NoError(t, err)
				} else {
					assert.EqualError(t, err, tt.wantWrite)
				}
				assert.Equal(t, tt.wantStatus, o.Status("C", "s1"))
				return
			}

			store := &fakeStore{}
			h := o.Attach("C", "s1", store)
			require.NoError(t, h.Set(tt.conditions, true))
			require.NoError(t, h.ReportPhase(tt.phase))

			if err := h.RefuseWrite(); tt.wantWrite == "" {
				assert.NoError(t, err)
			} else {
				assert.EqualError(t, err, tt.wantWrite)
			}
			assert.Equal(t, tt.wantStatus, h.Status())
			p, status := store.last()
			if tt.phase != PhaseReady {
				assert.Zero(t, store.drives(), "only a ready store is driven")
				return
			}
			assert.Equal(t, tt.wantPause, p)
			assert.Equal(t, tt.wantStatus, status, "the store follows the status")
		})
	}
}

func TestShardHandleRefusesConfigUpdate(t *testing.T) {
	readOnly := func(reason string) string { return storagestate.ErrStatusReadOnlyWithReason(reason).Error() }
	tests := []struct {
		name       string
		user       bool
		pressure   bool
		conditions Condition
		want       string // "" takes the update
	}{
		{name: "writable"},
		{name: "user read-only refuses client writes only", user: true},
		{name: "an inverted reindex holds no vector index", conditions: ConditionReindexing | ConditionReindexSwapping},
		{name: "pressure refuses", pressure: true, want: readOnly(storagestate.ReasonResourcePressure)},
		{name: "a held migration refuses", conditions: ConditionMigrationHeld, want: readOnly(storagestate.ReasonMirrorUnarmed)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := &NodeOverlay{}
			o.Set("C", "s1", ConditionUserReadOnly, tt.user)
			o.SetNode(ConditionResourcePressure, tt.pressure)
			h, _ := readyShard(t, o, "s1")
			require.NoError(t, h.Set(tt.conditions, true))

			if err := h.RefuseConfigUpdate(); tt.want == "" {
				assert.NoError(t, err)
			} else {
				assert.EqualError(t, err, tt.want)
			}
		})
	}
}

func TestNodeOverlayDrivesReadyShards(t *testing.T) {
	t.Run("a node condition drives every ready shard, and again when unchanged", func(t *testing.T) {
		o := &NodeOverlay{}
		_, a := readyShard(t, o, "a")
		_, b := readyShard(t, o, "b")
		loading := &fakeStore{}
		o.Attach("C", "loading", loading)

		o.SetNode(ConditionResourcePressure, true)
		o.SetNode(ConditionResourcePressure, true)

		for _, store := range []*fakeStore{a, b} {
			p, status := store.last()
			assert.Equal(t, pause{true, true}, p)
			assert.Equal(t, storagestate.StatusReadOnly, status)
			assert.Equal(t, 3, store.drives())
		}
		assert.Zero(t, loading.drives(), "a loading shard is driven by its ready report")
	})

	t.Run("a shard ready under a condition starts paused", func(t *testing.T) {
		o := &NodeOverlay{}
		o.SetNode(ConditionResourcePressure, true)
		_, store := readyShard(t, o, "s1")
		p, _ := store.last()
		assert.Equal(t, pause{true, true}, p)
	})

	t.Run("a shut down shard is never driven again", func(t *testing.T) {
		o := &NodeOverlay{}
		h, store := readyShard(t, o, "s1")
		require.NoError(t, h.ReportPhase(PhaseShutdown))

		o.SetNode(ConditionResourcePressure, true)
		require.NoError(t, h.Set(ConditionReindexing, true))
		require.NoError(t, h.Sync())

		assert.Equal(t, 1, store.drives())
	})

	t.Run("a newer object of the shard takes over", func(t *testing.T) {
		o := &NodeOverlay{}
		old, oldStore := readyShard(t, o, "s1")
		_, newStore := readyShard(t, o, "s1")

		o.SetNode(ConditionResourcePressure, true)
		assert.Equal(t, 1, oldStore.drives(), "the overlay drives only the newer object")
		assert.Equal(t, 2, newStore.drives())

		require.NoError(t, old.ReportPhase(PhaseShutdown))
		o.SetNode(ConditionResourcePressure, false)
		assert.Equal(t, 3, newStore.drives(), "an older object shutting down leaves the newer attached")
	})

	t.Run("a failed drive stops neither the others nor the caller", func(t *testing.T) {
		o := &NodeOverlay{}
		failing, failingStore := readyShard(t, o, "failing")
		_, healthy := readyShard(t, o, "healthy")
		failingStore.err = errors.New("store closed")

		o.SetNode(ConditionResourcePressure, true)

		p, _ := healthy.last()
		assert.Equal(t, pause{true, true}, p)
		assert.EqualError(t, failing.Set(ConditionReindexing, true), "store closed", "the shard's own writer sees it")
	})
}

func TestNodeOverlayShardConditionDrivesOffTheCaller(t *testing.T) {
	o := &NodeOverlay{}
	_, store := readyShard(t, o, "s1")
	store.hold, store.held = make(chan struct{}), make(chan struct{}, 1)

	o.Set("C", "s1", ConditionUserReadOnly, true) // the RAFT apply must not wait on the store
	<-store.held
	close(store.hold)

	assert.Eventually(t, func() bool {
		_, status := store.last()
		return status == storagestate.StatusReadOnly
	}, time.Second, time.Millisecond)
}

func TestShardHandleShutdownWaitsForADrive(t *testing.T) {
	o := &NodeOverlay{}
	h, store := readyShard(t, o, "s1")
	store.hold, store.held = make(chan struct{}), make(chan struct{}, 1)

	driven := make(chan error, 1)
	go func() { driven <- h.Sync() }()
	<-store.held
	shut := make(chan error, 1)
	go func() { shut <- h.ReportPhase(PhaseShutdown) }()

	select {
	case <-shut:
		t.Fatal("the shutdown report did not wait for the drive in progress")
	case <-time.After(50 * time.Millisecond):
	}
	close(store.hold)
	require.NoError(t, <-driven)
	require.NoError(t, <-shut)
	store.hold, store.held = nil, nil
	require.NoError(t, h.Sync())
	assert.Equal(t, 2, store.drives(), "no drive after the shutdown")
}

// A shard ready while a node condition flips ends in the last condition: the overlay attaches it before
// its first drive reads the condition.
func TestShardReadyRacesANodeCondition(t *testing.T) {
	for i := range 200 {
		o := &NodeOverlay{}
		on := i%2 == 0
		o.SetNode(ConditionResourcePressure, !on)
		store := &fakeStore{}
		h := o.Attach("C", "s1", store)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); _ = h.ReportPhase(PhaseReady) }()
		go func() { defer wg.Done(); o.SetNode(ConditionResourcePressure, on) }()
		wg.Wait()

		p, _ := store.last()
		require.Equal(t, on, p.store, "iteration %d", i)
	}
}

func TestNodeOverlayWritePathAllocatesNothing(t *testing.T) {
	o := &NodeOverlay{}
	o.Set("C", "s1", ConditionUserReadOnly, true)
	h := o.Attach("C", "s2", &fakeStore{})

	assert.Zero(t, testing.AllocsPerRun(1000, func() {
		_ = o.RefuseClientWrite("C", "s2")
		_ = o.RefuseWrite()
		_ = h.RefuseWrite()
		_ = h.Phase()
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
			m.overlay.Set("C", "t", ConditionUserReadOnly, tc.preset)
			m.db = &shardStatusIndexer{err: tc.storeErr}

			err := m.UpdateShardStatus(shardStatusCmd(t, "C", "t", tc.status), false)

			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tc.want, userReadOnly(m.overlay, "C", "t"))
		})
	}
}

func TestUpdateShardStatusIsLostOnReplay(t *testing.T) {
	m := overlayManager(t, []string{"n1"})

	require.NoError(t, m.UpdateShardStatus(shardStatusCmd(t, "C", "t", "READONLY"), true))

	assert.False(t, userReadOnly(m.overlay, "C", "t"))
}

func TestUpdateShardStatusNeedsAShard(t *testing.T) {
	m := overlayManager(t, []string{"n1"})

	assert.ErrorIs(t, m.PreApplyFilter(shardStatusCmd(t, "C", "unknown", "READONLY")), ErrShardNotFound, "refused before the append")
	assert.ErrorIs(t, m.PreApplyFilter(shardStatusCmd(t, "unknown", "t", "READONLY")), ErrClassNotFound)
	assert.NoError(t, m.PreApplyFilter(shardStatusCmd(t, "C", "t", "READONLY")))
	assert.ErrorIs(t, m.UpdateShardStatus(shardStatusCmd(t, "C", "unknown", "READONLY"), false), ErrShardNotFound)
	assert.ErrorIs(t, m.UpdateShardStatus(shardStatusCmd(t, "unknown", "t", "READONLY"), false), ErrClassNotFound)
	assert.False(t, userReadOnly(m.overlay, "C", "unknown"))
}

func TestDeleteClearsReadOnly(t *testing.T) {
	t.Run("tenant delete", func(t *testing.T) {
		m := overlayManager(t, []string{"n1"})
		m.overlay.Set("C", "t", ConditionUserReadOnly, true)
		sub, err := proto.Marshal(&command.DeleteTenantsRequest{Tenants: []string{"t"}})
		require.NoError(t, err)

		require.NoError(t, m.DeleteTenants(&command.ApplyRequest{Class: "C", SubCommand: sub}, true))

		assert.False(t, userReadOnly(m.overlay, "C", "t"), "a re-created tenant must not inherit it")
	})

	t.Run("class delete", func(t *testing.T) {
		m := overlayManager(t, []string{"n1"})
		m.overlay.Set("C", "t", ConditionUserReadOnly, true)

		require.NoError(t, m.DeleteClass(&command.ApplyRequest{Class: "C"}, true, false))

		assert.False(t, userReadOnly(m.overlay, "C", "t"))
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
			_, store := readyShard(t, sm.overlay, "shard1")
			sm.overlay.Set("C", "shard1", ConditionUserReadOnly, true)
			require.Eventually(t, func() bool {
				_, status := store.last()
				return status == storagestate.StatusReadOnly
			}, time.Second, time.Millisecond)

			require.NoError(t, restore(t, sm))

			assert.False(t, userReadOnly(sm.overlay, "C", "shard1"), "a restart drops the flag too")
			assert.Eventually(t, func() bool {
				_, status := store.last()
				return status == storagestate.StatusReady
			}, time.Second, time.Millisecond, "the shard is driven again")
		})
	}
}
