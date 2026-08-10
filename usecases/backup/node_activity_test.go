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

package backup

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/clusterprobe"
)

func TestNodeActivityProbe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		claim func(participant *Handler, scheduler *Scheduler)
		want  NodeActivity
	}{
		{
			name: "coordinator backup",
			claim: func(_ *Handler, s *Scheduler) {
				s.backupper.lastOp.renew("coord-backup", "path", "", "")
			},
			want: NodeActivity{Busy: true, Kind: NodeActivityKindBackup, ID: "coord-backup"},
		},
		{
			name: "coordinator restore",
			claim: func(_ *Handler, s *Scheduler) {
				s.restorer.lastOp.renew("coord-restore", "path", "", "")
			},
			want: NodeActivity{Busy: true, Kind: NodeActivityKindRestore, ID: "coord-restore"},
		},
		{
			name: "participant backup",
			claim: func(h *Handler, _ *Scheduler) {
				h.backupper.lastOp.renew("part-backup", "path", "", "")
			},
			want: NodeActivity{Busy: true, Kind: NodeActivityKindBackup, ID: "part-backup"},
		},
		{
			name: "participant restore",
			claim: func(h *Handler, _ *Scheduler) {
				h.restorer.lastOp.renew("part-restore", "path", "", "")
			},
			want: NodeActivity{Busy: true, Kind: NodeActivityKindRestore, ID: "part-restore"},
		},
		{
			name: "coordinator outranks participant",
			claim: func(h *Handler, s *Scheduler) {
				h.restorer.lastOp.renew("part-restore", "path", "", "")
				s.backupper.lastOp.renew("coord-backup", "path", "", "")
			},
			want: NodeActivity{Busy: true, Kind: NodeActivityKindBackup, ID: "coord-backup"},
		},
		{
			name: "status cleared but slot still held",
			claim: func(h *Handler, _ *Scheduler) {
				h.backupper.lastOp.renew("part-backup", "path", "", "")
				h.backupper.lastOp.set(backup.Transferring)
			},
			want: NodeActivity{Busy: true, Kind: NodeActivityKindBackup, ID: "part-backup"},
		},
		{
			name: "released after reset",
			claim: func(h *Handler, s *Scheduler) {
				h.backupper.lastOp.renew("part-backup", "path", "", "")
				s.restorer.lastOp.renew("coord-restore", "path", "", "")
				h.backupper.lastOp.reset()
				s.restorer.lastOp.reset()
			},
			want: NodeActivity{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			participant := createManager(nil, nil, nil, nil)
			scheduler := newFakeScheduler(nil).scheduler()

			probe := NewNodeActivityProbe(participant)
			probe.AttachScheduler(scheduler)
			tt.claim(participant, scheduler)

			assert.Equal(t, tt.want, probe.Activity())
		})
	}
}

// TestNodeActivityResponseRoundTrip pins the wire form against its own reader:
// what [NewNodeActivityResponse] emits is exactly what [NodeActivityResponse.Activity]
// reads back. Emission is otherwise only pinned in clusterapi and the marker check
// only in adapters/clients, so a change to either side alone goes unnoticed here.
func TestNodeActivityResponseRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		activity NodeActivity
	}{
		{name: "idle", activity: NodeActivity{}},
		{
			name:     "backup",
			activity: NodeActivity{Busy: true, Kind: NodeActivityKindBackup, ID: "backup-1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var decoded NodeActivityResponse
			encoded, err := json.Marshal(NewNodeActivityResponse(tt.activity))
			require.NoError(t, err)
			require.NoError(t, json.Unmarshal(encoded, &decoded))

			got, err := decoded.Activity()
			require.NoError(t, err)
			assert.Equal(t, tt.activity, got)
		})
	}
}

// TestNodeActivityResponseRejects covers the answers that must not be read as a
// verdict at all, so a probe that cannot be trusted refuses rather than reporting
// the node free.
func TestNodeActivityResponseRejects(t *testing.T) {
	t.Parallel()

	busy, idle := true, false
	tests := []struct {
		name string
		resp NodeActivityResponse
	}{
		{
			name: "wrong marker",
			resp: NodeActivityResponse{Probe: "something-else", Busy: &idle},
		},
		{
			name: "no busy field",
			resp: NodeActivityResponse{Probe: clusterprobe.BackupNodeActivityMarker},
		},
		{
			name: "busy without a kind",
			resp: NodeActivityResponse{Probe: clusterprobe.BackupNodeActivityMarker, Busy: &busy, ID: "1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := tt.resp.Activity()
			require.Error(t, err)
			assert.Equal(t, NodeActivity{}, got)
		})
	}
}

func TestNodeActivityProbeMissingSlots(t *testing.T) {
	t.Parallel()

	t.Run("nil participant", func(t *testing.T) {
		probe := NewNodeActivityProbe(nil)
		assert.Equal(t, NodeActivity{}, probe.Activity())
	})

	t.Run("nil coordinators", func(t *testing.T) {
		probe := NewNodeActivityProbe(createManager(nil, nil, nil, nil))
		probe.AttachScheduler(&Scheduler{})
		assert.Equal(t, NodeActivity{}, probe.Activity())
	})

	t.Run("nil participant with busy coordinator", func(t *testing.T) {
		scheduler := newFakeScheduler(nil).scheduler()
		scheduler.backupper.lastOp.renew("coord-backup", "path", "", "")

		probe := NewNodeActivityProbe(nil)
		probe.AttachScheduler(scheduler)
		assert.Equal(t, NodeActivity{Busy: true, Kind: NodeActivityKindBackup, ID: "coord-backup"},
			probe.Activity())
	})
}

func TestNodeActivityProbeConcurrent(t *testing.T) {
	t.Parallel()

	participant := createManager(nil, nil, nil, nil)
	scheduler := newFakeScheduler(nil).scheduler()
	probe := NewNodeActivityProbe(participant)

	var wg sync.WaitGroup
	wg.Add(4)

	go func() {
		defer wg.Done()
		probe.AttachScheduler(scheduler)
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			participant.backupper.lastOp.renew("part-backup", "path", "", "")
			participant.backupper.lastOp.reset()
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			scheduler.restorer.lastOp.renew("coord-restore", "path", "", "")
			scheduler.restorer.lastOp.reset()
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			probe.Activity()
		}
	}()

	wg.Wait()
	assert.Equal(t, NodeActivity{}, probe.Activity())
}

// TestNodeActivityProbeSlotExpiresWithPreCommit pins that the slot self-clears
// when the pre-commit window lapses, with no coordinator follow-up needed.
func TestNodeActivityProbeSlotExpiresWithPreCommit(t *testing.T) {
	t.Parallel()

	var (
		ctx      = context.Background()
		backupID = "expiring-1"
		nodeHome = backupID + "/" + nodeName
		path     = "bucket/backups/" + nodeHome
	)

	sourcer := &fakeSourcer{}
	sourcer.On("Backupable", ctx, []string{"Class-A"}).Return(nil)
	sourcer.On("BackupDescriptors", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return((<-chan backup.ClassDescriptor)(nil))
	sourcer.On("ReleaseBackup", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	backend := &fakeBackend{}
	backend.On("HomeDir", mock.Anything, mock.Anything, mock.Anything).Return(path)
	backend.On("GetObject", ctx, nodeHome, BackupFile).Return(nil, errNotFound)
	backend.On("Initialize", ctx, nodeHome).Return(nil)

	participant := createManager(sourcer, nil, backend, nil)
	probe := NewNodeActivityProbe(participant)

	resp := participant.OnCanCommit(ctx, &Request{
		Method:   OpCreate,
		ID:       backupID,
		Classes:  []string{"Class-A"},
		Backend:  "gcs",
		Duration: 20 * time.Millisecond,
	})
	require.Empty(t, resp.Err)
	require.Equal(t, NodeActivity{Busy: true, Kind: NodeActivityKindBackup, ID: backupID}, probe.Activity())

	assert.Eventually(t, func() bool { return !probe.Activity().Busy },
		5*time.Second, 5*time.Millisecond,
		"pre-commit window lapsed but the backup slot is still held")
}
