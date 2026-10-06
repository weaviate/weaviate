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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/usecases/config"
)

const slotTestClass = "Class-A"

var slotTestNodes = []string{"N1", "N2"}

func panicNow(mock.Arguments) { panic("injected panic") }

// newSlotTestCoordinator wires a two-node coordinator whose mocks let any create or restore succeed; setup registers overrides first so they match first.
func newSlotTestCoordinator(t *testing.T, planner DedupePlanner, setup func(c *coordinator, fc *fakeCoordinator)) (*coordinator, *fakeCoordinator) {
	t.Helper()
	any := mock.Anything
	fc := newFakeCoordinator(newFakeNodeResolver(slotTestNodes))
	c := fc.coordinator()
	c.dedupePlanner = planner
	if setup != nil {
		setup(c, fc)
	}
	fc.selector.On("Shards", any, slotTestClass).Return(slotTestNodes, nil)
	for _, n := range slotTestNodes {
		fc.client.On("CanCommit", any, n, any).Return(&CanCommitResponse{Timeout: maxBooking(false), DedupeHonored: true}, nil)
		fc.client.On("Commit", any, n, any).Return(nil)
		fc.client.On("Abort", any, n, any).Return(nil)
	}
	fc.client.On("Status", any, any, mock.MatchedBy(func(r *StatusRequest) bool { return r.Method == OpCreate })).
		Return(&StatusResponse{Status: backup.Success, Method: OpCreate}, nil)
	fc.client.On("Status", any, any, mock.MatchedBy(func(r *StatusRequest) bool { return r.Method == OpRestore })).
		Return(&StatusResponse{Status: backup.Success, Method: OpRestore}, nil)
	fc.backend.On("HomeDir", any, any, any).Return("home")
	fc.backend.On("PutObject", any, any, any, any).Return(nil)
	fc.backend.On("GetObject", any, any, GlobalRestoreFile).Return(nil, backup.ErrNotFound{})
	fc.backend.On("GetObject", any, any, any).Return(marshalMeta(backup.BackupDescriptor{Status: backup.Success}), nil)
	provider := NewMockBackupBackendProvider(t)
	provider.EXPECT().BackupBackend(any, any).Return(fc.backend, nil).Maybe()
	c.backends = provider
	return c, fc
}

func slotTestDescriptor(id string) *backup.DistributedBackupDescriptor {
	now := time.Now().UTC()
	nodes := make(map[string]*backup.NodeDescriptor, len(slotTestNodes))
	for _, n := range slotTestNodes {
		nodes[n] = &backup.NodeDescriptor{Classes: []string{slotTestClass}, Status: backup.Success}
	}
	return &backup.DistributedBackupDescriptor{
		StartedAt: now, CompletedAt: now, ID: id, Status: backup.Success,
		Version: Version, ServerVersion: config.ServerVersion, Nodes: nodes,
	}
}

// callRecovering runs op and reports a panic instead of propagating it.
func callRecovering(op func() error) (err error, panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			panicked = true
		}
	}()
	return op(), false
}

func withDedupe(r *Request) { r.DedupeReplicas = true }

func runSlotTestOp(c *coordinator, fc *fakeCoordinator, op Op, id string, edit func(*Request)) error {
	store := coordStore{objectStore{fc.backend, id, "", "", ""}}
	if op == OpRestore {
		req := newReq(nil, "s3", "")
		if edit != nil {
			edit(&req)
		}
		return c.Restore(context.Background(), store, &req, slotTestDescriptor(id), nil, rolesAndUsersBlobs{})
	}
	req := newReq([]string{slotTestClass}, "s3", id)
	if edit != nil {
		edit(&req)
	}
	return c.Backup(context.Background(), store, &req)
}

func TestCoordinatorReleasesSlotOnSynchronousPanic(t *testing.T) {
	any := mock.Anything
	cases := []struct {
		name    string
		op      Op
		edit    func(*Request)
		planner *fakeDedupePlanner
		setup   func(c *coordinator, fc *fakeCoordinator)
	}{
		{
			name: "create: planner panics", op: OpCreate, edit: withDedupe,
			planner: &fakeDedupePlanner{panicWith: "injected panic"},
		},
		{
			name: "create: abort after a refused canCommit panics", op: OpCreate,
			setup: func(_ *coordinator, fc *fakeCoordinator) {
				fc.client.On("CanCommit", any, "N2", any).Return(nil, ErrAny).Once()
				fc.client.On("Abort", any, "N1", any).Run(panicNow).Return(nil).Once()
			},
		},
		{
			name: "create: descriptor write panics", op: OpCreate,
			setup: func(_ *coordinator, fc *fakeCoordinator) {
				fc.backend.On("PutObject", any, any, GlobalBackupFile, any).Run(panicNow).Return(nil).Once()
			},
		},
		{
			name: "create: cancelled descriptor write panics", op: OpCreate, edit: withDedupe,
			planner: &fakeDedupePlanner{plan: &DedupePlan{}},
			setup: func(c *coordinator, fc *fakeCoordinator) {
				c.dedupePlanner.(*fakeDedupePlanner).onPlan = func() { c.lastOp.cancelIfInFlight("first") }
				fc.backend.On("PutObject", any, any, GlobalBackupFile, any).Run(panicNow).Return(nil).Once()
			},
		},
		{
			name: "restore: abort after a refused canCommit panics", op: OpRestore,
			setup: func(_ *coordinator, fc *fakeCoordinator) {
				fc.client.On("CanCommit", any, "N2", any).Return(nil, ErrAny).Once()
				fc.client.On("Abort", any, "N1", any).Run(panicNow).Return(nil).Once()
			},
		},
		{
			name: "restore: initial descriptor write panics", op: OpRestore,
			setup: func(_ *coordinator, fc *fakeCoordinator) {
				fc.backend.On("PutObject", any, any, GlobalRestoreFile, any).Run(panicNow).Return(nil).Once()
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var planner DedupePlanner
			if tc.planner != nil {
				planner = tc.planner
			}
			c, fc := newSlotTestCoordinator(t, planner, tc.setup)

			err, panicked := callRecovering(func() error { return runSlotTestOp(c, fc, tc.op, "first", tc.edit) })
			assert.False(t, panicked, "the panic must become an error")
			assert.ErrorContains(t, err, "panic")
			assert.Empty(t, c.lastOp.get().ID, "a panic must release the operation slot")
			reason, _ := c.lastOp.rememberedFailure("first")
			assert.Contains(t, reason, "panic")

			require.NoError(t, runSlotTestOp(c, fc, tc.op, "second", nil))
			require.Eventually(t, func() bool { return c.lastOp.get().ID == "" }, 5*time.Second, 10*time.Millisecond)
			assert.Equal(t, backup.Success, fc.backend.globalMetaStatus())
		})
	}
}

func TestCoordinatorReleasesSlotOnSynchronousError(t *testing.T) {
	any := mock.Anything
	cases := []struct {
		name    string
		op      Op
		edit    func(*Request)
		setup   func(c *coordinator, fc *fakeCoordinator)
		wantErr string
	}{
		{
			name: "create: invalid compression level", op: OpCreate,
			edit:    func(r *Request) { r.Level = CompressionLevel(-42) },
			wantErr: "invalid compression level",
		},
		{
			name: "create: canCommit refused", op: OpCreate,
			setup: func(_ *coordinator, fc *fakeCoordinator) {
				fc.client.On("CanCommit", any, "N2", any).Return(nil, ErrAny).Once()
			},
			wantErr: ErrAny.Error(),
		},
		{
			name: "create: initial descriptor write fails", op: OpCreate,
			setup: func(_ *coordinator, fc *fakeCoordinator) {
				fc.backend.On("PutObject", any, any, GlobalBackupFile, any).Return(ErrAny).Once()
			},
			wantErr: "cannot init meta file",
		},
		{
			name: "restore: canCommit refused", op: OpRestore,
			setup: func(_ *coordinator, fc *fakeCoordinator) {
				fc.client.On("CanCommit", any, "N2", any).Return(nil, ErrAny).Once()
			},
			wantErr: ErrAny.Error(),
		},
		{
			name: "restore: initial descriptor write fails", op: OpRestore,
			setup: func(_ *coordinator, fc *fakeCoordinator) {
				fc.backend.On("PutObject", any, any, GlobalRestoreFile, any).Return(ErrAny).Once()
			},
			wantErr: "put initial metadata",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, fc := newSlotTestCoordinator(t, nil, tc.setup)

			require.ErrorContains(t, runSlotTestOp(c, fc, tc.op, "first", tc.edit), tc.wantErr)
			assert.Empty(t, c.lastOp.get().ID, "an error must release the operation slot")

			require.NoError(t, runSlotTestOp(c, fc, tc.op, "second", nil))
			require.Eventually(t, func() bool { return c.lastOp.get().ID == "" }, 5*time.Second, 10*time.Millisecond)
			assert.Equal(t, backup.Success, fc.backend.globalMetaStatus())
		})
	}
}

func TestCoordinatorSlotOwnership(t *testing.T) {
	any := mock.Anything
	for _, op := range []Op{OpCreate, OpRestore} {
		t.Run(string(op)+": already in progress leaves the owner's slot", func(t *testing.T) {
			c, fc := newSlotTestCoordinator(t, nil, nil)
			require.Empty(t, c.lastOp.renew("owner", "", "p", "", ""))

			err := runSlotTestOp(c, fc, op, "intruder", nil)

			require.ErrorContains(t, err, "already in progress")
			assert.Equal(t, "owner", c.lastOp.get().ID)
		})
	}

	t.Run("create: a panic before the slot is taken propagates and leaves the owner's slot", func(t *testing.T) {
		c, fc := newSlotTestCoordinator(t, nil, func(_ *coordinator, fc *fakeCoordinator) {
			fc.selector.On("Shards", any, slotTestClass).Run(panicNow).Return(nil, nil).Once()
		})
		require.Empty(t, c.lastOp.renew("owner", "", "p", "", ""))

		_, panicked := callRecovering(func() error { return runSlotTestOp(c, fc, OpCreate, "intruder", nil) })
		assert.True(t, panicked)
		assert.Equal(t, "owner", c.lastOp.get().ID)
	})

	t.Run("restore: a panic before the slot is taken propagates and leaves the owner's slot", func(t *testing.T) {
		c, fc := newSlotTestCoordinator(t, nil, func(_ *coordinator, fc *fakeCoordinator) {
			fc.backend.On("GetObject", any, any, GlobalRestoreFile).Run(panicNow).Return(nil, nil).Once()
		})
		require.Empty(t, c.lastOp.renew("owner", "", "p", "", ""))

		_, panicked := callRecovering(func() error { return runSlotTestOp(c, fc, OpRestore, "intruder", nil) })
		assert.True(t, panicked)
		assert.Equal(t, "owner", c.lastOp.get().ID)
	})

	t.Run("restore: a cancelling restore descriptor leaves the owner's slot", func(t *testing.T) {
		cancelling := slotTestDescriptor("intruder")
		cancelling.Status = backup.Cancelling
		c, fc := newSlotTestCoordinator(t, nil, func(_ *coordinator, fc *fakeCoordinator) {
			fc.backend.On("GetObject", any, any, GlobalRestoreFile).Return(marshalCoordinatorMeta(*cancelling), nil).Once()
		})
		require.Empty(t, c.lastOp.renew("owner", "", "p", "", ""))

		require.NoError(t, runSlotTestOp(c, fc, OpRestore, "intruder", nil))
		assert.Equal(t, "owner", c.lastOp.get().ID)
		fc.client.AssertNotCalled(t, "CanCommit", any, any, any)
	})

	t.Run("create: a panic in the commit goroutine is left to the goroutine's own release", func(t *testing.T) {
		release := make(chan struct{})
		c, fc := newSlotTestCoordinator(t, nil, func(_ *coordinator, fc *fakeCoordinator) {
			fc.client.On("Commit", any, "N1", any).Run(func(mock.Arguments) {
				<-release
				panic("injected panic")
			}).Return(nil).Once()
		})

		require.NoError(t, runSlotTestOp(c, fc, OpCreate, "handed-off", nil))
		assert.Equal(t, "handed-off", c.lastOp.get().ID, "the commit goroutine owns the slot once launched")
		close(release)
		require.Eventually(t, func() bool { return c.lastOp.get().ID == "" }, 5*time.Second, 10*time.Millisecond)
	})
}
