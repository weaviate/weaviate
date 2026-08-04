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

package shard

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	shardproto "github.com/weaviate/weaviate/cluster/shard/proto"
	"github.com/weaviate/weaviate/cluster/shard/sharedlog"
	"go.etcd.io/raft/v3/raftpb"
)

// TestStore_CoreGoroutinePanic_FailsGroupLoudly pins the W-B containment
// contract of minor-issues.md #9: a panic on any of the store's guarded core
// goroutines must fail the group VISIBLY — counted, logged, pending
// operations drained with a terminal error, store stopped — never the
// pre-fix behavior (GoWrapper recovery logs and the goroutine dies silently,
// leaving a zombie group that drops inbound messages and hangs its waiters
// until process restart; observed live 2026-07-30).
func TestStore_CoreGoroutinePanic_FailsGroupLoudly(t *testing.T) {
	s := BuildTestStore(t, "PanicClass", "shard-a", "node-1", []string{"node-1"}, nil)
	require.NoError(t, s.Start(context.Background()))

	before := testutil.ToFloat64(shardRaftStorePanics.WithLabelValues("PanicClass", "shard-a", "test_probe"))

	// A pending Apply waiting at panic time must be failed, not stranded.
	pending := &pendingApply{done: make(chan applyResult, 1), proposedAt: time.Now()}
	s.pending.Store(uint64(42), pending)

	// Panic on a goroutine launched through the production guard.
	s.goGuarded("test_probe", func() { panic("injected core-goroutine panic") })

	// The waiter is drained with the terminal group-failure error.
	select {
	case res := <-pending.done:
		require.ErrorIs(t, res.err, ErrGroupFailed)
	case <-time.After(5 * time.Second):
		t.Fatal("pending Apply was stranded — the pre-fix zombie behavior")
	}

	// The panic is counted, and the store is torn down: new operations fail
	// fast instead of hanging against a dead group.
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(shardRaftStorePanics.WithLabelValues("PanicClass", "shard-a", "test_probe"))-before == 1
	}, 5*time.Second, 10*time.Millisecond, "panic must be counted")
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		_, err := s.Apply(ctx, &shardproto.ApplyRequest{})
		// ErrAlreadyClosed while stopping, ErrNotStarted once Stop finished
		// (it resets started) — either is the required fail-fast, never a
		// hang against a zombie.
		return errors.Is(err, ErrAlreadyClosed) || errors.Is(err, ErrNotStarted)
	}, 5*time.Second, 10*time.Millisecond, "store must stop after a core-goroutine panic")

	// Idempotent: a second panic only logs; the counter site is per-goroutine
	// and failGroup's teardown ran once.
	s.goGuarded("test_probe_second", func() { panic("cascade during teardown") })
	require.Never(t, func() bool {
		return testutil.ToFloat64(shardRaftStorePanics.WithLabelValues("PanicClass", "shard-a", "test_probe_second")) > 0
	}, 300*time.Millisecond, 50*time.Millisecond, "failGroup must be once-only")
}

// TestStore_Start_RefusesPoisonedGroup pins the W-C poisoning gate: a group
// whose WAL state failed boot validation (here: an entry range starting above
// 1 with no covering snapshot — the split-brain that panicked etcd pre-W-A)
// must refuse to start with the named error, per group, without affecting the
// WAL open itself.
func TestStore_Start_RefusesPoisonedGroup(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), sharedRaftLogName)
	snapRoot := t.TempDir()
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	// Damaged-state fixture: the on-disk shape Constructor C of
	// minor-issues.md #9 produces (entries whose authorizing snapshot record
	// was lost while its compaction effects survived).
	gid := hashGroupID("PoisonClass", "shard-p")
	sl, err := sharedlog.Open(sharedlog.Options{Path: logPath, Logger: logger})
	require.NoError(t, err)
	require.NoError(t, sl.Append(context.Background(), sharedlog.GroupWrite{
		GroupID: gid,
		Entries: []raftpb.Entry{
			{Index: 5, Term: 1, Data: []byte("orphan")},
			{Index: 6, Term: 1, Data: []byte("orphan")},
		},
		HardState: &raftpb.HardState{Term: 1, Commit: 6},
	}))
	require.NoError(t, sl.Close())

	store, closeInfra := BuildTestStoreAt(t, "PoisonClass", "shard-p", "node-1", logPath, snapRoot, 0, nil)
	defer closeInfra()

	err = store.Start(context.Background())
	require.ErrorIs(t, err, ErrGroupPoisoned)
	require.ErrorContains(t, err, "no snapshot")
}
