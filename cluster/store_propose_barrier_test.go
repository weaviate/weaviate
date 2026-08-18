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

package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/utils"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// singleShardState is the minimum AddClass accepts.
func singleShardState(shard string) *sharding.State {
	return &sharding.State{Physical: map[string]sharding.Physical{
		shard: {Name: shard, BelongsToNodes: []string{"Node-1"}},
	}}
}

// setupSingleNodeRaft brings up a single-node service on real raft. notify
// triggers the bootstrap that elects it; without it the node stays a follower. A
// non-empty blockClass holds the FSM inside that class's apply until the returned
// release runs, which is how an entry is left committed but unapplied.
func setupSingleNodeRaft(t *testing.T, notify bool, blockClass string) (*Raft, func()) {
	t.Helper()

	blocked := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(blocked) }) }

	m := NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort())
	// Nothing else may propose inside the window these tests measure log-index
	// deltas over. onLeaderFound's 1s ticker would otherwise commit a cluster-id
	// entry mid-test, and — since Store.Execute drains first — a barrier with it,
	// stamping fsmCaughtUpTerm before the first AddClass is measured. The ticker
	// normally fires after the measurement, so leaving telemetry on is a rare
	// flake rather than a hard failure; off removes the race entirely.
	m.store.cfg.TelemetryEnabled = false
	m.indexer.On("Open", mock.Anything).Return(nil)
	m.indexer.On("Close", mock.Anything).Return(nil)
	m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
	m.indexer.On("AddClass", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		if blockClass == "" {
			return
		}
		req, ok := args.Get(0).(command.AddClassRequest)
		if ok && req.Class != nil && req.Class.Class == blockClass {
			<-blocked
		}
	})
	m.parser.On("ParseClass", mock.Anything).Return(nil)

	srv := NewRaft(mocks.NewMockNodeSelector(), m.store, nil)
	ctx := context.Background()
	require.NoError(t, srv.Open(ctx, m.indexer))
	t.Cleanup(func() {
		release()
		srv.Close(ctx)
	})

	if notify {
		electRaftLeader(t, srv, &m)
	}

	return srv, release
}

// settledLastIndex waits out the configuration entry and the leader no-op that
// raft appends on its own at election, so a delta measured after it belongs to
// the propose under test.
func settledLastIndex(t *testing.T, srv *Raft) uint64 {
	t.Helper()

	var last uint64
	require.True(t, tryNTimesWithWait(50, 100*time.Millisecond, func() bool {
		idx := srv.store.raft.LastIndex()
		settled := idx != 0 && idx == last
		last = idx
		return settled
	}), "raft log did not settle after the election")
	return last
}

// waitForIndexPast blocks until raft has appended past want.
func waitForIndexPast(t *testing.T, srv *Raft, want uint64, msg string) {
	t.Helper()

	require.True(t, tryNTimesWithWait(50, 100*time.Millisecond, func() bool {
		return srv.store.raft.LastIndex() > want
	}), msg)
}

// TestProposeBarrier_OnePerLeaderTerm pins that the first propose of a term
// appends a barrier entry of its own and a later one does not. The delta is what
// makes this a drain: a leadership check would satisfy every other assertion here
// while appending nothing and confirming nothing about the FSM.
func TestProposeBarrier_OnePerLeaderTerm(t *testing.T) {
	srv, _ := setupSingleNodeRaft(t, true, "")
	ctx := context.Background()

	require.Zero(t, srv.store.fsmCaughtUpTerm.Load(), "no propose has been admitted yet")
	before := settledLastIndex(t, srv)

	_, err := srv.AddClass(ctx, &models.Class{Class: "First"}, singleShardState("S1"))
	require.NoError(t, err)

	term := srv.store.raft.CurrentTerm()
	assert.Equal(t, before+2, srv.store.raft.LastIndex(),
		"the first propose appends a barrier and then its own command")
	assert.Equal(t, term, srv.store.fsmCaughtUpTerm.Load(),
		"a confirmed drain is recorded against the term it was confirmed in")

	before = srv.store.raft.LastIndex()
	_, err = srv.AddClass(ctx, &models.Class{Class: "Second"}, singleShardState("S1"))
	require.NoError(t, err)

	assert.Equal(t, before+1, srv.store.raft.LastIndex(),
		"a later propose in the same term reuses the barrier")
	assert.Equal(t, term, srv.store.fsmCaughtUpTerm.Load())
}

// TestProposeBarrier_RefusesADuplicateTheFSMHasNotApplied pins the bug the barrier
// exists for. A leader holding committed entries it has not applied must refuse a
// class the log already contains, rather than admitting a second copy of it.
func TestProposeBarrier_RefusesADuplicateTheFSMHasNotApplied(t *testing.T) {
	srv, release := setupSingleNodeRaft(t, true, "Blocker")
	ctx := context.Background()
	staged := settledLastIndex(t, srv)

	// Blocker stops the FSM. Target then commits behind it, so the committed log
	// holds a class the schema map does not.
	go srv.AddClass(ctx, &models.Class{Class: "Blocker"}, singleShardState("S1"))
	waitForIndexPast(t, srv, staged+1, "Blocker's barrier and command were not appended")

	staged = srv.store.raft.LastIndex()
	go srv.AddClass(ctx, &models.Class{Class: "Target"}, singleShardState("S1"))
	waitForIndexPast(t, srv, staged, "Target was not appended")

	require.False(t, srv.store.SchemaReader().ClassInfo("Target").Exists,
		"Target must be committed but not yet applied")

	// The state a leader is in right after an election: entries inherited, no
	// drain confirmed for the term.
	srv.store.fsmCaughtUpTerm.Store(0)
	go func() {
		time.Sleep(time.Second)
		release()
	}()

	_, err := srv.AddClass(ctx, &models.Class{Class: "Target"}, singleShardState("S1"))
	require.Error(t, err)
	assert.ErrorContains(t, err, "class name Target already exists",
		"PreApplyFilter must refuse the duplicate, rather than the apply rejecting it later")
}

// TestProposeBarrier_FailureIsRetryableAndNotRecorded covers the arm a leader
// losing its election lands on. Raft.Execute classifies by the sentinel, so it has
// to survive wrapping, and a failed barrier must record no drain.
func TestProposeBarrier_FailureIsRetryableAndNotRecorded(t *testing.T) {
	srv, _ := setupSingleNodeRaft(t, false, "")
	require.False(t, srv.store.IsLeader(), "an un-notified node must not be leader")

	err := srv.store.waitLeaderFSMCaughtUp()
	require.Error(t, err)
	assert.ErrorIs(t, err, raft.ErrNotLeader)
	assert.Zero(t, srv.store.fsmCaughtUpTerm.Load(), "a failed barrier confirms no term")

	var permanent *backoff.PermanentError
	assert.False(t, errors.As(classifyLeaderErr(err), &permanent),
		"losing the election is worth retrying")
}

// TestClassifyLeaderErr pins which failures Raft.Execute retries against a leader.
// Retrying anything else repeats a wait that has already timed out.
func TestClassifyLeaderErr(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		retryable bool
	}{
		{name: "nil stays nil", err: nil},
		{name: "not leader", err: raft.ErrNotLeader, retryable: true},
		{name: "leadership lost", err: raft.ErrLeadershipLost, retryable: true},
		{name: "wrapped not leader", err: fmt.Errorf("barrier: %w", raft.ErrNotLeader), retryable: true},
		{name: "enqueue timeout", err: fmt.Errorf("barrier: %w", raft.ErrEnqueueTimeout)},
		{name: "raft shutdown", err: fmt.Errorf("barrier: %w", raft.ErrRaftShutdown)},
		{name: "aborted by restore", err: fmt.Errorf("barrier: %w", raft.ErrAbortedByRestore)},
		{name: "leadership transfer in progress", err: raft.ErrLeadershipTransferInProgress},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyLeaderErr(tt.err)
			if tt.err == nil {
				require.NoError(t, got)
				return
			}

			var permanent *backoff.PermanentError
			assert.Equal(t, !tt.retryable, errors.As(got, &permanent))
			assert.ErrorIs(t, got, tt.err, "the sentinel must survive classification")
		})
	}
}
