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
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/types"
	"github.com/weaviate/weaviate/cluster/utils"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// Telemetry off so only the test moves the log index.
func newBarrierTestStore(t *testing.T, tweaks ...func(*Config)) (*Raft, *MockStore) {
	t.Helper()

	m := NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort(), tweaks...)
	m.cfg.TelemetryEnabled = false
	m.store.cfg.TelemetryEnabled = false

	m.indexer.On("Open", mock.Anything).Return(nil)
	m.indexer.On("Close", mock.Anything).Return(nil)
	m.indexer.On("AddClass", mock.Anything).Return(nil)
	m.indexer.On("UpdateClass", mock.Anything).Return(nil)
	m.indexer.On("DeleteClass", mock.Anything).Return(nil)
	m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
	m.parser.On("ParseClass", mock.Anything).Return(nil)
	m.replicationFSM.EXPECT().
		HasActiveReplicationForCollection(mock.Anything).Return(false).Maybe()

	srv := NewRaft(mocks.NewMockNodeSelector(), m.store, nil)
	ctx := context.Background()
	require.Nil(t, srv.Open(ctx, m.indexer))
	t.Cleanup(func() { srv.Close(ctx) })

	addr := fmt.Sprintf("%s:%d", m.cfg.Host, m.cfg.RaftPort)
	require.Nil(t, srv.store.Notify(m.cfg.NodeID, addr))
	require.Nil(t, srv.WaitUntilDBRestored(ctx, time.Second, make(chan struct{})))
	require.True(t, tryNTimesWithWait(40, 200*time.Millisecond, srv.store.IsLeader),
		"node never became leader")
	return srv, &m
}

func addClassCmd(t *testing.T, name string) *command.ApplyRequest {
	t.Helper()
	sub, err := json.Marshal(&command.AddClassRequest{
		Class: &models.Class{Class: name}, State: &sharding.State{},
	})
	require.NoError(t, err)
	return &command.ApplyRequest{
		Type: command.ApplyRequest_TYPE_ADD_CLASS, Class: name, SubCommand: sub,
	}
}

// settledLogIndex waits out the configuration entry bootstrap appends around
// the election, so the delta measured after it belongs to the barrier alone.
func settledLogIndex(t *testing.T, st *Store) uint64 {
	t.Helper()
	last := st.raft.Load().LastIndex()
	stable := 0
	for i := 0; i < 100; i++ {
		time.Sleep(20 * time.Millisecond)
		if cur := st.raft.Load().LastIndex(); cur != last {
			last, stable = cur, 0
			continue
		}
		if stable++; stable == 5 {
			return last
		}
	}
	t.Fatal("raft log index never settled")
	return 0
}

func barrierCount(t *testing.T, st *Store) float64 {
	t.Helper()
	return testutil.ToFloat64(st.metrics.leaderFSMBarriers)
}

// Red if the gate becomes any leadership-only check.
func TestProposeBarrier_WaitsForTheFSMNotJustTheLog(t *testing.T) {
	srv, m := newBarrierTestStore(t)
	st := srv.store

	var applied atomic.Bool
	m.indexer.ExpectedCalls = nil
	m.indexer.On("Open", mock.Anything).Return(nil)
	m.indexer.On("Close", mock.Anything).Return(nil)
	m.indexer.On("TriggerSchemaUpdateCallbacks").Return()
	m.indexer.On("AddClass", mock.Anything).
		Run(func(mock.Arguments) {
			// Hold the FSM so the log runs ahead of the state machine.
			time.Sleep(300 * time.Millisecond)
			applied.Store(true)
		}).Return(nil)

	// Append without waiting, so the entry is in flight.
	cmdBytes, err := proto.Marshal(addClassCmd(t, "InFlight"))
	require.NoError(t, err)
	fut := st.raft.Load().Apply(cmdBytes, st.applyTimeout)

	// A fresh leader has stamped no term yet.
	st.fsmCaughtUpTerm.Store(0)
	require.NoError(t, st.waitLeaderFSMCaughtUp())

	require.True(t, applied.Load(),
		"barrier returned while the FSM was still behind its log")
	require.NoError(t, fut.Error())
}

// A barrier is a log entry. Red at 0 if the gate goes, at 3 if the memo goes.
func TestProposeBarrier_OnePerLeaderTerm(t *testing.T) {
	srv, _ := newBarrierTestStore(t)
	st := srv.store

	before := barrierCount(t, st)
	for i := 0; i < 3; i++ {
		_, err := st.Execute(addClassCmd(t, fmt.Sprintf("Memoised%d", i)))
		require.NoError(t, err)
	}
	require.Equal(t, float64(1), barrierCount(t, st)-before,
		"expected exactly one barrier for the term")
}

// Red if Barrier is swapped for VerifyLeader, which appends nothing.
func TestProposeBarrier_IsACatchUpBarrierNotALeadershipCheck(t *testing.T) {
	srv, _ := newBarrierTestStore(t)
	st := srv.store

	st.fsmCaughtUpTerm.Store(0)
	before := settledLogIndex(t, st)
	require.NoError(t, st.waitLeaderFSMCaughtUp())
	require.Equal(t, uint64(1), st.raft.Load().LastIndex()-before,
		"a catch-up barrier is a log entry")
}

// Without the term != 0 guard the two zeros match and the gate reports caught
// up having never run.
func TestProposeBarrier_TermZeroDoesNotShortCircuit(t *testing.T) {
	m := NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort())
	st := m.store

	require.Equal(t, uint64(0), st.fsmCaughtUpTerm.Load())
	require.False(t, st.fsmCaughtUpForTerm(0),
		"term 0 must never read as caught up")

	st.fsmCaughtUpTerm.Store(7)
	require.True(t, st.fsmCaughtUpForTerm(7))
	require.False(t, st.fsmCaughtUpForTerm(8), "a later term must re-barrier")
}

// Concurrent first-of-term proposes must share one barrier. Each is a
// replicated log entry, and they land on the hottest failover path.
func TestProposeBarrier_ConcurrentCallersShareOneBarrier(t *testing.T) {
	srv, _ := newBarrierTestStore(t)
	st := srv.store

	const callers = 50
	before := barrierCount(t, st)
	st.fsmCaughtUpTerm.Store(0)

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			require.NoError(t, st.waitLeaderFSMCaughtUp())
		}()
	}
	close(start)
	wg.Wait()

	require.Equal(t, float64(1), barrierCount(t, st)-before,
		"%d concurrent first-of-term callers appended one barrier each", callers)
}

// A caller can wait on the tenant or task lock long enough for leadership to turn over, so waitLeaderFSMCaughtUp must run after the lock.
// Red if it moves above a lock, since the caller then trusts the stale memo and issues no barrier.
func TestProposeBarrier_ReconfirmsAfterWaitingOnALock(t *testing.T) {
	const class = "Docs"
	addT1, err := proto.Marshal(&command.AddTenantsRequest{
		ClusterNodes: []string{"Node-1"},
		Tenants:      []*command.Tenant{{Name: "t1", Status: "HOT"}},
	})
	require.NoError(t, err)
	deleteT1, err := proto.Marshal(&command.DeleteTenantsRequest{Tenants: []string{"t1"}})
	require.NoError(t, err)

	for _, tc := range []struct {
		name  string
		locks func(*Store) *evictingKeyRWLocker
		key   string
		req   *command.ApplyRequest
	}{
		{
			name: "tenant add waits on the tenant lock", locks: func(st *Store) *evictingKeyRWLocker { return st.tenantAddLocks }, key: class,
			req: &command.ApplyRequest{Type: command.ApplyRequest_TYPE_ADD_TENANT, Class: class, SubCommand: addT1},
		},
		{
			name: "tenant delete waits on the task lock", locks: func(st *Store) *evictingKeyRWLocker { return st.taskMovementLocks }, key: classLockKey(class),
			req: &command.ApplyRequest{Type: command.ApplyRequest_TYPE_DELETE_TENANT, Class: class, SubCommand: deleteT1},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, m := newBarrierTestStore(t)
			st := srv.store
			m.indexer.On("AddTenants", mock.Anything, mock.Anything).Return(nil).Maybe()
			st.schemaManager.SetTenantLimit(func() int { return 100 }, nil)
			locks := tc.locks(st)

			// A warm memo is what lets a gate above the lock issue nothing.
			require.NoError(t, st.waitLeaderFSMCaughtUp())

			locks.Lock(tc.key)
			done := make(chan struct{})
			go func() {
				defer close(done)
				_, _ = st.Execute(tc.req)
			}()

			// Invalidating the memo stands in for the leadership turnover.
			require.Eventually(t, func() bool { return lockRefs(locks, tc.key) > 1 },
				10*time.Second, time.Millisecond, "the caller never reached the lock")
			st.fsmCaughtUpTerm.Store(0)
			before := barrierCount(t, st)

			locks.Unlock(tc.key)
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("Execute never returned")
			}

			require.Equal(t, float64(1), barrierCount(t, st)-before,
				"a stale confirmation must be re-established after the lock wait")
		})
	}
}

// A task query on a leader that has not applied its inherited log is refused at once and answered after one
// background barrier. Refusing never waits: the query can run inside an FSM apply, which a barrier waits on.
func TestQuery_RefusesTaskQueriesUntilTheLeaderCaughtUp(t *testing.T) {
	for _, tc := range []struct {
		name          string
		query         command.QueryRequest_Type
		follower      bool
		catchUpLocked bool
	}{
		{name: "task list", query: command.QueryRequest_TYPE_DISTRIBUTED_TASK_LIST},
		{name: "task headers", query: command.QueryRequest_TYPE_DISTRIBUTED_TASK_HEADERS},
		{name: "task list while a catch-up holds its lock", query: command.QueryRequest_TYPE_DISTRIBUTED_TASK_LIST, catchUpLocked: true},
		{name: "task headers on a node that cannot confirm", query: command.QueryRequest_TYPE_DISTRIBUTED_TASK_HEADERS, follower: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &command.QueryRequest{Type: tc.query}
			if tc.follower {
				m := NewMockStore(t, "node-1", 0)
				attachFollowerRaft(t, m.store)
				_, err := m.store.Query(req)
				require.ErrorIs(t, err, types.ErrFSMNotCaughtUp)
				return
			}
			srv, _ := newBarrierTestStore(t)
			st := srv.store
			add, err := json.Marshal(&command.AddDistributedTaskRequest{Namespace: "ns", Id: "Pending", UnitIds: []string{"u"}})
			require.NoError(t, err)
			require.NoError(t, st.distributedTasksManager.AddTask(&command.ApplyRequest{SubCommand: add}, 1))
			st.fsmCaughtUpTerm.Store(0)
			before := barrierCount(t, st)
			if tc.catchUpLocked {
				st.fsmCatchUpMu.Lock()
			}

			start := time.Now()
			_, err = srv.Query(context.Background(), req)
			require.ErrorIs(t, err, types.ErrFSMNotCaughtUp)
			require.Less(t, time.Since(start), 100*time.Millisecond)
			if tc.catchUpLocked {
				st.fsmCatchUpMu.Unlock()
			}

			var resp *command.QueryResponse
			require.Eventually(t, func() bool {
				resp, err = srv.Query(context.Background(), req)
				return err == nil
			}, 10*time.Second, 10*time.Millisecond)
			require.Contains(t, string(resp.Payload), `"Pending"`)
			require.Equal(t, float64(1), barrierCount(t, st)-before)
		})
	}
}
