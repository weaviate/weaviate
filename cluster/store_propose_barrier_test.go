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
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/utils"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// Telemetry off so only the test moves the log index.
func newBarrierTestStore(t *testing.T) (*Raft, *MockStore) {
	t.Helper()

	m := NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort())
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
	last := st.raft.LastIndex()
	stable := 0
	for i := 0; i < 100; i++ {
		time.Sleep(20 * time.Millisecond)
		if cur := st.raft.LastIndex(); cur != last {
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
	fut := st.raft.Apply(cmdBytes, st.applyTimeout)

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
	require.Equal(t, uint64(1), st.raft.LastIndex()-before,
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
