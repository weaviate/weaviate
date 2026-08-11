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
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/distributedtask"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/utils"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
)

// terminalObserverProbe records tasks handed to a TerminalObserver.
// Guarded by a mutex: the dispatch contract allows concurrent calls.
type terminalObserverProbe struct {
	mu    sync.Mutex
	tasks []string // "id:status" per delivery
}

func (p *terminalObserverProbe) record(id, status string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tasks = append(p.tasks, fmt.Sprintf("%s:%s", id, status))
}

func (p *terminalObserverProbe) snapshot() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.tasks...)
}

func (p *terminalObserverProbe) sawExactly(want string) bool {
	got := p.snapshot()
	return len(got) == 1 && got[0] == want
}

// taskVersion looks up a task's FSM version so the test can issue a
// correctly-versioned cancel.
func taskVersion(t *testing.T, st *Store, namespace, id string) uint64 {
	t.Helper()
	tasks, err := st.distributedTasksManager.ListDistributedTasks(context.Background())
	require.NoError(t, err)
	for _, task := range tasks[namespace] {
		if task.ID == id {
			return task.Version
		}
	}
	t.Fatalf("task %s/%s not found", namespace, id)
	return 0
}

// Pins the Raft → Store → Manager registration wiring end-to-end: an observer
// registered through Raft.RegisterDistributedTaskTerminalObserver must see a
// cancel applied through a live single-node raft, and Store.Close (opened
// path) must stop further dispatch.
func TestRaftTerminalObserverWiring(t *testing.T) {
	const namespace = "terminal-observer-wiring-test"
	ctx := context.Background()
	m := NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort())
	addr := fmt.Sprintf("%s:%d", m.cfg.Host, m.cfg.RaftPort)
	srv := NewRaft(mocks.NewMockNodeSelector(), m.store, nil)

	var probe terminalObserverProbe
	srv.RegisterDistributedTaskTerminalObserver(namespace, func(task *distributedtask.Task) {
		probe.record(task.ID, string(task.Status))
	})

	m.indexer.On("Open", Anything).Return(nil)
	require.NoError(t, srv.Open(ctx, m.indexer))
	require.NoError(t, srv.store.Notify(m.cfg.NodeID, addr))
	require.NoError(t, srv.WaitUntilDBRestored(ctx, time.Second, make(chan struct{})))
	require.True(t, tryNTimesWithWait(20, time.Millisecond*200, srv.store.IsLeader))
	require.True(t, tryNTimesWithWait(10, time.Millisecond*200, srv.Ready))

	require.NoError(t, srv.AddDistributedTask(ctx, namespace, "task-1", map[string]string{"k": "v"}, []string{"u1"}))
	require.NoError(t, srv.CancelDistributedTask(ctx, namespace, "task-1", taskVersion(t, srv.store, namespace, "task-1")))

	require.Eventually(t, func() bool { return probe.sawExactly("task-1:CANCELLED") },
		2*time.Second, 10*time.Millisecond,
		"a cancel applied through raft must reach the observer registered via Raft")

	// task-2 stays non-terminal across Close so the post-close cancel below
	// has something to (not) dispatch.
	require.NoError(t, srv.AddDistributedTask(ctx, namespace, "task-2", map[string]string{"k": "v"}, []string{"u1"}))
	task2Version := taskVersion(t, srv.store, namespace, "task-2")

	m.indexer.On("Close", Anything).Return(nil)
	require.NoError(t, srv.Close(ctx))

	// Cancel through the FSM directly (raft is down); the state change lands
	// but the closed manager must drop the dispatch.
	cancelBytes, err := json.Marshal(&cmd.CancelDistributedTaskRequest{
		Namespace: namespace, Id: "task-2", Version: task2Version,
		CancelledAtUnixMillis: time.Now().UnixMilli(),
	})
	require.NoError(t, err)
	require.NoError(t, srv.store.distributedTasksManager.CancelTask(
		&cmd.ApplyRequest{SubCommand: cancelBytes}, false))

	require.Never(t, func() bool { return len(probe.snapshot()) > 1 },
		300*time.Millisecond, 25*time.Millisecond,
		"Store.Close on the opened path must stop terminal dispatch")
}

// Pins the reconcile half of the delivery contract. An observer that misses an
// ending has exactly one remedy — the leader-routed task list — and the
// contract has to say when it can be called, because the registration point it
// mandates is not it.
func TestTerminalObserverReconcileNeedsACaughtUpNode(t *testing.T) {
	const namespace = "terminal-observer-reconcile-test"

	// Control for the subtest below: an unopened store has no RAFT at all, so
	// the failure here is structural rather than an election outcome. It shows
	// the shape of the advice, not that a leader was unreachable.
	t.Run("at registration time the list has no leader to reach", func(t *testing.T) {
		// Registered before the Close below so LIFO cleanup stops the drainer
		// before the leak check looks for it.
		t.Cleanup(leaktest.Check(t))
		m := NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort())
		srv := NewRaft(mocks.NewMockNodeSelector(), m.store, nil)
		t.Cleanup(m.store.distributedTasksManager.Close)

		var probe terminalObserverProbe
		srv.RegisterDistributedTaskTerminalObserver(namespace, func(task *distributedtask.Task) {
			probe.record(task.ID, string(task.Status))
		})

		// Bounded: without a deadline the query backs off for ten election
		// timeouts before giving up.
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		_, err := srv.ListDistributedTasks(ctx)
		require.Error(t, err,
			"registration happens before Open, where this node is not the leader and knows of none, "+
				"so reconciling here cannot be what the contract asks for")
		require.Empty(t, probe.snapshot(),
			"registering must not replay anything by itself")
	})

	t.Run("once the node is caught up the list reports an ending nobody announced", func(t *testing.T) {
		ctx := context.Background()
		m := NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort())
		addr := fmt.Sprintf("%s:%d", m.cfg.Host, m.cfg.RaftPort)
		srv := NewRaft(mocks.NewMockNodeSelector(), m.store, nil)

		var probe terminalObserverProbe
		srv.RegisterDistributedTaskTerminalObserver(namespace, func(task *distributedtask.Task) {
			probe.record(task.ID, string(task.Status))
		})

		m.indexer.On("Open", Anything).Return(nil)
		m.indexer.On("Close", Anything).Return(nil)
		require.NoError(t, srv.Open(ctx, m.indexer))
		t.Cleanup(func() { require.NoError(t, srv.Close(ctx)) })
		require.NoError(t, srv.store.Notify(m.cfg.NodeID, addr))
		require.NoError(t, srv.WaitUntilDBRestored(ctx, time.Second, make(chan struct{})))
		require.True(t, tryNTimesWithWait(20, time.Millisecond*200, srv.store.IsLeader))
		require.True(t, tryNTimesWithWait(10, time.Millisecond*200, srv.Ready))

		require.NoError(t, srv.AddDistributedTask(ctx, namespace, "task-1", map[string]string{"k": "v"}, []string{"u1"}))

		// Apply the ending with catchingUp set, the way Store.Apply does for a
		// log entry that was already on disk when the node opened. That is the
		// case the contract sends a consumer to the list for.
		cancelBytes, err := json.Marshal(&cmd.CancelDistributedTaskRequest{
			Namespace: namespace, Id: "task-1",
			Version:               taskVersion(t, srv.store, namespace, "task-1"),
			CancelledAtUnixMillis: time.Now().UnixMilli(),
		})
		require.NoError(t, err)
		require.NoError(t, srv.store.distributedTasksManager.CancelTask(
			&cmd.ApplyRequest{SubCommand: cancelBytes}, true))

		require.Never(t, func() bool { return len(probe.snapshot()) > 0 },
			300*time.Millisecond, 25*time.Millisecond,
			"a replayed ending must stay silent; that silence is what reconciling exists to cover")

		listCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		tasks, err := srv.ListDistributedTasks(listCtx)
		require.NoError(t, err,
			"once the node is caught up the remedy the contract names must actually run")
		require.Len(t, tasks[namespace], 1)
		require.Equal(t, "task-1", tasks[namespace][0].ID)
		require.Equal(t, distributedtask.TaskStatusCancelled, tasks[namespace][0].Status,
			"reconciling must show the terminal status the observer was never told about")
	})
}

// Pins that Store.Close on a never-opened store still shuts the task
// manager's dispatch down: its drainer starts at observer registration, in
// New, before Open ever runs.
func TestStoreCloseWithoutOpenClosesTaskManager(t *testing.T) {
	const namespace = "terminal-observer-unopened-test"

	setup := func(t *testing.T) (*Store, *terminalObserverProbe) {
		t.Helper()
		m := NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort())
		st := m.store
		var probe terminalObserverProbe
		st.RegisterDistributedTaskTerminalObserver(namespace, func(task *distributedtask.Task) {
			probe.record(task.ID, string(task.Status))
		})

		addBytes, err := json.Marshal(&cmd.AddDistributedTaskRequest{
			Namespace: namespace, Id: "task-1", Payload: []byte(`{}`),
			SubmittedAtUnixMillis: 1, UnitIds: []string{"u1"},
		})
		require.NoError(t, err)
		require.NoError(t, st.distributedTasksManager.AddTask(&cmd.ApplyRequest{SubCommand: addBytes}, 1))
		return st, &probe
	}

	cancelTask1 := func(t *testing.T, st *Store) {
		t.Helper()
		cancelBytes, err := json.Marshal(&cmd.CancelDistributedTaskRequest{
			Namespace: namespace, Id: "task-1", Version: 1, CancelledAtUnixMillis: 2,
		})
		require.NoError(t, err)
		require.NoError(t, st.distributedTasksManager.CancelTask(&cmd.ApplyRequest{SubCommand: cancelBytes}, false))
	}

	t.Run("without Close the observer fires (control)", func(t *testing.T) {
		st, probe := setup(t)
		defer st.distributedTasksManager.Close()

		cancelTask1(t, st)
		require.Eventually(t, func() bool { return probe.sawExactly("task-1:CANCELLED") },
			2*time.Second, 10*time.Millisecond,
			"with the manager open the cancel must reach the observer")
	})

	t.Run("after Close the observer stays silent", func(t *testing.T) {
		st, probe := setup(t)
		require.NoError(t, st.Close(context.Background()))

		cancelTask1(t, st)
		require.Never(t, func() bool { return len(probe.snapshot()) > 0 },
			300*time.Millisecond, 25*time.Millisecond,
			"Store.Close on the not-open path must stop terminal dispatch")
	})
}
