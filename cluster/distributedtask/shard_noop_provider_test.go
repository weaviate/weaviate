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

package distributedtask

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockShardLister implements ShardLister for unit tests.
type mockShardLister struct {
	mu     sync.Mutex
	shards map[string][]string // collection → local shard names
	err    error

	// errCalls refuses that many calls first, and emptyCalls answers
	// ([]string{}, nil) for that many after. That is the order a class create
	// takes, since the index is absent before it is present and empty.
	errCalls   int
	emptyCalls int
	calls      int
	// onCall runs after each call is counted, so a row can terminate its task
	// from inside a chosen attempt.
	onCall func(calls int)
}

func (m *mockShardLister) GetLocalShardNames(collection string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.calls++
	if m.onCall != nil {
		m.onCall(m.calls)
	}
	notFound := fmt.Errorf("collection %q not found", collection)
	switch {
	case m.calls <= m.errCalls:
		return nil, notFound
	case m.calls <= m.errCalls+m.emptyCalls:
		return []string{}, nil
	case m.err != nil:
		return nil, m.err
	}
	names, ok := m.shards[collection]
	if !ok {
		return nil, notFound
	}
	return names, nil
}

func (m *mockShardLister) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

// mockRecorder implements TaskCompletionRecorder for unit tests.
type mockRecorder struct {
	mu         sync.Mutex
	progresses map[string]float32 // suID → last progress
	completed  []string           // suIDs that completed
	failed     map[string]string  // suID → error message
}

func newMockRecorder() *mockRecorder {
	return &mockRecorder{
		progresses: make(map[string]float32),
		failed:     make(map[string]string),
	}
}

func (r *mockRecorder) UpdateDistributedTaskUnitProgress(_ context.Context, _, _ string, _ uint64, _, suID string, progress float32) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.progresses[suID] = progress
	return nil
}

func (r *mockRecorder) RecordDistributedTaskUnitCompletion(_ context.Context, _, _ string, _ uint64, _, suID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.completed = append(r.completed, suID)
	return nil
}

func (r *mockRecorder) RecordDistributedTaskUnitFailure(_ context.Context, _, _ string, _ uint64, _, suID, errMsg string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failed[suID] = errMsg
	return nil
}

func (r *mockRecorder) getCompleted() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string{}, r.completed...)
}

func (r *mockRecorder) getFailed() map[string]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make(map[string]string, len(r.failed))
	for k, v := range r.failed {
		result[k] = v
	}
	return result
}

// providerFixture bundles the provider and recorder created for each test,
// eliminating repeated setup boilerplate.
type providerFixture struct {
	provider *ShardNoopProvider
	recorder *mockRecorder
	// hook holds what the provider's goroutine logged, which is the only signal
	// a test has for a run that claims no unit.
	hook *logrustest.Hook
}

// startTaskAndAssertNoProgress starts the task, waits briefly, and asserts that
// no units were completed. Returns the handle (caller should defer Terminate).
func (f *providerFixture) startTaskAndAssertNoProgress(t *testing.T, task *Task, msg string) TaskHandle {
	t.Helper()
	handle, err := f.provider.StartTask(task)
	require.NoError(t, err)
	// awaitEntry joins on the provider's goroutine first, so an empty completion
	// set means the units were rejected rather than never reached.
	awaitEntry(t, f.hook, logrus.InfoLevel)
	time.Sleep(500 * time.Millisecond)
	assert.Empty(t, f.recorder.getCompleted(), msg)
	return handle
}

func newProviderFixture(t *testing.T, nodeID string, lister ShardLister) *providerFixture {
	t.Helper()
	logger, hook := logrustest.NewNullLogger()
	rec := newMockRecorder()
	// Use os.MkdirTemp instead of t.TempDir() because async marker writes may
	// still be in flight when t.TempDir cleanup runs, causing spurious failures.
	dataRoot, err := os.MkdirTemp("", "shard-noop-test-*")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dataRoot) })
	p := NewShardNoopProvider(nodeID, logger, lister, dataRoot)
	p.SetCompletionRecorder(rec)
	return &providerFixture{provider: p, recorder: rec, hook: hook}
}

// hasEntry reports whether the hook holds an entry at level.
func hasEntry(hook *logrustest.Hook, level logrus.Level) bool {
	for _, e := range hook.AllEntries() {
		if e.Level == level {
			return true
		}
	}
	return false
}

// awaitEntry waits for the provider's goroutine to log at level and returns that
// entry. Processing runs asynchronously, so the log is what a test joins on.
func awaitEntry(t *testing.T, hook *logrustest.Hook, level logrus.Level) *logrus.Entry {
	t.Helper()

	at := func() *logrus.Entry {
		for _, e := range hook.AllEntries() {
			if e.Level == level {
				return e
			}
		}
		return nil
	}
	require.Eventually(t, func() bool { return at() != nil }, 5*time.Second, 10*time.Millisecond,
		"the provider never logged at %s", level)
	return at()
}

// newTask creates a Task with sensible defaults (ID "test-task", Version 1,
// ShardNoopProviderNamespace, TaskStatusStarted) and the given units.
func (f *providerFixture) newTask(units map[string]*Unit) *Task {
	return &Task{
		TaskDescriptor: TaskDescriptor{ID: "test-task", Version: 1},
		Namespace:      ShardNoopProviderNamespace,
		Status:         TaskStatusStarted,
		Units:          units,
	}
}

// newTaskWithPayload is like newTask but also marshals the given payload.
func (f *providerFixture) newTaskWithPayload(payload ShardNoopProviderPayload, units map[string]*Unit) *Task {
	raw, _ := json.Marshal(payload)
	return &Task{
		TaskDescriptor: TaskDescriptor{ID: "test-task", Version: 1},
		Namespace:      ShardNoopProviderNamespace,
		Status:         TaskStatusStarted,
		Payload:        raw,
		Units:          units,
	}
}

// startAndAwaitCompleted starts the task, waits until expectedCount units
// complete, and returns the handle (caller should defer handle.Terminate()) and
// the completed unit IDs.
func (f *providerFixture) startAndAwaitCompleted(t *testing.T, task *Task, expectedCount int) (TaskHandle, []string) {
	t.Helper()
	handle, err := f.provider.StartTask(task)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return len(f.recorder.getCompleted()) == expectedCount
	}, 5*time.Second, 50*time.Millisecond)

	return handle, f.recorder.getCompleted()
}

func TestShardNoopProvider_SyntheticUnits_NilShardLister(t *testing.T) {
	f := newProviderFixture(t, "node1", nil)
	task := f.newTask(map[string]*Unit{
		"u-1": {Status: UnitStatusPending},
		"u-2": {Status: UnitStatusPending},
	})

	handle, completed := f.startAndAwaitCompleted(t, task, 2)
	defer handle.Terminate()

	assert.ElementsMatch(t, []string{"u-1", "u-2"}, completed)
}

func TestShardNoopProvider_SyntheticUnits_SkipsOtherNodes(t *testing.T) {
	f := newProviderFixture(t, "node1", nil)
	task := f.newTask(map[string]*Unit{
		"u-1": {Status: UnitStatusPending, NodeID: "node1"},
		"u-2": {Status: UnitStatusPending, NodeID: "node2"}, // belongs to another node
		"u-3": {Status: UnitStatusPending},                  // unassigned → claimed by this node
	})

	handle, completed := f.startAndAwaitCompleted(t, task, 2)
	defer handle.Terminate()

	assert.ElementsMatch(t, []string{"u-1", "u-3"}, completed)
}

func TestShardNoopProvider_CollectionAware_OnlyProcessesLocalShards(t *testing.T) {
	lister := &mockShardLister{
		shards: map[string][]string{
			"MyClass": {"shardA", "shardC"},
		},
	}
	f := newProviderFixture(t, "node1", lister)

	task := f.newTaskWithPayload(
		ShardNoopProviderPayload{Collection: "MyClass"},
		map[string]*Unit{
			"shardA": {Status: UnitStatusPending},
			"shardB": {Status: UnitStatusPending}, // not local
			"shardC": {Status: UnitStatusPending},
		},
	)

	handle, completed := f.startAndAwaitCompleted(t, task, 2)
	defer handle.Terminate()

	assert.ElementsMatch(t, []string{"shardA", "shardC"}, completed)
}

// The node holds a shard of the collection, just not the unit's, so the
// ownership filter is what leaves the unit unclaimed. A lister answering no
// shards would instead still be retrying when the assertion runs.
func TestShardNoopProvider_CollectionAware_UnitShardNotLocal(t *testing.T) {
	lister := &mockShardLister{
		shards: map[string][]string{
			"MyClass": {"otherShard"},
		},
	}
	f := newProviderFixture(t, "node1", lister)

	task := f.newTaskWithPayload(
		ShardNoopProviderPayload{Collection: "MyClass"},
		map[string]*Unit{
			"shardA": {Status: UnitStatusPending},
		},
	)

	handle := f.startTaskAndAssertNoProgress(t, task, "no unit should be processed for a shard this node lacks")
	defer handle.Terminate()
}

func TestShardNoopProvider_CollectionAware_FailUnit(t *testing.T) {
	lister := &mockShardLister{
		shards: map[string][]string{
			"MyClass": {"shardA", "shardB"},
		},
	}
	f := newProviderFixture(t, "node1", lister)

	task := f.newTaskWithPayload(
		ShardNoopProviderPayload{
			Collection: "MyClass",
			FailUnitID: "shardA",
		},
		map[string]*Unit{
			"shardA": {Status: UnitStatusPending},
			"shardB": {Status: UnitStatusPending},
		},
	)

	handle, err := f.provider.StartTask(task)
	require.NoError(t, err)
	defer handle.Terminate()

	// Wait for the failure to be recorded
	require.Eventually(t, func() bool {
		return len(f.recorder.getFailed()) > 0
	}, 5*time.Second, 50*time.Millisecond)

	failed := f.recorder.getFailed()
	assert.Contains(t, failed, "shardA")
	assert.Equal(t, "dummy failure", failed["shardA"])
}

func TestShardNoopProvider_CollectionAware_EmptyPayloadFallsBackToSynthetic(t *testing.T) {
	lister := &mockShardLister{
		shards: map[string][]string{
			"MyClass": {"shardA"},
		},
	}
	// Even with a ShardLister, if the payload has no Collection, synthetic mode is used
	f := newProviderFixture(t, "node1", lister)

	task := f.newTask(map[string]*Unit{
		"u-1": {Status: UnitStatusPending},
		"u-2": {Status: UnitStatusPending, NodeID: "node2"},
	})

	handle, completed := f.startAndAwaitCompleted(t, task, 1)
	defer handle.Terminate()

	assert.ElementsMatch(t, []string{"u-1"}, completed,
		"only u-1 should be processed in synthetic mode (u-2 belongs to node2)")
}

func TestShardNoopProvider_OnGroupCompleted(t *testing.T) {
	f := newProviderFixture(t, "node1", nil)
	task := f.newTask(nil)

	f.provider.OnGroupCompleted(task, "", []string{"u-1", "u-2"})

	finalized := f.provider.GetFinalizedUnits(task.TaskDescriptor)
	assert.ElementsMatch(t, []string{"u-1", "u-2"}, finalized)
}

func TestShardNoopProvider_OnGroupCompleted_MultipleGroups(t *testing.T) {
	f := newProviderFixture(t, "node1", nil)
	task := f.newTask(nil)

	f.provider.OnGroupCompleted(task, "groupA", []string{"u-1"})
	f.provider.OnGroupCompleted(task, "groupB", []string{"u-2", "u-3"})

	groups := f.provider.GetFinalizedGroups(task.TaskDescriptor)
	assert.ElementsMatch(t, []string{"u-1"}, groups["groupA"])
	assert.ElementsMatch(t, []string{"u-2", "u-3"}, groups["groupB"])

	// GetFinalizedUnits should return all across groups
	all := f.provider.GetFinalizedUnits(task.TaskDescriptor)
	assert.ElementsMatch(t, []string{"u-1", "u-2", "u-3"}, all)
}

func TestShardNoopProvider_OnTaskCompleted(t *testing.T) {
	f := newProviderFixture(t, "node1", nil)
	task := f.newTask(nil)

	assert.False(t, f.provider.IsTaskCompleted(task.TaskDescriptor))
	f.provider.OnTaskCompleted(task)
	assert.True(t, f.provider.IsTaskCompleted(task.TaskDescriptor))
}

func TestShardNoopProvider_PerReplicaUnits_OnlyProcessesLocalShards(t *testing.T) {
	lister := &mockShardLister{
		shards: map[string][]string{
			"MyClass": {"s1", "s2"}, // nodeA has both s1 and s2
		},
	}
	f := newProviderFixture(t, "nodeA", lister)

	task := f.newTaskWithPayload(
		ShardNoopProviderPayload{
			Collection: "MyClass",
			UnitToShard: map[string]string{
				"s1__nodeA": "s1",
				"s1__nodeB": "s1", // same shard, but belongs to nodeB
				"s2__nodeA": "s2",
				"s2__nodeC": "s2", // belongs to nodeC
			},
			UnitToNode: map[string]string{
				"s1__nodeA": "nodeA",
				"s1__nodeB": "nodeB",
				"s2__nodeA": "nodeA",
				"s2__nodeC": "nodeC",
			},
			ProcessingDelayMs: 10,
		},
		map[string]*Unit{
			"s1__nodeA": {Status: UnitStatusPending},
			"s1__nodeB": {Status: UnitStatusPending},
			"s2__nodeA": {Status: UnitStatusPending},
			"s2__nodeC": {Status: UnitStatusPending},
		},
	)

	// Only s1__nodeA and s2__nodeA should be processed (nodeA's units per UnitToNode).
	// s1__nodeB and s2__nodeC belong to other nodes.
	handle, completed := f.startAndAwaitCompleted(t, task, 2)
	defer handle.Terminate()

	assert.ElementsMatch(t, []string{"s1__nodeA", "s2__nodeA"}, completed)
}

func TestShardNoopProvider_PerReplicaUnits_UnknownUnitSkipped(t *testing.T) {
	lister := &mockShardLister{
		shards: map[string][]string{
			"MyClass": {"s1"},
		},
	}
	f := newProviderFixture(t, "nodeA", lister)

	task := f.newTaskWithPayload(
		ShardNoopProviderPayload{
			Collection: "MyClass",
			UnitToShard: map[string]string{
				"s1__nodeA":     "s1",
				"s1__otherNode": "s1",
				// "unknown" is not in the mapping → skipped
			},
			UnitToNode: map[string]string{
				"s1__nodeA":     "nodeA",
				"s1__otherNode": "otherNode",
			},
			ProcessingDelayMs: 10,
		},
		map[string]*Unit{
			"s1__nodeA":     {Status: UnitStatusPending},
			"unknown":       {Status: UnitStatusPending}, // not in UnitToShard
			"s1__otherNode": {Status: UnitStatusPending}, // in mapping but wrong node
		},
	)

	handle, completed := f.startAndAwaitCompleted(t, task, 1)
	defer handle.Terminate()

	assert.ElementsMatch(t, []string{"s1__nodeA"}, completed)
}

func TestShardNoopProvider_SlowUnit(t *testing.T) {
	f := newProviderFixture(t, "node1", nil)

	task := f.newTaskWithPayload(
		ShardNoopProviderPayload{
			SlowUnitID:        "u-slow",
			SlowUnitDelayMs:   500,
			ProcessingDelayMs: 10,
		},
		map[string]*Unit{
			"u-fast": {Status: UnitStatusPending},
			"u-slow": {Status: UnitStatusPending},
		},
	)

	start := time.Now()
	handle, completed := f.startAndAwaitCompleted(t, task, 2)
	defer handle.Terminate()

	elapsed := time.Since(start)
	assert.Greater(t, elapsed, 400*time.Millisecond, "slow unit delay should be applied")

	assert.ElementsMatch(t, []string{"u-fast", "u-slow"}, completed)
}

func TestShardNoopProvider_ProcessingDelayOverride(t *testing.T) {
	f := newProviderFixture(t, "node1", nil)

	task := f.newTaskWithPayload(
		ShardNoopProviderPayload{
			ProcessingDelayMs: 10, // fast override
		},
		map[string]*Unit{
			"u-1": {Status: UnitStatusPending},
			"u-2": {Status: UnitStatusPending},
			"u-3": {Status: UnitStatusPending},
		},
	)

	start := time.Now()
	handle, completed := f.startAndAwaitCompleted(t, task, 3)
	defer handle.Terminate()

	elapsed := time.Since(start)
	// With 10ms delay per unit, should be much faster than default 100ms * 3 = 300ms
	assert.Less(t, elapsed, 200*time.Millisecond, "processing should use fast delay override")

	assert.ElementsMatch(t, []string{"u-1", "u-2", "u-3"}, completed)
}

func TestShardNoopProvider_TaskLifecycle(t *testing.T) {
	f := newProviderFixture(t, "node1", nil)

	desc := TaskDescriptor{ID: "test-task", Version: 1}
	assert.Empty(t, f.provider.GetLocalTasks())

	task := f.newTask(nil)
	task.Units = nil

	handle, err := f.provider.StartTask(task)
	require.NoError(t, err)
	defer handle.Terminate()

	assert.Equal(t, []TaskDescriptor{desc}, f.provider.GetLocalTasks())

	require.NoError(t, f.provider.CleanupTask(desc))
	assert.Empty(t, f.provider.GetLocalTasks())
}

// The retry loop has to tell four answers apart: an index that is not there
// yet, shards that register late, a lister that never answers, and a node that
// legitimately holds none.
func TestListLocalShards(t *testing.T) {
	// Every row below shortens the backoff, so the production window is asserted
	// here. Both the cap and the base are read, since it is their product.
	logger, _ := logrustest.NewNullLogger()
	require.Equal(t, 500*time.Millisecond,
		NewShardNoopProvider("node1", logger, nil, t.TempDir()).retryBase)
	require.Equal(t, 22500*time.Millisecond,
		time.Duration(maxListAttempts*(maxListAttempts-1)/2)*500*time.Millisecond,
		"the ramped waits have to add up to the window the godoc promises")

	tests := []struct {
		name        string
		lister      *mockShardLister
		stopAtCall  int
		wantNames   []string
		wantProceed bool
		wantGaveUp  bool
		wantCalls   int
	}{
		{
			// This is the production sequence. RAFT has not applied the class
			// create, so the lister errors before it ever answers empty.
			name: "an error window before the shards register",
			lister: &mockShardLister{
				shards:   map[string][]string{"MyClass": {"shardA"}},
				errCalls: 2,
			},
			wantNames: []string{"shardA"}, wantProceed: true, wantCalls: 3,
		},
		{
			// Returning on the first empty answer would hand back an empty set
			// while this node's shards were still registering.
			name: "shards register after two empty answers",
			lister: &mockShardLister{
				shards:     map[string][]string{"MyClass": {"shardA"}},
				emptyCalls: 2,
			},
			wantNames: []string{"shardA"}, wantProceed: true, wantCalls: 3,
		},
		{
			name:       "the lister never answers",
			lister:     &mockShardLister{err: fmt.Errorf("collection index is closing")},
			wantGaveUp: true, wantCalls: maxListAttempts,
		},
		{
			// An empty answer is as final as a populated one, so it comes back as
			// an answer and the give-up line stays absent.
			name:      "the node holds none of the collection's shards",
			lister:    &mockShardLister{shards: map[string][]string{"MyClass": {}}},
			wantNames: []string{}, wantProceed: true, wantCalls: maxListAttempts,
		},
		{
			// A terminated task stops inside the wait rather than running out
			// its attempts, and stopping is not a give-up.
			name:       "a terminated task stops on its first wait",
			lister:     &mockShardLister{err: fmt.Errorf("collection index is closing")},
			stopAtCall: 1,
			wantCalls:  1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newProviderFixture(t, "node1", tc.lister)
			f.provider.retryBase = time.Millisecond
			handle := &shardNoopTaskHandle{stopCh: make(chan struct{}), doneCh: make(chan struct{})}
			if tc.stopAtCall > 0 {
				tc.lister.onCall = func(n int) {
					if n == tc.stopAtCall {
						close(handle.stopCh)
					}
				}
			}

			names, proceed := f.provider.listLocalShards("test-task", handle, "MyClass")

			assert.Equal(t, tc.wantProceed, proceed)
			assert.Equal(t, tc.wantNames, names)
			assert.Equal(t, tc.wantCalls, tc.lister.callCount())

			// Every attempt logs, and an empty answer has no error to print, so a
			// single un-split line would render error=<nil> here.
			warn := awaitEntry(t, f.hook, logrus.WarnLevel)
			assert.Equal(t, "MyClass", warn.Data["collection"])
			assert.Equal(t, "test-task", warn.Data["taskID"], "an operator runs two of these at once")
			assert.NotContains(t, warn.Data, logrus.ErrorKey)

			if !tc.wantGaveUp {
				for _, e := range f.hook.AllEntries() {
					assert.NotEqual(t, logrus.ErrorLevel, e.Level, e.Message)
				}
				return
			}
			entry := awaitEntry(t, f.hook, logrus.ErrorLevel)
			assert.Contains(t, entry.Message, "failed to list local shards after retries")
			assert.Contains(t, entry.Message, "restarts the task on each poll until the lister answers",
				"one line has to say what clears the give-up, since nothing cancels this task")
			assert.Contains(t, entry.Message, "collection index is closing",
				"an operator reading one line needs why the lister refused")
			assert.Equal(t, "MyClass", entry.Data["collection"])
			assert.Equal(t, "test-task", entry.Data["taskID"])
		})
	}
}

// The last attempt has nothing left to wait for, so it must not enter the wait.
// Both rows close the stop channel from inside that attempt. Without the guard
// the closed channel wins the select, so the call loses the answer that attempt
// just produced.
func TestListLocalShardsSkipsTheLastWait(t *testing.T) {
	tests := []struct {
		name      string
		lister    *mockShardLister
		wantNames []string
		wantOK    bool
	}{
		{
			name:   "an exhausted lister still reports the give-up",
			lister: &mockShardLister{err: fmt.Errorf("collection index is closing")},
		},
		{
			name:      "an empty answer still comes back as an answer",
			lister:    &mockShardLister{shards: map[string][]string{"MyClass": {}}},
			wantNames: []string{}, wantOK: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newProviderFixture(t, "node1", tc.lister)
			f.provider.retryBase = 10 * time.Millisecond
			handle := &shardNoopTaskHandle{stopCh: make(chan struct{}), doneCh: make(chan struct{})}
			tc.lister.onCall = func(n int) {
				if n == maxListAttempts {
					close(handle.stopCh)
				}
			}

			names, proceed := f.provider.listLocalShards("test-task", handle, "MyClass")

			assert.Equal(t, tc.wantOK, proceed)
			assert.Equal(t, tc.wantNames, names)
			assert.Equal(t, maxListAttempts, tc.lister.callCount())
			if !tc.wantOK {
				assert.Contains(t, awaitEntry(t, f.hook, logrus.ErrorLevel).Message,
					"failed to list local shards after retries")
			}
		})
	}
}

// listLocalShards' own return cannot show what processUnits does with each
// answer. A set of names has to reach the ownership filter, an empty set has to
// reach it too, and a give-up has to stop short of it.
func TestShardNoopProviderWaitsForShards(t *testing.T) {
	tests := []struct {
		name        string
		lister      *mockShardLister
		wantClaimed []string
		wantGaveUp  bool
	}{
		{
			name: "shards register late and the unit still completes",
			lister: &mockShardLister{
				shards:     map[string][]string{"MyClass": {"shardA"}},
				emptyCalls: 2,
			},
			wantClaimed: []string{"shardA"},
		},
		{
			// Building the set only for a non-empty answer leaves it nil.
			// shouldProcessUnit then falls through to the node-ID filter and
			// claims every unit of a collection this node holds no shard of.
			name:   "a node holding none of the shards claims nothing",
			lister: &mockShardLister{shards: map[string][]string{"MyClass": {}}},
		},
		{
			// Without the caller's give-up check the run reaches unit processing
			// with an empty set, which claims nothing but announces that it started.
			name:       "a give-up stops before unit processing",
			lister:     &mockShardLister{err: fmt.Errorf("collection index is closing")},
			wantGaveUp: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newProviderFixture(t, "node1", tc.lister)
			f.provider.retryBase = time.Millisecond

			task := f.newTaskWithPayload(
				ShardNoopProviderPayload{Collection: "MyClass"},
				map[string]*Unit{
					"shardA": {Status: UnitStatusPending},
					"shardB": {Status: UnitStatusPending},
				},
			)
			handle, err := f.provider.StartTask(task)
			require.NoError(t, err)
			defer handle.Terminate()

			if tc.wantGaveUp {
				awaitEntry(t, f.hook, logrus.ErrorLevel)
				require.Never(t, func() bool { return hasEntry(f.hook, logrus.InfoLevel) },
					200*time.Millisecond, 10*time.Millisecond,
					"a node that gave up never reaches unit processing")
				assert.Empty(t, f.recorder.getCompleted())
				return
			}
			// The Info line sits just past the give-up check, so reaching it says
			// the run fell through with whatever set the lister produced.
			awaitEntry(t, f.hook, logrus.InfoLevel)
			if len(tc.wantClaimed) == 0 {
				assert.Never(t, func() bool { return len(f.recorder.getCompleted()) > 0 },
					300*time.Millisecond, 20*time.Millisecond)
				return
			}
			require.Eventually(t, func() bool {
				return len(f.recorder.getCompleted()) == len(tc.wantClaimed)
			}, 5*time.Second, 10*time.Millisecond)
			assert.ElementsMatch(t, tc.wantClaimed, f.recorder.getCompleted())
		})
	}
}
