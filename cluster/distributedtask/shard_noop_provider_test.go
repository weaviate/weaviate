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

	// errCalls fails that many calls first, and emptyCalls answers ([]string{}, nil)
	// for that many after.
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

func (r *mockRecorder) RecordDistributedTaskRetryableUnitFailure(_ context.Context, _, _ string, _ uint64, _, suID, errMsg string) error {
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
	// hook captures the provider's log, the only signal of a run that claims no unit.
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

// awaitEntry waits for the provider's goroutine to log at level and returns that entry.
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

// The node holds a shard of the collection, just not the unit's. A lister answering
// no shards would keep listLocalShards retrying past the assertion.
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

func TestListLocalShards(t *testing.T) {
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
			// RAFT has not applied the class create on this node yet.
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
			// After the last attempt an empty answer comes back without the give-up line.
			name:      "the node holds none of the collection's shards",
			lister:    &mockShardLister{shards: map[string][]string{"MyClass": {}}},
			wantNames: []string{}, wantProceed: true, wantCalls: maxListAttempts,
		},
		{
			// Stopping is not a give-up.
			name:       "a terminated task stops on its first wait",
			lister:     &mockShardLister{err: fmt.Errorf("collection index is closing")},
			stopAtCall: 1,
			wantCalls:  1,
		},
		{
			// Waiting after the last attempt would turn this give-up into a silent stop.
			name:       "a stop during the last attempt still gives up",
			lister:     &mockShardLister{err: fmt.Errorf("collection index is closing")},
			stopAtCall: maxListAttempts,
			wantGaveUp: true, wantCalls: maxListAttempts,
		},
		{
			// Entering the wait here would discard the empty answer the last
			// attempt just produced.
			name:       "a stop during the last attempt keeps its empty answer",
			lister:     &mockShardLister{shards: map[string][]string{"MyClass": {}}},
			stopAtCall: maxListAttempts,
			wantNames:  []string{}, wantProceed: true, wantCalls: maxListAttempts,
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

			assert.Equal(t, tc.wantGaveUp, hasEntry(f.hook, logrus.ErrorLevel),
				"only a lister that never answers logs the give-up")
		})
	}
}

// listLocalShards' return cannot show what processUnits does with each answer.
func TestShardNoopProviderWaitsForShards(t *testing.T) {
	tests := []struct {
		name       string
		lister     *mockShardLister
		wantGaveUp bool
	}{
		{
			// A nil set makes shouldProcessUnit fall back to the node-ID filter and
			// claim every unit, though this node holds no shard of the collection.
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
			// processUnits logs the Info line just past its give-up check.
			awaitEntry(t, f.hook, logrus.InfoLevel)
			assert.Never(t, func() bool { return len(f.recorder.getCompleted()) > 0 },
				300*time.Millisecond, 20*time.Millisecond)
		})
	}
}
