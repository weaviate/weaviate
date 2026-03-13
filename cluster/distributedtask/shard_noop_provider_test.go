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
	"sync"
	"testing"
	"time"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockShardLister implements ShardLister for unit tests.
type mockShardLister struct {
	mu     sync.Mutex
	shards map[string][]string // collection → local shard names
	err    error
}

func (m *mockShardLister) GetLocalShardNames(collection string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.err != nil {
		return nil, m.err
	}
	names, ok := m.shards[collection]
	if !ok {
		return nil, fmt.Errorf("collection %q not found", collection)
	}
	return names, nil
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

func (r *mockRecorder) UpdateDistributedTaskSubUnitProgress(_ context.Context, _, _ string, _ uint64, _, suID string, progress float32) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.progresses[suID] = progress
	return nil
}

func (r *mockRecorder) RecordDistributedTaskSubUnitCompletion(_ context.Context, _, _ string, _ uint64, _, suID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.completed = append(r.completed, suID)
	return nil
}

func (r *mockRecorder) RecordDistributedTaskSubUnitFailure(_ context.Context, _, _ string, _ uint64, _, suID, errMsg string) error {
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
}

// startTaskAndAssertNoProgress starts the task, waits briefly, and asserts that
// no sub-units were completed. Returns the handle (caller should defer Terminate).
func (f *providerFixture) startTaskAndAssertNoProgress(t *testing.T, task *Task, msg string) TaskHandle {
	t.Helper()
	handle, err := f.provider.StartTask(task)
	require.NoError(t, err)
	time.Sleep(500 * time.Millisecond)
	assert.Empty(t, f.recorder.getCompleted(), msg)
	return handle
}

func newProviderFixture(t *testing.T, nodeID string, lister ShardLister) *providerFixture {
	t.Helper()
	logger, _ := logrustest.NewNullLogger()
	rec := newMockRecorder()
	p := NewShardNoopProvider(nodeID, logger, lister)
	p.SetCompletionRecorder(rec)
	return &providerFixture{provider: p, recorder: rec}
}

// newTask creates a Task with sensible defaults (ID "test-task", Version 1,
// ShardNoopProviderNamespace, TaskStatusStarted) and the given sub-units.
func (f *providerFixture) newTask(subUnits map[string]*SubUnit) *Task {
	return &Task{
		TaskDescriptor: TaskDescriptor{ID: "test-task", Version: 1},
		Namespace:      ShardNoopProviderNamespace,
		Status:         TaskStatusStarted,
		SubUnits:       subUnits,
	}
}

// newTaskWithPayload is like newTask but also marshals the given payload.
func (f *providerFixture) newTaskWithPayload(payload ShardNoopProviderPayload, subUnits map[string]*SubUnit) *Task {
	raw, _ := json.Marshal(payload)
	return &Task{
		TaskDescriptor: TaskDescriptor{ID: "test-task", Version: 1},
		Namespace:      ShardNoopProviderNamespace,
		Status:         TaskStatusStarted,
		Payload:        raw,
		SubUnits:       subUnits,
	}
}

// startAndAwaitCompleted starts the task, waits until expectedCount sub-units
// complete, and returns the handle (caller should defer handle.Terminate()) and
// the completed sub-unit IDs.
func (f *providerFixture) startAndAwaitCompleted(t *testing.T, task *Task, expectedCount int) (TaskHandle, []string) {
	t.Helper()
	handle, err := f.provider.StartTask(task)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return len(f.recorder.getCompleted()) == expectedCount
	}, 5*time.Second, 50*time.Millisecond)

	return handle, f.recorder.getCompleted()
}

func TestShardNoopProvider_SyntheticSubUnits_NilShardLister(t *testing.T) {
	f := newProviderFixture(t, "node1", nil)
	task := f.newTask(map[string]*SubUnit{
		"su-1": {Status: SubUnitStatusPending},
		"su-2": {Status: SubUnitStatusPending},
	})

	handle, completed := f.startAndAwaitCompleted(t, task, 2)
	defer handle.Terminate()

	assert.ElementsMatch(t, []string{"su-1", "su-2"}, completed)
}

func TestShardNoopProvider_SyntheticSubUnits_SkipsOtherNodes(t *testing.T) {
	f := newProviderFixture(t, "node1", nil)
	task := f.newTask(map[string]*SubUnit{
		"su-1": {Status: SubUnitStatusPending, NodeID: "node1"},
		"su-2": {Status: SubUnitStatusPending, NodeID: "node2"}, // belongs to another node
		"su-3": {Status: SubUnitStatusPending},                  // unassigned → claimed by this node
	})

	handle, completed := f.startAndAwaitCompleted(t, task, 2)
	defer handle.Terminate()

	assert.ElementsMatch(t, []string{"su-1", "su-3"}, completed)
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
		map[string]*SubUnit{
			"shardA": {Status: SubUnitStatusPending},
			"shardB": {Status: SubUnitStatusPending}, // not local
			"shardC": {Status: SubUnitStatusPending},
		},
	)

	handle, completed := f.startAndAwaitCompleted(t, task, 2)
	defer handle.Terminate()

	assert.ElementsMatch(t, []string{"shardA", "shardC"}, completed)
}

func TestShardNoopProvider_CollectionAware_NoLocalShards(t *testing.T) {
	lister := &mockShardLister{
		shards: map[string][]string{
			"MyClass": {}, // no local shards
		},
	}
	f := newProviderFixture(t, "node1", lister)

	task := f.newTaskWithPayload(
		ShardNoopProviderPayload{Collection: "MyClass"},
		map[string]*SubUnit{
			"shardA": {Status: SubUnitStatusPending},
		},
	)

	handle := f.startTaskAndAssertNoProgress(t, task, "no sub-units should be processed when no local shards")
	defer handle.Terminate()
}

func TestShardNoopProvider_CollectionAware_ShardListerError(t *testing.T) {
	lister := &mockShardLister{
		err: fmt.Errorf("collection not found"),
	}
	f := newProviderFixture(t, "node1", lister)

	task := f.newTaskWithPayload(
		ShardNoopProviderPayload{Collection: "NonExistent"},
		map[string]*SubUnit{
			"su-1": {Status: SubUnitStatusPending},
		},
	)

	handle := f.startTaskAndAssertNoProgress(t, task, "no sub-units should be processed on lister error")
	defer handle.Terminate()
}

func TestShardNoopProvider_CollectionAware_FailSubUnit(t *testing.T) {
	lister := &mockShardLister{
		shards: map[string][]string{
			"MyClass": {"shardA", "shardB"},
		},
	}
	f := newProviderFixture(t, "node1", lister)

	task := f.newTaskWithPayload(
		ShardNoopProviderPayload{
			Collection:    "MyClass",
			FailSubUnitID: "shardA",
		},
		map[string]*SubUnit{
			"shardA": {Status: SubUnitStatusPending},
			"shardB": {Status: SubUnitStatusPending},
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

	task := f.newTask(map[string]*SubUnit{
		"su-1": {Status: SubUnitStatusPending},
		"su-2": {Status: SubUnitStatusPending, NodeID: "node2"},
	})

	handle, completed := f.startAndAwaitCompleted(t, task, 1)
	defer handle.Terminate()

	assert.ElementsMatch(t, []string{"su-1"}, completed,
		"only su-1 should be processed in synthetic mode (su-2 belongs to node2)")
}

func TestShardNoopProvider_OnGroupCompleted(t *testing.T) {
	f := newProviderFixture(t, "node1", nil)
	task := f.newTask(nil)

	f.provider.OnGroupCompleted(task, "", []string{"su-1", "su-2"})

	finalized := f.provider.GetFinalizedSubUnits(task.TaskDescriptor)
	assert.ElementsMatch(t, []string{"su-1", "su-2"}, finalized)
}

func TestShardNoopProvider_OnGroupCompleted_MultipleGroups(t *testing.T) {
	f := newProviderFixture(t, "node1", nil)
	task := f.newTask(nil)

	f.provider.OnGroupCompleted(task, "groupA", []string{"su-1"})
	f.provider.OnGroupCompleted(task, "groupB", []string{"su-2", "su-3"})

	groups := f.provider.GetFinalizedGroups(task.TaskDescriptor)
	assert.ElementsMatch(t, []string{"su-1"}, groups["groupA"])
	assert.ElementsMatch(t, []string{"su-2", "su-3"}, groups["groupB"])

	// GetFinalizedSubUnits should return all across groups
	all := f.provider.GetFinalizedSubUnits(task.TaskDescriptor)
	assert.ElementsMatch(t, []string{"su-1", "su-2", "su-3"}, all)
}

func TestShardNoopProvider_OnTaskCompleted(t *testing.T) {
	f := newProviderFixture(t, "node1", nil)
	task := f.newTask(nil)

	assert.False(t, f.provider.IsTaskCompleted(task.TaskDescriptor))
	f.provider.OnTaskCompleted(task)
	assert.True(t, f.provider.IsTaskCompleted(task.TaskDescriptor))
}

func TestShardNoopProvider_PerReplicaSubUnits_OnlyProcessesLocalShards(t *testing.T) {
	lister := &mockShardLister{
		shards: map[string][]string{
			"MyClass": {"s1", "s2"}, // nodeA has both s1 and s2
		},
	}
	f := newProviderFixture(t, "nodeA", lister)

	task := f.newTaskWithPayload(
		ShardNoopProviderPayload{
			Collection: "MyClass",
			SubUnitToShard: map[string]string{
				"s1__nodeA": "s1",
				"s1__nodeB": "s1", // same shard, but belongs to nodeB
				"s2__nodeA": "s2",
				"s2__nodeC": "s2", // belongs to nodeC
			},
			SubUnitToNode: map[string]string{
				"s1__nodeA": "nodeA",
				"s1__nodeB": "nodeB",
				"s2__nodeA": "nodeA",
				"s2__nodeC": "nodeC",
			},
			ProcessingDelayMs: 10,
		},
		map[string]*SubUnit{
			"s1__nodeA": {Status: SubUnitStatusPending},
			"s1__nodeB": {Status: SubUnitStatusPending},
			"s2__nodeA": {Status: SubUnitStatusPending},
			"s2__nodeC": {Status: SubUnitStatusPending},
		},
	)

	// Only s1__nodeA and s2__nodeA should be processed (nodeA's sub-units per SubUnitToNode).
	// s1__nodeB and s2__nodeC belong to other nodes.
	handle, completed := f.startAndAwaitCompleted(t, task, 2)
	defer handle.Terminate()

	assert.ElementsMatch(t, []string{"s1__nodeA", "s2__nodeA"}, completed)
}

func TestShardNoopProvider_PerReplicaSubUnits_UnknownSubUnitSkipped(t *testing.T) {
	lister := &mockShardLister{
		shards: map[string][]string{
			"MyClass": {"s1"},
		},
	}
	f := newProviderFixture(t, "nodeA", lister)

	task := f.newTaskWithPayload(
		ShardNoopProviderPayload{
			Collection: "MyClass",
			SubUnitToShard: map[string]string{
				"s1__nodeA":     "s1",
				"s1__otherNode": "s1",
				// "unknown" is not in the mapping → skipped
			},
			SubUnitToNode: map[string]string{
				"s1__nodeA":     "nodeA",
				"s1__otherNode": "otherNode",
			},
			ProcessingDelayMs: 10,
		},
		map[string]*SubUnit{
			"s1__nodeA":     {Status: SubUnitStatusPending},
			"unknown":       {Status: SubUnitStatusPending}, // not in SubUnitToShard
			"s1__otherNode": {Status: SubUnitStatusPending}, // in mapping but wrong node
		},
	)

	handle, completed := f.startAndAwaitCompleted(t, task, 1)
	defer handle.Terminate()

	assert.ElementsMatch(t, []string{"s1__nodeA"}, completed)
}

func TestShardNoopProvider_SlowSubUnit(t *testing.T) {
	f := newProviderFixture(t, "node1", nil)

	task := f.newTaskWithPayload(
		ShardNoopProviderPayload{
			SlowSubUnitID:      "su-slow",
			SlowSubUnitDelayMs: 500,
			ProcessingDelayMs:  10,
		},
		map[string]*SubUnit{
			"su-fast": {Status: SubUnitStatusPending},
			"su-slow": {Status: SubUnitStatusPending},
		},
	)

	start := time.Now()
	handle, completed := f.startAndAwaitCompleted(t, task, 2)
	defer handle.Terminate()

	elapsed := time.Since(start)
	assert.Greater(t, elapsed, 400*time.Millisecond, "slow sub-unit delay should be applied")

	assert.ElementsMatch(t, []string{"su-fast", "su-slow"}, completed)
}

func TestShardNoopProvider_ProcessingDelayOverride(t *testing.T) {
	f := newProviderFixture(t, "node1", nil)

	task := f.newTaskWithPayload(
		ShardNoopProviderPayload{
			ProcessingDelayMs: 10, // fast override
		},
		map[string]*SubUnit{
			"su-1": {Status: SubUnitStatusPending},
			"su-2": {Status: SubUnitStatusPending},
			"su-3": {Status: SubUnitStatusPending},
		},
	)

	start := time.Now()
	handle, completed := f.startAndAwaitCompleted(t, task, 3)
	defer handle.Terminate()

	elapsed := time.Since(start)
	// With 10ms delay per sub-unit, should be much faster than default 100ms * 3 = 300ms
	assert.Less(t, elapsed, 200*time.Millisecond, "processing should use fast delay override")

	assert.ElementsMatch(t, []string{"su-1", "su-2", "su-3"}, completed)
}

func TestShardNoopProvider_TaskLifecycle(t *testing.T) {
	f := newProviderFixture(t, "node1", nil)

	desc := TaskDescriptor{ID: "test-task", Version: 1}
	assert.Empty(t, f.provider.GetLocalTasks())

	task := f.newTask(nil)
	task.SubUnits = nil

	handle, err := f.provider.StartTask(task)
	require.NoError(t, err)
	defer handle.Terminate()

	assert.Equal(t, []TaskDescriptor{desc}, f.provider.GetLocalTasks())

	require.NoError(t, f.provider.CleanupTask(desc))
	assert.Empty(t, f.provider.GetLocalTasks())
}
