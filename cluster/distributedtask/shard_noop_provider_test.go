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
	nodesDone  bool
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

func (r *mockRecorder) RecordDistributedTaskNodeFailure(_ context.Context, _, _ string, _ uint64, _ string) error {
	return nil
}

func (r *mockRecorder) RecordDistributedTaskNodeCompletion(_ context.Context, _, _ string, _ uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nodesDone = true
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

func TestShardNoopProvider_SyntheticSubUnits_NilShardLister(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	recorder := newMockRecorder()

	provider := NewShardNoopProvider("node1", logger, nil)
	provider.SetCompletionRecorder(recorder)

	task := &Task{
		TaskDescriptor: TaskDescriptor{ID: "test-task", Version: 1},
		Namespace:      ShardNoopProviderNamespace,
		Status:         TaskStatusStarted,
		SubUnits: map[string]*SubUnit{
			"su-1": {Status: SubUnitStatusPending},
			"su-2": {Status: SubUnitStatusPending},
		},
	}

	handle, err := provider.StartTask(task)
	require.NoError(t, err)
	defer handle.Terminate()

	require.Eventually(t, func() bool {
		return len(recorder.getCompleted()) == 2
	}, 5*time.Second, 50*time.Millisecond)

	completed := recorder.getCompleted()
	assert.ElementsMatch(t, []string{"su-1", "su-2"}, completed)
}

func TestShardNoopProvider_SyntheticSubUnits_SkipsOtherNodes(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	recorder := newMockRecorder()

	provider := NewShardNoopProvider("node1", logger, nil)
	provider.SetCompletionRecorder(recorder)

	task := &Task{
		TaskDescriptor: TaskDescriptor{ID: "test-task", Version: 1},
		Namespace:      ShardNoopProviderNamespace,
		Status:         TaskStatusStarted,
		SubUnits: map[string]*SubUnit{
			"su-1": {Status: SubUnitStatusPending, NodeID: "node1"},
			"su-2": {Status: SubUnitStatusPending, NodeID: "node2"}, // belongs to another node
			"su-3": {Status: SubUnitStatusPending},                  // unassigned → claimed by this node
		},
	}

	handle, err := provider.StartTask(task)
	require.NoError(t, err)
	defer handle.Terminate()

	require.Eventually(t, func() bool {
		return len(recorder.getCompleted()) == 2
	}, 5*time.Second, 50*time.Millisecond)

	completed := recorder.getCompleted()
	assert.ElementsMatch(t, []string{"su-1", "su-3"}, completed)
}

func TestShardNoopProvider_CollectionAware_OnlyProcessesLocalShards(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	recorder := newMockRecorder()

	lister := &mockShardLister{
		shards: map[string][]string{
			"MyClass": {"shardA", "shardC"},
		},
	}

	provider := NewShardNoopProvider("node1", logger, lister)
	provider.SetCompletionRecorder(recorder)

	payload, _ := json.Marshal(ShardNoopProviderPayload{Collection: "MyClass"})
	task := &Task{
		TaskDescriptor: TaskDescriptor{ID: "test-task", Version: 1},
		Namespace:      ShardNoopProviderNamespace,
		Status:         TaskStatusStarted,
		Payload:        payload,
		SubUnits: map[string]*SubUnit{
			"shardA": {Status: SubUnitStatusPending},
			"shardB": {Status: SubUnitStatusPending}, // not local
			"shardC": {Status: SubUnitStatusPending},
		},
	}

	handle, err := provider.StartTask(task)
	require.NoError(t, err)
	defer handle.Terminate()

	require.Eventually(t, func() bool {
		return len(recorder.getCompleted()) == 2
	}, 5*time.Second, 50*time.Millisecond)

	completed := recorder.getCompleted()
	assert.ElementsMatch(t, []string{"shardA", "shardC"}, completed)
}

func TestShardNoopProvider_CollectionAware_NoLocalShards(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	recorder := newMockRecorder()

	lister := &mockShardLister{
		shards: map[string][]string{
			"MyClass": {}, // no local shards
		},
	}

	provider := NewShardNoopProvider("node1", logger, lister)
	provider.SetCompletionRecorder(recorder)

	payload, _ := json.Marshal(ShardNoopProviderPayload{Collection: "MyClass"})
	task := &Task{
		TaskDescriptor: TaskDescriptor{ID: "test-task", Version: 1},
		Namespace:      ShardNoopProviderNamespace,
		Status:         TaskStatusStarted,
		Payload:        payload,
		SubUnits: map[string]*SubUnit{
			"shardA": {Status: SubUnitStatusPending},
		},
	}

	handle, err := provider.StartTask(task)
	require.NoError(t, err)
	defer handle.Terminate()

	// Give it time to process — should exit quickly since there are no local shards
	time.Sleep(500 * time.Millisecond)
	assert.Empty(t, recorder.getCompleted(), "no sub-units should be processed when no local shards")
}

func TestShardNoopProvider_CollectionAware_ShardListerError(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	recorder := newMockRecorder()

	lister := &mockShardLister{
		err: fmt.Errorf("collection not found"),
	}

	provider := NewShardNoopProvider("node1", logger, lister)
	provider.SetCompletionRecorder(recorder)

	payload, _ := json.Marshal(ShardNoopProviderPayload{Collection: "NonExistent"})
	task := &Task{
		TaskDescriptor: TaskDescriptor{ID: "test-task", Version: 1},
		Namespace:      ShardNoopProviderNamespace,
		Status:         TaskStatusStarted,
		Payload:        payload,
		SubUnits: map[string]*SubUnit{
			"su-1": {Status: SubUnitStatusPending},
		},
	}

	handle, err := provider.StartTask(task)
	require.NoError(t, err)
	defer handle.Terminate()

	// Provider should return early on error — no sub-units processed
	time.Sleep(500 * time.Millisecond)
	assert.Empty(t, recorder.getCompleted(), "no sub-units should be processed on lister error")
}

func TestShardNoopProvider_CollectionAware_FailSubUnit(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	recorder := newMockRecorder()

	lister := &mockShardLister{
		shards: map[string][]string{
			"MyClass": {"shardA", "shardB"},
		},
	}

	provider := NewShardNoopProvider("node1", logger, lister)
	provider.SetCompletionRecorder(recorder)

	payload, _ := json.Marshal(ShardNoopProviderPayload{
		Collection:    "MyClass",
		FailSubUnitID: "shardA",
	})
	task := &Task{
		TaskDescriptor: TaskDescriptor{ID: "test-task", Version: 1},
		Namespace:      ShardNoopProviderNamespace,
		Status:         TaskStatusStarted,
		Payload:        payload,
		SubUnits: map[string]*SubUnit{
			"shardA": {Status: SubUnitStatusPending},
			"shardB": {Status: SubUnitStatusPending},
		},
	}

	handle, err := provider.StartTask(task)
	require.NoError(t, err)
	defer handle.Terminate()

	// Wait for the failure to be recorded
	require.Eventually(t, func() bool {
		return len(recorder.getFailed()) > 0
	}, 5*time.Second, 50*time.Millisecond)

	failed := recorder.getFailed()
	assert.Contains(t, failed, "shardA")
	assert.Equal(t, "dummy failure", failed["shardA"])
}

func TestShardNoopProvider_CollectionAware_EmptyPayloadFallsBackToSynthetic(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	recorder := newMockRecorder()

	lister := &mockShardLister{
		shards: map[string][]string{
			"MyClass": {"shardA"},
		},
	}

	// Even with a ShardLister, if the payload has no Collection, synthetic mode is used
	provider := NewShardNoopProvider("node1", logger, lister)
	provider.SetCompletionRecorder(recorder)

	task := &Task{
		TaskDescriptor: TaskDescriptor{ID: "test-task", Version: 1},
		Namespace:      ShardNoopProviderNamespace,
		Status:         TaskStatusStarted,
		SubUnits: map[string]*SubUnit{
			"su-1": {Status: SubUnitStatusPending},
			"su-2": {Status: SubUnitStatusPending, NodeID: "node2"},
		},
	}

	handle, err := provider.StartTask(task)
	require.NoError(t, err)
	defer handle.Terminate()

	require.Eventually(t, func() bool {
		return len(recorder.getCompleted()) == 1
	}, 5*time.Second, 50*time.Millisecond)

	completed := recorder.getCompleted()
	assert.ElementsMatch(t, []string{"su-1"}, completed,
		"only su-1 should be processed in synthetic mode (su-2 belongs to node2)")
}

func TestShardNoopProvider_LegacyTask_NoSubUnits(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	recorder := newMockRecorder()

	provider := NewShardNoopProvider("node1", logger, nil)
	provider.SetCompletionRecorder(recorder)

	task := &Task{
		TaskDescriptor: TaskDescriptor{ID: "test-task", Version: 1},
		Namespace:      ShardNoopProviderNamespace,
		Status:         TaskStatusStarted,
	}

	handle, err := provider.StartTask(task)
	require.NoError(t, err)
	defer handle.Terminate()

	require.Eventually(t, func() bool {
		recorder.mu.Lock()
		defer recorder.mu.Unlock()
		return recorder.nodesDone
	}, 5*time.Second, 50*time.Millisecond)
}

func TestShardNoopProvider_OnSubUnitsCompleted(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	provider := NewShardNoopProvider("node1", logger, nil)

	task := &Task{
		TaskDescriptor: TaskDescriptor{ID: "test-task", Version: 1},
		Namespace:      ShardNoopProviderNamespace,
	}

	provider.OnSubUnitsCompleted(task, []string{"su-1", "su-2"})

	finalized := provider.GetFinalizedSubUnits(task.TaskDescriptor)
	assert.ElementsMatch(t, []string{"su-1", "su-2"}, finalized)
}

func TestShardNoopProvider_OnTaskCompleted(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	provider := NewShardNoopProvider("node1", logger, nil)

	task := &Task{
		TaskDescriptor: TaskDescriptor{ID: "test-task", Version: 1},
		Namespace:      ShardNoopProviderNamespace,
	}

	assert.False(t, provider.IsTaskCompleted(task.TaskDescriptor))
	provider.OnTaskCompleted(task)
	assert.True(t, provider.IsTaskCompleted(task.TaskDescriptor))
}

func TestShardNoopProvider_TaskLifecycle(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	provider := NewShardNoopProvider("node1", logger, nil)

	desc := TaskDescriptor{ID: "test-task", Version: 1}

	assert.Empty(t, provider.GetLocalTasks())

	recorder := newMockRecorder()
	provider.SetCompletionRecorder(recorder)

	task := &Task{
		TaskDescriptor: desc,
		Namespace:      ShardNoopProviderNamespace,
		Status:         TaskStatusStarted,
	}

	handle, err := provider.StartTask(task)
	require.NoError(t, err)
	defer handle.Terminate()

	assert.Equal(t, []TaskDescriptor{desc}, provider.GetLocalTasks())

	require.NoError(t, provider.CleanupTask(desc))
	assert.Empty(t, provider.GetLocalTasks())
}
