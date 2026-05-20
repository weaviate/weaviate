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
	"errors"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/weaviate/weaviate/cluster/proto/api"
	clusterschema "github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestStore_Apply_LogTypes(t *testing.T) {
	ms, _ := setupApplyTest(t)

	tests := []struct {
		name     string
		logType  raft.LogType
		expected bool
	}{
		{
			name:     "Valid LogCommand type",
			logType:  raft.LogCommand,
			expected: true,
		},
		{
			name:     "LogNoop type",
			logType:  raft.LogNoop,
			expected: true, // Noop logs are valid but don't do anything
		},
		{
			name:     "LogBarrier type",
			logType:  raft.LogBarrier,
			expected: true, // Barrier logs are valid but don't do anything
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := &raft.Log{
				Index: 1,
				Type:  tt.logType,
				Data:  []byte{},
			}

			result := ms.store.Apply(log)
			resp, ok := result.(Response)
			assert.True(t, ok)
			assert.NoError(t, resp.Error) // All log types return no error, but LogCommand is the only one that processes data

			// Verify all mock expectations
			ms.parser.AssertExpectations(t)
			ms.indexer.AssertExpectations(t)
		})
	}
}

func TestStore_Apply_CommandTypes(t *testing.T) {
	// Create test data that will be reused
	cls := &models.Class{
		Class: "TestClass",
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: true,
		},
	}

	ss := &sharding.State{
		Physical: map[string]sharding.Physical{
			"T1": {
				Name:           "T1",
				BelongsToNodes: []string{"Node-1"},
				Status:         "HOT",
			},
		},
	}

	tests := []struct {
		name        string
		cmdType     api.ApplyRequest_Type
		setupMocks  func(MockStore)
		expectError bool
		cmdData     interface{}
		preApply    func(MockStore) // Function to run before applying the command
	}{
		{
			name:    "AddClass command",
			cmdType: api.ApplyRequest_TYPE_ADD_CLASS,
			setupMocks: func(ms MockStore) {
				ms.parser.On("ParseClass", mock.Anything).Return(nil)
				ms.indexer.On("AddClass", mock.Anything).Return(nil)
				ms.indexer.On("TriggerSchemaUpdateCallbacks").Return()
			},
			expectError: false,
			cmdData:     api.AddClassRequest{Class: cls, State: ss},
		},
		{
			name:    "UpdateClass command",
			cmdType: api.ApplyRequest_TYPE_UPDATE_CLASS,
			setupMocks: func(ms MockStore) {
				// For UpdateClass, we need to set up ParseClassUpdate
				// Note: ParseClass is called by the schema manager during update
				ms.parser.On("ParseClass", mock.Anything).Return(nil)
				ms.parser.On("ParseClassUpdate", mock.Anything, mock.Anything).Return(cls, nil)
				ms.indexer.On("UpdateClass", mock.Anything).Return(nil)
				ms.indexer.On("TriggerSchemaUpdateCallbacks").Return()
			},
			expectError: false,
			cmdData:     api.UpdateClassRequest{Class: cls, State: ss},
			preApply: func(ms MockStore) {
				// First add the class so it exists for update
				addLog := &raft.Log{
					Index: 1,
					Type:  raft.LogCommand,
					Data:  cmdAsBytes("TestClass", api.ApplyRequest_TYPE_ADD_CLASS, api.AddClassRequest{Class: cls, State: ss}, nil),
				}
				// Set up separate mock expectations for the add operation
				ms.parser.On("ParseClass", mock.Anything).Return(nil)
				ms.indexer.On("AddClass", mock.Anything).Return(nil)
				ms.indexer.On("TriggerSchemaUpdateCallbacks").Return()
				ms.store.Apply(addLog)
				// Reset mock expectations after add operation
				ms.parser.ExpectedCalls = nil
				ms.indexer.ExpectedCalls = nil
			},
		},
		{
			name:    "DeleteClass command",
			cmdType: api.ApplyRequest_TYPE_DELETE_CLASS,
			setupMocks: func(ms MockStore) {
				ms.indexer.On("DeleteClass", mock.Anything).Return(nil)
				ms.indexer.On("TriggerSchemaUpdateCallbacks").Return()
				ms.replicationFSM.On("DeleteReplicationsByCollection", mock.Anything).Return(nil)
			},
			expectError: false,
			cmdData:     nil,
		},
		{
			name:    "Unknown command type",
			cmdType: api.ApplyRequest_Type(999), // Non-existent type
			setupMocks: func(ms MockStore) {
				// No mocks needed for unknown type
			},
			expectError: false, // Unknown commands don't return errors, they just log
			cmdData:     nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ms, log := setupApplyTest(t)

			// Run pre-apply setup if needed
			if tt.preApply != nil {
				tt.preApply(ms)
			}

			// Set up mocks after pre-apply to ensure they're not affected by pre-apply operations
			tt.setupMocks(ms)

			// Update log with test command type and data
			log.Data = cmdAsBytes("TestClass", tt.cmdType, tt.cmdData, nil)

			result := ms.store.Apply(log)
			resp, ok := result.(Response)
			assert.True(t, ok)

			if tt.expectError {
				assert.Error(t, resp.Error)
			} else {
				assert.NoError(t, resp.Error)
			}

			// Verify all mock expectations
			ms.parser.AssertExpectations(t)
			ms.indexer.AssertExpectations(t)
		})
	}
}

func TestStore_Apply_CatchingUp(t *testing.T) {
	tests := []struct {
		name       string
		logIndex   uint64
		schemaOnly bool
	}{
		{
			name:       "Catching up (index <= lastAppliedIndexToDB)",
			logIndex:   50,
			schemaOnly: true,
		},
		{
			name:       "Caught up (index > lastAppliedIndexToDB)",
			logIndex:   150,
			schemaOnly: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ms, log := setupApplyTest(t)
			// Set lastAppliedIndexToDB to simulate catching up scenario
			ms.store.lastAppliedIndexToDB.Store(uint64(100))
			log.Index = tt.logIndex

			// Setup mock to verify schemaOnly parameter
			ms.parser.On("ParseClass", mock.Anything).Return(nil)
			if !tt.schemaOnly {
				ms.indexer.On("AddClass", mock.Anything).Return(nil)
				ms.indexer.On("TriggerSchemaUpdateCallbacks").Return()
			}

			// The snapshot store is now properly initialized in setupApplyTest
			result := ms.store.Apply(log)
			resp, ok := result.(Response)
			assert.True(t, ok)
			assert.NoError(t, resp.Error)

			// Verify schemaOnly was set correctly by checking if the class was added
			class := ms.store.SchemaReader().ReadOnlyClass("TestClass")
			if tt.schemaOnly {
				assert.NotNil(t, class, "Class should be added in schema-only mode")
			} else {
				assert.NotNil(t, class, "Class should be added in full mode")
			}

			// Verify all mock expectations
			ms.parser.AssertExpectations(t)
			ms.indexer.AssertExpectations(t)
		})
	}
}

func TestStore_Apply_ReloadDB(t *testing.T) {
	// runFirstApplyTriggeringReload: shared phase-1 setup. Seeds the store
	// with lastAppliedIndexToDB=100, then applies at index 150 to trigger
	// the DB-reload path (which resets lastAppliedIndexToDB to 0).
	runFirstApplyTriggeringReload := func(t *testing.T) (MockStore, *raft.Log) {
		t.Helper()
		ms, log := setupApplyTest(t)
		ms.store.lastAppliedIndexToDB.Store(100)
		log.Index = 150 // Greater than lastAppliedIndexToDB → triggers reload

		ms.parser.On("ParseClass", mock.Anything).Return(nil)
		ms.indexer.On("AddClass", mock.Anything).Return(nil)
		ms.indexer.On("TriggerSchemaUpdateCallbacks").Return()

		result := ms.store.Apply(log)
		resp, ok := result.(Response)
		assert.True(t, ok)
		assert.NoError(t, resp.Error)
		// lastAppliedIndexToDB must be reset to 0 once the reload runs.
		assert.Equal(t, uint64(0), ms.store.lastAppliedIndexToDB.Load())
		return ms, log
	}

	t.Run("Reload DB when caught up", func(t *testing.T) {
		ms, _ := runFirstApplyTriggeringReload(t)
		// Verify all mock expectations
		ms.parser.AssertExpectations(t)
		ms.indexer.AssertExpectations(t)
	})

	t.Run("No reload on subsequent higher indices", func(t *testing.T) {
		ms, log := runFirstApplyTriggeringReload(t)

		// Reset mocks for second apply
		ms.parser.ExpectedCalls = nil
		ms.indexer.ExpectedCalls = nil

		// Create a different class for the second apply
		cls2 := &models.Class{
			Class: "TestClass2",
			MultiTenancyConfig: &models.MultiTenancyConfig{
				Enabled: true,
			},
		}

		// Create sharding state for the second class
		ss2 := &sharding.State{
			Physical: map[string]sharding.Physical{
				"T1": {
					Name:           "T1",
					BelongsToNodes: []string{"Node-1"},
					Status:         "HOT",
				},
			},
		}

		// Setup mocks for second apply
		ms.parser.On("ParseClass", mock.Anything).Return(nil)
		ms.indexer.On("AddClass", mock.Anything).Return(nil)
		ms.indexer.On("TriggerSchemaUpdateCallbacks").Return()

		// Second apply with higher index should not trigger reload
		log.Index = 200
		log.Data = cmdAsBytes("TestClass2", api.ApplyRequest_TYPE_ADD_CLASS, api.AddClassRequest{Class: cls2, State: ss2}, nil)
		result := ms.store.Apply(log)
		resp, ok := result.(Response)
		assert.True(t, ok)
		assert.NoError(t, resp.Error)

		// Verify lastAppliedIndexToDB is still 0
		assert.Equal(t, uint64(0), ms.store.lastAppliedIndexToDB.Load())

		// Verify all mock expectations
		ms.parser.AssertExpectations(t)
		ms.indexer.AssertExpectations(t)
	})
}

func TestStore_Apply_Metrics(t *testing.T) {
	t.Run("Metrics are updated correctly", func(t *testing.T) {
		ms, log := setupApplyTest(t)

		// Setup mocks
		ms.parser.On("ParseClass", mock.Anything).Return(nil)
		ms.indexer.On("AddClass", mock.Anything).Return(nil)
		ms.indexer.On("TriggerSchemaUpdateCallbacks").Return()

		// Apply the log
		result := ms.store.Apply(log)
		resp, ok := result.(Response)
		assert.True(t, ok)
		assert.NoError(t, resp.Error)

		// Verify metrics were updated
		assert.Equal(t, float64(1), testutil.ToFloat64(ms.store.metrics.fsmLastAppliedIndex))
		assert.Equal(t, float64(1), testutil.ToFloat64(ms.store.metrics.raftLastAppliedIndex))
		assert.Equal(t, float64(0), testutil.ToFloat64(ms.store.metrics.applyFailures))

		// Verify all mock expectations
		ms.parser.AssertExpectations(t)
		ms.indexer.AssertExpectations(t)
	})
}

func TestStore_Apply_ErrorHandling(t *testing.T) {
	tests := []struct {
		name        string
		setupMocks  func(MockStore)
		expectError bool
		expectPanic bool
	}{
		{
			name: "Schema manager error",
			setupMocks: func(ms MockStore) {
				ms.parser.On("ParseClass", mock.Anything).Return(errors.New("schema error"))
			},
			expectError: true,
			expectPanic: false,
		},
		{
			name: "Invalid proto data",
			setupMocks: func(ms MockStore) {
				// No mocks needed, we'll modify the log data directly
			},
			expectError: true,
			expectPanic: true, // The Apply function panics on invalid proto data
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ms, log := setupApplyTest(t)
			tt.setupMocks(ms)

			if tt.name == "Invalid proto data" {
				log.Data = []byte("invalid proto data")
			}

			if tt.expectPanic {
				// Use assert.Panics to verify that the function panics as expected
				assert.PanicsWithValue(t, "error proto un-marshalling log data", func() {
					ms.store.Apply(log)
				})
				// For panic cases, we still want to verify mock expectations
				ms.parser.AssertExpectations(t)
				ms.indexer.AssertExpectations(t)
				return
			}

			result := ms.store.Apply(log)
			resp, ok := result.(Response)
			assert.True(t, ok)

			if tt.expectError {
				assert.Error(t, resp.Error)
				assert.Equal(t, float64(1), testutil.ToFloat64(ms.store.metrics.applyFailures))
			} else {
				assert.NoError(t, resp.Error)
				assert.Equal(t, float64(0), testutil.ToFloat64(ms.store.metrics.applyFailures))
			}

			// Verify all mock expectations
			ms.parser.AssertExpectations(t)
			ms.indexer.AssertExpectations(t)
		})
	}
}

func TestStore_Apply_DeleteClass_CaseInsensitive(t *testing.T) {
	// This test verifies the case-insensitive handling during RAFT log replay
	t.Run("Case-insensitive deletion during RAFT replay", func(t *testing.T) {
		// Use the existing setupApplyTest helper to avoid mock setup issues
		ms, _ := setupApplyTest(t)

		// Enable schemaOnly mode by setting lastAppliedIndexToDB high
		ms.store.lastAppliedIndexToDB.Store(100)

		// Create a class with mixed case
		cls := &models.Class{
			Class: "FooBar",
			MultiTenancyConfig: &models.MultiTenancyConfig{
				Enabled: true,
			},
		}

		// Step 1: Add class "FooBar"
		addLog := &raft.Log{
			Index: 1,
			Type:  raft.LogCommand,
			Data:  cmdAsBytes("FooBar", api.ApplyRequest_TYPE_ADD_CLASS, api.AddClassRequest{Class: cls, State: &sharding.State{}}, nil),
		}

		// Setup mocks for adding class (schemaOnly mode - no AddClass on database)
		ms.parser.On("ParseClass", mock.Anything).Return(nil)

		// Apply add operation
		result := ms.store.Apply(addLog)
		resp, ok := result.(Response)
		assert.True(t, ok)
		assert.NoError(t, resp.Error, "Failed to add FooBar class")

		// Verify FooBar exists in memory schema
		schemaReader := ms.store.SchemaReader()
		foundClass := schemaReader.ClassEqual("FooBar")
		assert.Equal(t, "FooBar", foundClass, "FooBar should exist in memory schema")

		// Step 2: Delete class "foobar" (different case) during RAFT replay
		deleteLog := &raft.Log{
			Index: 2,
			Type:  raft.LogCommand,
			Data:  cmdAsBytes("foobar", api.ApplyRequest_TYPE_DELETE_CLASS, nil, nil),
		}

		// Apply delete operation
		result = ms.store.Apply(deleteLog)
		resp, ok = result.(Response)
		assert.True(t, ok)
		assert.NoError(t, resp.Error, "Case-insensitive deletion should succeed")

		// Step 3: Verify that FooBar was deleted from memory schema
		// This is the key test - case-insensitive handling should have found and deleted FooBar
		foundClass = schemaReader.ClassEqual("FooBar")
		assert.Empty(t, foundClass, "FooBar should be deleted from memory schema after case-insensitive deletion")

		// Also verify case-insensitive search returns empty
		foundClassCaseInsensitive := schemaReader.ClassEqual("foobar")
		assert.Empty(t, foundClassCaseInsensitive, "Case-insensitive search should return empty after deletion")

		// Verify mock expectations
		ms.indexer.AssertExpectations(t)
		ms.parser.AssertExpectations(t)
	})
}

func setupApplyTest(t *testing.T) (MockStore, *raft.Log) {
	mockStore := NewMockStore(t, "Node-1", 0)
	mockStore.store.metrics = newStoreMetrics("Node-1", prometheus.NewPedanticRegistry())

	// Create a temporary directory for the snapshot store
	tmpDir := t.TempDir()
	snapshotStore, err := raft.NewFileSnapshotStore(tmpDir, 3, nil)
	if err != nil {
		t.Fatalf("failed to create snapshot store: %v", err)
	}
	mockStore.store.snapshotStore = snapshotStore

	// Create a basic class for testing
	cls := &models.Class{
		Class: "TestClass",
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: true,
		},
	}

	ss := &sharding.State{
		Physical: map[string]sharding.Physical{
			"T1": {
				Name:           "T1",
				BelongsToNodes: []string{"Node-1"},
				Status:         "HOT",
			},
		},
	}

	// Create a basic log entry
	log := &raft.Log{
		Index: 1,
		Type:  raft.LogCommand,
		Data:  cmdAsBytes("TestClass", api.ApplyRequest_TYPE_ADD_CLASS, api.AddClassRequest{Class: cls, State: ss}, nil),
	}

	// Initialize the schema manager with replication FSM
	mockStore.store.schemaManager = clusterschema.NewSchemaManager("Node-1", mockStore.indexer, mockStore.parser, prometheus.NewPedanticRegistry(), mockStore.logger)
	mockStore.store.schemaManager.SetReplicationFSM(mockStore.replicationFSM)

	return mockStore, log
}

func setupCascadeTestStore(t *testing.T, className string) (*MockStore, *raft.Log, *raft.Log) {
	t.Helper()
	ms := NewMockStore(t, "Node-1", 0)
	ms.store.metrics = newStoreMetrics("Node-1", prometheus.NewPedanticRegistry())

	tmpDir := t.TempDir()
	snapshotStore, err := raft.NewFileSnapshotStore(tmpDir, 3, nil)
	if err != nil {
		t.Fatalf("snapshot store: %v", err)
	}
	ms.store.snapshotStore = snapshotStore

	ms.indexer.On("Open", mock.Anything).Return(nil)
	ms.indexer.On("AddClass", mock.Anything).Return(nil)
	ms.indexer.On("DeleteClass", mock.Anything).Return(nil)
	ms.indexer.On("TriggerSchemaUpdateCallbacks").Return()
	ms.parser.On("ParseClass", mock.Anything).Return(nil)
	// Optional: skipped on schemaOnly catchup-replay (updateStore branch).
	ms.replicationFSM.On("DeleteReplicationsByCollection", mock.Anything).Return(nil).Maybe()

	cls := &models.Class{Class: className}
	state := &sharding.State{
		Physical: map[string]sharding.Physical{
			"T1": {Name: "T1", BelongsToNodes: []string{"Node-1"}, Status: "HOT"},
		},
	}
	addLog := &raft.Log{
		Index: 1,
		Type:  raft.LogCommand,
		Data: cmdAsBytes(className, api.ApplyRequest_TYPE_ADD_CLASS,
			api.AddClassRequest{Class: cls, State: state}, nil),
	}
	deleteLog := &raft.Log{
		Index: 2,
		Type:  raft.LogCommand,
		Data:  cmdAsBytes(className, api.ApplyRequest_TYPE_DELETE_CLASS, nil, nil),
	}
	return &ms, addLog, deleteLog
}

func applyOrFail(t *testing.T, ms *MockStore, log *raft.Log, label string) {
	t.Helper()
	r, ok := ms.store.Apply(log).(Response)
	if !ok || r.Error != nil {
		t.Fatalf("apply %s: ok=%v err=%v", label, ok, r.Error)
	}
}

// Pins the nil-cascade contract: a partial harness without a wired
// distributed-task manager must still apply DELETE_CLASS without panicking.
func TestStore_Apply_DeleteClass_NilCascadeIsSafe(t *testing.T) {
	ms, addLog, deleteLog := setupCascadeTestStore(t, "Bareback")

	ms.store.schemaManager.SetDistributedTaskManager(nil)

	applyOrFail(t, ms, addLog, "add-class")
	applyOrFail(t, ms, deleteLog, "delete-class with nil cascade")
}

// Pins weaviate/0-weaviate-issues#231 end-to-end through the FSM apply
// path: a same-name recreate must not inherit the prior incarnation's
// task records.
func TestStore_Apply_DeleteClass_CascadesToDistributedTasks(t *testing.T) {
	ms, addLog, deleteLog := setupCascadeTestStore(t, "Foo")

	ms.store.distributedTasksManager.RegisterCollectionExtractor(
		"test-namespace",
		func(payload []byte) (string, bool) {
			var p struct {
				Collection string `json:"collection"`
			}
			if err := json.Unmarshal(payload, &p); err != nil {
				return "", false
			}
			return p.Collection, p.Collection != ""
		},
	)

	// Raft indexes must be monotonic across the test (Store.Apply reads
	// l.Index for version/metrics/catchingUp): 1=add-class, 2..4=add-task,
	// 5=delete-class.
	addTaskAtIndex := func(t *testing.T, idx uint64, id string, collection string) {
		t.Helper()
		payloadBytes, err := json.Marshal(map[string]string{"collection": collection})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		result := ms.store.Apply(&raft.Log{
			Index: idx,
			Type:  raft.LogCommand,
			Data: cmdAsBytes("", api.ApplyRequest_TYPE_DISTRIBUTED_TASK_ADD,
				&api.AddDistributedTaskRequest{
					Namespace:             "test-namespace",
					Id:                    id,
					Payload:               payloadBytes,
					SubmittedAtUnixMillis: 1,
					UnitIds:               []string{"u-" + id},
				}, nil),
		})
		if resp, ok := result.(Response); !ok || resp.Error != nil {
			t.Fatalf("apply add-task %s: ok=%v err=%v", id, ok, resp.Error)
		}
	}

	applyOrFail(t, ms, addLog, "add-class")

	addTaskAtIndex(t, 2, "foo-1", "Foo")
	addTaskAtIndex(t, 3, "foo-2", "Foo")
	addTaskAtIndex(t, 4, "bar-1", "Bar")

	preTasks, err := ms.store.distributedTasksManager.ListDistributedTasks(context.Background())
	if err != nil {
		t.Fatalf("list pre: %v", err)
	}
	if got, want := len(preTasks["test-namespace"]), 3; got != want {
		t.Fatalf("pre-delete task count: got %d want %d", got, want)
	}

	deleteLog.Index = 5 // keep the index sequence monotonic
	applyOrFail(t, ms, deleteLog, "delete-class")

	postTasks, err := ms.store.distributedTasksManager.ListDistributedTasks(context.Background())
	if err != nil {
		t.Fatalf("list post: %v", err)
	}

	survivors := make([]string, 0)
	for _, ts := range postTasks["test-namespace"] {
		survivors = append(survivors, ts.ID)
	}
	if len(survivors) != 1 || survivors[0] != "bar-1" {
		t.Fatalf("post-delete survivors: got %v want [bar-1]", survivors)
	}
}

// Pins the schemaOnly=true catchup-replay path: a node restarting and
// replaying its RAFT log past lastAppliedIndexToDB hits updateSchema
// only, with updateStore skipped (cluster/store_apply.go:108). If the
// cascade lived in updateStore (as it did pre-ff3a199a39), DELETE_CLASS
// on replay would NOT remove tasks that earlier TYPE_DISTRIBUTED_TASK_ADD
// applies re-created — the exact resurrection shape QA Claude flagged
// on weaviate/weaviate#11345.
func TestStore_Apply_DeleteClass_CascadesOnSchemaOnlyReplay(t *testing.T) {
	ms, addLog, deleteLog := setupCascadeTestStore(t, "Foo")

	ms.store.distributedTasksManager.RegisterCollectionExtractor(
		"test-namespace",
		func(payload []byte) (string, bool) {
			var p struct {
				Collection string `json:"collection"`
			}
			if err := json.Unmarshal(payload, &p); err != nil {
				return "", false
			}
			return p.Collection, p.Collection != ""
		},
	)

	// Apply ADD_CLASS + ADD_TASK at low indexes, then set
	// lastAppliedIndexToDB above them and apply DELETE_CLASS at an
	// index ≤ lastAppliedIndexToDB. That forces catchingUp=true and
	// schemaOnly=true on the DELETE_CLASS apply.
	applyOrFail(t, ms, addLog, "add-class")

	payloadBytes, err := json.Marshal(map[string]string{"collection": "Foo"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	addTaskLog := &raft.Log{
		Index: 2,
		Type:  raft.LogCommand,
		Data: cmdAsBytes("", api.ApplyRequest_TYPE_DISTRIBUTED_TASK_ADD,
			&api.AddDistributedTaskRequest{
				Namespace:             "test-namespace",
				Id:                    "foo-1",
				Payload:               payloadBytes,
				SubmittedAtUnixMillis: 1,
				UnitIds:               []string{"u-foo-1"},
			}, nil),
	}
	applyOrFail(t, ms, addTaskLog, "add-task")

	// Force schemaOnly=true: any DELETE_CLASS apply with Index ≤
	// lastAppliedIndexToDB triggers the catchup branch.
	ms.store.lastAppliedIndexToDB.Store(100)
	deleteLog.Index = 50

	applyOrFail(t, ms, deleteLog, "delete-class on catchup replay")

	postTasks, err := ms.store.distributedTasksManager.ListDistributedTasks(context.Background())
	if err != nil {
		t.Fatalf("list post: %v", err)
	}
	if got := len(postTasks["test-namespace"]); got != 0 {
		t.Fatalf("post-replay-delete: got %d tasks, want 0 (cascade must fire even on schemaOnly apply)", got)
	}
}
