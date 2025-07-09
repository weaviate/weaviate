//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package cluster

import (
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
	t.Run("Reload DB when caught up", func(t *testing.T) {
		ms, log := setupApplyTest(t)
		// Set lastAppliedIndexToDB to trigger DB reload
		ms.store.lastAppliedIndexToDB.Store(100)
		log.Index = 150 // Greater than lastAppliedIndexToDB

		// Setup mocks
		ms.parser.On("ParseClass", mock.Anything).Return(nil)
		ms.indexer.On("AddClass", mock.Anything).Return(nil)
		ms.indexer.On("TriggerSchemaUpdateCallbacks").Return()

		result := ms.store.Apply(log)
		resp, ok := result.(Response)
		assert.True(t, ok)
		assert.NoError(t, resp.Error)

		// Verify lastAppliedIndexToDB was reset to 0
		assert.Equal(t, uint64(0), ms.store.lastAppliedIndexToDB.Load())

		// Verify all mock expectations
		ms.parser.AssertExpectations(t)
		ms.indexer.AssertExpectations(t)
	})

	t.Run("No reload on subsequent higher indices", func(t *testing.T) {
		ms, log := setupApplyTest(t)
		// Set lastAppliedIndexToDB to trigger initial DB reload
		ms.store.lastAppliedIndexToDB.Store(100)
		log.Index = 150 // Greater than lastAppliedIndexToDB

		// Setup mocks for first apply
		ms.parser.On("ParseClass", mock.Anything).Return(nil)
		ms.indexer.On("AddClass", mock.Anything).Return(nil)
		ms.indexer.On("TriggerSchemaUpdateCallbacks").Return()

		// First apply should trigger reload
		result := ms.store.Apply(log)
		resp, ok := result.(Response)
		assert.True(t, ok)
		assert.NoError(t, resp.Error)
		assert.Equal(t, uint64(0), ms.store.lastAppliedIndexToDB.Load())

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
		result = ms.store.Apply(log)
		resp, ok = result.(Response)
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
