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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/mocks"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/rbac"
	"github.com/weaviate/weaviate/cluster/utils"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// TestSchemaSnapshotPersistAndRestore tests snapshot persistence and restoration
func TestSchemaSnapshotPersistAndRestore(t *testing.T) {
	// Setup test schema data directly using the Store's Apply method
	source, snapshotter := NewMockStoreWithSnapshotterExpectations(t, "source-node", utils.MustGetFreeTCPPort())
	setupTestSchema(t, source)
	snapshotter.On("SnapShot").Return(nil, nil)

	snapshotSink := &mocks.SnapshotSink{
		Buffer: bytes.NewBuffer(nil),
	}

	snapshot, err := source.store.Snapshot()
	assert.NoError(t, err)

	err = snapshot.Persist(snapshotSink)
	assert.NoError(t, err)

	target, snapshotter := NewMockStoreWithSnapshotterExpectations(t, "target-node", utils.MustGetFreeTCPPort())
	target.store.init()
	snapshotter.On("Restore", mock.Anything).Return(nil)

	target.parser.On("ParseClass", mock.Anything).Return(nil)
	target.indexer.On("TriggerSchemaUpdateCallbacks").Return()
	target.indexer.On("RestoreClassDir", mock.Anything).Return(nil)
	target.indexer.On("UpdateShardStatus", mock.Anything).Return(nil)
	target.indexer.On("AddClass", mock.Anything).Return(nil)

	snapshotReader := io.NopCloser(bytes.NewReader(snapshotSink.Buffer.Bytes()))

	err = target.store.Restore(snapshotReader)
	assert.NoError(t, err)

	verifySchemaRestoration(t, source, target)
}

// TestSchemaSnapshotEmptyStore tests snapshot persistence and restoration with an empty store
func TestSchemaSnapshotEmptyStore(t *testing.T) {
	source, snapshotter := NewMockStoreWithSnapshotterExpectations(t, "empty-source-node", utils.MustGetFreeTCPPort())
	snapshotter.On("SnapShot").Return(nil, nil)

	snapshotSink := &mocks.SnapshotSink{
		Buffer: bytes.NewBuffer(nil),
	}

	snapshot, err := source.store.Snapshot()
	assert.NoError(t, err)

	err = snapshot.Persist(snapshotSink)
	assert.NoError(t, err)

	target, snapshotter := NewMockStoreWithSnapshotterExpectations(t, "empty-target-node", utils.MustGetFreeTCPPort())
	target.store.init()
	snapshotter.On("Restore", mock.Anything).Return(nil)

	target.parser.On("ParseClass", mock.Anything).Return(nil)
	target.indexer.On("TriggerSchemaUpdateCallbacks").Return()

	snapshotReader := io.NopCloser(bytes.NewReader(snapshotSink.Buffer.Bytes()))

	err = target.store.Restore(snapshotReader)
	assert.NoError(t, err)

	assert.Equal(t, 0, source.store.SchemaReader().Len(), "Source schema should be empty")
	assert.Equal(t, 0, target.store.SchemaReader().Len(), "Target schema should be empty")
}

// TestSchemaSnapshotPersistError tests handling errors during snapshot persistence
func TestSchemaSnapshotPersistError(t *testing.T) {
	// Create source store with schema data
	source, snapshotter := NewMockStoreWithSnapshotterExpectations(t, "error-source-node", utils.MustGetFreeTCPPort())
	setupTestSchema(t, source)
	snapshotter.On("SnapShot").Return(nil, nil)

	errorSink := &mocks.SnapshotSink{
		Buffer:     bytes.NewBuffer(nil),
		WriteError: errors.New("simulated write error"),
	}

	snapshot, err := source.store.Snapshot()
	assert.NoError(t, err)

	// Persist the snapshot - should return an error
	err = snapshot.Persist(errorSink)
	assert.Error(t, err, "Expected an error during snapshot persistence")
	assert.Contains(t, err.Error(), "simulated write error", "Error should contain the specific write error")
}

// TestSchemaSnapshotRestoreError tests handling errors during snapshot restoration
func TestSchemaSnapshotRestoreError(t *testing.T) {
	// Create source store with schema data
	source, snapshotter := NewMockStoreWithSnapshotterExpectations(t, "restore-error-source-node", utils.MustGetFreeTCPPort())
	setupTestSchema(t, source)
	snapshotter.On("SnapShot").Return(nil, nil)

	snapshotSink := &mocks.SnapshotSink{
		Buffer: bytes.NewBuffer(nil),
	}

	snapshot, err := source.store.Snapshot()
	assert.NoError(t, err)

	err = snapshot.Persist(snapshotSink)
	assert.NoError(t, err)

	target, _ := NewMockStoreWithSnapshotterExpectations(t, "restore-error-target-node", utils.MustGetFreeTCPPort())
	target.store.init()

	snapshotReader := io.NopCloser(bytes.NewReader(snapshotSink.Buffer.Bytes()))

	// Restore from snapshot - should return an error
	target.parser.On("ParseClass", mock.Anything).Return(errors.New("simulated parse error"))
	err = target.store.Restore(snapshotReader)
	assert.Error(t, err, "Expected an error during snapshot restoration")
	assert.Contains(t, err.Error(), "simulated parse error", "Error should contain the specific parse error")
}

// TestSchemaSnapshotCorruptedData tests restoration from corrupted snapshot data
func TestSchemaSnapshotCorruptedData(t *testing.T) {
	target := NewMockStore(t, "corrupt-target-node", utils.MustGetFreeTCPPort())

	target.parser.On("ParseClass", mock.Anything).Return(nil)
	target.indexer.On("TriggerSchemaUpdateCallbacks").Return()

	corruptedData := bytes.NewBufferString(`{invalid`)
	snapshotReader := io.NopCloser(corruptedData)

	// Restore from snapshot - should return an error
	err := target.store.Restore(snapshotReader)
	assert.Error(t, err, "Expected an error when restoring from corrupted data")
}

// TestRBACSnapshotPersistAndRestore tests that RBAC policies are correctly persisted and restored
func TestRBACSnapshotPersistAndRestore(t *testing.T) {
	// Create source store with RBAC policies
	source := NewMockStore(t, "rbac-source-node", utils.MustGetFreeTCPPort())
	source.store.init()
	mockPolicy := [][]string{
		{"role1", "resource1", "action1"},
		{"role1", "resource2", "action2"},
		{"role2", "resource1", "action1"},
	}
	mockGroupingPolicy := [][]string{
		{"user1", "role1"},
		{"user2", "role2"},
	}

	mockSourceSnapshotter := &mocks.MockSnapshotter{
		Snapshot: &authorization.Snapshot{
			Policy:         mockPolicy,
			GroupingPolicy: mockGroupingPolicy,
		},
	}
	sourceManager := rbac.NewManager(nil, mockSourceSnapshotter, source.logger)
	source.store.authZManager = sourceManager

	snapshotSink := &mocks.SnapshotSink{
		Buffer: bytes.NewBuffer(nil),
	}

	snapshot, err := source.store.Snapshot()
	assert.NoError(t, err)

	err = snapshot.Persist(snapshotSink)
	assert.NoError(t, err)

	target := NewMockStore(t, "rbac-target-node", utils.MustGetFreeTCPPort())
	target.store.init()

	target.parser.On("ParseClass", mock.Anything).Return(nil)
	target.indexer.On("TriggerSchemaUpdateCallbacks").Return()

	mockTargetSnapshotter := &mocks.MockSnapshotter{}
	targetManager := rbac.NewManager(nil, mockTargetSnapshotter, target.logger)
	target.store.authZManager = targetManager

	snapshotReader := io.NopCloser(bytes.NewReader(snapshotSink.Buffer.Bytes()))

	err = target.store.Restore(snapshotReader)
	assert.NoError(t, err)

	var decodedSnapshot FSMSnapshot
	err = json.NewDecoder(bytes.NewReader(snapshotSink.Buffer.Bytes())).Decode(&decodedSnapshot)
	assert.NoError(t, err, "Should decode snapshot JSON without error")

	assert.NotNil(t, decodedSnapshot.RBAC, "Snapshot should contain RBAC data")

	assert.Equal(t, mockPolicy, decodedSnapshot.RBAC.Policy,
		"RBAC snapshot should contain the expected policy data")
	assert.Equal(t, mockGroupingPolicy, decodedSnapshot.RBAC.GroupingPolicy,
		"RBAC snapshot should contain the expected grouping policy data")

	assert.True(t, mockTargetSnapshotter.RestoreCalled,
		"Restore method should have been called on the snapshotter")
}

// TestRBACSnapshotPersistError tests handling errors during RBAC snapshot persistence
func TestRBACSnapshotPersistError(t *testing.T) {
	source := NewMockStore(t, "rbac-persist-error-node", utils.MustGetFreeTCPPort())

	mockSourceSnapshotter := &mocks.MockSnapshotter{
		SnapshotError: errors.New("simulated RBAC snapshot error"),
	}

	sourceManager := rbac.NewManager(nil, mockSourceSnapshotter, source.logger)
	source.store.authZManager = sourceManager

	snapshotSink := &mocks.SnapshotSink{
		Buffer: bytes.NewBuffer(nil),
	}

	snapshot, err := source.store.Snapshot()
	assert.NoError(t, err)

	// Persist the snapshot - should return the error from our mock snapshotter
	err = snapshot.Persist(snapshotSink)
	assert.Error(t, err, "Expected an error during snapshot persistence")
	assert.Contains(t, err.Error(), "simulated RBAC snapshot error",
		"Error should contain the specific error message from the RBAC snapshotter")
}

// TestRBACSnapshotRestoreError tests handling errors during RBAC snapshot restoration
func TestRBACSnapshotRestoreError(t *testing.T) {
	source := NewMockStore(t, "rbac-restore-error-source-node", utils.MustGetFreeTCPPort())
	mockPolicy := [][]string{
		{"role1", "resource1", "action1"},
		{"role1", "resource2", "action2"},
	}
	mockGroupingPolicy := [][]string{
		{"user1", "role1"},
	}

	mockSourceSnapshotter := &mocks.MockSnapshotter{
		Snapshot: &authorization.Snapshot{
			Policy:         mockPolicy,
			GroupingPolicy: mockGroupingPolicy,
		},
	}

	sourceManager := rbac.NewManager(nil, mockSourceSnapshotter, source.logger)
	source.store.authZManager = sourceManager

	snapshotSink := &mocks.SnapshotSink{
		Buffer: bytes.NewBuffer(nil),
	}

	snapshot, err := source.store.Snapshot()
	assert.NoError(t, err)

	err = snapshot.Persist(snapshotSink)
	assert.NoError(t, err)

	target := NewMockStore(t, "rbac-restore-error-target-node", utils.MustGetFreeTCPPort())
	target.store.init()

	target.parser.On("ParseClass", mock.Anything).Return(nil)
	target.indexer.On("TriggerSchemaUpdateCallbacks").Return()

	mockTargetSnapshotter := &mocks.MockSnapshotter{
		RestoreError: errors.New("simulated RBAC restore error"),
	}

	targetManager := rbac.NewManager(nil, mockTargetSnapshotter, target.logger)
	target.store.authZManager = targetManager

	snapshotReader := io.NopCloser(bytes.NewReader(snapshotSink.Buffer.Bytes()))

	err = target.store.Restore(snapshotReader)
	assert.Error(t, err, "Expected an error during snapshot restoration")
	assert.Contains(t, err.Error(), "simulated RBAC restore error",
		"Error should contain the specific error message from the RBAC snapshotter")

	// Verify that the Restore method was called despite the error
	assert.True(t, mockTargetSnapshotter.RestoreCalled,
		"Restore method should have been called on the snapshotter")
}

// TestCombinedSchemaRBACSnapshot tests both schema and RBAC snapshot persistence and restoration
func TestCombinedSchemaRBACSnapshot(t *testing.T) {
	// Create source store with both schema and RBAC data
	source := NewMockStore(t, "combined-source-node", utils.MustGetFreeTCPPort())
	setupTestSchema(t, source)

	mockPolicy := [][]string{
		{"admin", "schema/collections/*", "manage"},
		{"user", "schema/collections/Product", "read"},
	}
	mockGroupingPolicy := [][]string{
		{"john", "admin"},
		{"jane", "user"},
	}

	mockSourceSnapshotter := &mocks.MockSnapshotter{
		Snapshot: &authorization.Snapshot{
			Policy:         mockPolicy,
			GroupingPolicy: mockGroupingPolicy,
		},
	}

	sourceManager := rbac.NewManager(nil, mockSourceSnapshotter, source.logger)
	source.store.authZManager = sourceManager

	snapshotSink := &mocks.SnapshotSink{
		Buffer: bytes.NewBuffer(nil),
	}

	snapshot, err := source.store.Snapshot()
	assert.NoError(t, err)

	err = snapshot.Persist(snapshotSink)
	assert.NoError(t, err)

	target := NewMockStore(t, "combined-target-node", utils.MustGetFreeTCPPort())
	target.store.init()

	target.parser.On("ParseClass", mock.Anything).Return(nil)
	target.indexer.On("TriggerSchemaUpdateCallbacks").Return()
	target.indexer.On("RestoreClassDir", mock.Anything).Return(nil)
	target.indexer.On("UpdateShardStatus", mock.Anything).Return(nil)
	target.indexer.On("AddClass", mock.Anything).Return(nil)

	mockTargetSnapshotter := &mocks.MockSnapshotter{}

	targetManager := rbac.NewManager(nil, mockTargetSnapshotter, target.logger)
	target.store.authZManager = targetManager

	snapshotReader := io.NopCloser(bytes.NewReader(snapshotSink.Buffer.Bytes()))

	err = target.store.Restore(snapshotReader)
	assert.NoError(t, err)

	var decodedSnapshot FSMSnapshot
	err = json.NewDecoder(bytes.NewReader(snapshotSink.Buffer.Bytes())).Decode(&decodedSnapshot)
	assert.NoError(t, err, "Should decode snapshot JSON without error")

	assert.NotEmpty(t, decodedSnapshot.Classes, "Schema classes should exist in the snapshot")
	assert.NotNil(t, decodedSnapshot.RBAC, "RBAC snapshot should exist")

	verifySchemaRestoration(t, source, target)

	// Verify RBAC snapshot data
	assert.Equal(t, mockPolicy, decodedSnapshot.RBAC.Policy,
		"RBAC snapshot should contain the expected policy data")
	assert.Equal(t, mockGroupingPolicy, decodedSnapshot.RBAC.GroupingPolicy,
		"RBAC snapshot should contain the expected grouping policy data")

	// Verify that the Restore method was called on the target RBAC snapshotter
	assert.True(t, mockTargetSnapshotter.RestoreCalled,
		"Restore method should have been called on the RBAC snapshotter")
}

// TestRBACEmptySnapshot tests snapshot persistence and restoration with empty RBAC policies
func TestRBACEmptySnapshot(t *testing.T) {
	source := NewMockStore(t, "rbac-empty-source-node", utils.MustGetFreeTCPPort())
	source.store.init()
	mockSourceSnapshotter := &mocks.MockSnapshotter{
		Snapshot: &authorization.Snapshot{
			Policy:         [][]string{},
			GroupingPolicy: [][]string{},
		},
	}

	sourceManager := rbac.NewManager(nil, mockSourceSnapshotter, source.logger)
	source.store.authZManager = sourceManager

	snapshotSink := &mocks.SnapshotSink{
		Buffer: bytes.NewBuffer(nil),
	}

	snapshot, err := source.store.Snapshot()
	assert.NoError(t, err)

	err = snapshot.Persist(snapshotSink)
	assert.NoError(t, err)

	target := NewMockStore(t, "rbac-empty-target-node", utils.MustGetFreeTCPPort())
	target.store.init()

	target.indexer.On("TriggerSchemaUpdateCallbacks").Return()
	mockTargetSnapshotter := &mocks.MockSnapshotter{}

	targetManager := rbac.NewManager(nil, mockTargetSnapshotter, target.logger)
	target.store.authZManager = targetManager

	snapshotReader := io.NopCloser(bytes.NewReader(snapshotSink.Buffer.Bytes()))

	err = target.store.Restore(snapshotReader)
	assert.NoError(t, err)

	// Verify that Restore was called with empty policies
	assert.True(t, mockTargetSnapshotter.RestoreCalled,
		"Restore method should have been called on the snapshotter")

	// If the snapshotter populated the RestoredSnapshot field, check it
	if mockTargetSnapshotter.RestoredSnapshot != nil {
		assert.Empty(t, mockTargetSnapshotter.RestoredSnapshot.Policy,
			"Restored policy should be empty")
		assert.Empty(t, mockTargetSnapshotter.RestoredSnapshot.GroupingPolicy,
			"Restored grouping policy should be empty")
	}
}

// TestSnapshotEncodingDecoding tests that the snapshot encoding and decoding matches
func TestSnapshotEncodingDecoding(t *testing.T) {
	source := NewMockStore(t, "encoding-test-node", utils.MustGetFreeTCPPort())
	setupTestSchema(t, source)

	mockPolicy := [][]string{
		{"admin", "schema/*", "manage"},
		{"user", "objects/*", "read"},
	}
	mockGroupingPolicy := [][]string{
		{"user1", "admin"},
		{"user2", "user"},
	}

	mockSourceSnapshotter := &mocks.MockSnapshotter{
		Snapshot: &authorization.Snapshot{
			Policy:         mockPolicy,
			GroupingPolicy: mockGroupingPolicy,
		},
	}
	sourceManager := rbac.NewManager(nil, mockSourceSnapshotter, source.logger)
	source.store.authZManager = sourceManager

	snapshotSink := &mocks.SnapshotSink{
		Buffer: bytes.NewBuffer(nil),
	}
	snapshot, err := source.store.Snapshot()
	assert.NoError(t, err)
	err = snapshot.Persist(snapshotSink)
	assert.NoError(t, err)

	// Now decode the snapshot
	var decodedSnapshot FSMSnapshot
	err = json.NewDecoder(bytes.NewReader(snapshotSink.Buffer.Bytes())).Decode(&decodedSnapshot)
	assert.NoError(t, err, "Should decode snapshot JSON without error")

	// Verify the structure is as expected
	assert.NotEmpty(t, decodedSnapshot.NodeID, "NodeID should be populated")
	assert.NotEmpty(t, decodedSnapshot.SnapshotID, "SnapshotID should be populated")
	assert.NotEmpty(t, decodedSnapshot.Classes, "Classes should be populated")
	assert.NotNil(t, decodedSnapshot.RBAC, "RBAC should be present")

	// Verify RBAC data
	assert.Equal(t, mockPolicy, decodedSnapshot.RBAC.Policy,
		"RBAC policy should match expected data")
	assert.Equal(t, mockGroupingPolicy, decodedSnapshot.RBAC.GroupingPolicy,
		"RBAC grouping policy should match expected data")

	// Verify this is actually a valid snapshot by restoring it
	target := NewMockStore(t, "decoding-test-target", utils.MustGetFreeTCPPort())
	target.store.init()
	target.parser.On("ParseClass", mock.Anything).Return(nil)
	target.indexer.On("TriggerSchemaUpdateCallbacks").Return()
	target.indexer.On("RestoreClassDir", mock.Anything).Return(nil)
	target.indexer.On("UpdateShardStatus", mock.Anything).Return(nil)
	target.indexer.On("AddClass", mock.Anything).Return(nil)

	mockTargetSnapshotter := &mocks.MockSnapshotter{}
	targetManager := rbac.NewManager(nil, mockTargetSnapshotter, target.logger)
	target.store.authZManager = targetManager

	snapshotReader := io.NopCloser(bytes.NewReader(snapshotSink.Buffer.Bytes()))
	err = target.store.Restore(snapshotReader)
	assert.NoError(t, err, "Restored snapshot should be valid")
}

// TestConcurrentSnapshotOperations tests the thread safety of snapshot operations
// when performed concurrently
func TestConcurrentSnapshotOperations(t *testing.T) {
	source := NewMockStore(t, "concurrent-snapshot-node", utils.MustGetFreeTCPPort())
	setupTestSchema(t, source)

	mockPolicy := [][]string{
		{"admin", "schema/*", "manage"},
		{"user", "objects/*", "read"},
	}
	mockGroupingPolicy := [][]string{
		{"user1", "admin"},
		{"user2", "user"},
	}

	mockSourceSnapshotter := &mocks.MockSnapshotter{
		Snapshot: &authorization.Snapshot{
			Policy:         mockPolicy,
			GroupingPolicy: mockGroupingPolicy,
		},
	}
	sourceManager := rbac.NewManager(nil, mockSourceSnapshotter, source.logger)
	source.store.authZManager = sourceManager

	const numGoroutines = 5
	const iterations = 10

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 3) // For persistence, restoration, and reads

	persistErrors := make(chan error, numGoroutines*iterations)
	restoreErrors := make(chan error, numGoroutines*iterations)
	readErrors := make(chan error, numGoroutines*iterations)

	// Test concurrent Snapshot and Persist operations
	for i := 0; i < numGoroutines; i++ {
		go func(routineNum int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				// Create a unique sink for each iteration
				sink := &mocks.SnapshotSink{
					Buffer: bytes.NewBuffer(nil),
				}
				// Create and persist snapshot
				snapshot, err := source.store.Snapshot()
				if err != nil {
					persistErrors <- fmt.Errorf("routine %d, iteration %d, snapshot creation: %w", routineNum, j, err)
					continue
				}
				err = snapshot.Persist(sink)
				if err != nil {
					persistErrors <- fmt.Errorf("routine %d, iteration %d, snapshot persist: %w", routineNum, j, err)
				}
				time.Sleep(time.Millisecond) // Small delay to increase chance of concurrency issues
			}
		}(i)
	}

	baseSnapshotSink := &mocks.SnapshotSink{
		Buffer: bytes.NewBuffer(nil),
	}
	baseSnapshot, err := source.store.Snapshot()
	require.NoError(t, err)
	err = baseSnapshot.Persist(baseSnapshotSink)
	require.NoError(t, err)
	baseSnapshotBytes := baseSnapshotSink.Buffer.Bytes()

	for i := 0; i < numGoroutines; i++ {
		go func(routineNum int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				// Create a target store for restoration
				target := NewMockStore(t, fmt.Sprintf("concurrent-restore-%d-%d", routineNum, j), utils.MustGetFreeTCPPort())
				target.store.init()

				// Set up mocks for restoration
				target.parser.On("ParseClass", mock.Anything).Return(nil)
				target.indexer.On("TriggerSchemaUpdateCallbacks").Return()
				target.indexer.On("RestoreClassDir", mock.Anything).Return(nil)
				target.indexer.On("UpdateShardStatus", mock.Anything).Return(nil)
				target.indexer.On("AddClass", mock.Anything).Return(nil)

				// Create a mock RBAC snapshotter
				mockTargetSnapshotter := &mocks.MockSnapshotter{}
				targetManager := rbac.NewManager(nil, mockTargetSnapshotter, target.logger)
				target.store.authZManager = targetManager

				// Create a new reader for each restoration to avoid position issues
				snapshotReader := io.NopCloser(bytes.NewReader(baseSnapshotBytes))

				// Restore from snapshot
				err := target.store.Restore(snapshotReader)
				if err != nil {
					restoreErrors <- fmt.Errorf("routine %d, iteration %d, restore: %w", routineNum, j, err)
					continue
				}

				// Validate that restoration was successful
				targetSchema := target.store.SchemaReader()
				if targetSchema.Len() == 0 {
					restoreErrors <- fmt.Errorf("routine %d, iteration %d: schema is empty after restore", routineNum, j)
				}

				// Verify RBAC restoration
				if !mockTargetSnapshotter.RestoreCalled {
					restoreErrors <- fmt.Errorf("routine %d, iteration %d: RBAC snapshotter restore not called", routineNum, j)
				}

				time.Sleep(time.Millisecond) // Small delay
			}
		}(i)
	}

	// Test concurrent reads while snapshot operations are happening
	for i := 0; i < numGoroutines; i++ {
		go func(routineNum int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				// Perform read operations that would normally happen during snapshot operations
				schema := source.store.SchemaReader().ReadOnlySchema()

				// Check that the schema has expected classes
				if len(schema.Classes) < 2 { // We expect at least Product and Category
					readErrors <- fmt.Errorf("routine %d, iteration %d: schema has fewer classes than expected: %d",
						routineNum, j, len(schema.Classes))
				}

				// Try to take a snapshot while other operations are ongoing
				snap, err := source.store.Snapshot()
				if err != nil {
					readErrors <- fmt.Errorf("routine %d, iteration %d, snapshot during read: %w", routineNum, j, err)
				} else {
					// Just check it's not nil, don't persist it
					if snap == nil {
						readErrors <- fmt.Errorf("routine %d, iteration %d: snapshot is nil", routineNum, j)
					}
				}

				time.Sleep(time.Millisecond) // Small delay
			}
		}(i)
	}

	wg.Wait()
	close(persistErrors)
	close(restoreErrors)
	close(readErrors)

	errCount := 0
	for err := range persistErrors {
		t.Errorf("Persist error: %v", err)
		errCount++
	}
	for err := range restoreErrors {
		t.Errorf("Restore error: %v", err)
		errCount++
	}
	for err := range readErrors {
		t.Errorf("Read error: %v", err)
		errCount++
	}

	if errCount > 0 {
		t.Fatalf("Found %d errors during concurrent snapshot operations", errCount)
	}
}

func setupTestSchema(t *testing.T, ms MockStore) {
	// Set up mock behaviors
	ms.parser.On("ParseClass", mock.Anything).Return(nil)
	ms.indexer.On("AddClass", mock.Anything).Return(nil)
	ms.indexer.On("TriggerSchemaUpdateCallbacks").Return()

	productClass := &models.Class{
		Class: "Product",
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: true,
		},
	}

	shardingState := &sharding.State{
		Physical: map[string]sharding.Physical{
			"tenant1": {
				Name:           "tenant1",
				BelongsToNodes: []string{ms.cfg.NodeID},
				Status:         models.TenantActivityStatusHOT,
			},
			"tenant2": {
				Name:           "tenant2",
				BelongsToNodes: []string{ms.cfg.NodeID},
				Status:         models.TenantActivityStatusCOLD,
			},
		},
	}

	categoryClass := &models.Class{
		Class: "Category",
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: true,
		},
	}

	categoryShardingState := &sharding.State{
		Physical: map[string]sharding.Physical{
			"tenant3": {
				Name:           "tenant3",
				BelongsToNodes: []string{ms.cfg.NodeID},
				Status:         models.TenantActivityStatusHOT,
			},
		},
	}

	// Create mock raft logs to use with the Store's Apply method
	productLog := raft.Log{
		Data: cmdAsBytes("Product",
			cmd.ApplyRequest_TYPE_ADD_CLASS,
			cmd.AddClassRequest{
				Class: productClass,
				State: shardingState,
			}, nil),
	}

	categoryLog := raft.Log{
		Data: cmdAsBytes("Category",
			cmd.ApplyRequest_TYPE_ADD_CLASS,
			cmd.AddClassRequest{
				Class: categoryClass,
				State: categoryShardingState,
			}, nil),
	}

	// Apply the logs to add the classes
	ms.store.Apply(&productLog)
	ms.store.Apply(&categoryLog)

	// Verify classes were added
	assert.NotNil(t, ms.store.SchemaReader().ReadOnlyClass("Product"), "Product class should be added")
	assert.NotNil(t, ms.store.SchemaReader().ReadOnlyClass("Category"), "Category class should be added")
}

func verifySchemaRestoration(t *testing.T, source, target MockStore) {
	// Get source and target schema readers
	sourceSchema := source.store.SchemaReader()
	targetSchema := target.store.SchemaReader()

	// Verify classes count
	assert.Equal(t, sourceSchema.Len(), targetSchema.Len(), "Schema class count should match")

	// Get all classes via ReadOnlySchema
	sourceClasses := sourceSchema.ReadOnlySchema().Classes
	targetClasses := targetSchema.ReadOnlySchema().Classes

	// Verify class count
	assert.Equal(t, len(sourceClasses), len(targetClasses), "Number of classes should match")

	// Create a map for easier lookup of target classes
	targetClassMap := make(map[string]*models.Class)
	for _, class := range targetClasses {
		targetClassMap[class.Class] = class
	}

	// Verify class properties and configuration
	for _, sourceClass := range sourceClasses {
		targetClass, exists := targetClassMap[sourceClass.Class]
		assert.True(t, exists, "Class %s should exist in target", sourceClass.Class)

		if exists {
			// Compare properties
			assert.Equal(t, len(sourceClass.Properties), len(targetClass.Properties),
				"Number of properties should match for class %s", sourceClass.Class)

			// Compare vector configs
			assert.Equal(t, len(sourceClass.VectorConfig), len(targetClass.VectorConfig),
				"Vector config count should match for class %s", sourceClass.Class)

			// Compare sharding state
			sourceShardingState := sourceSchema.CopyShardingState(sourceClass.Class)
			targetShardingState := targetSchema.CopyShardingState(sourceClass.Class)

			if sourceShardingState != nil && targetShardingState != nil {
				assert.Equal(t, len(sourceShardingState.Physical), len(targetShardingState.Physical),
					"Number of tenants should match for class %s", sourceClass.Class)

				// Compare each tenant's configuration
				for tenantName, sourceTenant := range sourceShardingState.Physical {
					targetTenant, exists := targetShardingState.Physical[tenantName]
					assert.True(t, exists, "Tenant %s should exist in target for class %s", tenantName, sourceClass.Class)

					if exists {
						assert.Equal(t, sourceTenant.Status, targetTenant.Status,
							"Tenant status should match for %s in class %s", tenantName, sourceClass.Class)
						assert.Equal(t, len(sourceTenant.BelongsToNodes), len(targetTenant.BelongsToNodes),
							"Node count should match for tenant %s in class %s", tenantName, sourceClass.Class)
					}
				}
			}
		}
	}
}
