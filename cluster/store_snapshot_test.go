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
	"github.com/weaviate/weaviate/cluster/utils"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// TestSchemaSnapshotPersistAndRestore tests snapshot persistence and restoration
func TestSchemaSnapshotPersistAndRestore(t *testing.T) {
	// Setup test schema data directly using the Store's Apply method
	source := NewMockStore(t, "source-node", utils.MustGetFreeTCPPort())
	setupTestSchema(t, source)
	snapshotSink := &mocks.SnapshotSink{
		Buffer: bytes.NewBuffer(nil),
	}

	snapshot, err := source.store.Snapshot()
	assert.NoError(t, err)

	err = snapshot.Persist(snapshotSink)
	assert.NoError(t, err)

	target := NewMockStore(t, "target-node", utils.MustGetFreeTCPPort())
	target.store.init()
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
	source := NewMockStore(t, "empty-source-node", utils.MustGetFreeTCPPort())

	snapshotSink := &mocks.SnapshotSink{
		Buffer: bytes.NewBuffer(nil),
	}

	snapshot, err := source.store.Snapshot()
	assert.NoError(t, err)

	err = snapshot.Persist(snapshotSink)
	assert.NoError(t, err)

	target := NewMockStore(t, "empty-target-node", utils.MustGetFreeTCPPort())
	target.store.init()

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
	source := NewMockStore(t, "error-source-node", utils.MustGetFreeTCPPort())

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
	source := NewMockStore(t, "restore-error-source-node", utils.MustGetFreeTCPPort())
	setupTestSchema(t, source)

	snapshotSink := &mocks.SnapshotSink{
		Buffer: bytes.NewBuffer(nil),
	}

	snapshot, err := source.store.Snapshot()
	assert.NoError(t, err)

	err = snapshot.Persist(snapshotSink)
	assert.NoError(t, err)

	target := NewMockStore(t, "restore-error-target-node", utils.MustGetFreeTCPPort())
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

// TestConcurrentSnapshotOperations tests the thread safety of snapshot operations
// when performed concurrently
func TestConcurrentSnapshotOperations(t *testing.T) {
	source := NewMockStore(t, "concurrent-snapshot-node", utils.MustGetFreeTCPPort())
	setupTestSchema(t, source)

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
