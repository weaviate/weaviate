//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/replication"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestIndex_ObjectStorageSize_Comprehensive(t *testing.T) {
	ctx := context.Background()
	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()

	tests := []struct {
		name                   string
		className              string
		shardName              string
		objectCount            int
		objectSize             int // approximate size in bytes per object
		expectedObjectCount    int
		expectedStorageSizeMin int64 // minimum expected storage size
		expectedStorageSizeMax int64 // maximum expected storage size (allowing for overhead)
		setupData              bool
		description            string
	}{
		{
			name:        "empty shard",
			className:   "TestClass",
			shardName:   "test-shard-empty",
			setupData:   false,
			description: "Empty shard should have zero storage size",
		},
		{
			name:                   "shard with small objects",
			className:              "TestClass",
			shardName:              "test-shard-small",
			objectCount:            10,
			objectSize:             100, // ~100 bytes per object
			expectedObjectCount:    10,
			expectedStorageSizeMin: int64(10 * 100),     // minimum: just the data
			expectedStorageSizeMax: int64(10 * 100 * 5), // maximum: data + overhead (increased to 5x)
			setupData:              true,
			description:            "Shard with small objects should have proportional storage size",
		},
		{
			name:                   "shard with medium objects",
			className:              "TestClass",
			shardName:              "test-shard-medium",
			objectCount:            50,
			objectSize:             500, // ~500 bytes per object
			expectedObjectCount:    50,
			expectedStorageSizeMin: int64(50 * 500),     // minimum: just the data
			expectedStorageSizeMax: int64(50 * 500 * 3), // maximum: data + overhead
			setupData:              true,
			description:            "Shard with medium objects should have proportional storage size",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test class
			class := &models.Class{
				Class: tt.className,
				Properties: []*models.Property{
					{
						Name:         "name",
						DataType:     schema.DataTypeText.PropString(),
						Tokenization: models.PropertyTokenizationWhitespace,
					},
					{
						Name:         "description",
						DataType:     schema.DataTypeText.PropString(),
						Tokenization: models.PropertyTokenizationWhitespace,
					},
					{
						Name:     "count",
						DataType: schema.DataTypeInt.PropString(),
					},
				},
				InvertedIndexConfig: &models.InvertedIndexConfig{},
			}

			// Create fake schema
			fakeSchema := schema.Schema{
				Objects: &models.Schema{
					Classes: []*models.Class{class},
				},
			}

			// Create sharding state
			shardState := &sharding.State{
				Physical: map[string]sharding.Physical{
					tt.shardName: {
						Name:           tt.shardName,
						BelongsToNodes: []string{"test-node"},
						Status:         models.TenantActivityStatusHOT,
					},
				},
			}
			shardState.SetLocalName("test-node")

			// Create scheduler
			scheduler := queue.NewScheduler(queue.SchedulerOptions{
				Logger:  logger,
				Workers: 1,
			})

			// Create mock schema getter
			mockSchema := schemaUC.NewMockSchemaGetter(t)
			mockSchema.EXPECT().GetSchemaSkipAuth().Maybe().Return(fakeSchema)
			mockSchema.EXPECT().ReadOnlyClass(tt.className).Maybe().Return(class)
			mockSchema.EXPECT().CopyShardingState(tt.className).Maybe().Return(shardState)
			mockSchema.EXPECT().NodeName().Maybe().Return("test-node")
			mockSchema.EXPECT().ShardFromUUID("TestClass", mock.Anything).Return(tt.shardName).Maybe()
			mockSchema.EXPECT().ShardOwner(tt.className, tt.shardName).Maybe().Return("test-node", nil)

			// Create index
			index, err := NewIndex(ctx, IndexConfig{
				RootPath:              dirName,
				ClassName:             schema.ClassName(tt.className),
				ReplicationFactor:     1,
				ShardLoadLimiter:      NewShardLoadLimiter(monitoring.NoopRegisterer, 1),
				TrackVectorDimensions: true,
			}, shardState, inverted.ConfigFromModel(class.InvertedIndexConfig),
				enthnsw.UserConfig{
					VectorCacheMaxObjects: 1000,
				}, nil, nil, mockSchema, nil, logger, nil, nil, nil, &replication.GlobalConfig{}, nil, class, nil, scheduler, nil, nil, NewShardReindexerV3Noop())
			require.NoError(t, err)
			defer index.Shutdown(ctx)

			// Add properties
			for _, prop := range class.Properties {
				err = index.addProperty(ctx, prop)
				require.NoError(t, err)
			}

			if tt.setupData {
				// Create objects with varying sizes
				for i := 0; i < tt.objectCount; i++ {
					// Create object with properties that approximate the desired size
					obj := &models.Object{
						Class: tt.className,
						ID:    strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012d", i)),
						Properties: map[string]interface{}{
							"name":        fmt.Sprintf("test-object-%d", i),
							"description": generateStringOfSize(tt.objectSize - 50), // Leave room for other properties
							"count":       i,
						},
					}
					storageObj := storobj.FromObject(obj, nil, nil, nil)
					err := index.putObject(ctx, storageObj, nil, 0)
					require.NoError(t, err)
				}

				// Wait for indexing to complete
				time.Sleep(2 * time.Second)

				// Test object storage size
				shard, release, err := index.GetShard(ctx, tt.shardName)
				require.NoError(t, err)
				require.NotNil(t, shard)
				defer release()

				objectStorageSize := shard.ObjectStorageSize(ctx)
				objectCount := shard.ObjectCount()

				// Verify object count
				assert.Equal(t, tt.expectedObjectCount, objectCount, "Object count should match expected")

				// Verify storage size is within expected range
				assert.GreaterOrEqual(t, objectStorageSize, tt.expectedStorageSizeMin,
					"Storage size should be at least the minimum expected size")
				assert.LessOrEqual(t, objectStorageSize, tt.expectedStorageSizeMax,
					"Storage size should not exceed the maximum expected size")

			} else {
				// Test empty shard
				shard, release, err := index.GetShard(ctx, tt.shardName)
				require.NoError(t, err)
				require.NotNil(t, shard)
				defer release()

				objectStorageSize := shard.ObjectStorageSize(ctx)
				objectCount := shard.ObjectCount()

				assert.Equal(t, tt.expectedObjectCount, objectCount, "Empty shard should have 0 objects")
				assert.Equal(t, tt.expectedStorageSizeMin, objectStorageSize, "Empty shard should have 0 storage size")
			}
			mockSchema.AssertExpectations(t)
		})
	}
}

func TestIndex_CalculateUnloadedObjectsMetrics_ActiveVsUnloaded(t *testing.T) {
	ctx := context.Background()
	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()

	className := "TestClass"
	tenantName := "test-tenant"
	objectCount := 50
	objectSize := 500 // ~500 bytes per object

	// Create test class with multi-tenancy enabled
	class := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:         "name",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
			{
				Name:         "description",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
		InvertedIndexConfig: &models.InvertedIndexConfig{},
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: true,
		},
	}

	// Create fake schema
	fakeSchema := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{class},
		},
	}

	// Create sharding state with multi-tenancy enabled
	shardState := &sharding.State{
		Physical: map[string]sharding.Physical{
			tenantName: {
				Name:           tenantName,
				BelongsToNodes: []string{"test-node"},
				Status:         models.TenantActivityStatusHOT,
			},
		},
		PartitioningEnabled: true,
	}
	shardState.SetLocalName("test-node")

	// Create scheduler
	scheduler := queue.NewScheduler(queue.SchedulerOptions{
		Logger:  logger,
		Workers: 1,
	})

	// Create mock schema getter
	mockSchema := schemaUC.NewMockSchemaGetter(t)
	mockSchema.EXPECT().GetSchemaSkipAuth().Maybe().Return(fakeSchema)
	mockSchema.EXPECT().ReadOnlyClass(className).Maybe().Return(class)
	mockSchema.EXPECT().CopyShardingState(className).Maybe().Return(shardState)
	mockSchema.EXPECT().NodeName().Maybe().Return("test-node")
	mockSchema.EXPECT().ShardFromUUID("TestClass", mock.Anything).Return(tenantName).Maybe()
	mockSchema.EXPECT().ShardOwner(className, tenantName).Maybe().Return("test-node", nil)
	mockSchema.EXPECT().TenantsShards(ctx, className, tenantName).Maybe().Return(map[string]string{tenantName: models.TenantActivityStatusHOT}, nil)

	// Create index with lazy loading disabled to test active calculation methods
	index, err := NewIndex(ctx, IndexConfig{
		RootPath:              dirName,
		ClassName:             schema.ClassName(className),
		ReplicationFactor:     1,
		ShardLoadLimiter:      NewShardLoadLimiter(monitoring.NoopRegisterer, 1),
		TrackVectorDimensions: true,
		DisableLazyLoadShards: true, // we have to make sure lazyload shard disabled to load directly
	}, shardState, inverted.ConfigFromModel(class.InvertedIndexConfig),
		enthnsw.UserConfig{
			VectorCacheMaxObjects: 1000,
		}, nil, nil, mockSchema, nil, logger, nil, nil, nil, &replication.GlobalConfig{}, nil, class, nil, scheduler, nil, nil, NewShardReindexerV3Noop())
	require.NoError(t, err)

	// Add properties
	for _, prop := range class.Properties {
		err = index.addProperty(ctx, prop)
		require.NoError(t, err)
	}

	// Add test objects
	for i := 0; i < objectCount; i++ {
		obj := &models.Object{
			Class:  className,
			ID:     strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012d", i)),
			Tenant: tenantName,
			Properties: map[string]interface{}{
				"name":        fmt.Sprintf("test-object-%d", i),
				"description": generateStringOfSize(objectSize - 50), // Leave room for other properties
			},
		}
		storageObj := storobj.FromObject(obj, nil, nil, nil)
		err := index.putObject(ctx, storageObj, nil, 0)
		require.NoError(t, err)
	}

	// Wait for indexing to complete
	time.Sleep(1 * time.Second)

	// Test active shard object storage size
	activeShard, release, err := index.GetShard(ctx, tenantName)
	require.NoError(t, err)
	require.NotNil(t, activeShard)

	// Force flush to ensure .cna files are created
	objectsBucket := activeShard.Store().Bucket(helpers.ObjectsBucketLSM)
	require.NotNil(t, objectsBucket)
	require.NoError(t, objectsBucket.FlushMemtable())

	activeObjectStorageSize := activeShard.ObjectStorageSize(ctx)
	activeObjectCount := activeShard.ObjectCount()
	assert.Greater(t, activeObjectStorageSize, int64(0), "Active shard calculation should have object storage size > 0")

	// Test that active calculations are correct
	assert.Equal(t, objectCount, activeObjectCount, "Active shard object count should match")
	assert.Greater(t, activeObjectStorageSize, int64(objectCount*objectSize/2), "Active object storage size should be reasonable")

	// Release the shard (this will flush all data to disk)
	release()

	// Explicitly shutdown all shards to ensure data is flushed to disk
	err = index.ForEachShard(func(name string, shard ShardLike) error {
		return shard.Shutdown(ctx)
	})
	require.NoError(t, err)

	// Wait a bit for all shards to complete shutdown and data to be flushed
	time.Sleep(1 * time.Second)

	// Unload the shard from memory to test inactive calculation methods
	index.shards.LoadAndDelete(tenantName)

	// Shut down the entire index to ensure all store metadata is persisted
	require.NoError(t, index.Shutdown(ctx))

	// Create a new index instance to test inactive calculation methods
	// This ensures we're testing the inactive methods on a fresh index that reads from disk
	newIndex, err := NewIndex(ctx, IndexConfig{
		RootPath:              dirName,
		ClassName:             schema.ClassName(className),
		ReplicationFactor:     1,
		ShardLoadLimiter:      NewShardLoadLimiter(monitoring.NoopRegisterer, 1),
		TrackVectorDimensions: true,
		DisableLazyLoadShards: false, // we have to make sure lazyload enabled
	}, shardState, inverted.ConfigFromModel(class.InvertedIndexConfig),
		enthnsw.UserConfig{
			VectorCacheMaxObjects: 1000,
		}, index.GetVectorIndexConfigs(), nil, mockSchema, nil, logger, nil, nil, nil, &replication.GlobalConfig{}, nil, class, nil, scheduler, nil, nil, NewShardReindexerV3Noop())
	require.NoError(t, err)
	defer newIndex.Shutdown(ctx)

	// Explicitly shutdown all shards to ensure data is flushed to disk
	require.NoError(t, newIndex.ForEachShard(func(name string, shard ShardLike) error {
		return shard.Shutdown(ctx)
	}))
	newIndex.shards.LoadAndDelete(tenantName)

	inactiveObjectCount, inactiveObjectStorageSize := newIndex.CalculateUnloadedObjectsMetrics(ctx, tenantName)

	// Compare active and inactive metrics
	assert.Equal(t, activeObjectCount, int(inactiveObjectCount), "Active and inactive object count should match")
	assert.InDelta(t, activeObjectStorageSize, inactiveObjectStorageSize, 1024, "Active and inactive object storage size should be close")

	// Verify all mock expectations were met
	mockSchema.AssertExpectations(t)
}

// Helper function to generate a string of approximately the given size
func generateStringOfSize(size int) string {
	if size <= 0 {
		return ""
	}

	// Use a repeating pattern to create a string of approximately the desired size
	pattern := "abcdefghijklmnopqrstuvwxyz0123456789"
	result := strings.Repeat(pattern, size/len(pattern))
	if remainder := size % len(pattern); remainder > 0 {
		result += pattern[:remainder]
	}
	return result
}
