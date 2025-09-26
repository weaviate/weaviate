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
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/replication"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

const (
	defaultVectorDimensions = 1536
	namedVectorDimensions   = 768
)

func TestIndex_CalculateUnloadedVectorsMetrics(t *testing.T) {
	ctx := context.Background()
	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()

	tests := []struct {
		name                      string
		className                 string
		shardName                 string
		vectorConfigs             map[string]schemaConfig.VectorIndexConfig
		objectCount               int
		vectorDimensions          int
		expectedVectorStorageSize int64
		setupData                 bool
	}{
		{
			name:      "empty shard with standard compression",
			className: "TestClass",
			shardName: "test-shard",
			vectorConfigs: map[string]schemaConfig.VectorIndexConfig{
				"": enthnsw.UserConfig{
					VectorCacheMaxObjects: 1000,
				},
			},
			objectCount:               0,
			vectorDimensions:          defaultVectorDimensions,
			expectedVectorStorageSize: 0,
			setupData:                 false,
		},
		{
			name:      "shard with data and standard compression",
			className: "TestClass",
			shardName: "test-shard",
			vectorConfigs: map[string]schemaConfig.VectorIndexConfig{
				"": enthnsw.UserConfig{
					VectorCacheMaxObjects: 1000,
				},
			},
			objectCount:               100,
			vectorDimensions:          defaultVectorDimensions,
			expectedVectorStorageSize: int64(100 * defaultVectorDimensions * 4), // 100 objects * defaultVectorDimensions dimensions * 4 bytes per float32
			setupData:                 true,
		},
		{
			name:      "shard with named vectors",
			className: "TestClass",
			shardName: "test-shard-named",
			vectorConfigs: map[string]schemaConfig.VectorIndexConfig{
				"text": enthnsw.UserConfig{
					VectorCacheMaxObjects: 1000,
				},
			},
			objectCount:               50,
			vectorDimensions:          namedVectorDimensions,
			expectedVectorStorageSize: int64(50 * namedVectorDimensions * 4), // 50 objects * namedVectorDimensions dimensions * 4 bytes per float32
			setupData:                 true,
		},
		{
			name:      "shard with PQ compression",
			className: "TestClass",
			shardName: "test-shard-pq",
			vectorConfigs: map[string]schemaConfig.VectorIndexConfig{
				"": enthnsw.UserConfig{
					VectorCacheMaxObjects: 1000,
					PQ: enthnsw.PQConfig{
						Enabled:   true,
						Segments:  96,
						Centroids: 256,
					},
				},
			},
			objectCount:               1000,
			vectorDimensions:          defaultVectorDimensions,
			expectedVectorStorageSize: 0, // Will be calculated based on actual compression ratio
			setupData:                 true,
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
			// Add ShardOwner expectation for all test cases
			mockSchema.EXPECT().ShardOwner(tt.className, tt.shardName).Maybe().Return("test-node", nil)

			// Create index
			var defaultVectorConfig schemaConfig.VectorIndexConfig
			var vectorConfigs map[string]schemaConfig.VectorIndexConfig

			if len(tt.vectorConfigs) > 0 && tt.vectorConfigs[""] != nil {
				// For legacy vector tests, only use the default config, not both
				defaultVectorConfig = tt.vectorConfigs[""]
				// Don't pass the empty string config in vectorConfigs to avoid duplication
				vectorConfigs = make(map[string]schemaConfig.VectorIndexConfig)
				for k, v := range tt.vectorConfigs {
					if k != "" {
						vectorConfigs[k] = v
					}
				}
			} else {
				// Use a default config for legacy vectors
				defaultVectorConfig = enthnsw.UserConfig{
					VectorCacheMaxObjects: 1000,
				}
				vectorConfigs = tt.vectorConfigs
			}

			index, err := NewIndex(ctx, IndexConfig{
				RootPath:              dirName,
				ClassName:             schema.ClassName(tt.className),
				ReplicationFactor:     1,
				ShardLoadLimiter:      NewShardLoadLimiter(monitoring.NoopRegisterer, 1),
				TrackVectorDimensions: true,
			}, shardState, inverted.ConfigFromModel(class.InvertedIndexConfig),
				defaultVectorConfig, vectorConfigs, nil, mockSchema, nil, nil, logger, nil, nil, nil, &replication.GlobalConfig{}, nil, class, nil, scheduler, nil, nil, NewShardReindexerV3Noop(), roaringset.NewBitmapBufPoolNoop())
			require.NoError(t, err)
			defer index.Shutdown(ctx)

			// Add properties
			err = index.addProperty(ctx, &models.Property{
				Name:         "name",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			})
			require.NoError(t, err)

			if tt.setupData {
				if len(tt.vectorConfigs) > 0 && tt.vectorConfigs["text"] != nil {
					// Named vector
					for i := 0; i < tt.objectCount; i++ {
						obj := &models.Object{
							Class: tt.className,
							ID:    strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012d", i)),
							Properties: map[string]interface{}{
								"name": fmt.Sprintf("test-object-%d", i),
							},
						}
						vectors := map[string][]float32{
							"text": make([]float32, tt.vectorDimensions),
						}
						for j := range vectors["text"] {
							vectors["text"][j] = float32(i+j) / 1000.0
						}
						storageObj := storobj.FromObject(obj, nil, vectors, nil)
						err := index.putObject(ctx, storageObj, nil, 0)
						require.NoError(t, err)
					}
				} else if len(tt.vectorConfigs) > 0 && tt.vectorConfigs[""] != nil {
					// Legacy vector
					for i := 0; i < tt.objectCount; i++ {
						obj := &models.Object{
							Class: tt.className,
							ID:    strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012d", i)),
							Properties: map[string]interface{}{
								"name": fmt.Sprintf("test-object-%d", i),
							},
						}
						vector := make([]float32, tt.vectorDimensions)
						for j := range vector {
							vector[j] = float32(i+j) / 1000.0
						}
						storageObj := storobj.FromObject(obj, vector, nil, nil)
						err := index.putObject(ctx, storageObj, nil, 0)
						require.NoError(t, err)
					}
				}

				// Wait for vector indexing to complete
				time.Sleep(1 * time.Second)

				// Vector dimensions are always aggregated from nodeWideMetricsObserver,
				// but we don't need DB for this test. Gimicky, but it does the job.
				db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
				publishVectorMetricsFromDB(t, db)

				// Test active shard vector storage size
				shard, release, err := index.GetShard(ctx, tt.shardName)
				require.NoError(t, err)
				require.NotNil(t, shard)

				// Get active metrics BEFORE releasing the shard
				vectorStorageSize, err := shard.VectorStorageSize(ctx)
				require.NoError(t, err)
				dimensions, err := shard.Dimensions(ctx, "")
				require.NoError(t, err)
				if len(tt.vectorConfigs) > 0 && tt.vectorConfigs["text"] != nil {
					// Named vector
					dimensions, err = shard.Dimensions(ctx, "text")
					require.NoError(t, err)
				}
				objectCount, err := shard.ObjectCount(ctx)
				require.NoError(t, err)

				// For PQ compression, we need to account for the actual compression ratio
				if len(tt.vectorConfigs) == 1 {
					if pqConfig, ok := tt.vectorConfigs[""].(enthnsw.UserConfig); ok && pqConfig.PQ.Enabled {
						// In test, PQ compression is not simulated, so expect uncompressed size
						expectedSize := int64(tt.objectCount * tt.vectorDimensions * 4)
						assert.Equal(t, expectedSize, vectorStorageSize)
					} else {
						assert.Equal(t, tt.expectedVectorStorageSize, vectorStorageSize)
					}
				} else {
					assert.Equal(t, tt.expectedVectorStorageSize, vectorStorageSize)
				}

				// Test dimensions tracking
				expectedDimensions := tt.vectorDimensions * tt.objectCount // Dimensions returns total across all objects
				assert.Equal(t, expectedDimensions, dimensions, "Dimensions should match expected")

				// Test object count
				assert.Equal(t, tt.objectCount, objectCount, "Object count should match expected")

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
				index.shards.LoadAndDelete(tt.shardName)
			} else {
				// Test empty shard
				shard, release, err := index.GetShard(ctx, tt.shardName)
				require.NoError(t, err)
				require.NotNil(t, shard)

				// Get active metrics BEFORE releasing the shard
				vectorStorageSize, err := shard.VectorStorageSize(ctx)
				require.NoError(t, err)
				dimensions, err := shard.Dimensions(ctx, "")
				require.NoError(t, err)
				objectCount, err := shard.ObjectCount(ctx)
				require.NoError(t, err)

				assert.Equal(t, tt.expectedVectorStorageSize, vectorStorageSize)
				assert.Equal(t, 0, dimensions, "Empty shard should have 0 dimensions")
				assert.Equal(t, 0, objectCount, "Empty shard should have 0 objects")

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
				index.shards.LoadAndDelete(tt.shardName)
			}

			// Verify all mock expectations were met
			mockSchema.AssertExpectations(t)
		})
	}
}

func TestIndex_CalculateUnloadedDimensionsUsage(t *testing.T) {
	ctx := context.Background()
	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()

	tests := []struct {
		name             string
		className        string
		shardName        string
		targetVector     string
		objectCount      int
		vectorDimensions int
		expectedCount    int
		expectedDims     int
		setupData        bool
	}{
		{
			name:             "empty shard",
			className:        "TestClass",
			shardName:        "test-shard",
			targetVector:     "",
			objectCount:      0,
			vectorDimensions: defaultVectorDimensions,
			expectedCount:    0,
			expectedDims:     0,
			setupData:        false,
		},
		{
			name:             "shard with legacy vector",
			className:        "TestClass",
			shardName:        "test-shard",
			targetVector:     "",
			objectCount:      100,
			vectorDimensions: defaultVectorDimensions,
			expectedCount:    100,
			expectedDims:     defaultVectorDimensions,
			setupData:        true,
		},
		{
			name:             "shard with named vector",
			className:        "TestClass",
			shardName:        "test-shard",
			targetVector:     "text",
			objectCount:      50,
			vectorDimensions: namedVectorDimensions,
			expectedCount:    50,
			expectedDims:     namedVectorDimensions,
			setupData:        true,
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
			mockSchema.EXPECT().ShardFromUUID("TestClass", mock.Anything).Return("test-shard").Maybe()

			// Create index with named vector config
			vectorConfigs := map[string]schemaConfig.VectorIndexConfig{
				"text": enthnsw.UserConfig{
					VectorCacheMaxObjects: 1000,
				},
			}
			index, err := NewIndex(ctx, IndexConfig{
				RootPath:              dirName,
				ClassName:             schema.ClassName(tt.className),
				ReplicationFactor:     1,
				ShardLoadLimiter:      NewShardLoadLimiter(monitoring.NoopRegisterer, 1),
				TrackVectorDimensions: true,
			}, shardState, inverted.ConfigFromModel(class.InvertedIndexConfig),
				enthnsw.UserConfig{
					VectorCacheMaxObjects: 1000,
				}, vectorConfigs, nil, mockSchema, nil, nil, logger, nil, nil, nil, &replication.GlobalConfig{}, nil, class, nil, scheduler, nil, nil, NewShardReindexerV3Noop(), roaringset.NewBitmapBufPoolNoop())
			require.NoError(t, err)
			defer index.Shutdown(ctx)

			// Add properties
			err = index.addProperty(ctx, &models.Property{
				Name:         "name",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			})
			require.NoError(t, err)

			if tt.setupData {
				// Add test objects with vectors
				for i := 0; i < tt.objectCount; i++ {
					obj := &models.Object{
						Class: tt.className,
						ID:    strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012d", i)),
						Properties: map[string]interface{}{
							"name": fmt.Sprintf("test-object-%d", i),
						},
					}

					// Create storage object with vectors
					var storageObj *storobj.Object
					if tt.targetVector != "" {
						// Named vector
						vectors := map[string][]float32{
							tt.targetVector: make([]float32, tt.vectorDimensions),
						}
						for j := range vectors[tt.targetVector] {
							vectors[tt.targetVector][j] = float32(i+j) / 1000.0
						}
						storageObj = storobj.FromObject(obj, nil, vectors, nil)
					} else {
						// Legacy vector
						vector := make([]float32, tt.vectorDimensions)
						for j := range vector {
							vector[j] = float32(i+j) / 1000.0
						}
						storageObj = storobj.FromObject(obj, vector, nil, nil)
					}

					err := index.putObject(ctx, storageObj, nil, 0)
					require.NoError(t, err)
				}

				// Wait for vector indexing to complete
				time.Sleep(1 * time.Second)

				// Vector dimensions are always aggregated from nodeWideMetricsObserver,
				// but we don't need DB for this test. Gimicky, but it does the job.
				db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
				publishVectorMetricsFromDB(t, db)

				// Test active shard dimensions usage
				shard, release, err := index.GetShard(ctx, tt.shardName)
				require.NoError(t, err)
				require.NotNil(t, shard)

				// Get active metrics BEFORE releasing the shard
				dimensionality, err := shard.DimensionsUsage(ctx, tt.targetVector)
				require.NoError(t, err)

				assert.Equal(t, tt.expectedCount, dimensionality.Count)
				assert.Equal(t, tt.expectedDims, dimensionality.Dimensions)

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
				index.shards.LoadAndDelete(tt.shardName)
			} else {
				// Test empty shard
				shard, release, err := index.GetShard(ctx, tt.shardName)
				require.NoError(t, err)
				require.NotNil(t, shard)

				// Get active metrics BEFORE releasing the shard
				dimensionality, err := shard.DimensionsUsage(ctx, tt.targetVector)
				require.NoError(t, err)

				assert.Equal(t, tt.expectedCount, dimensionality.Count)
				assert.Equal(t, tt.expectedDims, dimensionality.Dimensions)

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
				index.shards.LoadAndDelete(tt.shardName)
			}

			// Verify all mock expectations were met
			mockSchema.AssertExpectations(t)
		})
	}
}

func TestIndex_VectorStorageSize_ActiveVsUnloaded(t *testing.T) {
	ctx := context.Background()
	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()

	className := "TestClass"
	tenantNamePopulated := "test-tenant"
	tenantNameEmpty := "empty-tenant"
	objectCount := 50
	vectorDimensions := defaultVectorDimensions

	// Create test class
	class := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:         "name",
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

	// Create sharding state
	shardState := &sharding.State{
		Physical: map[string]sharding.Physical{
			tenantNamePopulated: {
				Name:           tenantNamePopulated,
				BelongsToNodes: []string{"test-node"},
				Status:         models.TenantActivityStatusHOT,
			},
			tenantNameEmpty: {
				Name:           tenantNameEmpty,
				BelongsToNodes: []string{"test-node"},
				Status:         models.TenantActivityStatusCOLD,
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
	mockSchema.EXPECT().TenantsShards(ctx, className, tenantNamePopulated).Maybe().
		Return(map[string]string{tenantNamePopulated: models.TenantActivityStatusHOT}, nil)

	// Create index with lazy loading disabled to test active calculation methods
	index, err := NewIndex(ctx, IndexConfig{
		RootPath:              dirName,
		ClassName:             schema.ClassName(className),
		ReplicationFactor:     1,
		ShardLoadLimiter:      NewShardLoadLimiter(monitoring.NoopRegisterer, 1),
		TrackVectorDimensions: true,
		DisableLazyLoadShards: true, // we have to make sure lazyload shard disabled to load directly
		MaxReuseWalSize:       4096, // with recovery from .wal
	}, shardState, inverted.ConfigFromModel(class.InvertedIndexConfig),
		enthnsw.UserConfig{
			VectorCacheMaxObjects: 1000,
		}, nil, nil, mockSchema, nil, nil, logger, nil, nil, nil, &replication.GlobalConfig{}, nil, class, nil, scheduler, nil, nil, NewShardReindexerV3Noop(), roaringset.NewBitmapBufPoolNoop())
	require.NoError(t, err)

	// Add properties
	err = index.addProperty(ctx, &models.Property{
		Name:         "name",
		DataType:     schema.DataTypeText.PropString(),
		Tokenization: models.PropertyTokenizationWhitespace,
	})
	require.NoError(t, err)

	// Add test objects
	for i := range objectCount {
		obj := &models.Object{
			Class: className,
			ID:    strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012d", i)),
			Properties: map[string]interface{}{
				"name": fmt.Sprintf("test-object-%d", i),
			},
			Tenant: tenantNamePopulated,
		}

		vector := make([]float32, vectorDimensions)
		for j := range vector {
			vector[j] = float32(i+j) / 1000.0
		}
		storageObj := storobj.FromObject(obj, vector, nil, nil)

		err := index.putObject(ctx, storageObj, nil, 0)
		require.NoError(t, err)
	}

	// Vector dimensions are always aggregated from nodeWideMetricsObserver,
	// but we don't need DB for this test. Gimicky, but it does the job.
	db := createTestDatabaseWithClass(t, monitoring.GetMetrics(), class)
	publishVectorMetricsFromDB(t, db)

	// Test active shard vector storage size
	activeShard, release, err := index.GetShard(ctx, tenantNamePopulated)
	require.NoError(t, err)
	require.NotNil(t, activeShard)

	activeVectorStorageSize, err := activeShard.VectorStorageSize(ctx)
	require.NoError(t, err)
	dimensionality, err := activeShard.DimensionsUsage(ctx, "")
	require.NoError(t, err)
	activeObjectCount, err := activeShard.ObjectCount(ctx)
	require.NoError(t, err)
	assert.Greater(t, activeVectorStorageSize, int64(0), "Active shard calculation should have vector storage size > 0")

	// Test that active calculations are correct
	expectedSize := int64(objectCount * vectorDimensions * 4)
	assert.Equal(t, expectedSize, activeVectorStorageSize, "Active vector storage size should be close to expected")
	assert.Equal(t, objectCount, dimensionality.Count, "Active shard object count should match")
	assert.Equal(t, vectorDimensions, dimensionality.Dimensions, "Active shard dimensions should match")
	assert.Equal(t, objectCount, activeObjectCount, "Active object count should match")

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
	index.shards.LoadAndDelete(tenantNamePopulated)

	// Shut down the entire index to ensure all store metadata is persisted
	require.NoError(t, index.Shutdown(ctx))

	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Read(className, mock.Anything).RunAndReturn(
		func(_ string, fn func(*models.Class, *sharding.State) error) error {
			return fn(nil, shardState)
		},
	)

	// Create a new index instance to test inactive calculation methods
	// This ensures we're testing the inactive methods on a fresh index that reads from disk
	newIndex, err := NewIndex(ctx, IndexConfig{
		RootPath:              dirName,
		ClassName:             schema.ClassName(className),
		ReplicationFactor:     1,
		ShardLoadLimiter:      NewShardLoadLimiter(monitoring.NoopRegisterer, 1),
		TrackVectorDimensions: true,
		MaxReuseWalSize:       4096,
		DisableLazyLoadShards: false, // we have to make sure lazyload enabled
	}, shardState, inverted.ConfigFromModel(class.InvertedIndexConfig),
		enthnsw.UserConfig{
			VectorCacheMaxObjects: 1000,
		}, index.GetVectorIndexConfigs(), nil, mockSchema, mockSchemaReader, nil, logger, nil, nil, nil, &replication.GlobalConfig{}, nil, class, nil, scheduler, nil, nil, NewShardReindexerV3Noop(), roaringset.NewBitmapBufPoolNoop())
	require.NoError(t, err)
	defer newIndex.Shutdown(ctx)

	// Explicitly shutdown all shards to ensure data is flushed to disk
	require.NoError(t, newIndex.ForEachShard(func(name string, shard ShardLike) error {
		return shard.Shutdown(ctx)
	}))
	newIndex.shards.LoadAndDelete(tenantNamePopulated)

	// Compare active and inactive metrics
	collectionUsage, err := newIndex.usageForCollection(ctx)
	require.NoError(t, err)
	for _, tenant := range collectionUsage.Shards {
		if tenant.Name == tenantNamePopulated {
			assert.Equal(t, uint64(activeVectorStorageSize), tenant.VectorStorageBytes, "Active and inactive vector storage size should be very similar")

			assert.Equal(t, objectCount, tenant.NamedVectors[0].Dimensionalities[0].Count, "Active and inactive object count should match")
			assert.Equal(t, vectorDimensions, tenant.NamedVectors[0].Dimensionalities[0].Dimensions, "Active and inactive dimensions should match")
		} else {
			// empty tenant
			assert.Equal(t, tenantNameEmpty, tenant.Name)
			assert.Equal(t, uint64(0), tenant.VectorStorageBytes, "Empty tenant should have 0 vector storage size")
			assert.Len(t, tenant.NamedVectors, 0)
			assert.Equal(t, int64(0), tenant.ObjectsCount)
		}
	}
	require.NoError(t, err)

	// Verify all mock expectations were met
	mockSchema.AssertExpectations(t)
}
