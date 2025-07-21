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

//go:build integrationTest

package db

import (
	"context"
	"strings"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestMigrator_UpdateIndex(t *testing.T) {
	var (
		class1Name        = "IdempotentClass"
		class2Name        = "MissingClass"
		missingDataClass1 = "missing_data_class_1"
		allDataClass1     = "all_data_class_1"
		allDataClass2     = "all_data_class_2"
		allDataClass1And2 = "all_data_class_1_and_2"
		intProp           = "someInt"
		textProp          = "someText"
		numberProp        = "someNumber"
		boolProp          = "someBool"
		vectorConfig      = map[string]models.VectorConfig{"vec1": {
			VectorIndexConfig: hnsw.NewDefaultUserConfig(),
			VectorIndexType:   "hnsw",
		}}
		singleTenantTestClasses = map[string]*models.Class{
			missingDataClass1: {
				Class: class1Name,
				Properties: []*models.Property{
					{Name: intProp, DataType: []string{"int"}},
					{Name: textProp, DataType: []string{"text"}},
				},
				InvertedIndexConfig: invertedConfig(),
				VectorConfig:        vectorConfig,
			},
			allDataClass1: {
				Class: class1Name,
				Properties: []*models.Property{
					{Name: intProp, DataType: []string{"int"}},
					{Name: textProp, DataType: []string{"text"}},
					{Name: numberProp, DataType: []string{"number"}},
					{Name: boolProp, DataType: []string{"boolean"}},
				},
				InvertedIndexConfig: invertedConfig(),
				VectorConfig:        vectorConfig,
			},
			allDataClass2: {
				Class: class2Name,
				Properties: []*models.Property{
					{Name: intProp, DataType: []string{"int"}},
				},
				InvertedIndexConfig: invertedConfig(),
				VectorConfig:        vectorConfig,
			},
		}
		singleTenantShardingState = map[string]*sharding.State{
			missingDataClass1: func() *sharding.State {
				ss := &sharding.State{
					Physical: map[string]sharding.Physical{
						"shard1": {
							Name:           "shard1",
							OwnsVirtual:    []string{"virtual1", "virtual2"},
							BelongsToNodes: []string{"node1"},
						},
					},
				}
				ss.SetLocalName("node1")
				return ss
			}(),
			allDataClass1: func() *sharding.State {
				ss := &sharding.State{
					Physical: map[string]sharding.Physical{
						"shard1": {
							Name:           "shard1",
							OwnsVirtual:    []string{"virtual1", "virtual2"},
							BelongsToNodes: []string{"node1"},
						},
						"shard2": {
							Name:           "shard2",
							OwnsVirtual:    []string{"virtual3", "virtual4"},
							BelongsToNodes: []string{"node1"},
						},
						"shard3": {
							Name:        "shard3",
							OwnsVirtual: []string{"virtual5", "virtual6"},
							// should not affect local repo, belongs to remote node
							BelongsToNodes: []string{"node2"},
						},
					},
				}
				ss.SetLocalName("node1")
				return ss
			}(),
			allDataClass1And2: func() *sharding.State {
				ss := &sharding.State{
					Physical: map[string]sharding.Physical{
						"shard1": {
							Name:           "shard1",
							OwnsVirtual:    []string{"virtual1", "virtual2"},
							BelongsToNodes: []string{"node1"},
						},
						"shard2": {
							Name:           "shard2",
							OwnsVirtual:    []string{"virtual3", "virtual4"},
							BelongsToNodes: []string{"node1"},
						},
						"shard3": {
							Name:        "shard3",
							OwnsVirtual: []string{"virtual5", "virtual6"},
							// should not affect local repo, belongs to remote node
							BelongsToNodes: []string{"node2"},
						},
						"shard4": {
							Name:           "shard4",
							OwnsVirtual:    []string{"virtual7", "virtual8"},
							BelongsToNodes: []string{"node1"},
						},
						"shard5": {
							Name:           "shard5",
							OwnsVirtual:    []string{"virtual9", "virtual10"},
							BelongsToNodes: []string{"node1"},
						},
					},
				}
				ss.SetLocalName("node1")
				return ss
			}(),
		}
		multiTenantTestClasses = map[string]*models.Class{
			missingDataClass1: {
				Class: class1Name,
				Properties: []*models.Property{
					{Name: intProp, DataType: []string{"int"}},
					{Name: textProp, DataType: []string{"text"}},
				},
				InvertedIndexConfig: invertedConfig(),
				VectorConfig:        vectorConfig,
				MultiTenancyConfig: &models.MultiTenancyConfig{
					Enabled: true,
				},
			},
			allDataClass1: {
				Class: class1Name,
				Properties: []*models.Property{
					{Name: intProp, DataType: []string{"int"}},
					{Name: textProp, DataType: []string{"text"}},
					{Name: numberProp, DataType: []string{"number"}},
					{Name: boolProp, DataType: []string{"boolean"}},
				},
				InvertedIndexConfig: invertedConfig(),
				VectorConfig:        vectorConfig,
				MultiTenancyConfig: &models.MultiTenancyConfig{
					Enabled: true,
				},
			},
		}
		multiTenantShardingState = map[string]*sharding.State{
			missingDataClass1: func() *sharding.State {
				ss := &sharding.State{
					Physical: map[string]sharding.Physical{
						"tenant1": {
							Name:           "tenant1",
							OwnsVirtual:    []string{"virtual1", "virtual2"},
							BelongsToNodes: []string{"node1"},
							Status:         models.TenantActivityStatusHOT,
						},
					},
					PartitioningEnabled: true,
				}
				ss.SetLocalName("node1")
				return ss
			}(),
			allDataClass1: func() *sharding.State {
				ss := &sharding.State{
					Physical: map[string]sharding.Physical{
						"tenant1": {
							Name:           "tenant1",
							OwnsVirtual:    []string{"virtual1", "virtual2"},
							BelongsToNodes: []string{"node1"},
							Status:         models.TenantActivityStatusHOT,
						},
						"tenant2": {
							Name:           "tenant2",
							OwnsVirtual:    []string{"virtual3", "virtual4"},
							BelongsToNodes: []string{"node1"},
							Status:         models.TenantActivityStatusHOT,
						},
						"tenant3": {
							Name:           "tenant3",
							OwnsVirtual:    []string{"virtual5", "virtual6"},
							BelongsToNodes: []string{"node1"},
							// should not affect local repo, not hot
							Status: models.TenantActivityStatusCOLD,
						},
						"tenant4": {
							Name:        "tenant4",
							OwnsVirtual: []string{"virtual7", "virtual8"},
							// should not affect local repo, belongs to remote node
							BelongsToNodes: []string{"node2"},
							Status:         models.TenantActivityStatusHOT,
						},
					},
					PartitioningEnabled: true,
				}
				ss.SetLocalName("node1")
				return ss
			}(),
		}
	)

	t.Run("single tenant, run multiple updates with missing class", func(t *testing.T) {
		var (
			ctx            = context.Background()
			iterations     = 5
			remoteDirName  = t.TempDir()
			localDirName   = t.TempDir()
			existingClass  = singleTenantTestClasses[allDataClass1]
			missingClass   = singleTenantTestClasses[allDataClass2]
			localSS        = singleTenantShardingState[allDataClass1]
			remoteSS       = singleTenantShardingState[allDataClass1And2]
			localMigrator  = setupTestMigrator(t, localDirName, localSS, existingClass)
			remoteMigrator = setupTestMigrator(t, remoteDirName, remoteSS, existingClass, missingClass)
		)

		defer func() {
			require.Nil(t, localMigrator.db.Shutdown(context.Background()))
			require.Nil(t, remoteMigrator.db.Shutdown(context.Background()))
		}()

		t.Run("before index update", func(t *testing.T) {
			existing := localMigrator.db.GetIndex(schema.ClassName(existingClass.Class))
			assert.NotNil(t, existing)
			missing := localMigrator.db.GetIndex(schema.ClassName(missingClass.Class))
			assert.Nil(t, missing)
		})

		t.Run("run update index", func(t *testing.T) {
			// UpdateIndex should be able to run an arbitrary number
			// of times without any changes to the internal DB state
			for i := 0; i < iterations; i++ {
				err := localMigrator.UpdateIndex(ctx, missingClass, remoteSS)
				require.Nil(t, err)
			}
		})

		t.Run("after index update", func(t *testing.T) {
			existing := localMigrator.db.GetIndex(schema.ClassName(existingClass.Class))
			assert.NotNil(t, existing)
			missing := localMigrator.db.GetIndex(schema.ClassName(missingClass.Class))
			assert.NotNil(t, missing)
		})
	})

	t.Run("single tenant, run multiple updates with multiple shards", func(t *testing.T) {
		var (
			ctx            = context.Background()
			iterations     = 5
			remoteDirName  = t.TempDir()
			localDirName   = t.TempDir()
			remoteClass    = singleTenantTestClasses[allDataClass1]
			localClass     = singleTenantTestClasses[missingDataClass1]
			localSS        = singleTenantShardingState[missingDataClass1]
			remoteSS       = singleTenantShardingState[allDataClass1]
			localMigrator  = setupTestMigrator(t, localDirName, localSS, localClass)
			remoteMigrator = setupTestMigrator(t, remoteDirName, remoteSS, remoteClass)
			someBuckets    = []string{
				helpers.BucketFromPropNameLSM(intProp),
				helpers.BucketFromPropNameLSM(textProp),
			}
			missingBuckets = []string{
				helpers.BucketFromPropNameLSM(numberProp),
				helpers.BucketFromPropNameLSM(boolProp),
			}
			allBuckets = append(someBuckets, missingBuckets...)
		)

		defer func() {
			require.Nil(t, localMigrator.db.Shutdown(context.Background()))
			require.Nil(t, remoteMigrator.db.Shutdown(context.Background()))
		}()

		t.Run("before index update", func(t *testing.T) {
			idx, ok := localMigrator.db.indices[strings.ToLower(class1Name)]
			require.True(t, ok)
			shardCount := 0
			idx.ForEachShard(func(_ string, shard ShardLike) error {
				for _, name := range someBuckets {
					bucket := shard.Store().Bucket(name)
					assert.NotNil(t, bucket)
				}
				for _, name := range missingBuckets {
					bucket := shard.Store().Bucket(name)
					assert.Nil(t, bucket)
				}
				shardCount++
				return nil
			})
			assert.Equal(t, 1, shardCount)
		})

		t.Run("run update index", func(t *testing.T) {
			// UpdateIndex should be able to run an arbitrary number
			// of times without any changes to the internal DB state
			for i := 0; i < iterations; i++ {
				err := localMigrator.UpdateIndex(ctx, remoteClass, remoteSS)
				require.Nil(t, err)
			}
		})

		t.Run("after index update", func(t *testing.T) {
			require.Len(t, localMigrator.db.indices, 1)
			idx, ok := localMigrator.db.indices[strings.ToLower(class1Name)]
			require.True(t, ok)

			shardCount := 0
			idx.ForEachShard(func(_ string, shard ShardLike) error {
				for _, name := range allBuckets {
					bucket := shard.Store().Bucket(name)
					assert.NotNil(t, bucket)
				}
				shardCount++
				return nil
			})
			assert.Equal(t, 2, shardCount)
		})
	})

	t.Run("multi-tenant, run multiple updates with tenants to add", func(t *testing.T) {
		var (
			ctx            = context.Background()
			iterations     = 5
			remoteDirName  = t.TempDir()
			localDirName   = t.TempDir()
			remoteClass    = multiTenantTestClasses[allDataClass1]
			localClass     = multiTenantTestClasses[missingDataClass1]
			localSS        = multiTenantShardingState[missingDataClass1]
			remoteSS       = multiTenantShardingState[allDataClass1]
			localMigrator  = setupTestMigrator(t, localDirName, localSS, localClass)
			remoteMigrator = setupTestMigrator(t, remoteDirName, remoteSS, remoteClass)
			someBuckets    = []string{
				helpers.BucketFromPropNameLSM(intProp),
				helpers.BucketFromPropNameLSM(textProp),
			}
			missingBuckets = []string{
				helpers.BucketFromPropNameLSM(numberProp),
				helpers.BucketFromPropNameLSM(boolProp),
			}
			allBuckets = append(someBuckets, missingBuckets...)
		)

		defer func() {
			require.Nil(t, localMigrator.db.Shutdown(context.Background()))
			require.Nil(t, remoteMigrator.db.Shutdown(context.Background()))
		}()

		t.Run("add tenants", func(t *testing.T) {
			err := localMigrator.NewTenants(ctx, localClass, []*schemaUC.CreateTenantPayload{
				{Name: "tenant1", Status: models.TenantActivityStatusHOT},
			})
			require.Nil(t, err)
			err = remoteMigrator.NewTenants(ctx, localClass, []*schemaUC.CreateTenantPayload{
				{Name: "tenant1", Status: models.TenantActivityStatusHOT},
				{Name: "tenant2", Status: models.TenantActivityStatusHOT},
				{Name: "tenant3", Status: models.TenantActivityStatusCOLD},
				{Name: "tenant4", Status: models.TenantActivityStatusHOT},
			})
			require.Nil(t, err)
		})

		t.Run("before index update", func(t *testing.T) {
			idx, ok := localMigrator.db.indices[strings.ToLower(class1Name)]
			require.True(t, ok)
			shardCount := 0
			idx.ForEachShard(func(_ string, shard ShardLike) error {
				for _, name := range someBuckets {
					bucket := shard.Store().Bucket(name)
					assert.NotNil(t, bucket)
				}
				for _, name := range missingBuckets {
					bucket := shard.Store().Bucket(name)
					assert.Nil(t, bucket)
				}
				shardCount++
				return nil
			})
			assert.Equal(t, 1, shardCount)
		})

		t.Run("run update index", func(t *testing.T) {
			// UpdateIndex should be able to run an arbitrary number
			// of times without any changes to the internal DB state
			for i := 0; i < iterations; i++ {
				err := localMigrator.UpdateIndex(ctx, remoteClass, remoteSS)
				require.Nil(t, err)
			}
		})

		t.Run("after index update", func(t *testing.T) {
			require.Len(t, localMigrator.db.indices, 1)
			idx, ok := localMigrator.db.indices[strings.ToLower(class1Name)]
			require.True(t, ok)

			shardCount := 0
			idx.ForEachShard(func(_ string, shard ShardLike) error {
				for _, name := range allBuckets {
					bucket := shard.Store().Bucket(name)
					assert.NotNil(t, bucket)
				}
				shardCount++
				return nil
			})
			assert.Equal(t, 2, shardCount)
		})
	})

	t.Run("multi-tenant, run multiple updates with tenants to delete", func(t *testing.T) {
		var (
			ctx              = context.Background()
			iterations       = 5
			remoteDirName    = t.TempDir()
			localDirName     = t.TempDir()
			remoteClass      = multiTenantTestClasses[allDataClass1]
			localClass       = multiTenantTestClasses[allDataClass1]
			localSS          = multiTenantShardingState[allDataClass1]
			remoteSS         = multiTenantShardingState[missingDataClass1]
			localMigrator    = setupTestMigrator(t, localDirName, localSS, localClass)
			remoteMigrator   = setupTestMigrator(t, remoteDirName, remoteSS, remoteClass)
			initialTenants   = []string{"tenant1", "tenant2"}
			remainingTenants = []string{"tenant1"}
		)

		defer func() {
			require.Nil(t, localMigrator.db.Shutdown(context.Background()))
			require.Nil(t, remoteMigrator.db.Shutdown(context.Background()))
		}()

		t.Run("add tenants", func(t *testing.T) {
			err := localMigrator.NewTenants(ctx, localClass, []*schemaUC.CreateTenantPayload{
				{Name: "tenant1", Status: models.TenantActivityStatusHOT},
				{Name: "tenant2", Status: models.TenantActivityStatusHOT},
			})
			require.Nil(t, err)
			err = remoteMigrator.NewTenants(ctx, localClass, []*schemaUC.CreateTenantPayload{
				{Name: "tenant1", Status: models.TenantActivityStatusHOT},
			})
			require.Nil(t, err)
		})

		t.Run("before index update", func(t *testing.T) {
			idx, ok := localMigrator.db.indices[strings.ToLower(class1Name)]
			require.True(t, ok)
			for _, tenant := range initialTenants {
				require.NotNil(t, idx.shards.Load(tenant))
			}
		})

		t.Run("run update index", func(t *testing.T) {
			// UpdateIndex should be able to run an arbitrary number
			// of times without any changes to the internal DB state
			for i := 0; i < iterations; i++ {
				err := localMigrator.UpdateIndex(ctx, remoteClass, remoteSS)
				require.Nil(t, err)
			}
		})

		t.Run("after index update", func(t *testing.T) {
			require.Len(t, localMigrator.db.indices, 1)
			idx, ok := localMigrator.db.indices[strings.ToLower(class1Name)]
			require.True(t, ok)

			shardCount := 0
			for _, tenant := range remainingTenants {
				assert.NotNil(t, idx.shards.Load(tenant))
			}
			idx.ForEachShard(func(_ string, shard ShardLike) error {
				shardCount++
				return nil
			})
			assert.Equal(t, 1, shardCount)
		})
	})
}

func setupTestMigrator(t *testing.T, rootDir string, shardState *sharding.State, classes ...*models.Class) *Migrator {
	logger, _ := test.NewNullLogger()
	schemaGetter := &fakeSchemaGetter{
		schema: schema.Schema{
			Objects: &models.Schema{
				Classes: classes,
			},
		},
		shardState: shardState,
	}
	repo, err := New(logger, Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  rootDir,
		QueryMaximumResults:       10,
		MaxImportGoroutinesFactor: 1,
	}, &fakeRemoteClient{}, &fakeNodeResolver{},
		&fakeRemoteNodeClient{}, &fakeReplicationClient{}, nil, nil)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(testCtx()))
	return NewMigrator(repo, logger)
}
