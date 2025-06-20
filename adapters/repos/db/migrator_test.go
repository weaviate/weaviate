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

package db

import (
	"context"
	"hash/crc32"
	"io"
	"slices"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestUpdateIndexTenants(t *testing.T) {
	tests := []struct {
		name           string
		originalStatus string
		incomingStatus string
		expectedStatus storagestate.Status
		getClass       bool
	}{
		{
			name:           "when tenant is marked as COLD in incoming state while being HOT in original index",
			originalStatus: models.TenantActivityStatusHOT,
			incomingStatus: models.TenantActivityStatusCOLD,
			expectedStatus: storagestate.StatusShutdown,
		},
		{
			name:           "when tenant is marked as HOT in incoming state while being COLD in original index",
			originalStatus: models.TenantActivityStatusCOLD,
			incomingStatus: models.TenantActivityStatusHOT,
			expectedStatus: storagestate.StatusReady,
			getClass:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockSchemaGetter := schemaUC.NewMockSchemaGetter(t)
			mockSchemaGetter.On("NodeName").Return("node1")

			class := &models.Class{
				Class:               "TestClass",
				InvertedIndexConfig: &models.InvertedIndexConfig{},
			}
			if tt.getClass {
				mockSchemaGetter.On("ReadOnlyClass", "TestClass").Return(class)
			}
			logger := logrus.New()
			scheduler := queue.NewScheduler(queue.SchedulerOptions{
				Logger:  logger,
				Workers: 1,
			})

			// Create original index state
			originalSS := &sharding.State{
				Physical: map[string]sharding.Physical{
					"shard1": {
						Name:           "shard1",
						BelongsToNodes: []string{"node1"},
						Status:         tt.originalStatus,
					},
				},
				PartitioningEnabled: true,
			}

			index, err := NewIndex(context.Background(), IndexConfig{
				ClassName:         schema.ClassName("TestClass"),
				RootPath:          t.TempDir(),
				ReplicationFactor: 1,
				ShardLoadLimiter:  NewShardLoadLimiter(monitoring.NoopRegisterer, 1),
			}, originalSS, inverted.ConfigFromModel(class.InvertedIndexConfig),
				hnsw.NewDefaultUserConfig(), nil, nil, mockSchemaGetter, nil, logger, nil, nil, nil, nil, nil, class, nil, scheduler, nil, nil, NewShardReindexerV3Noop())
			require.NoError(t, err)

			shard, err := NewShard(context.Background(), nil, "shard1", index, class, nil, scheduler, nil, NewShardReindexerV3Noop(), false)
			require.NoError(t, err)

			index.shards.Store("shard1", shard)

			migrator := &Migrator{
				db: &DB{
					schemaGetter: mockSchemaGetter,
				},
				nodeId: "node1",
			}

			// Create incoming state
			incomingSS := &sharding.State{
				Physical: map[string]sharding.Physical{
					"shard1": {
						Name:           "shard1",
						BelongsToNodes: []string{"node1"},
						Status:         tt.incomingStatus,
					},
				},
				PartitioningEnabled: true,
			}

			err = migrator.updateIndexTenants(context.Background(), index, incomingSS)
			require.NoError(t, err)

			mockSchemaGetter.AssertExpectations(t)

			// Verify the shard status
			require.Equal(t, tt.expectedStatus, shard.GetStatus())
		})
	}
}

func TestUpdateIndexShards(t *testing.T) {
	tests := []struct {
		name           string
		initialShards  []string
		newShards      []string
		expectedShards []string
		mustLoad       bool
	}{
		{
			name:           "add new shard with lazy loading",
			initialShards:  []string{"shard1", "shard2"},
			newShards:      []string{"shard1", "shard2", "shard3"},
			expectedShards: []string{"shard1", "shard2", "shard3"},
			mustLoad:       false,
		},
		{
			name:           "remove shard with lazy loading",
			initialShards:  []string{"shard1", "shard2", "shard3"},
			newShards:      []string{"shard1", "shard3"},
			expectedShards: []string{"shard1", "shard3"},
			mustLoad:       false,
		},
		{
			name:           "keep existing shards with lazy loading",
			initialShards:  []string{"shard1", "shard3"},
			newShards:      []string{"shard1", "shard3"},
			expectedShards: []string{"shard1", "shard3"},
			mustLoad:       false,
		},
		{
			name:           "add new shard with immediate loading",
			initialShards:  []string{"shard1", "shard2"},
			newShards:      []string{"shard1", "shard2", "shard3"},
			expectedShards: []string{"shard1", "shard2", "shard3"},
			mustLoad:       true,
		},
		{
			name:           "remove shard with immediate loading",
			initialShards:  []string{"shard1", "shard2", "shard3"},
			newShards:      []string{"shard1", "shard3"},
			expectedShards: []string{"shard1", "shard3"},
			mustLoad:       true,
		},
		{
			name:           "keep existing shards with immediate loading",
			initialShards:  []string{"shard1", "shard3"},
			newShards:      []string{"shard1", "shard3"},
			expectedShards: []string{"shard1", "shard3"},
			mustLoad:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			logger := logrus.New()

			mockSchemaGetter := schemaUC.NewMockSchemaGetter(t)
			mockSchemaGetter.On("NodeName").Return("node1")

			// Create a test class
			class := &models.Class{
				Class:               "TestClass",
				InvertedIndexConfig: &models.InvertedIndexConfig{},
			}
			mockSchemaGetter.On("ReadOnlyClass", "TestClass").Return(class).Maybe()

			// Create initial sharding state
			initialPhysical := make(map[string]sharding.Physical)
			for _, shard := range tt.initialShards {
				initialPhysical[shard] = sharding.Physical{
					Name:           shard,
					BelongsToNodes: []string{"node1"},
				}
			}
			initialState := &sharding.State{
				Physical: initialPhysical,
			}
			initialState.SetLocalName("node1")
			scheduler := queue.NewScheduler(queue.SchedulerOptions{
				Logger:  logger,
				Workers: 1,
			})

			// Create index with proper configuration
			index, err := NewIndex(ctx, IndexConfig{
				ClassName:         schema.ClassName("TestClass"),
				RootPath:          t.TempDir(),
				ReplicationFactor: 1,
				ShardLoadLimiter:  NewShardLoadLimiter(monitoring.NoopRegisterer, 1),
			}, initialState, inverted.ConfigFromModel(class.InvertedIndexConfig),
				hnsw.NewDefaultUserConfig(), nil, nil, mockSchemaGetter, nil, logger, nil, nil, nil, nil, nil, class, nil, scheduler, nil, memwatch.NewDummyMonitor(), NewShardReindexerV3Noop())
			require.NoError(t, err)

			// Initialize shards
			for _, shardName := range tt.initialShards {
				err := index.initLocalShardWithForcedLoading(ctx, class, shardName, tt.mustLoad, false)
				require.NoError(t, err)
			}

			migrator := &Migrator{
				db: &DB{
					schemaGetter: mockSchemaGetter,
				},
				nodeId: "node1",
			}

			// Create new sharding state
			newPhysical := make(map[string]sharding.Physical)
			for _, shard := range tt.newShards {
				newPhysical[shard] = sharding.Physical{
					Name:           shard,
					BelongsToNodes: []string{"node1"},
				}
			}
			newState := &sharding.State{
				Physical: newPhysical,
			}
			newState.SetLocalName("node1")

			// Update shards
			err = migrator.updateIndexShards(ctx, index, newState)
			require.NoError(t, err)

			// Verify expected shards exist and are ready
			for _, expectedShard := range tt.expectedShards {
				shard := index.shards.Load(expectedShard)
				require.NotNil(t, shard, "shard %s should exist", expectedShard)
				require.Equal(t, storagestate.StatusReady, shard.GetStatus(), "shard %s should be ready", expectedShard)
			}

			// Verify removed shards are dropped
			for _, initialShard := range tt.initialShards {
				if !slices.Contains(tt.newShards, initialShard) {
					shard := index.shards.Load(initialShard)
					require.Nil(t, shard, "shard %s should be dropped", initialShard)
				}
			}

			mockSchemaGetter.AssertExpectations(t)
		})
	}
}

func TestListAndGetFilesWithIntegrityChecking(t *testing.T) {
	mockSchemaGetter := schemaUC.NewMockSchemaGetter(t)
	mockSchemaGetter.On("NodeName").Return("node1")

	class := &models.Class{
		Class:               "TestClass",
		InvertedIndexConfig: &models.InvertedIndexConfig{},
	}
	mockSchemaGetter.On("ReadOnlyClass", "TestClass").Return(class).Maybe()

	mockSchemaGetter.On("ShardOwner", "TestClass", "shard1").Return("node1", nil)

	logger := logrus.New()
	scheduler := queue.NewScheduler(queue.SchedulerOptions{
		Logger:  logger,
		Workers: 1,
	})

	// Create original index state
	originalSS := &sharding.State{
		Physical: map[string]sharding.Physical{
			"shard1": {
				Name:           "shard1",
				BelongsToNodes: []string{"node1"},
				Status:         models.TenantActivityStatusHOT,
			},
		},
		PartitioningEnabled: true,
	}

	index, err := NewIndex(context.Background(), IndexConfig{
		ClassName:         schema.ClassName("TestClass"),
		RootPath:          t.TempDir(),
		ReplicationFactor: 1,
		ShardLoadLimiter:  NewShardLoadLimiter(monitoring.NoopRegisterer, 1),
	}, originalSS, inverted.ConfigFromModel(class.InvertedIndexConfig),
		hnsw.NewDefaultUserConfig(), nil, nil, mockSchemaGetter, nil, logger, nil, nil, nil, nil, nil, class, nil, scheduler, nil, nil, NewShardReindexerV3Noop())
	require.NoError(t, err)

	shard, err := NewShard(context.Background(), nil, "shard1", index, class, nil, scheduler, nil, NewShardReindexerV3Noop(), false)
	require.NoError(t, err)

	index.shards.Store("shard1", shard)

	ctx := context.Background()

	err = index.IncomingPutObject(ctx, "shard1", &storobj.Object{
		MarshallerVersion: 1,
		DocID:             0,
		Object: models.Object{
			ID:    strfmt.UUID("40d3be3e-2ecc-49c8-b37c-d8983164848b"),
			Class: "TestClass",
		},
	}, 0)
	require.NoError(t, err)

	err = index.IncomingPauseFileActivity(ctx, "shard1")
	require.NoError(t, err)

	files, err := index.IncomingListFiles(ctx, "shard1")
	require.NoError(t, err)
	require.NotEmpty(t, files)

	for i, f := range files {
		md, err := index.IncomingGetFileMetadata(ctx, "shard1", f)
		require.NoError(t, err)

		// object insertion should not affect file copy process
		err = index.IncomingPutObject(ctx, "shard1", &storobj.Object{
			MarshallerVersion: 1,
			DocID:             uint64(i) + 1,
			Object: models.Object{
				ID:    strfmt.UUID("40d3be3e-2ecc-49c8-b37c-d8983164848b"),
				Class: "TestClass",
			},
		}, 0)
		require.NoError(t, err)

		r, err := index.IncomingGetFile(ctx, "shard1", f)
		require.NoError(t, err)

		h := crc32.NewIEEE()

		_, err = io.Copy(h, r)
		require.NoError(t, err)

		require.Equal(t, md.CRC32, h.Sum32())
	}

	err = index.IncomingResumeFileActivity(ctx, "shard1")
	require.NoError(t, err)
}
