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

//go:build integrationTest

package db

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	shardusage "github.com/weaviate/weaviate/adapters/repos/db/shard_usage"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/cluster"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// startDimensionsTestRepo starts a db on dir, as a node restarting on its data.
func startDimensionsTestRepo(t *testing.T, dir string, state *sharding.State, getter *fakeSchemaGetter,
	configure func(*Config),
) *DB {
	t.Helper()
	logger, _ := test.NewNullLogger()
	schemaReader := schemaUC.NewMockSchemaReader(t)
	schemaReader.EXPECT().Shards(mock.Anything).Return(state.AllPhysicalShards(), nil).Maybe()
	schemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(className string, _ bool, readFunc func(*models.Class, *sharding.State) error) error {
			return readFunc(&models.Class{Class: className}, state)
		}).Maybe()
	schemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{}).Maybe()
	schemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	schemaReader.EXPECT().WaitForUpdate(mock.Anything, mock.Anything).Return(nil).Maybe()
	fsm := replicationTypes.NewMockReplicationFSMReader(t)
	fsm.EXPECT().HasActiveReplicationForShard(mock.Anything, mock.Anything).Return(false).Maybe()
	fsm.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	fsm.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	nodes := cluster.NewMockNodeSelector(t)
	nodes.EXPECT().LocalName().Return("node1").Maybe()
	nodes.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()

	config := Config{
		RootPath:                  dir,
		QueryMaximumResults:       1000,
		MaxImportGoroutinesFactor: 1,
		TrackVectorDimensions:     true,
		EnableLazyLoadShards:      boolPtr(true),
	}
	if configure != nil {
		configure(&config)
	}
	repo, err := New(logger, "node1", config, &FakeRemoteClient{}, nodes, &FakeRemoteNodeClient{},
		&FakeReplicationClient{}, nil, nil, nodes, schemaReader, fsm, nil)
	require.NoError(t, err)
	repo.SetSchemaGetter(getter)
	require.NoError(t, repo.WaitForStartup(testCtx()))
	return repo
}

// With either flag set, startup prepares the dimensions buckets of the local shards
// before loading any: inactive tenants and shards loaded lazily alike, without
// loading them.
func TestDB_PrepareDimensionsAtStartup(t *testing.T) {
	const objects, dims = 100, 128
	correctKey := string(binary.LittleEndian.AppendUint32(nil, dims))

	tests := []struct {
		name   string
		status string
		// seedMap replaces the dimensions bucket with a MapCollection one, with
		// the doc ids of all objects when correct, or a stray row otherwise
		seedMap, correct bool
		configure        func(*Config)
		expectStrategy   string
		expectDims       int
	}{
		{
			name: "reindex, active shard not loaded yet", status: models.TenantActivityStatusHOT,
			configure:      func(c *Config) { c.ReindexVectorDimensions = true },
			expectStrategy: lsmkv.StrategyRoaringSet, expectDims: objects * dims,
		},
		{
			name: "reindex, inactive tenant", status: models.TenantActivityStatusCOLD,
			configure:      func(c *Config) { c.ReindexVectorDimensions = true },
			expectStrategy: lsmkv.StrategyRoaringSet, expectDims: objects * dims,
		},
		{
			name: "reindex, inactive tenant with a wrong map bucket", status: models.TenantActivityStatusCOLD,
			seedMap:        true,
			configure:      func(c *Config) { c.ReindexVectorDimensions = true },
			expectStrategy: lsmkv.StrategyRoaringSet, expectDims: objects * dims,
		},
		{
			name: "migrate, inactive tenant", status: models.TenantActivityStatusCOLD,
			seedMap: true, correct: true,
			configure:      func(c *Config) { c.MigrateDimensionsToRoaringSet = true },
			expectStrategy: lsmkv.StrategyRoaringSet, expectDims: objects * dims,
		},
		{
			name: "neither, inactive tenant", status: models.TenantActivityStatusCOLD,
			seedMap: true, correct: true,
			expectStrategy: lsmkv.StrategyMapCollection, expectDims: objects * dims,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testCtx()
			dir := t.TempDir()
			state := singleShardState()
			shardName := state.AllPhysicalShards()[0]
			class := &models.Class{
				Class:               "Test",
				VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
				InvertedIndexConfig: invertedConfig(),
			}
			getter := &fakeSchemaGetter{schema: schema.Schema{Objects: &models.Schema{}}, shardState: state}

			// objects written without their dimensions tracked
			repo := startDimensionsTestRepo(t, dir, state, getter, nil)
			require.NoError(t, NewMigrator(repo, repo.logger, "node1").AddClass(ctx, class))
			getter.schema = schema.Schema{Objects: &models.Schema{Classes: []*models.Class{class}}}
			repo.config.TrackVectorDimensions = false
			for i := range objects {
				id := strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", i)).String())
				require.NoError(t, repo.PutObject(ctx, &models.Object{Class: class.Class, ID: id},
					make([]float32, dims), nil, nil, nil, 0))
			}
			indexPath := repo.GetIndex(schema.ClassName(class.Class)).path()
			require.NoError(t, repo.Shutdown(ctx))

			bucketPath := filepath.Join(indexPath, shardName, "lsm", helpers.DimensionsBucketLSM)
			if tt.seedMap {
				require.NoError(t, os.RemoveAll(bucketPath))
				b, err := lsmkv.NewBucketCreator().NewBucket(ctx, bucketPath, "", repo.logger, nil,
					cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
					lsmkv.WithStrategy(lsmkv.StrategyMapCollection))
				require.NoError(t, err)
				key, docIDs := correctKey, objects
				if !tt.correct {
					key, docIDs = string(binary.LittleEndian.AppendUint32(nil, 7)), 3
				}
				for docID := range docIDs {
					require.NoError(t, b.MapSet([]byte(key), lsmkv.MapPair{
						Key: binary.LittleEndian.AppendUint64(nil, uint64(docID)), Value: []byte{},
					}))
				}
				require.NoError(t, b.FlushAndSwitch())
				require.NoError(t, b.Shutdown(ctx))
			}

			physical := state.Physical[shardName]
			physical.Status = tt.status
			state.Physical[shardName] = physical
			restarted := startDimensionsTestRepo(t, dir, state, getter, tt.configure)
			defer restarted.Shutdown(context.Background())

			index := restarted.GetIndex(schema.ClassName(class.Class))
			switch shard := index.shards.Load(shardName).(type) {
			case nil:
				require.Equal(t, models.TenantActivityStatusCOLD, tt.status)
			case *LazyLoadShard:
				require.False(t, shard.isLoaded(), "the shard must not have been loaded")
			default:
				require.FailNowf(t, "unexpected shard", "%T", shard)
			}

			strategy, err := lsmkv.DetermineUnloadedBucketStrategyAmong(bucketPath, lsmkv.DimensionsBucketPrioritizedStrategies)
			require.NoError(t, err)
			assert.Equal(t, tt.expectStrategy, strategy)
			usage, err := shardusage.CalculateUnloadedDimensionsUsage(ctx, restarted.logger, index.path(), shardName, "")
			require.NoError(t, err)
			assert.Equal(t, tt.expectDims, usage.Count*usage.Dimensions)

			if tt.configure != nil && restarted.dimensionsReindex.enabled {
				require.NoError(t, NewMigrator(restarted, restarted.logger, "node1").ReportVectorDimensionsReindex(ctx),
					"nothing is left to reindex")
			}
		})
	}
}
