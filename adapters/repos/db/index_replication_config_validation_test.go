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

package db

import (
	"context"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestUpdateReplicationConfigInvalidAsyncConfigLeavesConfigUntouched(t *testing.T) {
	ctx := testCtx()
	repo, index := newReplConfigDeadlockFixture(t, "TornStateClass")
	t.Cleanup(func() { repo.Shutdown(context.Background()) })

	prevRF := index.ReplicationFactor()
	prevDS := index.DeletionStrategy()
	prevAsync := index.Config.AsyncReplicationConfig

	freq := int64(-1)
	err := index.updateReplicationConfig(ctx, &models.ReplicationConfig{
		Factor:           prevRF + 2,
		DeletionStrategy: models.ReplicationConfigDeletionStrategyTimeBasedResolution,
		AsyncConfig:      &models.ReplicationAsyncConfig{Frequency: &freq},
	})
	require.ErrorContains(t, err, "frequency must be >= 0")
	require.Equal(t, prevRF, index.ReplicationFactor())
	require.Equal(t, prevDS, index.DeletionStrategy())
	require.Equal(t, prevAsync, index.Config.AsyncReplicationConfig)
}

func TestUpdateReplicationConfigValidAsyncConfigApplies(t *testing.T) {
	ctx := testCtx()
	repo, index := newReplConfigDeadlockFixture(t, "ValidAsyncCfgClass")
	t.Cleanup(func() { repo.Shutdown(context.Background()) })

	freq := int64(60_000)
	err := index.updateReplicationConfig(ctx, &models.ReplicationConfig{
		Factor:      3,
		AsyncConfig: &models.ReplicationAsyncConfig{Frequency: &freq},
	})
	require.NoError(t, err)
	require.Equal(t, int64(3), index.ReplicationFactor())
	require.NotNil(t, index.Config.AsyncReplicationConfig.classOverrides.frequency)
	require.Equal(t, time.Minute, *index.Config.AsyncReplicationConfig.classOverrides.frequency)
}

// Like newLazyLoadRepo but seeds the schema before WaitForStartup so db.init sees the classes.
func newRepoWithStoredClasses(t *testing.T, shardState *sharding.State, classes []*models.Class) *DB {
	t.Helper()
	ctx := testCtx()
	logger, _ := test.NewNullLogger()

	baseMetrics := monitoring.GetMetrics()
	metricsCopy := *baseMetrics
	metricsCopy.Registerer = monitoring.NoopRegisterer
	metrics := &metricsCopy

	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: classes}},
		shardState: shardState,
	}
	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readFunc func(*models.Class, *sharding.State) error) error {
		return readFunc(&models.Class{Class: className}, shardState)
	}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()

	repo, err := New(logger, "node1", Config{
		RootPath:                  t.TempDir(),
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
		EnableLazyLoadShards:      boolPtr(true),
	},
		&FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{},
		&FakeReplicationClient{}, metrics, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader,
	)
	require.NoError(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.NoError(t, repo.WaitForStartup(ctx))

	return repo
}

func TestInitDegradesInvalidStoredAsyncReplicationConfig(t *testing.T) {
	freq := int64(-1)
	class := &models.Class{
		Class:               "PoisonedAsyncCfgClass",
		VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: invertedConfig(),
		ReplicationConfig: &models.ReplicationConfig{
			Factor:      1,
			AsyncConfig: &models.ReplicationAsyncConfig{Frequency: &freq},
		},
	}
	repo := newRepoWithStoredClasses(t, singleShardState(), []*models.Class{class})
	t.Cleanup(func() { repo.Shutdown(context.Background()) })

	index := repo.GetIndex(schema.ClassName("PoisonedAsyncCfgClass"))
	require.NotNil(t, index)

	logger, _ := test.NewNullLogger()
	expected, err := asyncReplicationConfigFromModel(false, nil, logger)
	require.NoError(t, err)
	require.Equal(t, expected, index.Config.AsyncReplicationConfig)
}
