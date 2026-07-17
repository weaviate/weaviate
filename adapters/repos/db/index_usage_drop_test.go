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

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"

	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/cluster/usage/types"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	entschema "github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/memwatch"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// TestDeleteIndexAbortsInFlightUsageScan verifies that dropping a collection aborts an
// in-flight usage scan of that collection instead of waiting behind its dropIndex.RLock,
// and that the aborted scan is reported as skipped (nil usage, nil error).
func TestDeleteIndexAbortsInFlightUsageScan(t *testing.T) {
	ctx := context.Background()
	nodeName := "test-node"
	className := "DropDuringUsage"
	shardName := "shard1"

	class := &models.Class{
		Class:             className,
		ReplicationConfig: &models.ReplicationConfig{Factor: 1},
		VectorConfig:      map[string]models.VectorConfig{"vec": {VectorIndexConfig: enthnsw.UserConfig{}}},
	}
	shardingState := &sharding.State{
		Physical: map[string]sharding.Physical{
			shardName: {Name: shardName, BelongsToNodes: []string{nodeName}, Status: models.TenantActivityStatusHOT},
		},
	}
	shardingState.SetLocalName(nodeName)

	scanInCollection := make(chan struct{}, 8)
	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ string, _ bool, fn func(*models.Class, *sharding.State) error) error {
			select {
			case scanInCollection <- struct{}{}:
			default:
			}
			return fn(class, shardingState)
		}).Maybe()
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardingState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().LocalShards(mock.Anything).Return([]string{shardName}, nil).Maybe()
	mockSchemaReader.EXPECT().LocalActiveShardsCount(mock.Anything).Return(1, nil).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{nodeName}, nil).Maybe()

	mockSchemaGetter := schemaUC.NewMockSchemaGetter(t)
	mockSchemaGetter.EXPECT().GetSchemaSkipAuth().Return(entschema.Schema{
		Objects: &models.Schema{Classes: []*models.Class{class}},
	}).Maybe()
	mockSchemaGetter.EXPECT().ReadOnlyClass(className).Return(class).Maybe()
	mockSchemaGetter.EXPECT().NodeName().Return(nodeName).Maybe()

	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return(nodeName).Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return(nodeName, true).Maybe()
	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{nodeName}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{nodeName}).Maybe()

	logger, _ := logrustest.NewNullLogger()
	repo, err := New(logger, nodeName, Config{
		RootPath:                  t.TempDir(),
		MaxImportGoroutinesFactor: 1,
	}, &FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{}, &FakeReplicationClient{}, nil,
		memwatch.NewDummyMonitor(), mockNodeSelector, mockSchemaReader, mockReplicationFSMReader)
	require.NoError(t, err)
	repo.SetSchemaGetter(mockSchemaGetter)
	require.NoError(t, repo.WaitForStartup(ctx))
	defer repo.Shutdown(ctx)

	// drain the shard-reader semaphore so the scan blocks while holding dropIndex.RLock —
	// only a scan abort can unblock it
	sem := semaphore.NewWeighted(1)
	require.NoError(t, sem.Acquire(ctx, 1))

	type scanResult struct {
		usage *types.CollectionUsage
		err   error
	}
	scanDone := make(chan scanResult, 1)
	enterrors.GoWrapper(func() {
		usage, err := repo.UsageForIndex(ctx, entschema.ClassName(className), sem, false, class.VectorConfig)
		scanDone <- scanResult{usage, err}
	}, logger)

	select {
	case <-scanInCollection:
	case <-time.After(5 * time.Second):
		t.Fatal("usage scan did not start")
	}

	deleteDone := make(chan error, 1)
	enterrors.GoWrapper(func() {
		deleteDone <- repo.DeleteIndex(entschema.ClassName(className))
	}, logger)

	select {
	case err := <-deleteDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("DeleteIndex is blocked behind the in-flight usage scan")
	}

	select {
	case res := <-scanDone:
		assert.NoError(t, res.err)
		assert.Nil(t, res.usage, "scan of a dropped collection should be skipped")
	case <-time.After(5 * time.Second):
		t.Fatal("usage scan did not abort")
	}

	assert.Nil(t, repo.GetIndex(entschema.ClassName(className)))
}
