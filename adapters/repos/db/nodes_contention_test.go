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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/memwatch"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// TestSnapshotIndicesLeavesIndicesDroppable pins that snapshotIndices hands back
// pointers only. Taking every index's drop lock up front would let a concurrent
// DeleteIndex block on dropIndex.Lock while holding db.indexLock, stalling every
// GetIndex in the process for as long as the caller held the snapshot.
func TestSnapshotIndicesLeavesIndicesDroppable(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()

	db := &DB{
		logger: logger,
		indices: map[string]*Index{
			"a": {Config: IndexConfig{ClassName: "A"}, logger: logger},
			"b": {Config: IndexConfig{ClassName: "B"}, logger: logger},
			"c": nil, // must be skipped, not panic
		},
	}

	indices := db.snapshotIndices()
	require.Len(t, indices, 2)

	for _, idx := range indices {
		assert.True(t, idx.dropIndex.TryLock(),
			"snapshotIndices must not hold %s's drop lock", idx.Config.ClassName)
		idx.dropIndex.Unlock()
	}
}

// TestWithDropRLockSkipsClosedIndex covers the window the just-in-time drop lock
// opens: an index can be dropped between being snapshotted and being used.
func TestWithDropRLockSkipsClosedIndex(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()

	tests := []struct {
		name    string
		closed  bool
		wantRun bool
	}{
		{name: "live index runs f", closed: false, wantRun: true},
		{name: "closed index skips f", closed: true, wantRun: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			idx := &Index{Config: IndexConfig{ClassName: "A"}, logger: logger, closed: test.closed}

			var ran bool
			require.NoError(t, idx.withDropRLock(func() error {
				ran = true
				return nil
			}))
			assert.Equal(t, test.wantRun, ran)

			assert.True(t, idx.dropIndex.TryLock(), "drop lock must be released either way")
			idx.dropIndex.Unlock()
		})
	}
}

// TestGetShardsReplicationDetailsScopedToLocalShards pins that the bulk schema
// read resolves only the shards this node hosts. Walking state.Physical instead
// would, for a multi-tenant class, copy an entry per tenant cluster-wide.
func TestGetShardsReplicationDetailsScopedToLocalShards(t *testing.T) {
	const className = "Multi"

	physical := map[string]sharding.Physical{
		"local-1": {Name: "local-1", BelongsToNodes: []string{"n1"}},
		"local-2": {Name: "local-2", BelongsToNodes: []string{"n1", "n2"}},
		"local-3": {Name: "local-3", BelongsToNodes: []string{"n1", "n2", "n3"}},
	}
	for n := range 1000 {
		name := fmt.Sprintf("remote-%d", n)
		physical[name] = sharding.Physical{Name: name, BelongsToNodes: []string{"n2"}}
	}
	state := &sharding.State{Physical: physical, ReplicationFactor: 3}

	tests := []struct {
		name       string
		localNames []string
		want       map[string]int64
	}{
		{
			name:       "replica counts are per shard, not per class",
			localNames: []string{"local-1", "local-2", "local-3"},
			want:       map[string]int64{"local-1": 1, "local-2": 2, "local-3": 3},
		},
		{
			name:       "single shard request resolves only that shard",
			localNames: []string{"local-2"},
			want:       map[string]int64{"local-2": 2},
		},
		{
			name:       "shard missing from the sharding state is omitted, not fatal",
			localNames: []string{"local-1", "vanished"},
			want:       map[string]int64{"local-1": 1},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logger, hook := logrustest.NewNullLogger()

			reader := schemaUC.NewMockSchemaReader(t)
			reader.EXPECT().Read(className, true, mock.Anything).RunAndReturn(
				func(_ string, _ bool, fn func(*models.Class, *sharding.State) error) error {
					return fn(&models.Class{Class: className}, state)
				}).Once()

			idx := &Index{
				Config:       IndexConfig{ClassName: schema.ClassName(className)},
				schemaReader: reader,
				logger:       logger,
			}

			replicationFactor, replicasPerShard := idx.getShardsReplicationDetails(test.localNames)

			assert.Equal(t, int64(3), replicationFactor)
			assert.Equal(t, test.want, replicasPerShard,
				"only the requested local shards may be resolved")
			assert.Empty(t, hook.AllEntries(), "resolving local shards must not log")
		})
	}
}

// TestNodeStatusDoesNotBlockConcurrentDrop verifies that a slow /nodes gather on
// one collection does not block a DeleteIndex of a different collection.
//
// DeleteIndex takes db.indexLock and then waits on dropIndex.Lock, so a gather
// holding every index's drop lock for its whole duration stalls the drop, and
// through db.indexLock every GetIndex in the process.
func TestNodeStatusDoesNotBlockConcurrentDrop(t *testing.T) {
	ctx := context.Background()
	nodeName := "test-node"
	slowClass := "SlowCollection"
	droppedClass := "DroppedCollection"
	shardName := "shard1"

	newClass := func(name string) *models.Class {
		return &models.Class{
			Class:               name,
			ReplicationConfig:   &models.ReplicationConfig{Factor: 1},
			VectorConfig:        map[string]models.VectorConfig{"vec": {VectorIndexConfig: enthnsw.UserConfig{}}},
			InvertedIndexConfig: &models.InvertedIndexConfig{CleanupIntervalSeconds: 60},
		}
	}
	classes := map[string]*models.Class{
		slowClass:    newClass(slowClass),
		droppedClass: newClass(droppedClass),
	}

	shardingState := &sharding.State{
		Physical: map[string]sharding.Physical{
			shardName: {Name: shardName, BelongsToNodes: []string{nodeName}, Status: models.TenantActivityStatusHOT},
		},
		ReplicationFactor: 1,
	}
	shardingState.SetLocalName(nodeName)

	// armed only once both indices exist, so class setup is not blocked
	var armed atomic.Bool
	gatherReachedSlowClass := make(chan struct{}, 1)
	releaseSlowClassCh := make(chan struct{})
	var releaseOnce sync.Once
	releaseSlowClass := func() { releaseOnce.Do(func() { close(releaseSlowClassCh) }) }

	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(class string, _ bool, fn func(*models.Class, *sharding.State) error) error {
			if armed.Load() && class == slowClass {
				select {
				case gatherReachedSlowClass <- struct{}{}:
				default:
				}
				<-releaseSlowClassCh
			}
			return fn(classes[class], shardingState)
		}).Maybe()
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardingState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().LocalShards(mock.Anything).Return([]string{shardName}, nil).Maybe()
	mockSchemaReader.EXPECT().LocalActiveShardsCount(mock.Anything).Return(1, nil).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{nodeName}, nil).Maybe()

	fakeSchema := schema.Schema{Objects: &models.Schema{
		Classes: []*models.Class{classes[slowClass], classes[droppedClass]},
	}}

	mockSchemaGetter := schemaUC.NewMockSchemaGetter(t)
	mockSchemaGetter.EXPECT().GetSchemaSkipAuth().Return(fakeSchema).Maybe()
	mockSchemaGetter.EXPECT().ReadOnlyClass(mock.Anything).RunAndReturn(
		func(name string) *models.Class { return classes[name] }).Maybe()
	mockSchemaGetter.EXPECT().NodeName().Return(nodeName).Maybe()
	mockSchemaGetter.EXPECT().ClusterHealthScore().Return(0).Maybe()
	mockSchemaGetter.EXPECT().Nodes().Return([]string{nodeName}).Maybe()
	mockSchemaGetter.EXPECT().ShardOwner(mock.Anything, mock.Anything).Return(nodeName, nil).Maybe()
	mockSchemaGetter.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{nodeName}, nil).Maybe()
	mockSchemaGetter.EXPECT().ResolveAlias(mock.Anything).Return("").Maybe()
	mockSchemaGetter.EXPECT().GetAliasesForClass(mock.Anything).Return(nil).Maybe()

	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return(nodeName).Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return(nodeName, true).Maybe()
	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{nodeName}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{nodeName}, nil).Maybe()

	logger, _ := logrustest.NewNullLogger()
	repo, err := New(logger, nodeName, Config{
		RootPath:                  t.TempDir(),
		MaxImportGoroutinesFactor: 1,
	}, &FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{}, &FakeReplicationClient{}, nil,
		memwatch.NewDummyMonitor(), mockNodeSelector, mockSchemaReader, mockReplicationFSMReader)
	require.NoError(t, err)
	repo.SetSchemaGetter(mockSchemaGetter)
	require.NoError(t, repo.WaitForStartup(ctx))
	defer func() {
		releaseSlowClass()
		repo.Shutdown(ctx)
	}()

	// both indices are created from the schema during startup
	require.NotNil(t, repo.GetIndex(schema.ClassName(slowClass)))
	require.NotNil(t, repo.GetIndex(schema.ClassName(droppedClass)))

	armed.Store(true)

	gatherDone := make(chan struct{})
	enterrors.GoWrapper(func() {
		defer close(gatherDone)
		var shards []*models.NodeShardStatus
		repo.localNodeShardStats(ctx, &shards, "", "")
	}, logger)

	select {
	case <-gatherReachedSlowClass:
	case <-time.After(10 * time.Second):
		t.Fatal("gather never reached the slow collection")
	}

	deleteDone := make(chan error, 1)
	enterrors.GoWrapper(func() {
		deleteDone <- repo.DeleteIndex(schema.ClassName(droppedClass))
	}, logger)

	select {
	case err := <-deleteDone:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("DeleteIndex is blocked behind an in-flight node status gather on an unrelated collection")
	}

	releaseSlowClass()

	select {
	case <-gatherDone:
	case <-time.After(10 * time.Second):
		t.Fatal("node status gather did not finish")
	}
}
