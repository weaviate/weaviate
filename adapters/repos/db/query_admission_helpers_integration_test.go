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
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/cluster"
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// admissionRepoParams carries the knobs that differ between the query-admission
// integration repos. promMetrics may be nil (the limiter registers to the
// global noop registerer, so its gauges are not isolated for reading); disabled
// may be nil (the runtime kill switch is not wired for the test).
type admissionRepoParams struct {
	budget      int
	maxQueue    int
	disabled    *configRuntime.DynamicValue[bool]
	promMetrics *monitoring.PrometheusMetrics
}

// newAdmissionRepo builds a single-node DB wired for the query-admission
// integration tests: a panic-level logger (bursts stay quiet), a single shard,
// the standard schema/replication/node-selector mocks with every expectation
// optional, and the given classes migrated in. It returns the repo and the
// shard backing className. Callers keep the parts that genuinely differ per
// test (budgets, class shapes, metrics wiring, object import) at their own call
// site.
func newAdmissionRepo(t *testing.T, p admissionRepoParams,
	className string, classes ...*models.Class,
) (*DB, ShardLike) {
	t.Helper()
	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel) // keep the burst quiet
	shardState := singleShardState()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: shardState,
	}

	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(cn string, retry bool, readFunc func(*models.Class, *sharding.State) error) error {
			return readFunc(&models.Class{Class: cn}, shardState)
		}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()

	repo, err := New(logger, "node1", Config{
		MemtablesFlushDirtyAfter:      60,
		RootPath:                      t.TempDir(),
		QueryMaximumResults:           10000,
		MaxImportGoroutinesFactor:     1,
		QueryAdmissionBudget:          p.budget,
		QueryAdmissionMaxQueue:        p.maxQueue,
		QueryAdmissionControlDisabled: p.disabled,
	}, &FakeRemoteClient{}, mockNodeSelector, &FakeRemoteNodeClient{}, nil, p.promMetrics, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader)
	require.Nil(t, err)
	repo.SetSchemaGetter(schemaGetter)
	require.Nil(t, repo.WaitForStartup(context.TODO()))

	schemaGetter.schema = schema.Schema{Objects: &models.Schema{Classes: classes}}
	migrator := NewMigrator(repo, logger, "node1")
	for _, class := range classes {
		require.NoError(t, migrator.AddClass(context.Background(), class))
	}

	return repo, admissionShard(t, repo, className)
}

// admissionShard returns the single shard backing className. The query-admission
// integration tests all run single-shard, so exactly one shard is expected.
func admissionShard(t *testing.T, repo *DB, className string) ShardLike {
	t.Helper()
	idx := repo.GetIndex(schema.ClassName(className))
	require.NotNil(t, idx)
	var shard ShardLike
	require.NoError(t, idx.ForEachShard(func(_ string, s ShardLike) error {
		shard = s
		return nil
	}))
	require.NotNil(t, shard)
	return shard
}
