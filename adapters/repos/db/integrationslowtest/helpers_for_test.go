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

package integrationslowtest

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/cluster/schema/local"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/sharding"
	shardingConfig "github.com/weaviate/weaviate/usecases/sharding/config"
)

// repoParams carries what differs per test. A nil logger means a discarding
// one, a nil promMetrics registers to the global noop registerer, and config
// edits the fixture defaults before the DB is built.
type repoParams struct {
	logger      logrus.FieldLogger
	promMetrics *monitoring.PrometheusMetrics
	config      func(*db.Config)
}

// newRepo builds a single-node DB with the given classes migrated in and
// shuts it down on cleanup.
func newRepo(t *testing.T, p repoParams, classes ...*models.Class) (*db.DB, *fakeSchemaGetter) {
	t.Helper()

	logger := p.logger
	if logger == nil {
		logger, _ = test.NewNullLogger()
	}

	cfg := db.Config{
		MemtablesFlushDirtyAfter:  60,
		RootPath:                  t.TempDir(),
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
		EnableLazyLoadShards:      boolPtr(true),
		HaltForTransferTimeout:    config.DefaultHaltForTransferTimeout,
	}
	if p.config != nil {
		p.config(&cfg)
	}

	shardState := singleShardState()
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{Classes: nil}},
		shardState: shardState,
	}
	classByName := func(name string) *models.Class {
		for _, class := range classes {
			if class.Class == name {
				return class
			}
		}
		return &models.Class{Class: name}
	}

	mockSchemaReader := local.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Shards(mock.Anything).Return(shardState.AllPhysicalShards(), nil).Maybe()
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(className string, retry bool, readFunc func(*models.Class, *sharding.State) error) error {
			return readFunc(classByName(className), shardState)
		}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlyClass(mock.Anything).RunAndReturn(classByName).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	mockSchemaReader.EXPECT().LocalShards(mock.Anything).Return([]string{"shard1"}, nil).Maybe()
	mockSchemaReader.EXPECT().LocalActiveShardsCount(mock.Anything).Return(1, nil).Maybe()
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	mockSchemaReader.EXPECT().WaitForUpdate(mock.Anything, mock.Anything).Return(nil).Maybe()
	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{"node1"}).Maybe()
	mockReplicationFSMReader.EXPECT().HasActiveReplicationForShard(mock.Anything, mock.Anything).Return(false).Maybe()
	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return("node1").Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()

	repo, err := db.New(logger, "node1", cfg, &db.FakeRemoteClient{}, mockNodeSelector,
		&db.FakeRemoteNodeClient{}, &db.FakeReplicationClient{}, p.promMetrics, memwatch.NewDummyMonitor(),
		mockNodeSelector, mockSchemaReader, mockReplicationFSMReader, nil)
	require.NoError(t, err)
	repo.SetSchemaGetter(schemaGetter)
	repo.SetShardReindexActivityLookup(func() db.ShardReindexActivityLookup {
		return func(string, string) bool { return false }
	})
	require.NoError(t, repo.WaitForStartup(context.Background()))
	t.Cleanup(func() {
		require.NoError(t, repo.Shutdown(context.Background()))
	})

	schemaGetter.schema = schema.Schema{Objects: &models.Schema{Classes: classes}}
	migrator := db.NewMigrator(repo, logger, "node1")
	for _, class := range classes {
		require.NoError(t, migrator.AddClass(context.Background(), class))
	}

	return repo, schemaGetter
}

// singleShard returns the one shard backing className; every fixture here is
// single-shard.
func singleShard(t *testing.T, repo *db.DB, className string) db.ShardLike {
	t.Helper()
	idx := repo.GetIndex(schema.ClassName(className))
	require.NotNil(t, idx)
	var shard db.ShardLike
	require.NoError(t, idx.ForEachShard(func(_ string, s db.ShardLike) error {
		shard = s
		return nil
	}))
	require.NotNil(t, shard)
	return shard
}

type fakeSchemaGetter struct {
	schema     schema.Schema
	shardState *sharding.State
}

func (f *fakeSchemaGetter) ReadOnlySchema() models.Schema {
	if f.schema.Objects == nil {
		return models.Schema{}
	}
	return *f.schema.Objects
}

func (f *fakeSchemaGetter) ReadOnlyClass(class string) *models.Class {
	return f.schema.GetClass(class)
}

func (f *fakeSchemaGetter) ResolveAlias(string) string {
	return ""
}

func (f *fakeSchemaGetter) GetAliasesForClass(string) []*models.Alias {
	return nil
}

func (f *fakeSchemaGetter) CopyShardingState(class string) *sharding.State {
	return f.shardState
}

func (f *fakeSchemaGetter) ShardOwner(class, shard string) (string, error) {
	x, ok := f.shardState.Physical[shard]
	if !ok {
		return "", fmt.Errorf("shard not found")
	}
	if len(x.BelongsToNodes) < 1 || x.BelongsToNodes[0] == "" {
		return "", fmt.Errorf("owner node not found")
	}
	return x.BelongsToNodes[0], nil
}

func (f *fakeSchemaGetter) ShardReplicas(class, shard string) ([]string, error) {
	x, ok := f.shardState.Physical[shard]
	if !ok {
		return nil, fmt.Errorf("shard not found")
	}
	return x.BelongsToNodes, nil
}

func (f *fakeSchemaGetter) TenantsShardsStatus(_ context.Context, class string, tenants ...string) (map[string]string, error) {
	res := map[string]string{}
	for _, t := range tenants {
		res[t] = models.TenantActivityStatusHOT
	}
	return res, nil
}

func (f *fakeSchemaGetter) OptimisticTenantStatus(_ context.Context, class string, tenant string, _ bool) (map[string]string, error) {
	return map[string]string{tenant: models.TenantActivityStatusHOT}, nil
}

func (f *fakeSchemaGetter) ShardFromUUID(class string, uuid []byte) string {
	return f.shardState.Shard("", string(uuid))
}

func (f *fakeSchemaGetter) Nodes() []string {
	return []string{"node1"}
}

func (f *fakeSchemaGetter) NodeName() string {
	return "node1"
}

func (f *fakeSchemaGetter) ClusterHealthScore() int {
	return 0
}

func (f *fakeSchemaGetter) ResolveParentNodes(_ string, shard string) (map[string]string, error) {
	return nil, nil
}

func (f *fakeSchemaGetter) Statistics() map[string]any {
	return nil
}

func singleShardState() *sharding.State {
	cfg, err := shardingConfig.ParseConfig(nil, 1)
	if err != nil {
		panic(err)
	}
	selector := mocks.NewMockNodeSelector("node1")
	s, err := sharding.InitState("test-index", cfg, selector.LocalName(), selector.StorageCandidates(), 1, false)
	if err != nil {
		panic(err)
	}
	return s
}

func invertedConfig() *models.InvertedIndexConfig {
	return &models.InvertedIndexConfig{
		CleanupIntervalSeconds: 60,
		Stopwords: &models.StopwordConfig{
			Preset: "none",
		},
		IndexNullState:      true,
		IndexPropertyLength: true,
		UsingBlockMaxWAND:   config.DefaultUsingBlockMaxWAND,
	}
}

func BM25FinvertedConfig(k1, b float32, stopWordPreset string) *models.InvertedIndexConfig {
	return &models.InvertedIndexConfig{
		Bm25: &models.BM25Config{
			K1: k1,
			B:  b,
		},
		CleanupIntervalSeconds: 60,
		Stopwords: &models.StopwordConfig{
			Preset: stopWordPreset,
		},
		IndexNullState:      true,
		IndexPropertyLength: true,
		UsingBlockMaxWAND:   config.DefaultUsingBlockMaxWAND,
	}
}

func EqualFloats(t *testing.T, expected, actual float32, significantFigures int) {
	s1 := fmt.Sprintf("%v", expected)
	s2 := fmt.Sprintf("%v", actual)
	if len(s1) < 2 || len(s2) < 2 {
		t.Fail()
	}
	if len(s1) <= significantFigures {
		significantFigures = len(s1) - 1
	}
	if len(s2) <= significantFigures {
		significantFigures = len(s2) - 1
	}
	require.Equal(t, s1[:significantFigures+1], s2[:significantFigures+1])
}

func boolPtr(b bool) *bool {
	return &b
}

func ptFloat32(in float32) *float32 {
	return &in
}

func intToUUID(i int) strfmt.UUID {
	return strfmt.UUID(uuid.MustParse(fmt.Sprintf("%032d", i)).String())
}

func randVector(dim int) []float32 {
	vec := make([]float32, dim)
	for i := range vec {
		vec[i] = rand.Float32()
	}
	return vec
}

func mustParseTime(in string) time.Time {
	asTime, err := time.Parse(time.RFC3339, in)
	if err != nil {
		panic(err)
	}
	return asTime
}
