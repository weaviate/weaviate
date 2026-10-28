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

package lazyshardunloading

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/cluster/schema/local"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/sharding"
)

const realDBNode = "node1"

type realDBSchema struct {
	class *models.Class
	state *sharding.State
}

func (s *realDBSchema) ReadOnlySchema() models.Schema {
	return models.Schema{Classes: []*models.Class{s.class}}
}

func (s *realDBSchema) ReadOnlyClass(name string) *models.Class {
	if name == s.class.Class {
		return s.class
	}
	return nil
}

func (s *realDBSchema) ResolveAlias(string) string { return "" }

func (s *realDBSchema) GetAliasesForClass(string) []*models.Alias { return nil }

func (s *realDBSchema) ShardOwner(_, shard string) (string, error) {
	if _, ok := s.state.Physical[shard]; !ok {
		return "", fmt.Errorf("shard %q not found", shard)
	}
	return realDBNode, nil
}

func (s *realDBSchema) TenantsShardsStatus(_ context.Context, _ string, tenants ...string) (map[string]string, error) {
	out := map[string]string{}
	for _, tenant := range tenants {
		out[tenant] = s.state.Physical[tenant].Status
	}
	return out, nil
}

func (s *realDBSchema) OptimisticTenantStatus(ctx context.Context, class, tenant string, _ bool) (map[string]string, error) {
	return s.TenantsShardsStatus(ctx, class, tenant)
}

func (s *realDBSchema) ShardFromUUID(string, []byte) string { return "" }

func (s *realDBSchema) ShardReplicas(string, string) ([]string, error) {
	return []string{realDBNode}, nil
}

type realDB struct {
	repo    *db.DB
	index   *db.Index
	schema  *realDBSchema
	tenants []string
	ids     map[string]strfmt.UUID
}

func newRealDB(t *testing.T, unloader db.ShardUnloader, tenants int) *realDB {
	t.Helper()
	logger, _ := logrustest.NewNullLogger()
	indexed := true
	class := &models.Class{
		Class:              "Unloadable",
		VectorIndexConfig:  enthnsw.NewDefaultUserConfig(),
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
		ReplicationConfig:  &models.ReplicationConfig{Factor: 1},
		InvertedIndexConfig: &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 60,
			Bm25:                   &models.BM25Config{K1: 1.2, B: 0.75},
			UsingBlockMaxWAND:      true,
		},
		Properties: []*models.Property{{
			Name: "name", DataType: schema.DataTypeText.PropString(), Tokenization: models.PropertyTokenizationWhitespace,
			IndexFilterable: &indexed, IndexSearchable: &indexed,
		}},
	}
	state := &sharding.State{Physical: map[string]sharding.Physical{}, PartitioningEnabled: true}
	names := make([]string, tenants)
	for i := range names {
		names[i] = fmt.Sprintf("tenant%02d", i)
		state.Physical[names[i]] = sharding.Physical{
			Name: names[i], BelongsToNodes: []string{realDBNode}, Status: models.TenantActivityStatusHOT,
		}
	}
	state.SetLocalName(realDBNode)
	sg := &realDBSchema{class: class, state: state}

	nodes := cluster.NewMockNodeSelector(t)
	nodes.EXPECT().LocalName().Return(realDBNode).Maybe()
	nodes.EXPECT().NodeHostname(mock.Anything).Return(realDBNode, true).Maybe()
	fsm := replicationTypes.NewMockReplicationFSMReader(t)
	fsm.EXPECT().HasActiveReplicationForShard(mock.Anything, mock.Anything).Return(false).Maybe()
	fsm.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).Return([]string{realDBNode}).Maybe()
	fsm.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).Return([]string{realDBNode}).Maybe()
	reader := local.NewMockSchemaReader(t)
	reader.EXPECT().Shards(mock.Anything).Return(state.AllPhysicalShards(), nil).Maybe()
	reader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ string, _ bool, read func(*models.Class, *sharding.State) error) error {
			return read(class, state)
		}).Maybe()
	reader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: []*models.Class{class}}).Maybe()
	reader.EXPECT().LocalShards(mock.Anything).Return(names, nil).Maybe()
	reader.EXPECT().LocalActiveShardsCount(mock.Anything).Return(tenants, nil).Maybe()
	reader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{realDBNode}, nil).Maybe()
	reader.EXPECT().WaitForUpdate(mock.Anything, mock.Anything).Return(nil).Maybe()

	metrics := *monitoring.GetMetrics()
	metrics.Registerer = monitoring.NoopRegisterer
	lazy := true
	repo, err := db.New(logger, realDBNode, db.Config{
		RootPath:                      t.TempDir(),
		MaxImportGoroutinesFactor:     1,
		QueryMaximumResults:           10000,
		EnableLazyLoadShards:          &lazy,
		LazyLoadShardWarmupMinObjects: -1,
		MemtablesFlushDirtyAfter:      int(time.Hour.Seconds()),
		ShardUnloader:                 unloader,
	}, &db.FakeRemoteClient{}, nodes, &db.FakeRemoteNodeClient{}, &db.FakeReplicationClient{}, &metrics,
		memwatch.NewDummyMonitor(), nodes, reader, fsm, nil)
	require.NoError(t, err)
	repo.SetShardReindexActivityLookup(func() db.ShardReindexActivityLookup { return nil })
	repo.SetSchemaGetter(sg)
	require.NoError(t, repo.WaitForStartup(context.Background()))
	t.Cleanup(func() { require.NoError(t, repo.Shutdown(context.Background())) })
	index := repo.GetIndex(schema.ClassName(class.Class))
	require.NotNil(t, index)
	return &realDB{repo: repo, index: index, schema: sg, tenants: names, ids: map[string]strfmt.UUID{}}
}

func (r *realDB) put(t *testing.T, tenant string) {
	t.Helper()
	shard, release, err := r.index.GetShard(context.Background(), tenant)
	require.NoError(t, err)
	defer release()
	obj := &storobj.Object{MarshallerVersion: 1, Object: models.Object{
		ID: strfmt.UUID(uuid.NewString()), Class: r.schema.class.Class, Tenant: tenant,
		Properties: map[string]any{"name": tenant},
	}}
	require.NoError(t, shard.PutObject(context.Background(), obj))
	r.ids[tenant] = obj.ID()
}

func (r *realDB) exists(t *testing.T, tenant string) bool {
	t.Helper()
	shard, release, err := r.index.GetShard(context.Background(), tenant)
	require.NoError(t, err)
	defer release()
	ok, err := shard.Exists(context.Background(), r.ids[tenant])
	require.NoError(t, err)
	return ok
}

func (r *realDB) loaded(t *testing.T) int {
	t.Helper()
	require.NoError(t, r.index.ForEachShard(func(_ string, s db.ShardLike) error {
		_, ok := s.(*db.LazyLoadShard)
		require.True(t, ok, "every shard stays a lazy wrapper, got %T", s)
		return nil
	}))
	n := 0
	require.NoError(t, r.index.ForEachLoadedShard(func(string, db.ShardLike) error { n++; return nil }))
	return n
}

func (r *realDB) requireTenantsUnchanged(t *testing.T) {
	t.Helper()
	statuses, err := r.schema.TenantsShardsStatus(context.Background(), r.schema.class.Class, r.tenants...)
	require.NoError(t, err)
	for _, tenant := range r.tenants {
		require.Equal(t, models.TenantActivityStatusHOT, statuses[tenant])
	}
	require.Len(t, r.tenants, r.shardCount(t))
}

func (r *realDB) shardCount(t *testing.T) int {
	t.Helper()
	n := 0
	require.NoError(t, r.index.ForEachShard(func(string, db.ShardLike) error { n++; return nil }))
	return n
}

func realKnobs(configure func(k *testKnobs)) Knobs {
	k := newTestKnobs()
	k.interval = runtime.NewDynamicValue(50 * time.Millisecond)
	k.idleCheckInterval = runtime.NewDynamicValue(50 * time.Millisecond)
	k.minResidency = runtime.NewDynamicValue(time.Duration(0))
	k.workers = runtime.NewDynamicValue(2)
	configure(k)
	return k.knobs()
}

func gatherCounter(t *testing.T, reg *prometheus.Registry, name, label, value string) float64 {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == label && l.GetValue() == value {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

func TestRealIndexMakesRoomAndDrainsToTheLimit(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	reg := prometheus.NewPedanticRegistry()
	u := New(Config{Logger: logger, Registerer: reg, Knobs: realKnobs(func(k *testKnobs) { set(t, k.maxLoaded, 3) })})
	r := newRealDB(t, u, 8)
	require.Zero(t, r.loaded(t))

	for _, tenant := range r.tenants {
		r.put(t, tenant)
		require.LessOrEqual(t, u.Registry().Loaded(), int64(4), "make-room keeps the node at the limit plus the load in flight")
	}
	require.Positive(t, gatherCounter(t, reg, "weaviate_lazy_shard_unloading_admissions_total", "outcome", "made_room"))
	require.Eventually(t, func() bool { return r.loaded(t) <= 3 }, 10*time.Second, 20*time.Millisecond)
	require.Positive(t, gatherCounter(t, reg, "weaviate_lazy_shard_unloading_unloads_total", "outcome", "ok"))

	for _, tenant := range r.tenants {
		require.True(t, r.exists(t, tenant), "tenant %s reloads with its data", tenant)
	}
	require.Eventually(t, func() bool { return r.loaded(t) <= 3 && u.Registry().Unloading() == 0 }, 10*time.Second, 20*time.Millisecond)
	require.EqualValues(t, r.loaded(t), u.Registry().Loaded())
	r.requireTenantsUnchanged(t)
}

func TestRealIndexUnloadsIdleShardsWithoutAnyLimit(t *testing.T) {
	logger, _ := logrustest.NewNullLogger()
	idle := runtime.NewDynamicValue(time.Second)
	u := New(Config{Logger: logger, Knobs: realKnobs(func(k *testKnobs) { k.idleTTL = idle })})
	r := newRealDB(t, u, 4)
	for _, tenant := range r.tenants {
		r.put(t, tenant)
	}
	require.Equal(t, 4, r.loaded(t))
	require.Eventually(t, func() bool { return r.loaded(t) == 0 && u.Registry().Loaded() == 0 }, 10*time.Second, 20*time.Millisecond)

	set(t, idle, 0)
	u.RefreshConfig()
	for _, tenant := range r.tenants {
		require.True(t, r.exists(t, tenant))
	}
	time.Sleep(2500 * time.Millisecond)
	require.Equal(t, 4, r.loaded(t), "idle_ttl 0 keeps idle shards loaded")
	r.requireTenantsUnchanged(t)
}

func TestRealIndexWithoutAnUnloaderKeepsEveryShardLoaded(t *testing.T) {
	r := newRealDB(t, nil, 6)
	for _, tenant := range r.tenants {
		r.put(t, tenant)
	}
	time.Sleep(500 * time.Millisecond)
	require.Equal(t, 6, r.loaded(t))
	require.Nil(t, r.repo.ShardUnloader())
}

var _ db.ShardUnloader = (*Unloader)(nil)
