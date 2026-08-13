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

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/memwatch"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// TestStartupShardCounting drives Migrator.AddClass — the path production builds an
// IndexConfig on — and asserts the DB's startup shard counters moved.
//
// The unit tests set db.startupShards by hand, so they pin the arithmetic in
// scanStartupProgress but nothing that feeds it. Delete the Add(1) calls in
// initAndStoreShards, or the StartupShards line in migrator.go, and they all
// stay green while a real node reports 0% for its entire startup.
func TestStartupShardCounting(t *testing.T) {
	tests := []struct {
		name      string
		tenants   []string // non-empty makes the class multi-tenant
		wantEager int64
		wantLazy  int64
	}{
		{
			name:      "a regular class loads its shard eagerly",
			wantEager: 1,
		},
		{
			// Empty tenants take the "avoid footprint of empty shards" branch and
			// are stored as LazyLoadShards, which is what keeps them out of the
			// denominator rather than stalling progress below 100%.
			name:     "empty tenants are stored lazily and counted apart",
			tenants:  []string{"t1", "t2", "t3"},
			wantLazy: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			class := &models.Class{
				Class:               "Counted",
				VectorIndexConfig:   enthnsw.NewDefaultUserConfig(),
				InvertedIndexConfig: BM25FinvertedConfig(1.2, 0.75, "none"),
			}
			state := singleShardState()
			if len(tt.tenants) > 0 {
				class.MultiTenancyConfig = &models.MultiTenancyConfig{Enabled: true}
				state = tenantShardState(tt.tenants)
			}

			db := newShardCountingDB(t, class, state)
			require.Zero(t, db.startupShards.eager.Load(), "nothing loaded yet")

			logger, _ := test.NewNullLogger()
			require.NoError(t, NewMigrator(db, logger, "node1").
				AddClass(context.Background(), class))

			assert.Equal(t, tt.wantEager, db.startupShards.eager.Load(), "eager count")
			assert.Equal(t, tt.wantLazy, db.startupShards.lazy.Load(), "lazy count")
		})
	}
}

// tenantShardState returns a partitioned state whose tenants are all local and
// HOT, so initAndStoreShards has to decide eager-vs-lazy for each.
func tenantShardState(tenants []string) *sharding.State {
	physical := make(map[string]sharding.Physical, len(tenants))
	for _, name := range tenants {
		physical[name] = sharding.Physical{
			Name:           name,
			BelongsToNodes: []string{"node1"},
			Status:         models.TenantActivityStatusHOT,
		}
	}
	s := &sharding.State{Physical: physical, PartitioningEnabled: true}
	s.SetLocalName("node1")
	return s
}

// newShardCountingDB returns a started DB that does not yet know about class,
// so AddClass builds a real index against real files.
func newShardCountingDB(t *testing.T, class *models.Class, state *sharding.State) *DB {
	t.Helper()
	logger, _ := test.NewNullLogger()

	// The schema is empty when the DB starts, exactly as it is before RAFT
	// restores it; the class arrives afterwards, via AddClass.
	schemaGetter := &fakeSchemaGetter{
		schema:     schema.Schema{Objects: &models.Schema{}},
		shardState: state,
	}

	sr := schemaUC.NewMockSchemaReader(t)
	sr.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ string, _ bool, read func(*models.Class, *sharding.State) error) error {
			return read(class, state)
		}).Maybe()
	sr.EXPECT().Shards(mock.Anything).Return(state.AllPhysicalShards(), nil).Maybe()
	sr.EXPECT().ShardReplicas(mock.Anything, mock.Anything).Return([]string{"node1"}, nil).Maybe()
	sr.EXPECT().WaitForUpdate(mock.Anything, mock.Anything).Return(nil).Maybe()
	sr.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: nil}).Maybe()
	sr.EXPECT().LocalActiveShardsCount(mock.Anything).Return(len(state.Physical), nil).Maybe()
	sr.EXPECT().LocalShards(mock.Anything).Return(state.AllPhysicalShards(), nil).Maybe()

	fsm := types.NewMockReplicationFSMReader(t)
	fsm.EXPECT().HasActiveReplicationForShard(mock.Anything, mock.Anything).Return(false).Maybe()
	fsm.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).
		Return([]string{"node1"}).Maybe()
	fsm.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).
		Return([]string{"node1"}).Maybe()

	nodes := cluster.NewMockNodeSelector(t)
	nodes.EXPECT().LocalName().Return("node1").Maybe()
	nodes.EXPECT().NodeHostname(mock.Anything).Return("node1", true).Maybe()

	db, err := New(logger, "node1", Config{
		RootPath:                  t.TempDir(),
		MemtablesFlushDirtyAfter:  60,
		QueryMaximumResults:       10000,
		MaxImportGoroutinesFactor: 1,
	}, &FakeRemoteClient{}, nodes, &FakeRemoteNodeClient{}, nil, nil,
		memwatch.NewDummyMonitor(), nodes, sr, fsm, nil)
	require.NoError(t, err)

	db.SetSchemaGetter(schemaGetter)
	require.NoError(t, db.WaitForStartup(context.Background()))
	t.Cleanup(func() { _ = db.Shutdown(context.Background()) })

	schemaGetter.schema = schema.Schema{Objects: &models.Schema{Classes: []*models.Class{class}}}
	return db
}
