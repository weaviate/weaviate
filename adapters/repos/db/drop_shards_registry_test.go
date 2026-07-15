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
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	resolver "github.com/weaviate/weaviate/adapters/repos/db/sharding"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/loadlimiter"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// newEmptyMTIndex builds a multi-tenant index with no local shards. dropShards
// for any tenant name therefore takes the "unloaded" (!ok) branch, which
// deletes the on-disk subtree without a Store to Shutdown each bucket.
func newEmptyMTIndex(t *testing.T) *Index {
	t.Helper()
	logger := logrus.New()

	mockSchemaGetter := schemaUC.NewMockSchemaGetter(t)
	mockSchemaGetter.On("NodeName").Return("node1").Maybe()

	class := &models.Class{
		Class:               "TestClass",
		InvertedIndexConfig: &models.InvertedIndexConfig{},
		MultiTenancyConfig:  &models.MultiTenancyConfig{Enabled: true},
	}
	mockSchemaGetter.On("ReadOnlyClass", "TestClass").Return(class).Maybe()

	ss := &sharding.State{
		Physical:            map[string]sharding.Physical{},
		PartitioningEnabled: true,
	}
	ss.SetLocalName("node1")

	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ string, _ bool, readFunc func(*models.Class, *sharding.State) error) error {
			return readFunc(class, ss)
		}).Maybe()

	scheduler := queue.NewScheduler(queue.SchedulerOptions{Logger: logger, Workers: 1})
	shardResolver := resolver.NewShardResolver(class.Class, true, mockSchemaGetter)

	index, err := NewIndex(context.Background(), IndexConfig{
		ClassName:         schema.ClassName("TestClass"),
		RootPath:          t.TempDir(),
		ReplicationFactor: 1,
		ShardLoadLimiter:  loadlimiter.NewLoadLimiter(monitoring.NoopRegisterer, "dummy", 1),
	}, inverted.ConfigFromModel(class.InvertedIndexConfig),
		hnsw.NewDefaultUserConfig(), nil, nil, shardResolver, mockSchemaGetter, mockSchemaReader, nil, logger, nil, nil, nil, nil, nil, class, nil, scheduler, nil, nil,
		NewShardReindexerV3Noop(), roaringset.NewBitmapBufPoolNoop(), false, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = index.Shutdown(context.Background()) })
	return index
}

func tenantIDBucketPath(idx *Index, tenant string) string {
	return filepath.Join(shardPathLSM(idx.path(), tenant),
		helpers.BucketFromPropNameLSM(filters.InternalPropID))
}

// V1: the dropShards unloaded (!ok) branch deletes a tenant's on-disk subtree
// but historically never touched the GlobalBucketRegistry. A bucket entry
// stranded by an earlier incomplete teardown therefore survived the delete and
// failed the same-name restore's TryAdd. The fix purges the subtree's registry
// keys by prefix — without over-matching a sibling tenant.
func TestDropShardsRegistry_StrandedEntryPurged(t *testing.T) {
	tests := []struct {
		name        string
		dropTenant  string
		leak        []string // tenants whose property__id we pre-register (simulated strand)
		wantPurged  []string
		wantSurvive []string // prefix-boundary guard: a sibling must NOT be purged
	}{
		{
			name:       "purges the dropped tenant's stranded id bucket",
			dropTenant: "t1",
			leak:       []string{"t1"},
			wantPurged: []string{"t1"},
		},
		{
			name:        "leaves a sibling tenant's entry intact (t1 must not purge t10)",
			dropTenant:  "t1",
			leak:        []string{"t1", "t10"},
			wantPurged:  []string{"t1"},
			wantSurvive: []string{"t10"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx := newEmptyMTIndex(t)

			for _, tenant := range tt.leak {
				p := tenantIDBucketPath(idx, tenant)
				require.NoError(t, lsmkv.GlobalBucketRegistry.TryAdd(p))
				t.Cleanup(func() { lsmkv.GlobalBucketRegistry.Remove(p) })
			}

			require.NoError(t, idx.dropShards([]string{tt.dropTenant}))

			for _, tenant := range tt.wantPurged {
				p := tenantIDBucketPath(idx, tenant)
				require.NoError(t, lsmkv.GlobalBucketRegistry.TryAdd(p),
					"dropShards must leave no registry residue for the deleted tenant's lsm subtree")
				lsmkv.GlobalBucketRegistry.Remove(p)
			}
			for _, tenant := range tt.wantSurvive {
				p := tenantIDBucketPath(idx, tenant)
				require.ErrorIs(t, lsmkv.GlobalBucketRegistry.TryAdd(p), lsmkv.ErrBucketAlreadyRegistered,
					"dropShards must not purge a sibling tenant's registry entry (prefix over-match)")
			}
		})
	}
}

// V1 (nil-deref): in the unloaded (!ok) branch, when os.RemoveAll fails the
// pre-fix code logged shard.ID() on a shard that is nil in this branch,
// panicking on a nil-interface deref. The fix logs the tenant name instead.
//
// The errgroup wrapper recovers panics by default and dropShards returns its
// error-compounder (not the errgroup error), which would mask the panic. We
// disable that recovery so the pre-fix deref is observable as a crash while the
// fixed tree returns the RemoveAll error cleanly.
func TestDropShardsRegistry_UnloadedRemoveAllFailureDoesNotPanic(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("runs as root: filesystem permissions are ignored, cannot inject a RemoveAll failure")
	}
	t.Setenv("DISABLE_RECOVERY_ON_PANIC", "true")

	idx := newEmptyMTIndex(t)

	const tenant = "t1"
	// Materialise the cold tenant's dir so os.RemoveAll has something to remove,
	// then revoke write on its parent so the removal fails with EACCES.
	require.NoError(t, os.MkdirAll(shardPath(idx.path(), tenant), 0o700))
	t.Cleanup(func() { _ = os.Chmod(idx.path(), 0o700) })
	require.NoError(t, os.Chmod(idx.path(), 0o500))

	err := idx.dropShards([]string{tenant})
	require.Error(t, err, "dropShards must surface the RemoveAll failure rather than panic")
}
