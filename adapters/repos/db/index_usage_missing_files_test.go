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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	resolver "github.com/weaviate/weaviate/adapters/repos/db/sharding"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/loadlimiter"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/replication"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// TestIndex_UsageForCollection_MissingShardFiles is a regression test for a
// production incident where a single tenant whose on-disk files disappeared
// mid-deletion aborted the entire node's usage report with:
//
//	failed to get usage data: collection X: unloaded lazy shard <id>:
//	open /var/lib/weaviate/x/<id>/lsm/objects: no such file or directory
//
// The schema still lists the tenant (so usage reaches the shard), but the
// shard dir lingers without its lsm/ contents — Shard.drop removes lsm/
// synchronously before renaming the dir away, and a failed/interrupted drop
// leaves exactly this shape behind. usageForCollection must record zero usage
// for the disappearing shard instead of failing the whole report.
func TestIndex_UsageForCollection_MissingShardFiles(t *testing.T) {
	tests := []struct {
		name string
		// loadAndDelete drops the shard from the in-memory map before usage
		// so it is processed via the INACTIVE/cold path; otherwise it stays a
		// registered-but-unloaded lazy shard (the exact reported journey).
		loadAndDelete bool
		// removeRelPath, relative to the shard directory, is deleted to simulate
		// the half-deleted on-disk state. "." removes the shard directory itself,
		// which is also the shape of a tenant that was never written to.
		removeRelPath string
	}{
		{
			name:          "unloaded lazy shard, whole shard dir gone",
			loadAndDelete: false,
			removeRelPath: ".",
		},
		{
			name:          "inactive shard, whole shard dir gone",
			loadAndDelete: true,
			removeRelPath: ".",
		},
		{
			name:          "unloaded lazy shard, missing lsm/objects",
			loadAndDelete: false,
			removeRelPath: filepath.Join("lsm", helpers.ObjectsBucketLSM),
		},
		{
			name:          "unloaded lazy shard, missing whole lsm dir",
			loadAndDelete: false,
			removeRelPath: "lsm",
		},
		{
			name:          "inactive shard, missing lsm/objects",
			loadAndDelete: true,
			removeRelPath: filepath.Join("lsm", helpers.ObjectsBucketLSM),
		},
		{
			name:          "inactive shard, missing whole lsm dir",
			loadAndDelete: true,
			removeRelPath: "lsm",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			tenantName := "test-tenant"

			index, _ := setupPopulatedLazyIndex(ctx, t, usageIndexParams{})
			t.Cleanup(func() { _ = index.Shutdown(ctx) })

			if tt.loadAndDelete {
				index.shards.LoadAndDelete(tenantName)
			}

			// Simulate the tenant being deleted underneath the usage calculation:
			// the shard directory still exists but its lsm contents are gone.
			shardDir := filepath.Join(index.path(), tenantName)
			require.DirExists(t, shardDir, "precondition: shard dir must exist")
			require.NoError(t, os.RemoveAll(filepath.Join(shardDir, tt.removeRelPath)))

			usage, err := index.usageForCollection(ctx, semaphore.NewWeighted(4), true, nil)
			require.NoError(t, err, "one disappearing shard must not fail the whole report")
			require.NotNil(t, usage)

			require.Len(t, usage.Shards, 1)
			shardUsage := usage.Shards[0]
			assert.Equal(t, tenantName, shardUsage.Name)
			assert.Equal(t, int64(0), shardUsage.ObjectsCount, "missing shard must be recorded as empty")
			assert.Equal(t, uint64(0), shardUsage.ObjectsStorageBytes)

			// an empty shard must spell its status the same way a computed one does
			wantStatus := strings.ToLower(models.TenantActivityStatusACTIVE)
			if tt.loadAndDelete {
				wantStatus = strings.ToLower(models.TenantActivityStatusINACTIVE)
			}
			assert.Equal(t, wantStatus, shardUsage.Status)
		})
	}
}

// dimensionTracking says which phases of setupPopulatedLazyIndex run with
// TRACK_VECTOR_DIMENSIONS on.
type dimensionTracking int

const (
	trackingOn dimensionTracking = iota
	trackingOff
	// trackingStoppedAfterImport tracks the imported objects and then returns an
	// index that no longer tracks, leaving dimensions on disk that no bucket in
	// memory covers.
	trackingStoppedAfterImport
)

// usageIndexParams parametrizes setupPopulatedLazyIndex.
type usageIndexParams struct {
	// namedVectors go on the class alongside the legacy vector. An entry with a nil
	// VectorIndexConfig is left out of the index's parsed configs, matching a
	// dropped vector, whose config the schema parser leaves nil.
	namedVectors map[string]models.VectorConfig
	// vectorDimensions, when positive, stores a vector of that many dimensions
	// for every object under every configured named vector.
	vectorDimensions  int
	dimensionTracking dimensionTracking
}

// setupPopulatedLazyIndex creates an index for a multi-tenant class, populates
// one tenant with objects, flushes it to disk, then returns a fresh index with
// lazy loading enabled so the populated tenant is a registered-but-unloaded
// lazy shard reading from disk. The returned map is what the schema would hand
// to the usage report for this index, legacy vector included.
func setupPopulatedLazyIndex(ctx context.Context, t *testing.T, params usageIndexParams) (*Index, map[string]models.VectorConfig) {
	t.Helper()

	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()

	className := "TestUsageClass"
	tenantName := "test-tenant"
	objectCount := int(populatedObjectsCount)

	shardState := &sharding.State{
		Physical: map[string]sharding.Physical{
			tenantName: {
				Name:           tenantName,
				BelongsToNodes: []string{"test-node"},
				Status:         models.TenantActivityStatusHOT,
			},
		},
		PartitioningEnabled: true,
	}
	shardState.SetLocalName("test-node")

	class := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:         "name",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
			},
		},
		InvertedIndexConfig: &models.InvertedIndexConfig{},
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: shardState.PartitioningEnabled,
		},
		VectorConfig: params.namedVectors,
	}

	legacyVectorConfig := enthnsw.UserConfig{VectorCacheMaxObjects: 1000}
	namedVectorConfigs := make(map[string]schemaConfig.VectorIndexConfig, len(params.namedVectors))
	reportedVectorConfigs := map[string]models.VectorConfig{
		"": {VectorIndexType: legacyVectorConfig.IndexType(), VectorIndexConfig: legacyVectorConfig},
	}
	for targetVector, vectorConfig := range params.namedVectors {
		reportedVectorConfigs[targetVector] = vectorConfig
		if vectorConfig.VectorIndexConfig == nil {
			continue
		}
		parsed, ok := vectorConfig.VectorIndexConfig.(schemaConfig.VectorIndexConfig)
		require.True(t, ok, "named vector %q needs a parsed config", targetVector)
		namedVectorConfigs[targetVector] = parsed
	}

	fakeSchema := schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{class},
		},
	}

	scheduler := queue.NewScheduler(queue.SchedulerOptions{
		Logger:  logger,
		Workers: 1,
	})

	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(className string, retryIfClassNotFound bool, readerFunc func(*models.Class, *sharding.State) error) error {
		return readerFunc(class, shardState)
	}).Maybe()

	mockSchema := schemaUC.NewMockSchemaGetter(t)
	mockSchema.EXPECT().GetSchemaSkipAuth().Maybe().Return(fakeSchema)
	mockSchema.EXPECT().ReadOnlyClass(className).Maybe().Return(class)
	mockSchema.EXPECT().NodeName().Maybe().Return("test-node")
	mockSchema.EXPECT().ShardOwner(className, tenantName).Maybe().Return("test-node", nil)
	mockSchema.EXPECT().TenantsShards(ctx, className, tenantName).Maybe().
		Return(map[string]string{tenantName: models.TenantActivityStatusHOT}, nil)

	mockRouter := types.NewMockRouter(t)
	mockRouter.EXPECT().GetWriteReplicasLocation(className, mock.Anything, mock.Anything).
		Return(types.WriteReplicaSet{
			Replicas: []types.Replica{{NodeName: "test-node", ShardName: tenantName, HostAddr: "110.12.15.23"}},
		}, nil).Maybe()
	mockRouter.EXPECT().GetReadReplicasLocation(className, mock.Anything, mock.Anything).
		Return(types.ReadReplicaSet{
			Replicas: []types.Replica{{NodeName: "test-node", ShardName: tenantName, HostAddr: "110.12.15.23"}},
		}, nil).Maybe()
	shardResolver := resolver.NewShardResolver(class.Class, class.MultiTenancyConfig.Enabled, mockSchema)

	newIndexFn := func(lazy, trackDimensions bool) *Index {
		idx, err := NewIndex(ctx, IndexConfig{
			RootPath:              dirName,
			ClassName:             schema.ClassName(className),
			ReplicationFactor:     1,
			ShardLoadLimiter:      loadlimiter.NewLoadLimiter(monitoring.NoopRegisterer, "dummy", 1),
			TrackVectorDimensions: trackDimensions,
			EnableLazyLoadShards:  lazy,
		}, inverted.ConfigFromModel(class.InvertedIndexConfig),
			legacyVectorConfig,
			namedVectorConfigs,
			mockRouter,
			shardResolver,
			mockSchema,
			mockSchemaReader,
			nil,
			logger,
			nil,
			nil,
			&FakeReplicationClient{},
			&replication.GlobalConfig{},
			monitoring.GetMetrics(),
			class,
			nil,
			scheduler,
			nil,
			memwatch.NewDummyMonitor(),
			NewShardReindexerV3Noop(),
			roaringset.NewBitmapBufPoolNoop(),
			false,
			nil,
		)
		require.NoError(t, err)
		return idx
	}

	// Phase 1: eagerly-loaded index, populate and flush to disk.
	index := newIndexFn(false, params.dimensionTracking != trackingOff)
	for _, prop := range class.Properties {
		require.NoError(t, index.addProperty(ctx, prop))
	}
	for i := range objectCount {
		obj := &models.Object{
			Class:  className,
			ID:     strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012d", i)),
			Tenant: tenantName,
			Properties: map[string]interface{}{
				"name": fmt.Sprintf("test-object-%d", i),
			},
		}
		var vectors map[string][]float32
		if params.vectorDimensions > 0 {
			vector := make([]float32, params.vectorDimensions)
			for dim := range vector {
				vector[dim] = float32(i*params.vectorDimensions + dim)
			}
			vectors = make(map[string][]float32, len(namedVectorConfigs))
			for targetVector := range namedVectorConfigs {
				vectors[targetVector] = vector
			}
		}
		require.NoError(t, index.putObject(ctx, storobj.FromObject(obj, nil, vectors, nil), nil, tenantName, 0))
	}

	shard, release, err := index.GetShard(ctx, tenantName)
	require.NoError(t, err)
	require.NotNil(t, shard)
	// FlushMemtables deactivates the background flush cycle before flushing, so
	// it cannot race with the cycle's own FlushAndSwitch (which would nil out
	// b.flushing under the other goroutine and panic). Poking a single bucket's
	// FlushMemtable directly skips that guard.
	require.NoError(t, shard.Store().FlushMemtables(ctx))
	release()

	require.NoError(t, index.Shutdown(ctx))

	// Phase 2: fresh lazy index reading the populated tenant from disk.
	return newIndexFn(true, params.dimensionTracking == trackingOn), reportedVectorConfigs
}
