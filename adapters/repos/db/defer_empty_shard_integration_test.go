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
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	resolver "github.com/weaviate/weaviate/adapters/repos/db/sharding"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/loadlimiter"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/replication"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// TestDeferEmptyMultiTenantShardOnInit checks that startup leaves an empty MT
// tenant (index counter 0) unloaded and materializes it on first access, while a
// tenant that has written objects (counter > 0, including data that would only be
// in an unflushed WAL) is loaded eagerly as a raw shard. It runs in eager mode,
// the path that would otherwise load every HOT shard on init.
func TestDeferEmptyMultiTenantShardOnInit(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	const (
		className = "TestDeferClass"
		nodeName  = "test-node"
	)

	tests := []struct {
		name string
		// objectCount is the persisted index counter; 0 means the tenant has never
		// held objects. It is written directly so the "loaded" branch is exercised
		// even without flushed segments (mirrors the reused-WAL case).
		objectCount  uint64
		wantDeferred bool
	}{
		{
			name:         "empty tenant (counter 0) is deferred",
			objectCount:  0,
			wantDeferred: true,
		},
		{
			name:         "tenant with objects is loaded, not deferred",
			objectCount:  10,
			wantDeferred: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dirName := t.TempDir()
			tenant := "empty-tenant"

			class := &models.Class{
				Class:               className,
				InvertedIndexConfig: &models.InvertedIndexConfig{},
				MultiTenancyConfig:  &models.MultiTenancyConfig{Enabled: true},
				ReplicationConfig:   &models.ReplicationConfig{Factor: 1},
			}
			fakeSchema := schema.Schema{Objects: &models.Schema{Classes: []*models.Class{class}}}

			shardState := &sharding.State{
				Physical: map[string]sharding.Physical{
					tenant: {
						Name:           tenant,
						BelongsToNodes: []string{nodeName},
						Status:         models.TenantActivityStatusHOT,
					},
				},
				PartitioningEnabled: true,
			}
			shardState.SetLocalName(nodeName)

			if tt.objectCount > 0 {
				// Persist an index counter as a real shard would, so the tenant reads
				// as non-empty without needing flushed segments.
				shardDir := filepath.Join(dirName, indexID(schema.ClassName(className)), tenant)
				require.NoError(t, os.MkdirAll(shardDir, os.ModePerm))
				var buf [8]byte
				binary.LittleEndian.PutUint64(buf[:], tt.objectCount)
				require.NoError(t, os.WriteFile(filepath.Join(shardDir, "indexcount"), buf[:], 0o644))
			}

			scheduler := queue.NewScheduler(queue.SchedulerOptions{Logger: logger, Workers: 1})

			mockSchemaReader := schemaUC.NewMockSchemaReader(t)
			mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
				func(_ string, _ bool, readerFunc func(*models.Class, *sharding.State) error) error {
					return readerFunc(class, shardState)
				}).Maybe()
			mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: []*models.Class{class}}).Maybe()

			mockSchema := schemaUC.NewMockSchemaGetter(t)
			mockSchema.EXPECT().GetSchemaSkipAuth().Maybe().Return(fakeSchema)
			mockSchema.EXPECT().ReadOnlyClass(className).Maybe().Return(class)
			mockSchema.EXPECT().NodeName().Maybe().Return(nodeName)
			mockSchema.EXPECT().TenantsShards(ctx, className, tenant).Maybe().
				Return(map[string]string{tenant: models.TenantActivityStatusHOT}, nil)

			mockRouter := types.NewMockRouter(t)
			mockRouter.EXPECT().GetWriteReplicasLocation(className, mock.Anything, tenant).
				Return(types.WriteReplicaSet{
					Replicas: []types.Replica{{NodeName: nodeName, ShardName: tenant, HostAddr: "10.0.0.1"}},
				}, nil).Maybe()
			mockRouter.EXPECT().GetReadReplicasLocation(className, tenant, tenant).
				Return(types.ReadReplicaSet{
					Replicas: []types.Replica{{NodeName: nodeName, ShardName: tenant, HostAddr: "10.0.0.1"}},
				}, nil).Maybe()

			schemaGetter := &fakeSchemaGetter{schema: fakeSchema, shardState: shardState}
			shardResolver := resolver.NewShardResolver(className, true, schemaGetter)

			index, err := NewIndex(ctx, IndexConfig{
				RootPath:             dirName,
				ClassName:            schema.ClassName(className),
				ReplicationFactor:    1,
				ShardLoadLimiter:     loadlimiter.NewLoadLimiter(monitoring.NoopRegisterer, "dummy", 1),
				EnableLazyLoadShards: false, // eager mode: without the fix every HOT shard loads at init
			}, inverted.ConfigFromModel(class.InvertedIndexConfig),
				enthnsw.UserConfig{VectorCacheMaxObjects: 1000}, nil, mockRouter, shardResolver,
				mockSchema, mockSchemaReader, nil, logger, nil, nil, nil, &replication.GlobalConfig{}, nil,
				class, nil, scheduler, nil, nil,
				NewShardReindexerV3Noop(), roaringset.NewBitmapBufPoolNoop(), false, nil)
			require.NoError(t, err)
			defer index.Shutdown(ctx)

			stored := index.shards.Load(tenant)
			require.NotNil(t, stored, "shard must be registered after init")

			if tt.wantDeferred {
				lazy, isLazy := stored.(*LazyLoadShard)
				require.True(t, isLazy, "empty tenant should be a deferred lazy wrapper, got %T", stored)
				require.False(t, lazy.isLoaded(), "empty tenant should be unloaded after init")

				require.NoError(t, lazy.Load(ctx))
				require.True(t, lazy.isLoaded(), "deferred tenant should materialize on access")
			} else {
				// Loaded eagerly, so it is stored as the raw shard, not a wrapper.
				_, stillLazy := stored.(*LazyLoadShard)
				require.False(t, stillLazy, "loaded tenant should be stored as a raw *Shard, got %T", stored)
			}
		})
	}
}
