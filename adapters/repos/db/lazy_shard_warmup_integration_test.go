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
	"time"

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

// TestLazyShardBackgroundWarmup pins both LazyLoadShardWarmupDisabled branches:
// the sweep loads a HOT tenant a second after init, and disabling it leaves the
// tenant cold with allShardsReady already published.
func TestLazyShardBackgroundWarmup(t *testing.T) {
	ctx := context.Background()

	const (
		className = "TestWarmupClass"
		nodeName  = "test-node"
		tenant    = "busy-tenant"
	)

	tests := []struct {
		name           string
		warmupDisabled bool
	}{
		{name: "warmup materializes the shard in the background", warmupDisabled: false},
		{name: "disabled warmup leaves the shard cold", warmupDisabled: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			dirName := t.TempDir()

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

			// Write a non-zero indexcount so Index.unloadedShardIsEmpty reports false:
			// the sweep skips empty tenants, which would make both cases look identical.
			shardDir := filepath.Join(dirName, indexID(schema.ClassName(className)), tenant)
			require.NoError(t, os.MkdirAll(shardDir, os.ModePerm))
			var buf [8]byte
			binary.LittleEndian.PutUint64(buf[:], 10)
			require.NoError(t, os.WriteFile(filepath.Join(shardDir, "indexcount"), buf[:], 0o644))

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
				RootPath:                    dirName,
				ClassName:                   schema.ClassName(className),
				ReplicationFactor:           1,
				ShardLoadLimiter:            loadlimiter.NewLoadLimiter(monitoring.NoopRegisterer, "dummy", 1),
				EnableLazyLoadShards:        true,
				LazyLoadShardWarmupDisabled: tt.warmupDisabled,
			}, inverted.ConfigFromModel(class.InvertedIndexConfig),
				enthnsw.UserConfig{VectorCacheMaxObjects: 1000}, nil, mockRouter, shardResolver,
				mockSchema, mockSchemaReader, nil, logger, nil, nil, nil, &replication.GlobalConfig{}, nil,
				class, nil, scheduler, nil, nil,
				NewShardReindexerV3Noop(), roaringset.NewBitmapBufPoolNoop(), false, nil)
			require.NoError(t, err)
			defer index.Shutdown(ctx)

			stored := index.shards.Load(tenant)
			require.NotNil(t, stored, "shard must be registered after init")
			lazy, isLazy := stored.(*LazyLoadShard)
			require.True(t, isLazy, "lazy mode should store a wrapper, got %T", stored)
			require.False(t, lazy.isLoaded(), "shard must not be loaded during init in lazy mode")

			if !tt.warmupDisabled {
				// The sweep ticks once per second, so allow generous slack.
				require.Eventually(t, lazy.isLoaded, 30*time.Second, 50*time.Millisecond,
					"background warmup should materialize the shard")
				require.Eventually(t, index.allShardsReady.Load, 30*time.Second, 50*time.Millisecond,
					"allShardsReady should be published once the sweep completes")
				return
			}

			require.True(t, index.allShardsReady.Load(),
				"allShardsReady must be published immediately when warmup is disabled")
			require.Never(t, lazy.isLoaded, 3*time.Second, 100*time.Millisecond,
				"no background sweep should run when warmup is disabled")

			// Disabling the sweep must leave the shard loadable on demand.
			require.NoError(t, lazy.Load(ctx))
			require.True(t, lazy.isLoaded())
		})
	}
}
