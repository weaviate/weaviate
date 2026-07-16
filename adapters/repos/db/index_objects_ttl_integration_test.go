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
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	resolver "github.com/weaviate/weaviate/adapters/repos/db/sharding"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/loadlimiter"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/replication"
	"github.com/weaviate/weaviate/entities/schema"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// TestTTLSkipsLazyUnloadedTenant guards that the TTL sweep leaves a lazy-unloaded HOT tenant
// unloaded. Reaching findUUIDs would call the unmocked router and force-load the shard,
// failing the test.
func TestTTLSkipsLazyUnloadedTenant(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	const (
		className = "TestTTLLazyClass"
		nodeName  = "test-node"
		tenant    = "idle-tenant"
	)

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

	scheduler := queue.NewScheduler(queue.SchedulerOptions{Logger: logger, Workers: 1})

	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ string, _ bool, readerFunc func(*models.Class, *sharding.State) error) error {
			return readerFunc(class, shardState)
		}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlySchema().Return(models.Schema{Classes: []*models.Class{class}}).Maybe()
	mockSchemaReader.EXPECT().Shards(className).Return([]string{tenant}, nil).Once()

	mockSchema := schemaUC.NewMockSchemaGetter(t)
	mockSchema.EXPECT().GetSchemaSkipAuth().Maybe().Return(fakeSchema)
	mockSchema.EXPECT().ReadOnlyClass(className).Maybe().Return(class)
	mockSchema.EXPECT().NodeName().Maybe().Return(nodeName)

	// No read-routing expectations: the skip means findUUIDs is never reached. Removing the
	// skip would call BuildReadRoutingPlan and fail on an unexpected mock call.
	mockRouter := types.NewMockRouter(t)

	schemaGetter := &fakeSchemaGetter{schema: fakeSchema, shardState: shardState}
	shardResolver := resolver.NewShardResolver(className, true, schemaGetter)

	index, err := NewIndex(ctx, IndexConfig{
		RootPath:             t.TempDir(),
		ClassName:            schema.ClassName(className),
		ReplicationFactor:    1,
		ShardLoadLimiter:     loadlimiter.NewLoadLimiter(monitoring.NoopRegisterer, "dummy", 1),
		EnableLazyLoadShards: true,
	}, inverted.ConfigFromModel(class.InvertedIndexConfig),
		enthnsw.UserConfig{VectorCacheMaxObjects: 1000}, nil, mockRouter, shardResolver,
		mockSchema, mockSchemaReader, nil, logger, nil, nil, nil, &replication.GlobalConfig{}, nil,
		class, nil, scheduler, nil, nil,
		NewShardReindexerV3Noop(), roaringset.NewBitmapBufPoolNoop(), false, nil)
	require.NoError(t, err)
	defer index.Shutdown(ctx)

	stored := index.shards.Load(tenant)
	lazy, isLazy := stored.(*LazyLoadShard)
	require.True(t, isLazy, "HOT tenant should be a lazy wrapper, got %T", stored)
	require.False(t, lazy.isLoaded(), "tenant must be unloaded before the sweep")

	eg := enterrors.NewErrorGroupWrapper(logger)
	ec := errorcompounder.New()
	index.incomingDeleteObjectsExpired(ctx, eg, ec, "expiresAt", time.Now(), time.Now(),
		func(int32) {}, 0)
	eg.Wait()

	require.NoError(t, ec.ToError())
	require.False(t, lazy.isLoaded(), "TTL sweep must not force-load a lazy-unloaded tenant")
}
