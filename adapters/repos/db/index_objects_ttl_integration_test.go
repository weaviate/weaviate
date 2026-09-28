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
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
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
	configRuntime "github.com/weaviate/weaviate/usecases/config/runtime"
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

	index, err := NewIndex(ctx, nil, IndexConfig{
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

// ttlMultiTenantClass is batchDeleteTTLClass with multi-tenancy on, which is what puts a
// sweep of it on the tenant loop rather than the shard loop.
func ttlMultiTenantClass() *models.Class {
	class := batchDeleteTTLClass()
	class.MultiTenancyConfig = &models.MultiTenancyConfig{Enabled: true}
	return class
}

func setupTTLTenantRepo(t *testing.T, batchSize, expiredCount, aliveCount int) *DB {
	t.Helper()

	shardState := batchDeleteTenantShardState(batchDeleteTenant)
	repo := setupTestDBWithShardState(t, t.TempDir(), shardState, func(cfg *Config) {
		cfg.ObjectsTTLBatchSize = configRuntime.NewDynamicValue(batchSize)
	}, ttlMultiTenantClass())
	t.Cleanup(func() { require.NoError(t, repo.Shutdown(context.Background())) })

	insertTTLObjects(t, repo, batchDeleteTenant, expiredCount, aliveCount)
	return repo
}

func ttlTenantIndex(t *testing.T, repo *DB) (*Index, *logrus.Logger) {
	t.Helper()

	index := repo.GetIndex(schema.ClassName(batchDeleteTTLClassName))
	require.NotNil(t, index)
	logger, ok := repo.logger.(*logrus.Logger)
	require.True(t, ok, "the test DB logs through a *logrus.Logger")
	return index, logger
}

// TestTTLSweepsATenantAcrossSeveralBatches drives the real multi-tenant delete closure,
// which the tenantTTLLoop unit tests replace with a fake. A sweep that reported its
// batches as having deleted nothing would stop after the first one.
func TestTTLSweepsATenantAcrossSeveralBatches(t *testing.T) {
	const (
		expiredCount = 12
		aliveCount   = 3
		sweepBatch   = 5
	)

	repo := setupTTLTenantRepo(t, sweepBatch, expiredCount, aliveCount)
	index, logger := ttlTenantIndex(t, repo)

	var deleted atomic.Int32
	eg := enterrors.NewErrorGroupWrapper(logger)
	ec := errorcompounder.New()
	index.incomingDeleteObjectsExpired(context.Background(), eg, ec, batchDeleteTTLProp,
		time.Now(), time.Now(), func(n int32) { deleted.Add(n) }, 0)
	eg.Wait()

	require.NoError(t, ec.ToError())
	require.Equal(t, int32(expiredCount), deleted.Load(),
		"every expired object of the tenant goes, over more than one batch of %d", sweepBatch)
}

// ttlFailingSchemaReader fails every WaitForUpdate, which makes the batch delete fail
// before any shard I/O while findUUIDs still resolves real uuids. It cancels the sweep
// after maxCalls, so a loop that does not stop fails on the count instead of hanging.
type ttlFailingSchemaReader struct {
	schemaUC.SchemaReader
	calls    *atomic.Int32
	maxCalls int32
	cancel   context.CancelCauseFunc
	err      error
}

func (r ttlFailingSchemaReader) WaitForUpdate(context.Context, uint64) error {
	if r.calls.Add(1) >= r.maxCalls {
		r.cancel(errors.New("the sweep kept retrying a delete that cannot succeed"))
	}
	return r.err
}

// TestTTLStopsSweepingATenantWhoseDeleteFails drives the real closure's error return. The
// error must reach findAndDelete rather than being filed inside the closure, which reported
// success and left the tenant to be re-found every round.
func TestTTLStopsSweepingATenantWhoseDeleteFails(t *testing.T) {
	const (
		expiredCount = 12
		aliveCount   = 3
		sweepBatch   = 5
		maxCalls     = int32(3)
	)
	deleteErr := errors.New("schema never caught up")

	repo := setupTTLTenantRepo(t, sweepBatch, expiredCount, aliveCount)
	index, logger := ttlTenantIndex(t, repo)

	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(nil) })
	calls := &atomic.Int32{}
	index.schemaReader = ttlFailingSchemaReader{
		SchemaReader: index.schemaReader, calls: calls, maxCalls: maxCalls,
		cancel: cancel, err: deleteErr,
	}

	var deleted atomic.Int32
	eg := enterrors.NewErrorGroupWrapper(logger)
	ec := errorcompounder.New()
	index.incomingDeleteObjectsExpired(ctx, eg, ec, batchDeleteTTLProp,
		time.Now(), time.Now(), func(n int32) { deleted.Add(n) }, 1)
	eg.Wait()

	require.Equal(t, int32(1), calls.Load(), "the sweep must stop after one failed batch")
	require.Equal(t, int32(0), deleted.Load())
	require.Equal(t, 1, ec.Len(), "one filing per swept tenant")
	err := ec.ToError()
	require.ErrorContains(t, err, "batch delete")
	require.ErrorIs(t, err, deleteErr)
}
