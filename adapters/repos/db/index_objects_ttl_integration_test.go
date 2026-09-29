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

	"github.com/go-openapi/strfmt"
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

// setupTTLRepo holds the given class over the given shards, with the sweep's batch size set and
// its expired and alive objects written. tenant is empty for a collection swept by shard.
func setupTTLRepo(t *testing.T, batchSize int, shardState *sharding.State, class *models.Class,
	tenant string, expiredCount, aliveCount int,
) *DB {
	t.Helper()

	repo := setupTestDBWithShardState(t, t.TempDir(), shardState, func(cfg *Config) {
		cfg.ObjectsTTLBatchSize = configRuntime.NewDynamicValue(batchSize)
	}, class)
	t.Cleanup(func() { require.NoError(t, repo.Shutdown(context.Background())) })

	insertTTLObjects(t, repo, tenant, expiredCount, aliveCount)
	return repo
}

func setupTTLTenantRepo(t *testing.T, batchSize, expiredCount, aliveCount int) *DB {
	t.Helper()

	return setupTTLRepo(t, batchSize, batchDeleteTenantShardState(batchDeleteTenant),
		ttlMultiTenantClass(), batchDeleteTenant, expiredCount, aliveCount)
}

// setupTTLShardRepo holds the same objects without multi-tenancy, which is what puts a sweep of
// them on the shard loop rather than the tenant loop.
func setupTTLShardRepo(t *testing.T, batchSize int, shardState *sharding.State,
	expiredCount, aliveCount int,
) *DB {
	t.Helper()

	return setupTTLRepo(t, batchSize, shardState, batchDeleteTTLClass(), "", expiredCount, aliveCount)
}

func ttlIndex(t *testing.T, repo *DB) (*Index, *logrus.Logger) {
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
	index, logger := ttlIndex(t, repo)

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

// ttlCountingSchemaReader counts the batch deletes a sweep runs. err, when set, fails every one
// of them before any shard I/O while findUUIDs still resolves real uuids.
type ttlCountingSchemaReader struct {
	schemaUC.SchemaReader
	calls *atomic.Int32
	err   error
}

func (r ttlCountingSchemaReader) WaitForUpdate(context.Context, uint64) error {
	r.calls.Add(1)
	return r.err
}

// ttlRoundCounter counts a sweep's rounds off the line each one logs as it searches. It cancels
// the sweep past maxRounds, so a loop that does not stop fails on the count rather than hanging.
type ttlRoundCounter struct {
	rounds    atomic.Int32
	maxRounds int32
	cancel    context.CancelCauseFunc
}

func (c *ttlRoundCounter) Levels() []logrus.Level { return logrus.AllLevels }

func (c *ttlRoundCounter) Fire(entry *logrus.Entry) error {
	if entry.Message != "find uuids started" {
		return nil
	}
	if c.rounds.Add(1) > c.maxRounds {
		c.cancel(errors.New("the sweep kept searching after a round that deleted nothing"))
	}
	return nil
}

func boundTTLRounds(t *testing.T, logger *logrus.Logger, maxRounds int32,
	cancel context.CancelCauseFunc,
) *ttlRoundCounter {
	t.Helper()

	counter := &ttlRoundCounter{maxRounds: maxRounds, cancel: cancel}
	logger.SetLevel(logrus.DebugLevel)
	logger.AddHook(counter)
	return counter
}

// TestTTLStopsSweepingATenantWhoseDeleteFails drives the real closure's error return. The
// error must reach findAndDelete rather than being filed inside the closure, which reported
// success and left the tenant to be re-found every round.
func TestTTLStopsSweepingATenantWhoseDeleteFails(t *testing.T) {
	const (
		expiredCount = 12
		aliveCount   = 3
		sweepBatch   = 5
		maxRounds    = int32(3)
	)
	deleteErr := errors.New("schema never caught up")

	repo := setupTTLTenantRepo(t, sweepBatch, expiredCount, aliveCount)
	index, logger := ttlIndex(t, repo)

	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(nil) })
	rounds := boundTTLRounds(t, logger, maxRounds, cancel)
	calls := &atomic.Int32{}
	index.schemaReader = ttlCountingSchemaReader{
		SchemaReader: index.schemaReader, calls: calls, err: deleteErr,
	}

	var deleted atomic.Int32
	eg := enterrors.NewErrorGroupWrapper(logger)
	ec := errorcompounder.New()
	index.incomingDeleteObjectsExpired(ctx, eg, ec, batchDeleteTTLProp,
		time.Now(), time.Now(), func(n int32) { deleted.Add(n) }, 1)
	eg.Wait()

	require.Equal(t, int32(1), rounds.rounds.Load(), "the tenant must be swept once")
	require.Equal(t, int32(1), calls.Load(), "the sweep must stop after one failed batch")
	require.Equal(t, int32(0), deleted.Load())
	require.Equal(t, 1, ec.Len(), "one filing per swept tenant")
	err := ec.ToError()
	require.ErrorContains(t, err, "batch delete")
	require.ErrorIs(t, err, deleteErr)
}

// TestTTLStopsSweepingShardsWhoseDeleteFails drives the real single-tenant delete closure's error
// return. The error must reach the round rather than only the compounder, which left the shard to
// be dispatched again every round.
func TestTTLStopsSweepingShardsWhoseDeleteFails(t *testing.T) {
	const (
		expiredCount = 12
		aliveCount   = 3
		sweepBatch   = 5
		maxRounds    = int32(3)
	)
	deleteErr := errors.New("schema never caught up")

	repo := setupTTLShardRepo(t, sweepBatch, singleShardState(), expiredCount, aliveCount)
	index, logger := ttlIndex(t, repo)

	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(nil) })
	rounds := boundTTLRounds(t, logger, maxRounds, cancel)
	calls := &atomic.Int32{}
	index.schemaReader = ttlCountingSchemaReader{
		SchemaReader: index.schemaReader, calls: calls, err: deleteErr,
	}

	var deleted atomic.Int32
	eg := enterrors.NewErrorGroupWrapper(logger)
	ec := errorcompounder.NewSafe()
	index.incomingDeleteObjectsExpired(ctx, eg, ec, batchDeleteTTLProp,
		time.Now(), time.Now(), func(n int32) { deleted.Add(n) }, 1)
	eg.Wait()

	require.Equal(t, int32(1), rounds.rounds.Load(), "the sweep must stop after one failed round")
	require.Equal(t, int32(1), calls.Load(), "one delete per shard the round swept")
	require.Equal(t, int32(0), deleted.Load())
	require.Equal(t, 1, ec.Len(), "one filing per swept shard")
	err := ec.ToError()
	require.ErrorContains(t, err, "batch delete")
	require.ErrorIs(t, err, deleteErr)
}

// TestTTLSweepsShardsAcrossSeveralRounds drives the same closure over more expired objects than
// one round can delete. A sweep that read its rounds as having deleted nothing would stop after
// the first and leave the rest behind.
func TestTTLSweepsShardsAcrossSeveralRounds(t *testing.T) {
	const (
		expiredCount = 12
		aliveCount   = 3
		sweepBatch   = 2
		// far above the rounds these objects need, since the cap only has to bound a
		// loop that will not stop on its own
		maxRounds = int32(40)
	)

	repo := setupTTLShardRepo(t, sweepBatch, multiShardState(), expiredCount, aliveCount)
	index, logger := ttlIndex(t, repo)

	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(nil) })
	rounds := boundTTLRounds(t, logger, maxRounds, cancel)
	calls := &atomic.Int32{}
	index.schemaReader = ttlCountingSchemaReader{
		SchemaReader: index.schemaReader, calls: calls,
	}

	var deleted atomic.Int32
	eg := enterrors.NewErrorGroupWrapper(logger)
	ec := errorcompounder.NewSafe()
	index.incomingDeleteObjectsExpired(ctx, eg, ec, batchDeleteTTLProp,
		time.Now(), time.Now(), func(n int32) { deleted.Add(n) }, 1)
	eg.Wait()

	require.NoError(t, ec.ToError())
	require.Equal(t, int32(expiredCount), deleted.Load(),
		"every expired object goes, over more than one round of %d per shard", sweepBatch)
	require.GreaterOrEqual(t, calls.Load(), int32(expiredCount/sweepBatch),
		"a round deletes at most %d per shard, so more than one ran", sweepBatch)
	require.Greater(t, rounds.rounds.Load(), int32(1), "the sweep ran more than one round")
}

// TestTTLReportsABatchSlotNoDeleteWroteTo covers a batch whose per-object delete panicked, which
// the recovery inside deleteSingleBatchInLSM leaves as an untouched slot. A sweep reads its
// progress off what a batch reports deleted, so an untouched slot has to report a failure.
func TestTTLReportsABatchSlotNoDeleteWroteTo(t *testing.T) {
	// the integration job disables recovery, under which the panic below kills the binary
	t.Setenv("DISABLE_RECOVERY_ON_PANIC", "false")

	shardState := singleShardState()
	repo := setupTTLShardRepo(t, 5, shardState, 1, 1)
	index, _ := ttlIndex(t, repo)

	var deleted atomic.Int32
	// uuid.MustParse panics on this id inside the per-object goroutine, so nothing writes the
	// slot that object was given
	err := index.incomingDeleteObjectsExpiredUuids(context.Background(), time.Now(),
		shardState.AllPhysicalShards()[0], "", []strfmt.UUID{"not-a-uuid"},
		func(n int32) { deleted.Add(n) }, defaultConsistency(), 0)

	require.Error(t, err, "a batch slot no delete wrote to must not read as a success")
	require.Equal(t, int32(0), deleted.Load(),
		"a batch whose object delete panicked deleted nothing, so its round made no progress")
}
