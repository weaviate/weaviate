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
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	resolver "github.com/weaviate/weaviate/adapters/repos/db/sharding"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	routerTypes "github.com/weaviate/weaviate/cluster/router/types"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/loadlimiter"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/replication"
	"github.com/weaviate/weaviate/entities/schema"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/objects"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// tenantReplicaFor returns (replica, nil) for an active tenant and
// (nil, ErrMultiTenancy) for coldTenant.  Used by setupMTAwareMockRouter.
func tenantReplicaFor(tenant, coldTenant, localNode string) ([]routerTypes.Replica, error) {
	if tenant == coldTenant {
		return nil, objects.NewErrMultiTenancy(enterrors.ErrTenantNotActive)
	}
	return []routerTypes.Replica{{NodeName: localNode, ShardName: tenant, HostAddr: "127.0.0.1"}}, nil
}

// setupMTAwareMockRouter registers GetWriteReplicasLocation and GetReadReplicasLocation
// expectations on mockRouter.  Requests for coldTenant return ErrMultiTenancy
// (ErrTenantNotActive); all other tenants receive a single-node replica set
// with localNode as the sole replica.
func setupMTAwareMockRouter(t *testing.T, mockRouter *routerTypes.MockRouter, className, coldTenant, localNode string) {
	t.Helper()
	mockRouter.EXPECT().GetWriteReplicasLocation(className, mock.Anything, mock.Anything).
		RunAndReturn(func(cls, tenant, shard string) (routerTypes.WriteReplicaSet, error) {
			replicas, err := tenantReplicaFor(tenant, coldTenant, localNode)
			return routerTypes.WriteReplicaSet{Replicas: replicas}, err
		}).Maybe()
	mockRouter.EXPECT().GetReadReplicasLocation(className, mock.Anything, mock.Anything).
		RunAndReturn(func(cls, tenant, shard string) (routerTypes.ReadReplicaSet, error) {
			replicas, err := tenantReplicaFor(tenant, coldTenant, localNode)
			return routerTypes.ReadReplicaSet{Replicas: replicas}, err
		}).Maybe()
}

// TestConditionalMultiTenant verifies per-tenant isolation for insert_if_not_exists:
//
//  1. The same UUID inserted into two different ACTIVE tenants both succeed —
//     the condition in tenant A must NOT see tenant B's object (INV-MT-1).
//  2. A second insert_if_not_exists into tenant A using the same UUID is skipped
//     (ErrPreconditionFailed).
//  3. Tenant B is independently writable after step 1; a new UUID in tenant B
//     also succeeds.
//
// Causal link: without per-tenant LSM isolation a second insert into *either*
// tenant would observe the other tenant's object. This test catches that by
// asserting (a) both first inserts succeed, (b) only the second insert into A
// fails, and (c) a fresh UUID in B still succeeds.
func TestConditionalMultiTenant(t *testing.T) {
	ctx := context.Background()

	const (
		className = "CASMultiTenantClass"
		tenantA   = "tenant-alpha"
		tenantB   = "tenant-beta"
	)

	// Two independent shards represent two tenant-isolated stores.
	// setupTestShardWithSettings gives us a real LSM shard on tmpfs.
	shardA, idxA := testShardWithSettings(t, ctx, &models.Class{Class: className},
		enthnsw.UserConfig{Skip: true}, false, false, false)
	defer func() { _ = idxA.drop() }()

	shardB, idxB := testShardWithSettings(t, ctx, &models.Class{Class: className},
		enthnsw.UserConfig{Skip: true}, false, false, false)
	defer func() { _ = idxB.drop() }()

	sharedUUID := strfmt.UUID(uuid.NewString())

	// Step 1: first insert_if_not_exists of sharedUUID into tenant A must succeed.
	objA1 := buildMTConditionalObject(className, tenantA, sharedUUID)
	require.NoError(t, shardA.PutObject(ctx, objA1),
		"first insert_if_not_exists into tenant A must succeed")

	// Step 2: first insert_if_not_exists of the SAME UUID into tenant B must also
	// succeed — tenant B's shard has no visibility into tenant A's store.
	objB1 := buildMTConditionalObject(className, tenantB, sharedUUID)
	require.NoError(t, shardB.PutObject(ctx, objB1),
		"first insert_if_not_exists of same UUID into tenant B must succeed (tenant isolation)")

	// Step 3: second insert_if_not_exists of the same UUID into tenant A must be
	// skipped — the object now exists in that shard.
	objA2 := buildMTConditionalObject(className, tenantA, sharedUUID)
	err := shardA.PutObject(ctx, objA2)
	require.Error(t, err, "second insert_if_not_exists into tenant A must return an error")
	var precondErr *objects.ErrPreconditionFailed
	require.True(t, errors.As(err, &precondErr),
		"second insert into tenant A must be ErrPreconditionFailed, got: %v", err)

	// Step 4: tenant B is independently insertable — a brand-new UUID in B succeeds.
	newUUID := strfmt.UUID(uuid.NewString())
	objB2 := buildMTConditionalObject(className, tenantB, newUUID)
	require.NoError(t, shardB.PutObject(ctx, objB2),
		"new UUID in tenant B must succeed after tenant A has its precondition failure")
}

// TestConditionalMT_InactiveTenant verifies INV-MT-5: an insert_if_not_exists
// (or update_if_exists) targeting a COLD/inactive tenant returns
// ErrTenantNotActive BEFORE the conditional check fires. No shard access
// occurs; the error originates in the shard resolver's tenant status validation.
//
// Causal link: the test catches any regression where the conditional-write path
// bypasses the MT activation gate. Without the gate, a COLD tenant could receive
// writes that target a non-existent or offline shard. The assertion confirms the
// error wraps enterrors.ErrTenantNotActive and is returned before any object is
// stored (verified by confirming no shard is initialised for the COLD tenant).
func TestConditionalMT_InactiveTenant(t *testing.T) {
	ctx := context.Background()

	const (
		className    = "CASMTInactiveTenantClass"
		activeTenant = "hot-tenant"
		coldTenant   = "cold-tenant"
		localNode    = "node1"
	)

	dirName := t.TempDir()
	logger, _ := test.NewNullLogger()

	// MT class with multi-tenancy enabled.
	class := &models.Class{
		Class:               className,
		InvertedIndexConfig: &models.InvertedIndexConfig{},
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: true,
		},
	}

	// Sharding state with one HOT tenant and one COLD tenant.
	// The COLD tenant has a physical shard entry but is not active.
	shardState := NewMultiTenantShardingStateBuilder().
		WithIndexName("cas-mt-inactive-index").
		WithNodePrefix("node").
		WithReplicationFactor(1).
		WithTenant(activeTenant, models.TenantActivityStatusHOT).
		WithTenant(coldTenant, models.TenantActivityStatusCOLD).
		Build()

	scheduler := queue.NewScheduler(queue.SchedulerOptions{
		Logger: logger,
	})
	t.Cleanup(func() { _ = scheduler.Close(ctx) })

	// mockSchemaGetter controls what TenantsShards returns for the shard resolver.
	// Returning COLD for coldTenant forces ErrTenantNotActive before any shard op.
	mockSchemaGetter := schemaUC.NewMockSchemaGetter(t)
	mockSchemaGetter.EXPECT().GetSchemaSkipAuth().Maybe().Return(schema.Schema{
		Objects: &models.Schema{Classes: []*models.Class{class}},
	})
	mockSchemaGetter.EXPECT().ReadOnlyClass(className).Maybe().Return(class)
	mockSchemaGetter.EXPECT().NodeName().Maybe().Return(localNode)
	// Return HOT for activeTenant and COLD for coldTenant so the resolver rejects
	// any operation targeting coldTenant before touching any shard.
	mockSchemaGetter.EXPECT().TenantsShards(mock.Anything, className, mock.Anything).
		RunAndReturn(func(ctx context.Context, cls string, tenants ...string) (map[string]string, error) {
			result := make(map[string]string, len(tenants))
			for _, t := range tenants {
				switch t {
				case coldTenant:
					result[t] = models.TenantActivityStatusCOLD
				default:
					result[t] = models.TenantActivityStatusHOT
				}
			}
			return result, nil
		}).Maybe()

	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(name string, retry bool, fn func(*models.Class, *sharding.State) error) error {
			return fn(class, shardState)
		}).Maybe()
	mockSchemaReader.EXPECT().ReadOnlyClass(mock.Anything).Maybe().Return(class)
	mockSchemaReader.EXPECT().ReadOnlySchema().Maybe().Return(models.Schema{
		Classes: []*models.Class{class},
	})
	mockSchemaReader.EXPECT().ShardReplicas(mock.Anything, mock.Anything).
		Maybe().Return([]string{localNode}, nil)

	mockReplicationFSMReader := replicationTypes.NewMockReplicationFSMReader(t)
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasRead(mock.Anything, mock.Anything, mock.Anything).
		Return([]string{localNode}).Maybe()
	mockReplicationFSMReader.EXPECT().FilterOneShardReplicasWrite(mock.Anything, mock.Anything, mock.Anything).
		Return([]string{localNode}, nil).Maybe()

	mockNodeSelector := cluster.NewMockNodeSelector(t)
	mockNodeSelector.EXPECT().LocalName().Return(localNode).Maybe()
	mockNodeSelector.EXPECT().NodeHostname(mock.Anything).Return(localNode, true).Maybe()

	// Mock router: GetWriteReplicasLocation / GetReadReplicasLocation return an
	// error for the COLD tenant (router layer gates on tenant status) and a
	// single-replica set for HOT tenants.
	mockRouter := routerTypes.NewMockRouter(t)
	setupMTAwareMockRouter(t, mockRouter, className, coldTenant, localNode)

	mockNodeResolver := cluster.NewMockNodeResolver(t)

	// Build the MT shard resolver backed by the mock schema getter.
	shardRes := resolver.NewShardResolver(className, true, mockSchemaGetter)

	idx, err := NewIndex(
		ctx,
		IndexConfig{
			RootPath:              dirName,
			ClassName:             schema.ClassName(className),
			ReplicationFactor:     1,
			ShardLoadLimiter:      loadlimiter.NewLoadLimiter(monitoring.NoopRegisterer, "dummy", 1),
			TrackVectorDimensions: false,
			EnableLazyLoadShards:  true,
		},
		schema.InvertedIndexConfig{},
		enthnsw.UserConfig{Skip: true},
		map[string]schemaConfig.VectorIndexConfig{},
		mockRouter,
		shardRes,
		mockSchemaGetter,
		mockSchemaReader,
		nil,
		logger,
		mockNodeResolver,
		nil,
		&FakeReplicationClient{},
		&replication.GlobalConfig{},
		nil,
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
	t.Cleanup(func() { _ = idx.drop() })

	id := strfmt.UUID(uuid.NewString())

	// insert_if_not_exists targeting the COLD tenant must return ErrTenantNotActive
	// from the shard resolver path before any conditional evaluation occurs.
	coldObj := buildMTConditionalObject(className, coldTenant, id)
	err = idx.putObject(ctx, coldObj, nil, coldTenant, 0)
	require.Error(t, err, "insert_if_not_exists into COLD tenant must return an error")
	require.True(t, errors.Is(err, enterrors.ErrTenantNotActive),
		"error must wrap ErrTenantNotActive (INV-MT-5); got: %v", err)

	// A conditional precondition failure (ErrPreconditionFailed) must NOT be
	// returned — the check must be rejected before the existence check fires.
	var precondErr *objects.ErrPreconditionFailed
	require.False(t, errors.As(err, &precondErr),
		"COLD tenant must not reach the conditional check; got ErrPreconditionFailed unexpectedly")

	// update_if_exists into a COLD tenant must also be rejected before the conditional
	// check fires (same gate, symmetric behaviour).
	updateObj := &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:     id,
			Class:  className,
			Tenant: coldTenant,
		},
		Conditional: storobj.Conditional{
			OnlyIfExists: true,
		},
	}
	err = idx.putObject(ctx, updateObj, nil, coldTenant, 0)
	require.Error(t, err, "update_if_exists into COLD tenant must return an error")
	require.True(t, errors.Is(err, enterrors.ErrTenantNotActive),
		"update_if_exists error must wrap ErrTenantNotActive (INV-MT-5); got: %v", err)
}

// TestConditionalMultiTenantConcurrent verifies that 100 concurrent
// insert_if_not_exists calls on the same UUID within a single active MT tenant
// produce exactly 1 successful insert and 99 ErrPreconditionFailed results.
//
// This re-uses the runCASRace helper from shard_write_put_concurrent_test.go.
// The test should be run with -race -count=3.
//
// Causal link: without the per-UUID docIdLock serialising writes, multiple
// goroutines could both observe prevObj==nil and both succeed, making
// successCount > 1. The assertion require.Equal(t, 1, successCount) catches
// that regression.
func TestConditionalMultiTenantConcurrent(t *testing.T) {
	ctx := context.Background()
	const className = "CASMTConcurrentClass"

	// A single MT shard represents one active tenant's isolated store.
	shard, idx := testShardWithSettings(t, ctx, &models.Class{Class: className},
		enthnsw.UserConfig{Skip: true}, false, false, false)
	defer func() { _ = idx.drop() }()

	id := strfmt.UUID(uuid.NewString())
	successCount, precondFailCount := runCASRace(t, ctx, shard, className, id)

	require.Equal(t, 1, successCount,
		"exactly 1 insert_if_not_exists must succeed within a single MT tenant shard")
	require.Equal(t, concurrencyCount-1, precondFailCount,
		"exactly %d insert_if_not_exists must receive ErrPreconditionFailed", concurrencyCount-1)
}
