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
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	routerTypes "github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/cluster"
	clusterMocks "github.com/weaviate/weaviate/usecases/cluster/mocks"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/replica"
	replicaerrors "github.com/weaviate/weaviate/usecases/replica/errors"
)

// fakeTTLTenantsManager implements the ttlTenantsManager interface for testing.
type fakeTTLTenantsManager struct {
	statusMap map[string]string // tenant name → activity status
	statusErr error             // if non-nil, returned by TenantsStatus

	deactivateCalled []deactivateCall // records each DeactivateTenants call
	deactivateErr    error            // if non-nil, returned by DeactivateTenants
}

type deactivateCall struct {
	ctxWasLive  bool // ctx.Err() == nil at the time of the call
	hasDeadline bool // context had a deadline (from WithTimeout)
	class       string
	tenant      string
}

func (f *fakeTTLTenantsManager) TenantsStatus(class string, tenants ...string) (map[string]string, error) {
	if f.statusErr != nil {
		return nil, f.statusErr
	}
	result := make(map[string]string, len(tenants))
	for _, t := range tenants {
		if s, ok := f.statusMap[t]; ok {
			result[t] = s
		}
	}
	return result, nil
}

func (f *fakeTTLTenantsManager) DeactivateTenants(ctx context.Context, class string, tenants ...string) error {
	_, hasDeadline := ctx.Deadline()
	for _, t := range tenants {
		f.deactivateCalled = append(f.deactivateCalled, deactivateCall{
			ctxWasLive:  ctx.Err() == nil,
			hasDeadline: hasDeadline,
			class:       class,
			tenant:      t,
		})
	}
	return f.deactivateErr
}

func newTestLoop(t *testing.T, mgr *fakeTTLTenantsManager, autoActivation bool,
	findFn func(ctx context.Context) ([]strfmt.UUID, error),
	batchFn func(ctx context.Context, uuids []strfmt.UUID) error,
) tenantTTLLoop {
	t.Helper()
	if batchFn == nil {
		batchFn = func(context.Context, []strfmt.UUID) error { return nil }
	}
	return tenantTTLLoop{
		class:                 "MyClass",
		tenant:                "tenant_0",
		autoActivationEnabled: autoActivation,
		mgr:                   mgr,
		findUUIDs:             findFn,
		processBatch:          batchFn,
	}
}

// TestShardIsLazyUnloaded checks that only a lazy shard not yet materialized is skipped;
// absent (COLD), loaded lazy, and non-lazy shards are not.
func TestShardIsLazyUnloaded(t *testing.T) {
	tests := []struct {
		name  string
		shard ShardLike // nil means the tenant is absent from the shard map
		want  bool
	}{
		{name: "absent tenant", shard: nil, want: false},
		{name: "lazy shard not loaded", shard: &LazyLoadShard{loaded: false}, want: true},
		{name: "lazy shard loaded", shard: &LazyLoadShard{loaded: true}, want: false},
		{name: "non-lazy shard", shard: &Shard{}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx := &Index{}
			if tt.shard != nil {
				idx.shards.Store("tenant_0", tt.shard)
			}
			assert.Equal(t, tt.want, idx.shardIsLazyUnloaded("tenant_0"))
		})
	}
}

// TestTenantTTLLoop_ContextCancelAfterActivation is the primary regression test for
// the server bug reported in:
//
//	test_tenant_auto_activation_during_ttl_deletion (weaviate-e2e-tests CI failure, 2026-04-13)
//
// Bug: when a previously-COLD tenant was auto-activated by the TTL loop and the TTL context
// was canceled mid-deletion (e.g. index drop, node shutdown, TTL round abort), the goroutine
// exited without calling DeactivateTenants. The tenant remained permanently HOT/ACTIVE.
//
// Fix: deferred DeactivateTenants with bounded timeout in tenantTTLLoop.ensureDeactivation.
func TestTenantTTLLoop_ContextCancelAfterActivation(t *testing.T) {
	mgr := &fakeTTLTenantsManager{
		statusMap: map[string]string{"tenant_0": models.TenantActivityStatusCOLD},
	}

	// cancelCtx simulates the TTL context being canceled mid-deletion
	// (e.g. index drop, node shutdown, or TTL round abort).
	cancelCtx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)

	batchesProcessed := 0
	loop := newTestLoop(t, mgr, true,
		func(ctx context.Context) ([]strfmt.UUID, error) {
			if batchesProcessed == 0 {
				return []strfmt.UUID{"uuid-1", "uuid-2", "uuid-3"}, nil
			}
			// If we reach this point, the loop failed to detect context cancellation.
			return nil, nil
		},
		func(ctx context.Context, _ []strfmt.UUID) error {
			batchesProcessed++
			// Simulate the TTL context being canceled after the first batch
			// (e.g. index drop, node shutdown, or TTL round abort).
			cancel(fmt.Errorf("concurrent raft deactivation canceled ttl context"))
			return nil
		},
	)

	ec := errorcompounder.New()
	loop.run(cancelCtx, ec)

	require.Equal(t, 1, batchesProcessed, "expected exactly one batch before context cancel")

	// KEY ASSERTION: DeactivateTenants must be called even though the context was canceled.
	require.Len(t, mgr.deactivateCalled, 1, "DeactivateTenants must be called exactly once")
	call := mgr.deactivateCalled[0]
	assert.Equal(t, "MyClass", call.class)
	assert.Equal(t, "tenant_0", call.tenant)

	// The deactivation call must use a live context (not the canceled TTL context)
	// so the RAFT call can complete.
	assert.True(t, call.ctxWasLive,
		"DeactivateTenants must be called with a non-canceled context")
	assert.True(t, call.hasDeadline,
		"DeactivateTenants must be called with a timeout-bounded context")
}

func TestTenantTTLLoop_NormalCompletion_Deactivates(t *testing.T) {
	mgr := &fakeTTLTenantsManager{
		statusMap: map[string]string{"tenant_0": models.TenantActivityStatusCOLD},
	}

	calls := atomic.Int32{}
	loop := newTestLoop(t, mgr, true,
		func(ctx context.Context) ([]strfmt.UUID, error) {
			if calls.Add(1) == 1 {
				return []strfmt.UUID{"uuid-1"}, nil
			}
			return nil, nil
		},
		nil,
	)

	ec := errorcompounder.New()
	loop.run(context.Background(), ec)

	assert.NoError(t, ec.ToError())
	require.Len(t, mgr.deactivateCalled, 1)
	assert.Equal(t, "tenant_0", mgr.deactivateCalled[0].tenant)
}

func TestTenantTTLLoop_ActiveTenant_NoDeactivation(t *testing.T) {
	mgr := &fakeTTLTenantsManager{
		statusMap: map[string]string{"tenant_0": models.TenantActivityStatusHOT},
	}

	loop := newTestLoop(t, mgr, true,
		func(ctx context.Context) ([]strfmt.UUID, error) { return nil, nil },
		nil,
	)

	ec := errorcompounder.New()
	loop.run(context.Background(), ec)

	assert.Empty(t, mgr.deactivateCalled, "active tenant must not be deactivated")
}

func TestTenantTTLLoop_AutoActivationDisabled_NoDeactivation(t *testing.T) {
	mgr := &fakeTTLTenantsManager{
		statusMap: map[string]string{"tenant_0": models.TenantActivityStatusCOLD},
	}

	loop := newTestLoop(t, mgr, false, // autoActivation disabled
		func(ctx context.Context) ([]strfmt.UUID, error) { return nil, nil },
		nil,
	)

	ec := errorcompounder.New()
	loop.run(context.Background(), ec)

	assert.Empty(t, mgr.deactivateCalled)
}

func TestTenantTTLLoop_TenantNotActive_NoDeactivation(t *testing.T) {
	// findUUIDs returning ErrTenantNotActive means the tenant was never successfully
	// activated. The loop should silently skip it AND must NOT trigger a RAFT
	// DeactivateTenants call (the tenant is already COLD).
	mgr := &fakeTTLTenantsManager{
		statusMap: map[string]string{"tenant_0": models.TenantActivityStatusCOLD},
	}

	loop := newTestLoop(t, mgr, true,
		func(ctx context.Context) ([]strfmt.UUID, error) {
			return nil, enterrors.ErrTenantNotActive
		},
		nil,
	)

	ec := errorcompounder.New()
	loop.run(context.Background(), ec)

	assert.NoError(t, ec.ToError(), "ErrTenantNotActive must be silently ignored")
	assert.Empty(t, mgr.deactivateCalled, "ErrTenantNotActive must not trigger deactivation")
}

func TestTenantTTLLoop_ContextAlreadyCanceled(t *testing.T) {
	mgr := &fakeTTLTenantsManager{
		statusMap: map[string]string{"tenant_0": models.TenantActivityStatusCOLD},
	}

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	cancel(errors.New("pre-canceled"))

	findCalled := false
	loop := newTestLoop(t, mgr, true,
		func(ctx context.Context) ([]strfmt.UUID, error) {
			findCalled = true
			return nil, nil
		},
		nil,
	)

	ec := errorcompounder.New()
	loop.run(ctx, ec)

	assert.False(t, findCalled, "findUUIDs must not be called when context is already canceled")
	// deactivate was never set (TenantsStatus was never called), so no deactivation.
	assert.Empty(t, mgr.deactivateCalled)
}

func TestTenantTTLLoop_DeactivateError_ReportedInEc(t *testing.T) {
	mgr := &fakeTTLTenantsManager{
		statusMap:     map[string]string{"tenant_0": models.TenantActivityStatusCOLD},
		deactivateErr: errors.New("raft timeout"),
	}

	loop := newTestLoop(t, mgr, true,
		func(ctx context.Context) ([]strfmt.UUID, error) { return nil, nil },
		nil,
	)

	ec := errorcompounder.New()
	loop.run(context.Background(), ec)

	err := ec.ToError()
	require.Error(t, err)
	assert.ErrorContains(t, err, "deactivate tenant")
	assert.ErrorContains(t, err, "raft timeout")
}

func TestTenantTTLLoop_DeactivateUsesTimeout(t *testing.T) {
	// Verify the deferred DeactivateTenants call uses a bounded-timeout context,
	// not a bare context.Background().
	mgr := &fakeTTLTenantsManager{
		statusMap: map[string]string{"tenant_0": models.TenantActivityStatusCOLD},
	}

	loop := newTestLoop(t, mgr, true,
		func(ctx context.Context) ([]strfmt.UUID, error) { return nil, nil },
		nil,
	)

	ec := errorcompounder.New()
	loop.run(context.Background(), ec)

	require.Len(t, mgr.deactivateCalled, 1)
	call := mgr.deactivateCalled[0]

	assert.True(t, call.ctxWasLive, "deactivation context must not be expired at call time")
	assert.True(t, call.hasDeadline, "deactivation context must have a deadline (from WithTimeout)")
}

// TestIndex_incomingDeleteObjectsExpired runs a single-tenant sweep through the real
// router and replicator with a majority of the shard's replicas missing from the node selector.
func TestIndex_incomingDeleteObjectsExpired(t *testing.T) {
	const (
		className = "Abc"
		shardName = "abc"
	)
	expired := []strfmt.UUID{
		"6ae8f8e1-39b4-4a5c-a8a1-91d5b01c5a7e",
		"73f2eb5f-5abf-447a-81ca-74b1dd168247",
		"b8b4a5b2-1e8d-4b6a-9f3c-2d7e5f8a9c01",
	}

	logger, _ := test.NewNullLogger()
	metrics, err := NewMetrics(logger, monitoring.GetMetrics(), className, "n/a")
	require.NoError(t, err, "create index metrics")

	for _, tt := range []struct {
		name     string
		replicas []string // Only node1 is known to the node selector.
	}{
		{name: "rf3 with two unreachable replicas", replicas: []string{"node1", "down-1", "down-2"}},
		{name: "rf2 with one unreachable replica", replicas: []string{"node1", "down-1"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			f := newUnreachableShardFixture(t, className, shardName, tt.replicas, clusterMocks.NewMockNodeSelector("node1"))
			f.schemaGetter.EXPECT().ReadOnlyClass(className).Return(&models.Class{Class: className})
			f.schemaReader.EXPECT().Shards(className).Return([]string{shardName}, nil)
			f.fsmReader.EXPECT().FilterOneShardReplicasWrite(className, shardName, tt.replicas).Return(tt.replicas)

			// Precondition: the router rejects QUORUM reads and writes of this shard.
			_, err := f.router.BuildReadRoutingPlan(
				f.router.BuildRoutingPlanOptions("", shardName, routerTypes.ConsistencyLevelQuorum, ""))
			require.ErrorIs(t, err, replicaerrors.ErrReplicas, "QUORUM read plan")
			_, err = f.router.BuildWriteRoutingPlan(
				f.router.BuildRoutingPlanOptions("", shardName, routerTypes.ConsistencyLevelQuorum, ""))
			require.ErrorIs(t, err, replicaerrors.ErrReplicas, "QUORUM write plan")

			client := newTTLReplicaClient(expired)
			replicator, err := replica.NewReplicator(className, f.router, cluster.NewMockNodeResolver(t), "node1",
				func() string { return models.ReplicationConfigDeletionStrategyNoAutomatedResolution },
				client, monitoring.GetMetrics(), logger)
			require.NoError(t, err, "create replicator")

			index := Index{
				router:     f.router,
				replicator: replicator,
				getSchema:  f.schemaGetter,
				Config:     IndexConfig{ClassName: schema.ClassName(className), ReplicationFactor: int64(len(tt.replicas))},
				metrics:    metrics,
				logger:     logger,
			}

			// Act
			var deleted atomic.Int32
			eg := enterrors.NewErrorGroupWrapper(logger)
			ec := errorcompounder.NewSafe()
			// A failing delete re-finds the same UUIDs forever; the deadline turns that hang into a fast failure.
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			index.incomingDeleteObjectsExpired(ctx, eg, ec, "expiresAt", time.Now(), time.Now(),
				func(n int32) { deleted.Add(n) }, 0)
			require.NoError(t, eg.Wait())

			// Assert
			require.NoError(t, ec.ToError(), "sweep errors")
			require.Equal(t, int32(len(expired)), deleted.Load(), "deleted objects")
			require.Empty(t, client.remaining(), "expired objects left on the reachable replica")
		})
	}
}

// ttlReplicaClient models one replica holding expired objects: lookups return the
// objects not yet deleted, and a committed delete removes its objects.
type ttlReplicaClient struct {
	FakeReplicationClient
	mu       sync.Mutex
	present  map[strfmt.UUID]struct{}
	prepared map[string][]strfmt.UUID // Request ID to the UUIDs of its pending delete.
}

func newTTLReplicaClient(uuids []strfmt.UUID) *ttlReplicaClient {
	c := &ttlReplicaClient{present: map[strfmt.UUID]struct{}{}, prepared: map[string][]strfmt.UUID{}}
	for _, id := range uuids {
		c.present[id] = struct{}{}
	}
	return c
}

func (c *ttlReplicaClient) remaining() []strfmt.UUID {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Collect(maps.Keys(c.present))
}

func (c *ttlReplicaClient) FindUUIDs(ctx context.Context,
	hostName, indexName, shardName string, filters *filters.LocalFilter, limit int,
) ([]strfmt.UUID, error) {
	return c.remaining(), nil
}

func (c *ttlReplicaClient) DeleteObjects(ctx context.Context, host, index, shard, requestID string,
	uuids []strfmt.UUID, deletionTime time.Time, dryRun bool, schemaVersion uint64,
) (replica.SimpleResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.prepared[requestID] = uuids
	return replica.SimpleResponse{}, nil
}

func (c *ttlReplicaClient) Commit(ctx context.Context, host, index, shard, requestID string, resp interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	batch := resp.(*replica.DeleteBatchResponse)
	for _, id := range c.prepared[requestID] {
		delete(c.present, id)
		batch.Batch = append(batch.Batch, replica.UUID2Error{UUID: id.String()})
	}
	return nil
}
