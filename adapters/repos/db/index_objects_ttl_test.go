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
	"sync/atomic"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
)

// fakeTTLTenantsManager implements the ttlTenantsManager interface for testing.
type fakeTTLTenantsManager struct {
	// statusMap controls what TenantsStatus returns: tenant name → activity status string.
	statusMap map[string]string
	// statusErr, if non-nil, is returned by TenantsStatus.
	statusErr error

	// deactivateCalled records each call to DeactivateTenants.
	deactivateCalled []deactivateCall
	// deactivateErr, if non-nil, is returned by DeactivateTenants.
	deactivateErr error
}

type deactivateCall struct {
	ctx    context.Context
	class  string
	tenant string
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
	for _, t := range tenants {
		f.deactivateCalled = append(f.deactivateCalled, deactivateCall{ctx: ctx, class: class, tenant: t})
	}
	return f.deactivateErr
}

// TestProcessTenantTTLLoop_ContextCancelAfterActivation is the primary regression test for
// the server bug reported in:
//
//	test_tenant_auto_activation_during_ttl_deletion (weaviate-e2e-tests CI failure, 2026-04-13)
//
// Root cause: when a previously-COLD (inactive) tenant was auto-activated by the TTL loop
// and a concurrent RAFT DeactivateTenants propagation from another node canceled the TTL
// context mid-deletion, the goroutine exited without calling DeactivateTenants. The tenant
// remained permanently HOT/ACTIVE.
//
// Fix: deferred DeactivateTenants call using context.Background() in processTenantTTLLoop.
func TestProcessTenantTTLLoop_ContextCancelAfterActivation(t *testing.T) {
	const (
		class  = "MyClass"
		tenant = "tenant_0"
	)

	// The tenant was COLD (inactive) before the TTL run — it should be re-deactivated on exit.
	mgr := &fakeTTLTenantsManager{
		statusMap: map[string]string{
			tenant: models.TenantActivityStatusCOLD,
		},
	}

	// cancelCtx simulates the TTL context being canceled mid-deletion (e.g. by a concurrent
	// RAFT DeactivateTenants propagation from another node completing its own TTL round).
	cancelCtx, cancel := context.WithCancelCause(context.Background())

	batchesProcessed := 0
	uuids := []strfmt.UUID{"uuid-1", "uuid-2", "uuid-3"}

	findUUIDs := func(ctx context.Context) ([]strfmt.UUID, error) {
		if batchesProcessed == 0 {
			// First call: return a non-empty batch so the loop makes progress.
			return uuids, nil
		}
		// Second call: context is already canceled; processTenantTTLLoop detects this at
		// the top of the loop and exits before reaching findUUIDs again. This path is
		// reached only if the loop doesn't check ctx.Err() between batches (a regression).
		return nil, nil
	}

	processBatch := func(ctx context.Context, got []strfmt.UUID) error {
		batchesProcessed++
		// Simulate a concurrent RAFT event canceling the TTL context after the first batch.
		cancel(fmt.Errorf("concurrent raft deactivation canceled ttl context"))
		return nil
	}

	ec := errorcompounder.New()
	processTenantTTLLoop(cancelCtx, ec, class, tenant,
		true /*autoActivationEnabled*/, mgr, findUUIDs, processBatch)

	// The loop must have exited (at the second ctx check after the first batch).
	require.Equal(t, 1, batchesProcessed, "expected exactly one batch before context cancel")

	// KEY ASSERTION: DeactivateTenants must be called even though the context was canceled.
	require.Len(t, mgr.deactivateCalled, 1, "DeactivateTenants must be called exactly once")
	call := mgr.deactivateCalled[0]
	assert.Equal(t, class, call.class)
	assert.Equal(t, tenant, call.tenant)

	// The deactivation call must use context.Background() — not the canceled TTL context —
	// so it completes even after the TTL round's context is done.
	assert.NoError(t, call.ctx.Err(),
		"DeactivateTenants must be called with a non-canceled context (context.Background())")
}

func TestProcessTenantTTLLoop_NormalCompletion_Deactivates(t *testing.T) {
	const (
		class  = "MyClass"
		tenant = "tenant_0"
	)

	mgr := &fakeTTLTenantsManager{
		statusMap: map[string]string{tenant: models.TenantActivityStatusCOLD},
	}

	calls := atomic.Int32{}
	findUUIDs := func(ctx context.Context) ([]strfmt.UUID, error) {
		n := calls.Add(1)
		if n == 1 {
			return []strfmt.UUID{"uuid-1"}, nil
		}
		return nil, nil // second call: empty → signals completion
	}
	processBatch := func(ctx context.Context, uuids []strfmt.UUID) error { return nil }

	ec := errorcompounder.New()
	processTenantTTLLoop(context.Background(), ec, class, tenant,
		true, mgr, findUUIDs, processBatch)

	assert.NoError(t, ec.ToError())
	require.Len(t, mgr.deactivateCalled, 1)
	assert.Equal(t, tenant, mgr.deactivateCalled[0].tenant)
}

func TestProcessTenantTTLLoop_ActiveTenant_NoDeactivation(t *testing.T) {
	// A tenant that was already HOT (active) must not be deactivated.
	mgr := &fakeTTLTenantsManager{
		statusMap: map[string]string{"tenant_hot": models.TenantActivityStatusHOT},
	}

	findUUIDs := func(ctx context.Context) ([]strfmt.UUID, error) { return nil, nil }
	processBatch := func(ctx context.Context, uuids []strfmt.UUID) error { return nil }

	ec := errorcompounder.New()
	processTenantTTLLoop(context.Background(), ec, "MyClass", "tenant_hot",
		true, mgr, findUUIDs, processBatch)

	assert.Empty(t, mgr.deactivateCalled, "active tenant must not be deactivated")
}

func TestProcessTenantTTLLoop_AutoActivationDisabled_NoDeactivation(t *testing.T) {
	// When autoActivationEnabled is false, TenantsStatus is never checked and
	// deactivation must never happen.
	mgr := &fakeTTLTenantsManager{
		statusMap: map[string]string{"tenant_0": models.TenantActivityStatusCOLD},
	}

	findUUIDs := func(ctx context.Context) ([]strfmt.UUID, error) { return nil, nil }
	processBatch := func(ctx context.Context, uuids []strfmt.UUID) error { return nil }

	ec := errorcompounder.New()
	processTenantTTLLoop(context.Background(), ec, "MyClass", "tenant_0",
		false /*autoActivationEnabled=false*/, mgr, findUUIDs, processBatch)

	assert.Empty(t, mgr.deactivateCalled)
}

func TestProcessTenantTTLLoop_TenantNotActive_Skipped(t *testing.T) {
	// findUUIDs returning ErrTenantNotActive should be silently skipped and must not
	// add anything to ec.
	mgr := &fakeTTLTenantsManager{
		statusMap: map[string]string{"tenant_0": models.TenantActivityStatusCOLD},
	}

	findUUIDs := func(ctx context.Context) ([]strfmt.UUID, error) {
		return nil, enterrors.ErrTenantNotActive
	}
	processBatch := func(ctx context.Context, uuids []strfmt.UUID) error { return nil }

	ec := errorcompounder.New()
	processTenantTTLLoop(context.Background(), ec, "MyClass", "tenant_0",
		true, mgr, findUUIDs, processBatch)

	assert.NoError(t, ec.ToError(), "ErrTenantNotActive must be silently ignored")
}

func TestProcessTenantTTLLoop_ContextAlreadyCanceled(t *testing.T) {
	// If the context is already canceled on entry the loop must exit immediately without
	// calling TenantsStatus, findUUIDs, or processBatch.
	mgr := &fakeTTLTenantsManager{
		statusMap: map[string]string{"tenant_0": models.TenantActivityStatusCOLD},
	}

	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(errors.New("pre-canceled"))

	called := false
	findUUIDs := func(ctx context.Context) ([]strfmt.UUID, error) {
		called = true
		return nil, nil
	}
	processBatch := func(ctx context.Context, uuids []strfmt.UUID) error { return nil }

	ec := errorcompounder.New()
	processTenantTTLLoop(ctx, ec, "MyClass", "tenant_0", true, mgr, findUUIDs, processBatch)

	assert.False(t, called, "findUUIDs must not be called when context is already canceled")
	// deactivate was never set (TenantsStatus was never called), so no deactivation.
	assert.Empty(t, mgr.deactivateCalled)
}

func TestProcessTenantTTLLoop_DeactivateError_ReportedInEc(t *testing.T) {
	// A failure in DeactivateTenants must be surfaced via ec, not silently swallowed.
	deactivateErr := errors.New("raft timeout")
	mgr := &fakeTTLTenantsManager{
		statusMap:     map[string]string{"tenant_0": models.TenantActivityStatusCOLD},
		deactivateErr: deactivateErr,
	}

	findUUIDs := func(ctx context.Context) ([]strfmt.UUID, error) { return nil, nil }
	processBatch := func(ctx context.Context, uuids []strfmt.UUID) error { return nil }

	ec := errorcompounder.New()
	processTenantTTLLoop(context.Background(), ec, "MyClass", "tenant_0",
		true, mgr, findUUIDs, processBatch)

	err := ec.ToError()
	require.Error(t, err)
	assert.ErrorContains(t, err, "deactivate tenant")
	assert.ErrorContains(t, err, "raft timeout")
}
