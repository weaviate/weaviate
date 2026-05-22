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
