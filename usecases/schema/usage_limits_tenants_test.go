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

package schema

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/config/runtime"
	"github.com/weaviate/weaviate/usecases/usagelimits"
)

// TestAddTenants_WithUsageLimit covers the per-collection tenant-count cap
// added by the Free-Tier guardrails RFC. Behavior:
//   - cap unset (-1)  → no enforcement
//   - room remains    → AddTenants succeeds
//   - exact-fit batch → AddTenants succeeds
//   - over-cap batch  → typed *LimitExceededError; no AddTenants call to the
//     schema manager (we reject pre-RAFT)
func TestAddTenants_WithUsageLimit(t *testing.T) {
	tests := []struct {
		name           string
		cap            int
		existingCount  int
		newTenants     int
		expectExceed   bool
		expectAddCalls bool
	}{
		{name: "cap unset (-1) → no enforcement", cap: -1, existingCount: 100, newTenants: 100, expectExceed: false, expectAddCalls: true},
		{name: "well under cap", cap: 10, existingCount: 5, newTenants: 1, expectExceed: false, expectAddCalls: true},
		{name: "exact fit at cap", cap: 10, existingCount: 8, newTenants: 2, expectExceed: false, expectAddCalls: true},
		{name: "one over cap", cap: 10, existingCount: 10, newTenants: 1, expectExceed: true, expectAddCalls: false},
		{name: "batch overflows by one", cap: 10, existingCount: 8, newTenants: 3, expectExceed: true, expectAddCalls: false},
		{name: "zero allowed cap", cap: 0, existingCount: 0, newTenants: 1, expectExceed: true, expectAddCalls: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler, fakeMgr := newTestHandler(t, &fakeDB{})
			handler.config.UsageLimits.MaxTenantsPerCollection = runtime.NewDynamicValue(tt.cap)

			// Build the existing-tenant list of length existingCount.
			existing := make([]*models.Tenant, tt.existingCount)
			for i := range existing {
				existing[i] = &models.Tenant{Name: "preexisting"}
			}
			fakeMgr.On("QueryTenants", "MTenabled", mock.Anything).
				Return(existing, uint64(0), nil).Maybe()

			if tt.expectAddCalls {
				fakeMgr.On("AddTenants", mock.Anything, mock.Anything).Return(nil)
			}

			tenants := make([]*models.Tenant, tt.newTenants)
			for i := range tenants {
				tenants[i] = &models.Tenant{Name: namedTenant(i)}
			}

			_, err := handler.AddTenants(context.Background(), nil, "MTenabled", tenants)
			if tt.expectExceed {
				require.Error(t, err)
				le, ok := usagelimits.AsLimitExceeded(err)
				require.True(t, ok, "expected *LimitExceededError, got %T: %v", err, err)
				assert.Equal(t, usagelimits.LimitTenants, le.Limit)
				assert.Equal(t, int64(tt.cap), le.Value)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func namedTenant(i int) string {
	return fmt.Sprintf("T_%d", i)
}

// TestAddTenants_WithUsageLimit_DedupesExistingNames asserts the per-collection
// tenant cap counts only tenants from the request that are not already in the
// collection, matching the RAFT-side check in cluster/schema/meta_class.go.
func TestAddTenants_WithUsageLimit_DedupesExistingNames(t *testing.T) {
	tests := []struct {
		name           string
		cap            int
		existingNames  []string
		requestedNames []string
		expectExceed   bool
	}{
		{
			name:           "at cap, only existing names re-asserted",
			cap:            3,
			existingNames:  []string{"T1", "T2", "T3"},
			requestedNames: []string{"T3"},
			expectExceed:   false,
		},
		{
			name:           "at cap, existing + one new — new pushes over",
			cap:            3,
			existingNames:  []string{"T1", "T2", "T3"},
			requestedNames: []string{"T3", "T4"},
			expectExceed:   true,
		},
		{
			name:           "below cap, existing + one new — exact fit",
			cap:            3,
			existingNames:  []string{"T1", "T2"},
			requestedNames: []string{"T2", "T3"},
			expectExceed:   false,
		},
		{
			name:           "below cap, existing + two new — one over",
			cap:            3,
			existingNames:  []string{"T1", "T2"},
			requestedNames: []string{"T2", "T3", "T4"},
			expectExceed:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler, fakeMgr := newTestHandler(t, &fakeDB{})
			handler.config.UsageLimits.MaxTenantsPerCollection = runtime.NewDynamicValue(tt.cap)

			existing := make([]*models.Tenant, len(tt.existingNames))
			for i, name := range tt.existingNames {
				existing[i] = &models.Tenant{Name: name}
			}
			fakeMgr.On("QueryTenants", "MTenabled", mock.Anything).
				Return(existing, uint64(0), nil).Maybe()

			if !tt.expectExceed {
				fakeMgr.On("AddTenants", mock.Anything, mock.Anything).Return(nil)
			}

			requested := make([]*models.Tenant, len(tt.requestedNames))
			for i, name := range tt.requestedNames {
				requested[i] = &models.Tenant{Name: name}
			}

			_, err := handler.AddTenants(context.Background(), nil, "MTenabled", requested)
			if tt.expectExceed {
				require.Error(t, err)
				le, ok := usagelimits.AsLimitExceeded(err)
				require.True(t, ok, "expected *LimitExceededError, got %T: %v", err, err)
				assert.Equal(t, usagelimits.LimitTenants, le.Limit)
				assert.Equal(t, int64(tt.cap), le.Value)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
