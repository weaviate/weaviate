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

package multitenancy_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/multitenancy"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/objects"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

// hasEmptyString returns true if ss contains "".
func hasEmptyString(ss []string) bool {
	for _, s := range ss {
		if s == "" {
			return true
		}
	}
	return false
}

// dedupeNonEmpty returns unique non-empty tenants (matches validator deduplication for non-empty).
func dedupeNonEmpty(tenants []string) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0, len(tenants))
	for _, t := range tenants {
		if t == "" {
			continue
		}
		if _, ok := seen[t]; ok {
			continue
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	return out
}

func classOrNil(exists bool) *models.Class {
	if exists {
		return &models.Class{Class: "TestClass"}
	}
	return nil
}

func Test_SingleTenantValidator(t *testing.T) {
	testCases := []struct {
		name        string
		tenants     []string
		expectError bool
	}{
		{
			name:    "no tenants provided",
			tenants: []string{},
		},
		{
			name:    "single empty tenant",
			tenants: []string{""},
		},
		{
			name:    "multiple empty tenants",
			tenants: []string{"", "", ""},
		},
		{
			name:        "single non-empty tenant",
			tenants:     []string{"tenant1"},
			expectError: true,
		},
		{
			name:        "mixed empty and non-empty tenants",
			tenants:     []string{"", "tenant1", ""},
			expectError: true,
		},
		{
			name:        "multiple non-empty tenants",
			tenants:     []string{"tenant1", "tenant2"},
			expectError: true,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// GIVEN
			schemaReader := schemaUC.NewMockSchemaReader(t)
			validator := multitenancy.NewTenantValidator("TestClass", false, schemaReader)

			// WHEN
			err := validator.ValidateTenants(context.Background(), 0, tc.tenants...)

			// THEN
			if tc.expectError {
				require.Error(t, err)
				var multiTenancyErr objects.ErrMultiTenancy
				require.ErrorAs(t, err, &multiTenancyErr)
				require.Contains(t, err.Error(), "multi-tenancy disabled")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func Test_MultiTenantValidator(t *testing.T) {
	testCases := []struct {
		name          string
		tenants       []string
		tenantShards  map[string]string
		classExists   bool
		expectError   bool
		errorContains string
	}{
		{
			name:        "no tenants provided",
			tenants:     []string{},
			expectError: true,
		},
		{
			name:         "single valid hot tenant",
			tenants:      []string{"tenant1"},
			tenantShards: map[string]string{"tenant1": models.TenantActivityStatusHOT},
			classExists:  true,
		},
		{
			name:    "multiple valid hot tenants",
			tenants: []string{"tenant1", "tenant2"},
			tenantShards: map[string]string{
				"tenant1": models.TenantActivityStatusHOT,
				"tenant2": models.TenantActivityStatusHOT,
			},
			classExists: true,
		},
		{
			name:          "single empty tenant",
			tenants:       []string{""},
			expectError:   true,
			errorContains: "without tenant",
		},
		{
			name:          "mixed empty and valid tenants",
			tenants:       []string{"tenant1", ""},
			tenantShards:  map[string]string{"tenant1": models.TenantActivityStatusHOT},
			classExists:   true,
			expectError:   true,
			errorContains: "without tenant",
		},
		{
			name:          "tenant not found with existing class",
			tenants:       []string{"tenant1"},
			tenantShards:  map[string]string{},
			classExists:   true,
			expectError:   true,
			errorContains: "tenant not found",
		},
		{
			name:          "tenant not found with missing class",
			tenants:       []string{"tenant1"},
			tenantShards:  map[string]string{},
			classExists:   false,
			expectError:   true,
			errorContains: "class",
		},
		{
			name:          "tenant not active (cold)",
			tenants:       []string{"tenant1"},
			tenantShards:  map[string]string{"tenant1": models.TenantActivityStatusCOLD},
			classExists:   true,
			expectError:   true,
			errorContains: "tenant not active",
		},
		{
			name:          "tenant not active (frozen)",
			tenants:       []string{"tenant1"},
			tenantShards:  map[string]string{"tenant1": models.TenantActivityStatusFROZEN},
			classExists:   true,
			expectError:   true,
			errorContains: "tenant not active",
		},
		{
			name:    "duplicate tenants handled correctly",
			tenants: []string{"tenant1", "tenant1", "tenant2"},
			tenantShards: map[string]string{
				"tenant1": models.TenantActivityStatusHOT,
				"tenant2": models.TenantActivityStatusHOT,
			},
			classExists: true,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// GIVEN
			schemaReader := schemaUC.NewMockSchemaReader(t)
			// Validator calls TenantsShardsWithVersion only when there are tenants and none are empty.
			uniqueTenants := dedupeNonEmpty(tc.tenants)
			reachesTenantsShardsCall := len(uniqueTenants) > 0 && !hasEmptyString(tc.tenants)
			if reachesTenantsShardsCall {
				tenantArgs := make([]interface{}, len(uniqueTenants))
				for i, tn := range uniqueTenants {
					tenantArgs[i] = tn
				}
				schemaReader.EXPECT().
					TenantsShardsWithVersion(mock.Anything, uint64(0), "TestClass", tenantArgs...).
					Return(tc.tenantShards, nil)
			}
			if reachesTenantsShardsCall && tc.expectError && len(tc.tenantShards) == 0 {
				schemaReader.EXPECT().ReadOnlyClass("TestClass").Return(classOrNil(tc.classExists))
			}
			validator := multitenancy.NewTenantValidator("TestClass", true, schemaReader)

			// WHEN
			err := validator.ValidateTenants(context.Background(), 0, tc.tenants...)

			// THEN
			if tc.expectError {
				require.Error(t, err)
				if tc.errorContains != "" {
					require.Contains(t, err.Error(), tc.errorContains)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func Test_MultiTenantValidator_SchemaErrors(t *testing.T) {
	testCases := []struct {
		name            string
		tenantsShardErr error
		expectError     bool
		errorContains   string
	}{
		{
			name:            "schema connection error",
			tenantsShardErr: fmt.Errorf("connection failed"),
			expectError:     true,
			errorContains:   "fetch tenant status",
		},
		{
			name:            "schema timeout error",
			tenantsShardErr: fmt.Errorf("timeout"),
			expectError:     true,
			errorContains:   "timeout",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// GIVEN
			schemaReader := schemaUC.NewMockSchemaReader(t)
			schemaReader.EXPECT().
				TenantsShardsWithVersion(mock.Anything, uint64(0), "TestClass", "tenant1").
				Return(nil, tc.tenantsShardErr)
			validator := multitenancy.NewTenantValidator("TestClass", true, schemaReader)

			// WHEN
			err := validator.ValidateTenants(context.Background(), 0, "tenant1")

			// THEN
			if tc.expectError {
				require.Error(t, err)
				if tc.errorContains != "" {
					require.Contains(t, err.Error(), tc.errorContains)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func Test_TenancyValidator_Builder(t *testing.T) {
	testCases := []struct {
		name                string
		multiTenancyEnabled bool
		validateWithTenant  string
		expectError         bool
	}{
		{
			name:                "single tenant mode rejects tenants",
			multiTenancyEnabled: false,
			validateWithTenant:  "tenant1",
			expectError:         true,
		},
		{
			name:                "single tenant mode accepts empty",
			multiTenancyEnabled: false,
			validateWithTenant:  "",
			expectError:         false,
		},
		{
			name:                "multi tenant mode rejects empty",
			multiTenancyEnabled: true,
			validateWithTenant:  "",
			expectError:         true,
		},
		{
			name:                "multi tenant mode accepts valid tenant",
			multiTenancyEnabled: true,
			validateWithTenant:  "tenant1",
			expectError:         false,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// GIVEN
			schemaReader := schemaUC.NewMockSchemaReader(t)
			if tc.multiTenancyEnabled && tc.validateWithTenant != "" {
				schemaReader.EXPECT().
					TenantsShardsWithVersion(mock.Anything, uint64(0), "TestClass", tc.validateWithTenant).
					Return(map[string]string{tc.validateWithTenant: models.TenantActivityStatusHOT}, nil)
			}
			validator := multitenancy.NewTenantValidator("TestClass", tc.multiTenancyEnabled, schemaReader)

			// WHEN
			err := validator.ValidateTenants(context.Background(), 0, tc.validateWithTenant)

			// THEN
			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
