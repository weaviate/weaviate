//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package multitenancy_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/multitenancy"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/objects"
)

// fakeSchemaReader is a single test fake that satisfies multitenancy.schemaReader
// (and resolver.schemaReader). It can behave as:
//   - A tenant status mapper: set tenantShards = map[string]string{"tenantA": models.TenantActivityStatusHOT} to simulate active tenants.
//   - A tenant error simulator: set tenantsShardErr to simulate schema connection failures.
//   - A class existence controller: set classExists = true/false to control whether ReadOnlyClass returns a class or nil.
//
// Example configs:
//
//	&fakeSchemaReader{tenantShards: map[string]string{"tenantA": models.TenantActivityStatusHOT}, classExists: true} // Valid active tenant
//	&fakeSchemaReader{tenantShards: map[string]string{"tenantA": models.TenantActivityStatusCOLD}, classExists: true} // Inactive tenant
//	&fakeSchemaReader{tenantShards: map[string]string{}, classExists: true} // No tenants exist
//	&fakeSchemaReader{tenantsShardErr: fmt.Errorf("connection failed"), classExists: true} // Schema error simulation
//	&fakeSchemaReader{tenantShards: map[string]string{}, classExists: false} // Class doesn't exist
type fakeSchemaReader struct {
	tenantShards    map[string]string
	tenantsShardErr error
	classExists     bool
}

// TenantsShards returns tenant status for requested tenants or the configured error.
// Only returns status for tenants that exist in the tenantShards map.
// Tenants not in the map are omitted from the result (simulating non-existent tenants).
func (f *fakeSchemaReader) TenantsShards(_ context.Context, _ string, tenants ...string) (map[string]string, error) {
	if f.tenantsShardErr != nil {
		return nil, f.tenantsShardErr
	}

	result := make(map[string]string)
	for _, tenant := range tenants {
		if status, exists := f.tenantShards[tenant]; exists {
			result[tenant] = status
		}
	}
	return result, nil
}

// ReadOnlyClass returns a minimal class stub if classExists is true, nil otherwise.
func (f *fakeSchemaReader) ReadOnlyClass(className string) *models.Class {
	if f.classExists {
		return &models.Class{Class: className}
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
		t.Run(tc.name, func(t *testing.T) {
			// GIVEN
			schemaReader := &fakeSchemaReader{classExists: true}
			validator := multitenancy.NewBuilder("TestClass", false, schemaReader).Build()

			// WHEN
			err := validator.ValidateTenants(context.Background(), tc.tenants...)

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
		t.Run(tc.name, func(t *testing.T) {
			// GIVEN
			schemaReader := &fakeSchemaReader{
				tenantShards: tc.tenantShards,
				classExists:  tc.classExists,
			}
			validator := multitenancy.NewBuilder("TestClass", true, schemaReader).Build()

			// WHEN
			err := validator.ValidateTenants(context.Background(), tc.tenants...)

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
		t.Run(tc.name, func(t *testing.T) {
			// GIVEN
			schemaReader := &fakeSchemaReader{
				tenantsShardErr: tc.tenantsShardErr,
				classExists:     true,
			}
			validator := multitenancy.NewBuilder("TestClass", true, schemaReader).Build()

			// WHEN
			err := validator.ValidateTenants(context.Background(), "tenant1")

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
		t.Run(tc.name, func(t *testing.T) {
			// GIVEN
			schemaReader := &fakeSchemaReader{
				tenantShards: map[string]string{"tenant1": models.TenantActivityStatusHOT},
				classExists:  true,
			}
			validator := multitenancy.NewBuilder("TestClass", tc.multiTenancyEnabled, schemaReader).Build()

			// WHEN
			err := validator.ValidateTenants(context.Background(), tc.validateWithTenant)

			// THEN
			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
