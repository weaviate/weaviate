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

package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
)

const (
	testTenantA = "tenant-a"
	testTenantB = "tenant-b"
	testTenantC = "tenant-c"
)

func TestApplyTenantPagination(t *testing.T) {
	tests := []struct {
		name     string
		tenants  []*models.Tenant
		cursor   *filters.Cursor
		expected []string // expected tenant names
	}{
		{
			name: "no pagination - returns all unsorted",
			tenants: []*models.Tenant{
				{Name: testTenantC},
				{Name: testTenantA},
				{Name: testTenantB},
			},
			cursor:   nil,
			expected: []string{testTenantC, testTenantA, testTenantB},
		},
		{
			name: "empty cursor - returns all sorted",
			tenants: []*models.Tenant{
				{Name: testTenantC},
				{Name: testTenantA},
				{Name: testTenantB},
			},
			cursor:   &filters.Cursor{},
			expected: []string{testTenantA, testTenantB, testTenantC},
		},
		{
			name: "limit only - returns first N sorted",
			tenants: []*models.Tenant{
				{Name: testTenantC},
				{Name: testTenantA},
				{Name: testTenantB},
			},
			cursor:   &filters.Cursor{Limit: 2},
			expected: []string{testTenantA, testTenantB},
		},
		{
			name: "after + limit",
			tenants: []*models.Tenant{
				{Name: "tenant-d"},
				{Name: testTenantA},
				{Name: testTenantC},
				{Name: testTenantB},
			},
			cursor:   &filters.Cursor{After: testTenantB, Limit: 2},
			expected: []string{testTenantC, "tenant-d"},
		},
		{
			name: "after at end",
			tenants: []*models.Tenant{
				{Name: testTenantA},
				{Name: testTenantB},
			},
			cursor:   &filters.Cursor{After: testTenantB, Limit: 10},
			expected: []string{},
		},
		{
			name: "after not found - starts from beginning",
			tenants: []*models.Tenant{
				{Name: testTenantA},
				{Name: testTenantC},
			},
			cursor:   &filters.Cursor{After: "tenant-x", Limit: 10},
			expected: []string{},
		},
		{
			name: "limit larger than available",
			tenants: []*models.Tenant{
				{Name: testTenantB},
				{Name: testTenantA},
			},
			cursor:   &filters.Cursor{Limit: 100},
			expected: []string{testTenantA, testTenantB},
		},
		{
			name: "zero limit - returns all sorted",
			tenants: []*models.Tenant{
				{Name: testTenantC},
				{Name: testTenantA},
			},
			cursor:   &filters.Cursor{Limit: 0},
			expected: []string{testTenantA, testTenantC},
		},
		{
			name: "negative limit (LimitFlagNotSet) - returns all sorted",
			tenants: []*models.Tenant{
				{Name: testTenantC},
				{Name: testTenantA},
			},
			cursor:   &filters.Cursor{Limit: filters.LimitFlagNotSet},
			expected: []string{testTenantA, testTenantC},
		},
		{
			name: "after with special characters",
			tenants: []*models.Tenant{
				{Name: "tenant-001"},
				{Name: "tenant-002"},
				{Name: "tenant_special"},
			},
			cursor:   &filters.Cursor{After: "tenant-001", Limit: 2},
			expected: []string{"tenant-002", "tenant_special"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := &Handler{}
			result := h.applyTenantPagination(tt.tenants, tt.cursor)

			actual := make([]string, len(result))
			for i, tenant := range result {
				actual[i] = tenant.Name
			}

			assert.Equal(t, tt.expected, actual, "tenant names should match")
		})
	}
}

func TestApplyTenantPaginationEdgeCases(t *testing.T) {
	t.Run("empty tenant list", func(t *testing.T) {
		h := &Handler{}
		result := h.applyTenantPagination([]*models.Tenant{}, &filters.Cursor{Limit: 10})
		assert.Empty(t, result)
	})

	t.Run("single tenant", func(t *testing.T) {
		h := &Handler{}
		tenants := []*models.Tenant{{Name: "only-one"}}
		result := h.applyTenantPagination(tenants, &filters.Cursor{Limit: 10})
		assert.Len(t, result, 1)
		assert.Equal(t, "only-one", result[0].Name)
	})

	t.Run("after equals last tenant", func(t *testing.T) {
		h := &Handler{}
		tenants := []*models.Tenant{
			{Name: testTenantA},
			{Name: testTenantB},
			{Name: testTenantC},
		}
		result := h.applyTenantPagination(tenants, &filters.Cursor{After: testTenantC, Limit: 10})
		assert.Empty(t, result)
	})

	t.Run("preserves tenant properties", func(t *testing.T) {
		h := &Handler{}
		tenants := []*models.Tenant{
			{Name: testTenantB, ActivityStatus: models.TenantActivityStatusHOT},
			{Name: testTenantA, ActivityStatus: models.TenantActivityStatusCOLD},
		}
		result := h.applyTenantPagination(tenants, &filters.Cursor{Limit: 1})
		assert.Len(t, result, 1)
		assert.Equal(t, testTenantA, result[0].Name)
		assert.Equal(t, models.TenantActivityStatusCOLD, result[0].ActivityStatus)
	})
}
