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
	"testing"

	"github.com/weaviate/weaviate/multitenancy"

	"github.com/weaviate/weaviate/entities/models"
)

func TestIsMultiTenant(t *testing.T) {
	tests := []struct {
		name     string
		config   *models.MultiTenancyConfig
		expected bool
	}{
		{
			name:     "nil config should return false",
			config:   nil,
			expected: false,
		},
		{
			name: "disabled multi-tenancy should return false",
			config: &models.MultiTenancyConfig{
				Enabled: false,
			},
			expected: false,
		},
		{
			name: "enabled multi-tenancy should return true",
			config: &models.MultiTenancyConfig{
				Enabled: true,
			},
			expected: true,
		},
		{
			name: "enabled with other fields should return true",
			config: &models.MultiTenancyConfig{
				Enabled:              true,
				AutoTenantCreation:   true,
				AutoTenantActivation: false,
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := multitenancy.IsMultiTenant(tt.config)
			if result != tt.expected {
				t.Errorf("IsMultiTenant() = %v, expected %v", result, tt.expected)
			}
		})
	}
}
