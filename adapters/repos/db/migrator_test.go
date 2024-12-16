//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/schema/mocks"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestTenantsWithStatus(t *testing.T) {
	tests := []struct {
		name            string
		className       string
		tenantNames     []string
		state           *sharding.State
		status          []string
		expectedCount   int
		expectedTenants []string
	}{
		{
			name:      "Single tenant with matching status",
			className: "Class",
			state: &sharding.State{
				Physical: map[string]sharding.Physical{
					"T1": {
						Status: models.TenantActivityStatusFREEZING,
					},
				},
			},
			tenantNames:     []string{"T1"},
			status:          []string{models.TenantActivityStatusFREEZING},
			expectedCount:   1,
			expectedTenants: []string{"T1"},
		},
		{
			name:      "Single tenant with non-matching status",
			className: "Class",
			state: &sharding.State{
				Physical: map[string]sharding.Physical{
					"T1": {
						Status: models.TenantActivityStatusFREEZING,
					},
				},
			},
			tenantNames:     []string{"T1"},
			status:          []string{models.TenantActivityStatusHOT},
			expectedCount:   0,
			expectedTenants: nil,
		},
		{
			name:        "Multiple tenants with mixed statuses, shall return only freezing and frozen",
			className:   "Class",
			tenantNames: []string{"T1", "T2"},
			state: &sharding.State{
				Physical: map[string]sharding.Physical{
					"T1": {
						Status: models.TenantActivityStatusFREEZING,
					},
					"T2": {
						Status: models.TenantActivityStatusFROZEN,
					},
					"T3": {
						Status: models.TenantActivityStatusCOLD,
					},
				},
			},
			status:          []string{models.TenantActivityStatusFREEZING, models.TenantActivityStatusFROZEN},
			expectedCount:   2,
			expectedTenants: []string{"T1", "T2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sg := mocks.NewSchemaGetter(t)
			m := Migrator{
				db: &DB{
					schemaGetter: sg,
				},
			}

			sg.On("CopyShardingState", tt.className).Return(tt.state)

			tenants := m.tenantsWithStatus(tt.className, tt.tenantNames, tt.status...)
			require.Equal(t, tt.expectedCount, len(tenants))

			// sort to make sure it's consistent
			slices.Sort(tt.expectedTenants)
			slices.Sort(tenants)
			require.Equal(t, tt.expectedTenants, tenants)
		})
	}
}
