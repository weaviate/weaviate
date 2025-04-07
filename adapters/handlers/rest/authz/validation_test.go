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

package authz

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
)

func TestValidatePermissions(t *testing.T) {
	tests := []struct {
		name        string
		permissions []*models.Permission
		allowEmpty  bool
		expectedErr string
	}{
		{
			name:        "no permissions - not allowed",
			permissions: []*models.Permission{},
			expectedErr: "role has to have at least 1 permission",
		},
		{
			name:        "no permissions - allowed",
			permissions: []*models.Permission{},
			allowEmpty:  true,
		},
		{
			name: "invalid collection name with space",
			permissions: []*models.Permission{
				{
					Collections: &models.PermissionCollections{
						Collection: String("Invalid class name"),
					},
				},
			},
			expectedErr: "not a valid class name",
		},
		{
			name: "invalid collection name with special character",
			permissions: []*models.Permission{
				{
					Collections: &models.PermissionCollections{
						Collection: String("InvalidClassName!"),
					},
				},
			},
			expectedErr: "not a valid class name",
		},
		{
			name: "invalid tenant name with space",
			permissions: []*models.Permission{
				{
					Tenants: &models.PermissionTenants{
						Collection: String("*"),
						Tenant:     String("Invalid Tenant Name"),
					},
				},
			},
			expectedErr: "is not a valid tenant name.",
		},
		{
			name: "invalid tenant name with special character",
			permissions: []*models.Permission{
				{
					Tenants: &models.PermissionTenants{
						Collection: String("*"),
						Tenant:     String("InvalidTenantName!"),
					},
				},
			},
			expectedErr: "is not a valid tenant name.",
		},
		{
			name: "invalid tenant name with one character",
			permissions: []*models.Permission{
				{
					Tenants: &models.PermissionTenants{
						Collection: String("*"),
						Tenant:     String("#"),
					},
				},
			},
			expectedErr: "is not a valid tenant name.",
		},
		{
			name: "valid collection regex name",
			permissions: []*models.Permission{
				{
					Collections: &models.PermissionCollections{
						Collection: String("ValidTenantName*"),
					},
				},
			},
		},
		{
			name: "valid collection *",
			permissions: []*models.Permission{
				{
					Collections: &models.PermissionCollections{
						Collection: String("*"),
					},
				},
			},
		},
		{
			name: "valid tenant regex name",
			permissions: []*models.Permission{
				{
					Tenants: &models.PermissionTenants{
						Collection: String("*"),
						Tenant:     String("Tenant*"),
					},
				},
			},
		},
		{
			name: "valid tenant *",
			permissions: []*models.Permission{
				{
					Tenants: &models.PermissionTenants{
						Collection: String("*"),
						Tenant:     String("*"),
					},
				},
			},
		},
		{
			name: "valid permissions",
			permissions: []*models.Permission{
				{
					Collections: &models.PermissionCollections{
						Collection: String("ValidCollectionName"),
					},
					Tenants: &models.PermissionTenants{
						Collection: String("*"),
						Tenant:     String("ValidTenantName"),
					},
					Data: &models.PermissionData{
						Collection: String("ValidCollectionName"),
						Tenant:     String("ValidTenantName"),
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validatePermissions(tt.allowEmpty, tt.permissions...)
			if tt.expectedErr != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
