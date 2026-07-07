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

package authz

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
)

func TestValidatePermissions(t *testing.T) {
	tests := []struct {
		name              string
		permissions       []*models.Permission
		allowEmpty        bool
		namespacesEnabled bool
		expectedErr       string
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
		{
			name:              "namespace-qualified collection valid when namespaces enabled",
			namespacesEnabled: true,
			permissions: []*models.Permission{
				{Collections: &models.PermissionCollections{Collection: String("customer1:Movies")}},
			},
		},
		{
			name:              "namespace-qualified wildcard valid when namespaces enabled",
			namespacesEnabled: true,
			permissions: []*models.Permission{
				{Collections: &models.PermissionCollections{Collection: String("customer1:*")}},
			},
		},
		{
			name:              "namespace-qualified collection rejected when namespaces disabled",
			namespacesEnabled: false,
			permissions: []*models.Permission{
				{Collections: &models.PermissionCollections{Collection: String("customer1:Movies")}},
			},
			expectedErr: "not a valid class name",
		},
		{
			name:              "invalid namespace prefix rejected when namespaces enabled",
			namespacesEnabled: true,
			permissions: []*models.Permission{
				{Collections: &models.PermissionCollections{Collection: String("Bad_NS:Movies")}},
			},
			expectedErr: "not a valid class name",
		},
		{
			// The tolerance is shared across collection fields, not just Collections.
			name:              "namespace-qualified data collection valid when namespaces enabled",
			namespacesEnabled: true,
			permissions: []*models.Permission{
				{Data: &models.PermissionData{Collection: String("customer1:Movies")}},
			},
		},
		{
			name: "namespace-qualified data collection rejected when namespaces disabled",
			permissions: []*models.Permission{
				{Data: &models.PermissionData{Collection: String("customer1:Movies")}},
			},
			expectedErr: "not a valid class name",
		},
		{
			// A valid namespace does not exempt the class part from its rules.
			name:              "valid namespace with invalid class part rejected",
			namespacesEnabled: true,
			permissions: []*models.Permission{
				{Collections: &models.PermissionCollections{Collection: String("customer1:invalid name")}},
			},
			expectedErr: "not a valid class name",
		},
		{
			name:              "namespace qualifier with empty class part rejected",
			namespacesEnabled: true,
			permissions: []*models.Permission{
				{Collections: &models.PermissionCollections{Collection: String("customer1:")}},
			},
			expectedErr: "not a valid class name",
		},
		{
			// An invalid earlier field must not be cleared by a valid later one.
			name: "invalid collection is not masked by a valid data collection",
			permissions: []*models.Permission{
				{
					Collections: &models.PermissionCollections{Collection: String("Invalid class name")},
					Data:        &models.PermissionData{Collection: String("ValidCollectionName")},
				},
			},
			expectedErr: "not a valid class name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validatePermissions(tt.namespacesEnabled, tt.allowEmpty, tt.permissions...)
			if tt.expectedErr != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
