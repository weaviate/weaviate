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
	"strings"
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
			name: "lowercase collection name is uppercased, not rejected",
			permissions: []*models.Permission{
				{
					Collections: &models.PermissionCollections{
						Collection: String("wrong_collection"),
					},
				},
			},
		},
		{
			name: "lowercase collection name in an alias permission is uppercased, not rejected",
			permissions: []*models.Permission{
				{
					Aliases: &models.PermissionAliases{
						Collection: String("wrong_collection"),
						Alias:      String("my_alias"),
					},
				},
			},
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
		{
			name: "invalid regex in data object is rejected (would panic the matcher)",
			permissions: []*models.Permission{
				{Data: &models.PermissionData{Collection: String("*"), Object: String("[")}},
			},
			expectedErr: "not a valid pattern",
		},
		{
			name: "invalid regex in user is rejected",
			permissions: []*models.Permission{
				{Users: &models.PermissionUsers{Users: String("x[")}},
			},
			expectedErr: "not a valid pattern",
		},
		{
			name: "invalid regex in role is rejected",
			permissions: []*models.Permission{
				{Roles: &models.PermissionRoles{Role: String("(")}},
			},
			expectedErr: "not a valid pattern",
		},
		{
			name: "collection passing the charset but not compiling is rejected",
			permissions: []*models.Permission{
				{Collections: &models.PermissionCollections{Collection: String("A[")}},
			},
			expectedErr: "not a valid pattern",
		},
		{
			name: "slash in object is rejected",
			permissions: []*models.Permission{
				{Data: &models.PermissionData{Collection: String("*"), Object: String("a/b")}},
			},
			expectedErr: "must not contain '/'",
		},
		{
			name: "valid object regex is accepted",
			permissions: []*models.Permission{
				{Data: &models.PermissionData{Collection: String("*"), Object: String("o|x")}},
			},
		},
		{
			name: "valid user regex is accepted",
			permissions: []*models.Permission{
				{Users: &models.PermissionUsers{Users: String("admin.*")}},
			},
		},
		{
			name: "valid oidc user with colon is accepted",
			permissions: []*models.Permission{
				{Users: &models.PermissionUsers{Users: String("okta:alice")}},
			},
		},
		{
			name: "unicode class escape in user rejected (KeyMatch5 brace rewrite would break it)",
			permissions: []*models.Permission{
				{Users: &models.PermissionUsers{Users: String(`\p{L}`)}},
			},
			expectedErr: "not a valid pattern",
		},
		{
			name: "unicode codepoint escape in object rejected",
			permissions: []*models.Permission{
				{Data: &models.PermissionData{Collection: String("*"), Object: String(`\x{263a}`)}},
			},
			expectedErr: "not a valid pattern",
		},
		{
			name: "collection with unicode class escape rejected",
			permissions: []*models.Permission{
				{Collections: &models.PermissionCollections{Collection: String(`A\p{L}`)}},
			},
			expectedErr: "not a valid pattern",
		},
		{
			name: "invalid regex in group rejected",
			permissions: []*models.Permission{
				{Groups: &models.PermissionGroups{Group: String("x["), GroupType: models.GroupTypeOidc}},
			},
			expectedErr: "not a valid pattern",
		},
		{
			name: "invalid regex in shard rejected",
			permissions: []*models.Permission{
				{Replicate: &models.PermissionReplicate{Collection: String("*"), Shard: String("[")}},
			},
			expectedErr: "not a valid pattern",
		},
		{
			name: "invalid regex in alias rejected",
			permissions: []*models.Permission{
				{Aliases: &models.PermissionAliases{Collection: String("*"), Alias: String("[")}},
			},
			expectedErr: "not a valid pattern",
		},
		{
			name: "invalid regex in tenant rejected",
			permissions: []*models.Permission{
				{Tenants: &models.PermissionTenants{Collection: String("*"), Tenant: String("[")}},
			},
			expectedErr: "not a valid pattern",
		},
		{
			name: "over-long user target rejected",
			permissions: []*models.Permission{
				{Users: &models.PermissionUsers{Users: String(strings.Repeat("a", 257))}},
			},
			expectedErr: "exceeds the maximum length",
		},
		{
			name: "user target at the length limit accepted",
			permissions: []*models.Permission{
				{Users: &models.PermissionUsers{Users: String(strings.Repeat("a", 256))}},
			},
		},
		{
			name: "valid group, shard and alias regex accepted",
			permissions: []*models.Permission{
				{
					Groups:    &models.PermissionGroups{Group: String("team.*"), GroupType: models.GroupTypeOidc},
					Replicate: &models.PermissionReplicate{Collection: String("*"), Shard: String("s|t")},
					Aliases:   &models.PermissionAliases{Collection: String("*"), Alias: String("a*")},
				},
			},
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

// TestValidatePermissions_AccumulatesErrors pins that every invalid field in a
// permission surfaces, not just the last one.
func TestValidatePermissions_AccumulatesErrors(t *testing.T) {
	err := validatePermissions(false, false, &models.Permission{
		Collections: &models.PermissionCollections{Collection: String("A[")},
		Data:        &models.PermissionData{Collection: String("*"), Object: String("(")},
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "collection 'A[' is not a valid pattern")
	assert.Contains(t, err.Error(), "object '(' is not a valid pattern")
}
