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

package conv

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestValidatePermissions(t *testing.T) {
	tests := []struct {
		name              string
		permissions       []*models.Permission
		check             PermissionCheck
		namespacesEnabled bool
		expectedErr       string
	}{
		{
			name:        "no permissions - not allowed on add",
			permissions: []*models.Permission{},
			check:       CheckAdd,
			expectedErr: "role has to have at least 1 permission",
		},
		{
			name:        "no permissions - not allowed on lookup",
			permissions: []*models.Permission{},
			check:       CheckLookup,
			expectedErr: "role has to have at least 1 permission",
		},
		{
			name:        "no permissions - allowed on create",
			permissions: []*models.Permission{},
			check:       CheckCreate,
		},
		{
			name: "invalid collection name with space",
			permissions: []*models.Permission{
				{
					Collections: &models.PermissionCollections{
						Collection: authorization.String("Invalid class name"),
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
						Collection: authorization.String("InvalidClassName!"),
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
						Collection: authorization.String("wrong_collection"),
					},
				},
			},
		},
		{
			// "ſ" and "ı" uppercase to ASCII "S" and "I", so the stored pattern
			// would name a different class.
			name: "collection starting with long s rejected",
			permissions: []*models.Permission{
				{Collections: &models.PermissionCollections{Collection: authorization.String("\u017fovies*")}},
			},
			expectedErr: "not a valid class name",
		},
		{
			name: "collection starting with dotless i rejected",
			permissions: []*models.Permission{
				{Collections: &models.PermissionCollections{Collection: authorization.String("\u0131tems")}},
			},
			expectedErr: "not a valid class name",
		},
		{
			name: "lowercase collection name in an alias permission is uppercased, not rejected",
			permissions: []*models.Permission{
				{
					Aliases: &models.PermissionAliases{
						Collection: authorization.String("wrong_collection"),
						Alias:      authorization.String("my_alias"),
					},
				},
			},
		},
		{
			name: "invalid tenant name with space",
			permissions: []*models.Permission{
				{
					Tenants: &models.PermissionTenants{
						Collection: authorization.String("*"),
						Tenant:     authorization.String("Invalid Tenant Name"),
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
						Collection: authorization.String("*"),
						Tenant:     authorization.String("InvalidTenantName!"),
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
						Collection: authorization.String("*"),
						Tenant:     authorization.String("#"),
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
						Collection: authorization.String("ValidTenantName*"),
					},
				},
			},
		},
		{
			name: "valid collection *",
			permissions: []*models.Permission{
				{
					Collections: &models.PermissionCollections{
						Collection: authorization.String("*"),
					},
				},
			},
		},
		{
			name: "valid tenant regex name",
			permissions: []*models.Permission{
				{
					Tenants: &models.PermissionTenants{
						Collection: authorization.String("*"),
						Tenant:     authorization.String("Tenant*"),
					},
				},
			},
		},
		{
			name: "valid tenant *",
			permissions: []*models.Permission{
				{
					Tenants: &models.PermissionTenants{
						Collection: authorization.String("*"),
						Tenant:     authorization.String("*"),
					},
				},
			},
		},
		{
			name: "valid permissions",
			permissions: []*models.Permission{
				{
					Collections: &models.PermissionCollections{
						Collection: authorization.String("ValidCollectionName"),
					},
					Tenants: &models.PermissionTenants{
						Collection: authorization.String("*"),
						Tenant:     authorization.String("ValidTenantName"),
					},
					Data: &models.PermissionData{
						Collection: authorization.String("ValidCollectionName"),
						Tenant:     authorization.String("ValidTenantName"),
					},
				},
			},
		},
		{
			name:              "namespace-qualified collection valid when namespaces enabled",
			namespacesEnabled: true,
			permissions: []*models.Permission{
				{Collections: &models.PermissionCollections{Collection: authorization.String("customer1:Movies")}},
			},
		},
		{
			name:              "namespace-qualified wildcard valid when namespaces enabled",
			namespacesEnabled: true,
			permissions: []*models.Permission{
				{Collections: &models.PermissionCollections{Collection: authorization.String("customer1:*")}},
			},
		},
		{
			name:              "namespace-qualified collection rejected when namespaces disabled",
			namespacesEnabled: false,
			permissions: []*models.Permission{
				{Collections: &models.PermissionCollections{Collection: authorization.String("customer1:Movies")}},
			},
			expectedErr: "not a valid class name",
		},
		{
			name:              "invalid namespace prefix rejected when namespaces enabled",
			namespacesEnabled: true,
			permissions: []*models.Permission{
				{Collections: &models.PermissionCollections{Collection: authorization.String("Bad_NS:Movies")}},
			},
			expectedErr: "not a valid class name",
		},
		{
			// The tolerance is shared across collection fields, not just Collections.
			name:              "namespace-qualified data collection valid when namespaces enabled",
			namespacesEnabled: true,
			permissions: []*models.Permission{
				{Data: &models.PermissionData{Collection: authorization.String("customer1:Movies")}},
			},
		},
		{
			name: "namespace-qualified data collection rejected when namespaces disabled",
			permissions: []*models.Permission{
				{Data: &models.PermissionData{Collection: authorization.String("customer1:Movies")}},
			},
			expectedErr: "not a valid class name",
		},
		{
			// A valid namespace does not exempt the class part from its rules.
			name:              "valid namespace with invalid class part rejected",
			namespacesEnabled: true,
			permissions: []*models.Permission{
				{Collections: &models.PermissionCollections{Collection: authorization.String("customer1:invalid name")}},
			},
			expectedErr: "not a valid class name",
		},
		{
			name:              "namespace qualifier with empty class part rejected",
			namespacesEnabled: true,
			permissions: []*models.Permission{
				{Collections: &models.PermissionCollections{Collection: authorization.String("customer1:")}},
			},
			expectedErr: "not a valid class name",
		},
		{
			// An invalid earlier field must not be cleared by a valid later one.
			name: "invalid collection is not masked by a valid data collection",
			permissions: []*models.Permission{
				{
					Collections: &models.PermissionCollections{Collection: authorization.String("Invalid class name")},
					Data:        &models.PermissionData{Collection: authorization.String("ValidCollectionName")},
				},
			},
			expectedErr: "not a valid class name",
		},
		{
			// Data.Object is deprecated and ignored, so nothing about it is validated
			// any more: every value that is rejected on a live target -- a regex that
			// does not compile, a '/', a brace-rewrite break, an over-long string --
			// is accepted here as a no-op.
			name: "deprecated data object is accepted whatever it holds",
			permissions: []*models.Permission{
				dataWithObject("["),
				dataWithObject("a/b"),
				dataWithObject(`\x{263a}`),
				dataWithObject(strings.Repeat("o", MaxTargetLength+1)),
			},
		},
		{
			name: "invalid regex in user is rejected",
			permissions: []*models.Permission{
				{Users: &models.PermissionUsers{Users: authorization.String("x[")}},
			},
			expectedErr: "not a valid pattern",
		},
		{
			name: "invalid regex in role is rejected",
			permissions: []*models.Permission{
				{Roles: &models.PermissionRoles{Role: authorization.String("(")}},
			},
			expectedErr: "not a valid pattern",
		},
		{
			name: "collection passing the charset but not compiling is rejected",
			permissions: []*models.Permission{
				{Collections: &models.PermissionCollections{Collection: authorization.String("A[")}},
			},
			expectedErr: "not a valid pattern",
		},
		{
			name: "valid user regex is accepted",
			permissions: []*models.Permission{
				{Users: &models.PermissionUsers{Users: authorization.String("admin.*")}},
			},
		},
		{
			name: "valid oidc user with colon is accepted",
			permissions: []*models.Permission{
				{Users: &models.PermissionUsers{Users: authorization.String("okta:alice")}},
			},
		},
		{
			name: "unicode class escape in user rejected (KeyMatch5 brace rewrite would break it)",
			permissions: []*models.Permission{
				{Users: &models.PermissionUsers{Users: authorization.String(`\p{L}`)}},
			},
			expectedErr: "not a valid pattern",
		},
		{
			name: "collection with unicode class escape rejected",
			permissions: []*models.Permission{
				{Collections: &models.PermissionCollections{Collection: authorization.String(`A\p{L}`)}},
			},
			expectedErr: "not a valid pattern",
		},
		{
			name: "invalid regex in group rejected",
			permissions: []*models.Permission{
				{Groups: &models.PermissionGroups{Group: authorization.String("x["), GroupType: models.GroupTypeOidc}},
			},
			expectedErr: "not a valid pattern",
		},
		{
			name: "invalid regex in shard rejected",
			permissions: []*models.Permission{
				{Replicate: &models.PermissionReplicate{Collection: authorization.String("*"), Shard: authorization.String("[")}},
			},
			expectedErr: "not a valid pattern",
		},
		{
			name: "invalid regex in alias rejected",
			permissions: []*models.Permission{
				{Aliases: &models.PermissionAliases{Collection: authorization.String("*"), Alias: authorization.String("[")}},
			},
			expectedErr: "not a valid pattern",
		},
		{
			name: "invalid regex in alias rejected on lookup",
			permissions: []*models.Permission{
				{Aliases: &models.PermissionAliases{Collection: authorization.String("*"), Alias: authorization.String("[")}},
			},
			check:       CheckLookup,
			expectedErr: "alias '[' is not a valid pattern",
		},
		{
			name: "invalid regex in tenant rejected",
			permissions: []*models.Permission{
				{Tenants: &models.PermissionTenants{Collection: authorization.String("*"), Tenant: authorization.String("[")}},
			},
			expectedErr: "not a valid pattern",
		},
		{
			name: "invalid regex in namespace rejected",
			permissions: []*models.Permission{
				{Namespaces: &models.PermissionNamespaces{Namespace: authorization.String("[")}},
			},
			expectedErr: "not a valid pattern",
		},
		{
			name: "slash in namespace is rejected",
			permissions: []*models.Permission{
				{Namespaces: &models.PermissionNamespaces{Namespace: authorization.String("a/b")}},
			},
			expectedErr: "must not contain '/'",
		},
		{
			name: "over-long namespace target rejected",
			permissions: []*models.Permission{
				{Namespaces: &models.PermissionNamespaces{Namespace: authorization.String(strings.Repeat("a", 257))}},
			},
			expectedErr: "exceeds the maximum length",
		},
		{
			name: "valid namespace regex is accepted",
			permissions: []*models.Permission{
				{Namespaces: &models.PermissionNamespaces{Namespace: authorization.String("ns|prod")}},
			},
		},
		{
			name: "over-long user target rejected",
			permissions: []*models.Permission{
				{Users: &models.PermissionUsers{Users: authorization.String(strings.Repeat("a", 257))}},
			},
			expectedErr: "exceeds the maximum length",
		},
		{
			name: "user target at the length limit accepted",
			permissions: []*models.Permission{
				{Users: &models.PermissionUsers{Users: authorization.String(strings.Repeat("a", 256))}},
			},
		},
		{
			name: "valid group, shard and alias regex accepted",
			permissions: []*models.Permission{
				{
					Groups:    &models.PermissionGroups{Group: authorization.String("team.*"), GroupType: models.GroupTypeOidc},
					Replicate: &models.PermissionReplicate{Collection: authorization.String("*"), Shard: authorization.String("s|t")},
					Aliases:   &models.PermissionAliases{Collection: authorization.String("*"), Alias: authorization.String("a*")},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidatePermissions(tt.namespacesEnabled, tt.check, tt.permissions...)
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
	err := ValidatePermissions(false, CheckAdd, &models.Permission{
		Collections: &models.PermissionCollections{Collection: authorization.String("A[")},
		Users:       &models.PermissionUsers{Users: authorization.String("(")},
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "collection 'A[' is not a valid pattern")
	assert.Contains(t, err.Error(), "users '(' is not a valid pattern")
}

// dataWithObject builds a data permission carrying the deprecated object field,
// which the server still accepts on the wire and ignores.
func dataWithObject(object string) *models.Permission {
	//nolint:staticcheck // pinning the deprecated field's no-op behaviour is the point
	return &models.Permission{Data: &models.PermissionData{Collection: authorization.String("*"), Object: authorization.String(object)}}
}

// TestValidateStorablePoliciesBraceRewrite checks that the stored-form compile
// rewrites "{...}" as KeyMatch5 does, so a target the matcher accepts is not refused.
func TestValidateStorablePoliciesBraceRewrite(t *testing.T) {
	require.NoError(t, ValidateStorablePolicies([]authorization.Policy{{Resource: "users/x{1001}"}}))
}

// TestValidatePermissionsAliasRule checks that create and add apply the
// class-name rule to the uppercased alias pattern and that lookups skip it.
func TestValidatePermissionsAliasRule(t *testing.T) {
	tests := []struct {
		name              string
		namespacesEnabled bool
		aliases           *models.PermissionAliases
		wantErr           string
	}{
		{name: "no aliases"},
		{name: "no alias", aliases: &models.PermissionAliases{Collection: authorization.String("*")}},
		{name: "wildcard", aliases: &models.PermissionAliases{Alias: authorization.String("*")}},
		{name: "lowercase first letter", aliases: &models.PermissionAliases{Alias: authorization.String("my_alias")}},
		{name: "prefix pattern", aliases: &models.PermissionAliases{Alias: authorization.String("Alias*")}},
		{name: "alternation", aliases: &models.PermissionAliases{Alias: authorization.String("Movies|Books")}},
		{name: "non-ASCII first letter", aliases: &models.PermissionAliases{Alias: authorization.String("ålias.*")}, wantErr: "'ålias.*' is not a valid alias name"},
		{name: "non-ASCII letter after the first", aliases: &models.PermissionAliases{Alias: authorization.String("Alïas")}, wantErr: "not a valid alias name"},
		{name: "long s, whose uppercase is ASCII S", aliases: &models.PermissionAliases{Alias: authorization.String("ſlias.*")}, wantErr: "not a valid alias name"},
		{name: "dotless i, whose uppercase is ASCII I", aliases: &models.PermissionAliases{Alias: authorization.String("ıtems")}, wantErr: "not a valid alias name"},
		{name: "starts with a regex operator", aliases: &models.PermissionAliases{Alias: authorization.String(".*_v2")}, wantErr: "not a valid alias name"},
		{name: "colon with namespaces off", aliases: &models.PermissionAliases{Alias: authorization.String("my:alias")}, wantErr: "not a valid alias name"},
		{name: "empty", aliases: &models.PermissionAliases{Alias: authorization.String("")}, wantErr: "not a valid alias name"},
		{name: "qualified with namespaces on", namespacesEnabled: true, aliases: &models.PermissionAliases{Alias: authorization.String("customer1:films*")}},
		{name: "empty namespace with namespaces on", namespacesEnabled: true, aliases: &models.PermissionAliases{Alias: authorization.String(":films")}, wantErr: "not a valid alias name"},
		{name: "qualified non-ASCII alias with namespaces on", namespacesEnabled: true, aliases: &models.PermissionAliases{Alias: authorization.String("customer1:\u00e5lias")}, wantErr: "not a valid alias name"},
		{name: "POSIX class with namespaces on", namespacesEnabled: true, aliases: &models.PermissionAliases{Alias: authorization.String("[[:alpha:]]+")}, wantErr: "not a valid alias name"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			perm := &models.Permission{Aliases: tt.aliases}
			for _, check := range []PermissionCheck{CheckCreate, CheckAdd} {
				err := ValidatePermissions(tt.namespacesEnabled, check, perm)
				if tt.wantErr == "" {
					require.NoError(t, err)
					continue
				}
				require.ErrorContains(t, err, tt.wantErr)
			}
			require.NoError(t, ValidatePermissions(tt.namespacesEnabled, CheckLookup, perm))
		})
	}
}

func TestValidateNoQualifiedNamespaceInPolicies(t *testing.T) {
	global := &models.Principal{Username: "root"}
	namespaced := &models.Principal{Username: "alice", Namespace: "customer1"}

	tests := []struct {
		name                 string
		namespacesEnabled    bool
		principal            *models.Principal
		policies             []authorization.Policy
		allowGlobalQualified bool
		wantErr              bool
	}{
		{
			name:              "namespaces disabled: qualified resource accepted",
			namespacesEnabled: false,
			principal:         namespaced,
			policies:          []authorization.Policy{{Resource: "schema/collections/customer1:Movies/shards/#"}},
		},
		{
			name:              "ns-relative resource accepted",
			namespacesEnabled: true,
			principal:         global,
			policies:          []authorization.Policy{{Resource: "schema/collections/Movies/shards/#"}},
		},
		{
			name:              "qualified collection segment rejected (global)",
			namespacesEnabled: true,
			principal:         global,
			policies:          []authorization.Policy{{Resource: "schema/collections/customer1:Movies/shards/#"}},
			wantErr:           true,
		},
		{
			name:              "qualified alias segment rejected",
			namespacesEnabled: true,
			principal:         global,
			policies:          []authorization.Policy{{Resource: "aliases/collections/Movies/aliases/customer1:Films"}},
			wantErr:           true,
		},
		{
			name:              "rejection scans every policy",
			namespacesEnabled: true,
			principal:         global,
			policies: []authorization.Policy{
				{Resource: "schema/collections/Movies/shards/#"},
				{Resource: "data/collections/customer1:Movies/shards/.*/objects/.*"},
			},
			wantErr: true,
		},
		// users/<id> and groups/<...> ids may contain ':' — opaque, not a
		// qualifier. Allowed for a global caller, rejected for a namespaced one
		// (must submit the short form).
		{
			name:              "global: colon-bearing user id accepted",
			namespacesEnabled: true,
			principal:         global,
			policies:          []authorization.Policy{{Resource: "users/urn:foo"}},
		},
		{
			name:              "global: colon-bearing group name accepted",
			namespacesEnabled: true,
			principal:         global,
			policies:          []authorization.Policy{{Resource: "groups/oidc/team:eng"}},
		},
		{
			name:              "namespaced: colon-bearing user id rejected",
			namespacesEnabled: true,
			principal:         namespaced,
			policies:          []authorization.Policy{{Resource: "users/customer2:bob"}},
			wantErr:           true,
		},
		{
			name:              "global: qualified user collection shape still rejected",
			namespacesEnabled: true,
			principal:         global,
			policies:          []authorization.Policy{{Resource: "schema/collections/customer1:Movies/shards/#"}},
			wantErr:           true,
		},
		// allowGlobalQualified lifts the restriction for a global caller (the
		// hasPermission read path), but never for a namespaced one.
		{
			name:                 "allowGlobalQualified: global qualified resource accepted",
			namespacesEnabled:    true,
			principal:            global,
			policies:             []authorization.Policy{{Resource: "schema/collections/customer1:Movies/shards/#"}},
			allowGlobalQualified: true,
		},
		{
			name:                 "allowGlobalQualified: namespaced qualified resource still rejected",
			namespacesEnabled:    true,
			principal:            namespaced,
			policies:             []authorization.Policy{{Resource: "schema/collections/customer2:Movies/shards/#"}},
			allowGlobalQualified: true,
			wantErr:              true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateNoQualifiedNamespaceInPolicies(tt.namespacesEnabled, tt.principal, tt.policies, tt.allowGlobalQualified)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "namespace-qualified")
			} else {
				require.NoError(t, err)
			}
		})
	}
}
