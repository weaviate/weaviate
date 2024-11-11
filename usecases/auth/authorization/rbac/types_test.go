package rbac

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
)

func Test_policy(t *testing.T) {
	tests := []struct {
		name       string
		permission *models.Permission
		policy     *Policy
	}{
		{
			name: "manage all roles",
			permission: &models.Permission{
				Action: String(manageRoles),
			},
			policy: &Policy{
				resource: "roles/*",
				verb:     "(C)|(R)|(U)|(D)",
				domain:   "roles",
			},
		},
		{
			name: "manage a role",
			permission: &models.Permission{
				Action: String(manageRoles),
				Role:   String("admin"),
			},
			policy: &Policy{
				resource: "roles/admin",
				verb:     "(C)|(R)|(U)|(D)",
				domain:   "roles",
			},
		},
		{
			name: "manage cluster",
			permission: &models.Permission{
				Action: String(manageCluster),
			},
			policy: &Policy{
				resource: "cluster/*",
				verb:     "(C)|(R)|(U)|(D)",
				domain:   "cluster",
			},
		},
		{
			name: "create all collections",
			permission: &models.Permission{
				Action: String(createCollections),
			},
			policy: &Policy{
				resource: "collections/*",
				verb:     "C",
				domain:   "collections",
			},
		},
		{
			name: "create a collection",
			permission: &models.Permission{
				Action:     String(createCollections),
				Collection: String("foo"),
			},
			policy: &Policy{
				resource: "collections/foo",
				verb:     "C",
				domain:   "collections",
			},
		},
		{
			name: "read all collections",
			permission: &models.Permission{
				Action: String(readCollections),
			},
			policy: &Policy{
				resource: "collections/*",
				verb:     "R",
				domain:   "collections",
			},
		},
		{
			name: "read a collection",
			permission: &models.Permission{
				Action:     String(readCollections),
				Collection: String("foo"),
			},
			policy: &Policy{
				resource: "collections/foo",
				verb:     "R",
				domain:   "collections",
			},
		},
		{
			name: "update all collections",
			permission: &models.Permission{
				Action: String(updateCollections),
			},
			policy: &Policy{
				resource: "collections/*",
				verb:     "U",
				domain:   "collections",
			},
		},
		{
			name: "update a collection",
			permission: &models.Permission{
				Action:     String(updateCollections),
				Collection: String("foo"),
			},
			policy: &Policy{
				resource: "collections/foo",
				verb:     "U",
				domain:   "collections",
			},
		},
		{
			name: "delete all collections",
			permission: &models.Permission{
				Action: String(deleteCollections),
			},
			policy: &Policy{
				resource: "collections/*",
				verb:     "D",
				domain:   "collections",
			},
		},
		{
			name: "delete a collection",
			permission: &models.Permission{
				Action:     String(deleteCollections),
				Collection: String("foo"),
			},
			policy: &Policy{
				resource: "collections/foo",
				verb:     "D",
				domain:   "collections",
			},
		},
		{
			name: "create all tenants",
			permission: &models.Permission{
				Action: String(createTenants),
			},
			policy: &Policy{
				resource: "collections/*/shards/*",
				verb:     "C",
				domain:   "tenants",
			},
		},
		{
			name: "create all tenants in a collection",
			permission: &models.Permission{
				Action:     String(createTenants),
				Collection: String("foo"),
			},
			policy: &Policy{
				resource: "collections/foo/shards/*",
				verb:     "C",
				domain:   "tenants",
			},
		},
		{
			name: "create a tenant in all collections",
			permission: &models.Permission{
				Action: String(createTenants),
				Tenant: String("bar"),
			},
			policy: &Policy{
				resource: "collections/*/shards/bar",
				verb:     "C",
				domain:   "tenants",
			},
		},
		{
			name: "create a tenant in a collection",
			permission: &models.Permission{
				Action:     String(createTenants),
				Collection: String("foo"),
				Tenant:     String("bar"),
			},
			policy: &Policy{
				resource: "collections/foo/shards/bar",
				verb:     "C",
				domain:   "tenants",
			},
		},
		{
			name: "read all tenants",
			permission: &models.Permission{
				Action: String(readTenants),
			},
			policy: &Policy{
				resource: "collections/*/shards/*",
				verb:     "R",
				domain:   "tenants",
			},
		},
		{
			name: "read all tenants in a collection",
			permission: &models.Permission{
				Action:     String(readTenants),
				Collection: String("foo"),
			},
			policy: &Policy{
				resource: "collections/foo/shards/*",
				verb:     "R",
				domain:   "tenants",
			},
		},
		{
			name: "read a tenant in all collections",
			permission: &models.Permission{
				Action: String(readTenants),
				Tenant: String("bar"),
			},
			policy: &Policy{
				resource: "collections/*/shards/bar",
				verb:     "R",
				domain:   "tenants",
			},
		},
		{
			name: "read a tenant in a collection",
			permission: &models.Permission{
				Action:     String(readTenants),
				Collection: String("foo"),
				Tenant:     String("bar"),
			},
			policy: &Policy{
				resource: "collections/foo/shards/bar",
				verb:     "R",
				domain:   "tenants",
			},
		},
		{
			name: "update all tenants",
			permission: &models.Permission{
				Action: String(updateTenants),
			},
			policy: &Policy{
				resource: "collections/*/shards/*",
				verb:     "U",
				domain:   "tenants",
			},
		},
		{
			name: "update all tenants in a collection",
			permission: &models.Permission{
				Action:     String(updateTenants),
				Collection: String("foo"),
			},
			policy: &Policy{
				resource: "collections/foo/shards/*",
				verb:     "U",
				domain:   "tenants",
			},
		},
		{
			name: "update a tenant in all collections",
			permission: &models.Permission{
				Action: String(updateTenants),
				Tenant: String("bar"),
			},
			policy: &Policy{
				resource: "collections/*/shards/bar",
				verb:     "U",
				domain:   "tenants",
			},
		},
		{
			name: "update a tenant in a collection",
			permission: &models.Permission{
				Action:     String(updateTenants),
				Collection: String("foo"),
				Tenant:     String("bar"),
			},
			policy: &Policy{
				resource: "collections/foo/shards/bar",
				verb:     "U",
				domain:   "tenants",
			},
		},
		{
			name: "delete all tenants",
			permission: &models.Permission{
				Action: String(deleteTenants),
			},
			policy: &Policy{
				resource: "collections/*/shards/*",
				verb:     "D",
				domain:   "tenants",
			},
		},
		{
			name: "delete all tenants in a collection",
			permission: &models.Permission{
				Action:     String(deleteTenants),
				Collection: String("foo"),
			},
			policy: &Policy{
				resource: "collections/foo/shards/*",
				verb:     "D",
				domain:   "tenants",
			},
		},
		{
			name: "delete a tenant in all collections",
			permission: &models.Permission{
				Action: String(deleteTenants),
				Tenant: String("bar"),
			},
			policy: &Policy{
				resource: "collections/*/shards/bar",
				verb:     "D",
				domain:   "tenants",
			},
		},
		{
			name: "delete a tenant in a collection",
			permission: &models.Permission{
				Action:     String(deleteTenants),
				Collection: String("foo"),
				Tenant:     String("bar"),
			},
			policy: &Policy{
				resource: "collections/foo/shards/bar",
				verb:     "D",
				domain:   "tenants",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy := policy(tt.permission)
			require.Equal(t, tt.policy, policy)
		})
	}
}

func Test_permission(t *testing.T) {
	tests := []struct {
		name       string
		policy     []string
		permission *models.Permission
	}{
		{
			name:   "manage all roles",
			policy: []string{"p", "roles/*", "(C)|(R)|(U)|(D)", "roles"},
			permission: &models.Permission{
				Action: String(manageRoles),
				Role:   String("*"),
			},
		},
		{
			name:   "manage cluster",
			policy: []string{"p", "cluster/*", "(C)|(R)|(U)|(D)", "cluster"},
			permission: &models.Permission{
				Action: String(manageCluster),
			},
		},
		{
			name:   "create all collections",
			policy: []string{"p", "collections/*", "C", "collections"},
			permission: &models.Permission{
				Action:     String(createCollections),
				Collection: String("*"),
			},
		},
		{
			name:   "create a collection",
			policy: []string{"p", "collections/foo", "C", "collections"},
			permission: &models.Permission{
				Action:     String(createCollections),
				Collection: String("foo"),
			},
		},
		{
			name:   "read all collections",
			policy: []string{"p", "collections/*", "R", "collections"},
			permission: &models.Permission{
				Action:     String(readCollections),
				Collection: String("*"),
			},
		},
		{
			name:   "read a collection",
			policy: []string{"p", "collections/foo", "R", "collections"},
			permission: &models.Permission{
				Action:     String(readCollections),
				Collection: String("foo"),
			},
		},
		{
			name:   "update all collections",
			policy: []string{"p", "collections/*", "U", "collections"},
			permission: &models.Permission{
				Action:     String(updateCollections),
				Collection: String("*"),
			},
		},
		{
			name:   "update a collection",
			policy: []string{"p", "collections/foo", "U", "collections"},
			permission: &models.Permission{
				Action:     String(updateCollections),
				Collection: String("foo"),
			},
		},
		{
			name:   "delete all collections",
			policy: []string{"p", "collections/*", "D", "collections"},
			permission: &models.Permission{
				Action:     String(deleteCollections),
				Collection: String("*"),
			},
		},
		{
			name:   "delete a collection",
			policy: []string{"p", "collections/foo", "D", "collections"},
			permission: &models.Permission{
				Action:     String(deleteCollections),
				Collection: String("foo"),
			},
		},
		{
			name:   "create all tenants",
			policy: []string{"p", "collections/*/shards/*", "C", "tenants"},
			permission: &models.Permission{
				Action:     String(createTenants),
				Collection: String("*"),
				Tenant:     String("*"),
			},
		},
		{
			name:   "create all tenants in a collection",
			policy: []string{"p", "collections/foo/shards/*", "C", "tenants"},
			permission: &models.Permission{
				Action:     String(createTenants),
				Collection: String("foo"),
				Tenant:     String("*"),
			},
		},
		{
			name:   "create a tenant in all collections",
			policy: []string{"p", "collections/*/shards/bar", "C", "tenants"},
			permission: &models.Permission{
				Action:     String(createTenants),
				Collection: String("*"),
				Tenant:     String("bar"),
			},
		},
		{
			name:   "create a tenant in a collection",
			policy: []string{"p", "collections/foo/shards/bar", "C", "tenants"},
			permission: &models.Permission{
				Action:     String(createTenants),
				Collection: String("foo"),
				Tenant:     String("bar"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			permission := permission(tt.policy)
			require.Equal(t, tt.permission, permission)
		})
	}
}

func String(s string) *string {
	return &s
}
