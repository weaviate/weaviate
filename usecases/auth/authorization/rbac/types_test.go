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

package rbac

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

type innerTest struct {
	permissionAction string
	testDescription  string
	policyVerb       string
}

var (
	foo = String("foo")
	bar = String("bar")
	baz = String("baz")

	createDesc = "create"
	readDesc   = "read"
	updateDesc = "update"
	deleteDesc = "delete"
	manageDesc = "manage"

	createVerb = authorization.CREATE
	readVerb   = authorization.READ
	updateVerb = authorization.UPDATE
	deleteVerb = authorization.DELETE
	manageVerb = authorization.CRUD

	rolesTests = []innerTest{
		{permissionAction: manageRoles, testDescription: manageDesc, policyVerb: manageVerb},
	}
	clusterTests = []innerTest{
		{permissionAction: manageCluster, testDescription: manageDesc, policyVerb: manageVerb},
	}
	collectionsTests = []innerTest{
		{permissionAction: createCollections, testDescription: createDesc, policyVerb: createVerb},
		{permissionAction: readCollections, testDescription: readDesc, policyVerb: readVerb},
		{permissionAction: updateCollections, testDescription: updateDesc, policyVerb: updateVerb},
		{permissionAction: deleteCollections, testDescription: deleteDesc, policyVerb: deleteVerb},
	}
	tenantsTests = []innerTest{
		{permissionAction: createTenants, testDescription: createDesc, policyVerb: createVerb},
		{permissionAction: readTenants, testDescription: readDesc, policyVerb: readVerb},
		{permissionAction: updateTenants, testDescription: updateDesc, policyVerb: updateVerb},
		{permissionAction: deleteTenants, testDescription: deleteDesc, policyVerb: deleteVerb},
	}
	objectsCollectionTests = []innerTest{
		{permissionAction: createObjectsCollection, testDescription: createDesc, policyVerb: createVerb},
		{permissionAction: readObjectsCollection, testDescription: readDesc, policyVerb: readVerb},
		{permissionAction: updateObjectsCollection, testDescription: updateDesc, policyVerb: updateVerb},
		{permissionAction: deleteObjectsCollection, testDescription: deleteDesc, policyVerb: deleteVerb},
	}
	objectsTenantTests = []innerTest{
		{permissionAction: createObjectsTenant, testDescription: createDesc, policyVerb: createVerb},
		{permissionAction: readObjectsTenant, testDescription: readDesc, policyVerb: readVerb},
		{permissionAction: updateObjectsTenant, testDescription: updateDesc, policyVerb: updateVerb},
		{permissionAction: deleteObjectsTenant, testDescription: deleteDesc, policyVerb: deleteVerb},
	}
)

func Test_policy(t *testing.T) {
	tests := []struct {
		name       string
		permission *models.Permission
		policy     *Policy
		tests      []innerTest
	}{
		{
			name:       "all roles",
			permission: &models.Permission{},
			policy: &Policy{
				resource: pRoles("*"),
				domain:   "roles",
			},
			tests: rolesTests,
		},
		{
			name: "a role",
			permission: &models.Permission{
				Role: String("admin"),
			},
			policy: &Policy{
				resource: pRoles("admin"),
				domain:   "roles",
			},
			tests: rolesTests,
		},
		{
			name:       "manage cluster",
			permission: &models.Permission{},
			policy: &Policy{
				resource: "cluster/*",
				domain:   "cluster",
			},
			tests: clusterTests,
		},
		{
			name:       "all collections",
			permission: &models.Permission{},
			policy: &Policy{
				resource: pCollections("*"),
				domain:   "collections",
			},
			tests: collectionsTests,
		},
		{
			name: "a collection",
			permission: &models.Permission{
				Collection: foo,
			},
			policy: &Policy{
				resource: pCollections("foo"),
				domain:   "collections",
			},
			tests: collectionsTests,
		},
		{
			name:       "all tenants in all collections",
			permission: &models.Permission{},
			policy: &Policy{
				resource: pShards("*", "*"),
				domain:   "tenants",
			},
			tests: tenantsTests,
		},
		{
			name: "all tenants in a collection",
			permission: &models.Permission{
				Collection: foo,
			},
			policy: &Policy{
				resource: pShards("foo", "*"),
				domain:   "tenants",
			},
			tests: tenantsTests,
		},
		{
			name: "a tenant in all collections",
			permission: &models.Permission{
				Tenant: bar,
			},
			policy: &Policy{
				resource: pShards("*", "bar"),
				domain:   "tenants",
			},
			tests: tenantsTests,
		},
		{
			name: "a tenant in a collection",
			permission: &models.Permission{
				Collection: foo,
				Tenant:     bar,
			},
			policy: &Policy{
				resource: pShards("foo", "bar"),
				domain:   "tenants",
			},
			tests: tenantsTests,
		},
		{
			name:       "all objects in all collections ST",
			permission: &models.Permission{},
			policy: &Policy{
				resource: pObjects("*", "*", "*"),
				domain:   "objects_collection",
			},
			tests: objectsCollectionTests,
		},
		{
			name: "all objects in a collection ST",
			permission: &models.Permission{
				Collection: foo,
			},
			policy: &Policy{
				resource: pObjects("foo", "*", "*"),
				domain:   "objects_collection",
			},
			tests: objectsCollectionTests,
		},
		{
			name: "an object in all collections ST",
			permission: &models.Permission{
				Object: baz,
			},
			policy: &Policy{
				resource: pObjects("*", "*", "baz"),
				domain:   "objects_collection",
			},
			tests: objectsCollectionTests,
		},
		{
			name: "an object in a collection ST",
			permission: &models.Permission{
				Collection: foo,
				Object:     baz,
			},
			policy: &Policy{
				resource: pObjects("foo", "*", "baz"),
				domain:   "objects_collection",
			},
			tests: objectsCollectionTests,
		},
		{
			name: "all objects in all tenants in a collection MT",
			permission: &models.Permission{
				Collection: foo,
			},
			policy: &Policy{
				resource: pObjects("foo", "*", "*"),
				domain:   "objects_tenant",
			},
			tests: objectsTenantTests,
		},
		{
			name: "all objects in a tenant in all collections MT",
			permission: &models.Permission{
				Tenant: bar,
			},
			policy: &Policy{
				resource: pObjects("*", "bar", "*"),
				domain:   "objects_tenant",
			},
			tests: objectsTenantTests,
		},
		{
			name: "all objects in a tenant in a collection MT",
			permission: &models.Permission{
				Collection: foo,
				Tenant:     bar,
			},
			policy: &Policy{
				resource: pObjects("foo", "bar", "*"),
				domain:   "objects_tenant",
			},
			tests: objectsTenantTests,
		},
		{
			name: "an object in all tenants in all collections MT",
			permission: &models.Permission{
				Object: baz,
			},
			policy: &Policy{
				resource: pObjects("*", "*", "baz"),
				domain:   "objects_tenant",
			},
			tests: objectsTenantTests,
		},
		{
			name: "an object in all tenants in a collection MT",
			permission: &models.Permission{
				Collection: foo,
				Object:     baz,
			},
			policy: &Policy{
				resource: pObjects("foo", "*", "baz"),
				domain:   "objects_tenant",
			},
			tests: objectsTenantTests,
		},
		{
			name: "an object in a tenant in all collections MT",
			permission: &models.Permission{
				Tenant: bar,
				Object: baz,
			},
			policy: &Policy{
				resource: pObjects("*", "bar", "baz"),
				domain:   "objects_tenant",
			},
			tests: objectsTenantTests,
		},
		{
			name: "an object in a tenant in a collection MT",
			permission: &models.Permission{
				Collection: foo,
				Tenant:     bar,
				Object:     baz,
			},
			policy: &Policy{
				resource: pObjects("foo", "bar", "baz"),
				domain:   "objects_tenant",
			},
			tests: objectsTenantTests,
		},
	}
	for _, tt := range tests {
		for _, ttt := range tt.tests {
			t.Run(fmt.Sprintf("%s %s", ttt.testDescription, tt.name), func(t *testing.T) {
				tt.permission.Action = String(ttt.permissionAction)
				tt.policy.verb = ttt.policyVerb
				policy, err := policy(tt.permission)
				require.Nil(t, err)
				require.Equal(t, tt.policy, policy)
			})
		}
	}
}

func Test_permission(t *testing.T) {
	tests := []struct {
		name       string
		policy     []string
		permission *models.Permission
		tests      []innerTest
	}{
		{
			name:   "all roles",
			policy: []string{"p", "roles/*", "", "roles"},
			permission: &models.Permission{
				Role: all,
			},
			tests: rolesTests,
		},
		{
			name:   "a role",
			policy: []string{"p", "roles/admin", "", "roles"},
			permission: &models.Permission{
				Role: String("admin"),
			},
			tests: rolesTests,
		},
		{
			name:       "cluster",
			policy:     []string{"p", "cluster/*", "", "cluster"},
			permission: &models.Permission{},
			tests:      clusterTests,
		},
		{
			name:   "all collections",
			policy: []string{"p", "collections/*", "", "collections"},
			permission: &models.Permission{
				Collection: all,
			},
			tests: collectionsTests,
		},
		{
			name:   "a collection",
			policy: []string{"p", "collections/foo", "", "collections"},
			permission: &models.Permission{
				Collection: foo,
			},
			tests: collectionsTests,
		},
		{
			name:   "all tenants in all collections",
			policy: []string{"p", "collections/*/shards/*", "", "tenants"},
			permission: &models.Permission{
				Collection: all,
				Tenant:     all,
			},
			tests: tenantsTests,
		},
		{
			name:   "all tenants in a collection",
			policy: []string{"p", "collections/foo/shards/*", "", "tenants"},
			permission: &models.Permission{
				Collection: foo,
				Tenant:     all,
			},
			tests: tenantsTests,
		},
		{
			name:   "a tenant in all collections",
			policy: []string{"p", "collections/*/shards/bar", "", "tenants"},
			permission: &models.Permission{
				Collection: all,
				Tenant:     bar,
			},
			tests: tenantsTests,
		},
		{
			name:   "a tenant in a collection",
			policy: []string{"p", "collections/foo/shards/bar", "", "tenants"},
			permission: &models.Permission{
				Collection: foo,
				Tenant:     bar,
			},
			tests: tenantsTests,
		},
		{
			name:   "all objects in all collections ST",
			policy: []string{"p", "collections/*/shards/*/objects/*", "", "objects_collection"},
			permission: &models.Permission{
				Collection: all,
				Tenant:     all,
				Object:     all,
			},
			tests: objectsCollectionTests,
		},
		{
			name:   "all objects in a collection ST",
			policy: []string{"p", "collections/foo/shards/*/objects/*", "", "objects_collection"},
			permission: &models.Permission{
				Collection: foo,
				Tenant:     all,
				Object:     all,
			},
			tests: objectsCollectionTests,
		},
		{
			name:   "an object in all collections ST",
			policy: []string{"p", "collections/*/shards/*/objects/baz", "", "objects_collection"},
			permission: &models.Permission{
				Collection: all,
				Tenant:     all,
				Object:     baz,
			},
			tests: objectsCollectionTests,
		},
		{
			name:   "an object in a collection ST",
			policy: []string{"p", "collections/foo/shards/*/objects/baz", "", "objects_collection"},
			permission: &models.Permission{
				Collection: foo,
				Tenant:     all,
				Object:     baz,
			},
			tests: objectsCollectionTests,
		},
		{
			name:   "all objects in all tenants in all collections MT",
			policy: []string{"p", "collections/*/shards/*/objects/*", "", "objects_tenant"},
			permission: &models.Permission{
				Collection: all,
				Tenant:     all,
				Object:     all,
			},
			tests: objectsTenantTests,
		},
		{
			name:   "all objects in all tenants in a collection MT",
			policy: []string{"p", "collections/foo/shards/*/objects/*", "", "objects_tenant"},
			permission: &models.Permission{
				Collection: foo,
				Tenant:     all,
				Object:     all,
			},
			tests: objectsTenantTests,
		},
		{
			name:   "all objects in a tenant in all collections MT",
			policy: []string{"p", "collections/*/shards/bar/objects/*", "", "objects_tenant"},
			permission: &models.Permission{
				Collection: all,
				Tenant:     bar,
				Object:     all,
			},
			tests: objectsTenantTests,
		},
		{
			name:   "all objects in a tenant in a collection MT",
			policy: []string{"p", "collections/foo/shards/bar/objects/*", "", "objects_tenant"},
			permission: &models.Permission{
				Collection: foo,
				Tenant:     bar,
				Object:     all,
			},
			tests: objectsTenantTests,
		},
		{
			name:   "an object in all tenants in all collections MT",
			policy: []string{"p", "collections/*/shards/*/objects/baz", "", "objects_tenant"},
			permission: &models.Permission{
				Collection: all,
				Tenant:     all,
				Object:     baz,
			},
			tests: objectsTenantTests,
		},
		{
			name:   "an object in all tenants in a collection MT",
			policy: []string{"p", "collections/foo/shards/*/objects/baz", "", "objects_tenant"},
			permission: &models.Permission{
				Collection: foo,
				Tenant:     all,
				Object:     baz,
			},
		},
		{
			name:   "an object in a tenant in all collections MT",
			policy: []string{"p", "collections/*/shards/bar/objects/baz", "", "objects_tenant"},
			permission: &models.Permission{
				Collection: all,
				Tenant:     bar,
				Object:     baz,
			},
			tests: objectsTenantTests,
		},
		{
			name:   "an object in a tenant in a collection MT",
			policy: []string{"p", "collections/foo/shards/bar/objects/baz", "", "objects_tenant"},
			permission: &models.Permission{
				Collection: foo,
				Tenant:     bar,
				Object:     baz,
			},
			tests: objectsTenantTests,
		},
	}
	for _, tt := range tests {
		for _, ttt := range tt.tests {
			t.Run(fmt.Sprintf("%s %s", ttt.testDescription, tt.name), func(t *testing.T) {
				tt.permission.Action = String(ttt.permissionAction)
				tt.policy[2] = ttt.policyVerb
				permission := permission(tt.policy)
				require.Equal(t, tt.permission, permission)
			})
		}
	}
}

func Test_pRoles(t *testing.T) {
	tests := []struct {
		role     string
		expected string
	}{
		{role: "", expected: "roles/.*"},
		{role: "*", expected: "roles/.*"},
		{role: "foo", expected: "roles/foo"},
	}
	for _, tt := range tests {
		name := fmt.Sprintf("role: %s", tt.role)
		t.Run(name, func(t *testing.T) {
			p := pRoles(tt.role)
			require.Equal(t, tt.expected, p)
		})
	}
}

func Test_pCollections(t *testing.T) {
	tests := []struct {
		collection string
		expected   string
	}{
		{collection: "", expected: "collections/.*/*"},
		{collection: "*", expected: "collections/.*/*"},
		{collection: "foo", expected: "collections/foo/*"},
	}
	for _, tt := range tests {
		name := fmt.Sprintf("collection: %s", tt.collection)
		t.Run(name, func(t *testing.T) {
			p := pCollections(tt.collection)
			require.Equal(t, tt.expected, p)
		})
	}
}

func Test_pShards(t *testing.T) {
	tests := []struct {
		collection string
		shard      string
		expected   string
	}{
		{collection: "", shard: "", expected: "collections/.*/shards/.*/*"},
		{collection: "*", shard: "*", expected: "collections/.*/shards/.*/*"},
		{collection: "foo", shard: "", expected: "collections/foo/shards/.*/*"},
		{collection: "foo", shard: "*", expected: "collections/foo/shards/.*/*"},
		{collection: "", shard: "bar", expected: "collections/.*/shards/bar/*"},
		{collection: "*", shard: "bar", expected: "collections/.*/shards/bar/*"},
		{collection: "foo", shard: "bar", expected: "collections/foo/shards/bar/*"},
	}
	for _, tt := range tests {
		name := fmt.Sprintf("collection: %s; shard: %s", tt.collection, tt.shard)
		t.Run(name, func(t *testing.T) {
			p := pShards(tt.collection, tt.shard)
			require.Equal(t, tt.expected, p)
		})
	}
}

func Test_pObjects(t *testing.T) {
	tests := []struct {
		collection string
		shard      string
		object     string
		expected   string
	}{
		{collection: "", shard: "", object: "", expected: "collections/.*/shards/.*/objects/.*"},
		{collection: "*", shard: "*", object: "*", expected: "collections/.*/shards/.*/objects/.*"},
		{collection: "foo", shard: "", object: "", expected: "collections/foo/shards/.*/objects/.*"},
		{collection: "foo", shard: "*", object: "*", expected: "collections/foo/shards/.*/objects/.*"},
		{collection: "", shard: "bar", object: "", expected: "collections/.*/shards/bar/objects/.*"},
		{collection: "*", shard: "bar", object: "*", expected: "collections/.*/shards/bar/objects/.*"},
		{collection: "", shard: "", object: "baz", expected: "collections/.*/shards/.*/objects/baz"},
		{collection: "*", shard: "*", object: "baz", expected: "collections/.*/shards/.*/objects/baz"},
		{collection: "foo", shard: "bar", object: "", expected: "collections/foo/shards/bar/objects/.*"},
		{collection: "foo", shard: "bar", object: "*", expected: "collections/foo/shards/bar/objects/.*"},
		{collection: "foo", shard: "", object: "baz", expected: "collections/foo/shards/.*/objects/baz"},
		{collection: "foo", shard: "*", object: "baz", expected: "collections/foo/shards/.*/objects/baz"},
		{collection: "", shard: "bar", object: "baz", expected: "collections/.*/shards/bar/objects/baz"},
		{collection: "*", shard: "bar", object: "baz", expected: "collections/.*/shards/bar/objects/baz"},
		{collection: "foo", shard: "bar", object: "baz", expected: "collections/foo/shards/bar/objects/baz"},
	}
	for _, tt := range tests {
		name := fmt.Sprintf("collection: %s; shard: %s; object: %s", tt.collection, tt.shard, tt.object)
		t.Run(name, func(t *testing.T) {
			p := pObjects(tt.collection, tt.shard, tt.object)
			require.Equal(t, tt.expected, p)
		})
	}
}

func Test_fromCasbinResource(t *testing.T) {
	tests := []struct {
		resource string
		expected string
	}{
		{resource: "collections/.*/shards/.*/objects/.*", expected: "collections/*/shards/*/objects/*"},
	}
	for _, tt := range tests {
		name := fmt.Sprintf("resource: %s", tt.resource)
		t.Run(name, func(t *testing.T) {
			p := fromCasbinResource(tt.resource)
			require.Equal(t, tt.expected, p)
		})
	}
}
