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

package conv

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
	foo = authorization.String("foo")
	bar = authorization.String("bar")
	baz = authorization.String("baz")

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

	usersTests = []innerTest{
		{permissionAction: authorization.ManageUsers, testDescription: manageDesc, policyVerb: manageVerb},
	}
	rolesTests = []innerTest{
		{permissionAction: authorization.ManageRoles, testDescription: manageDesc, policyVerb: manageVerb},
	}
	clusterTests = []innerTest{
		{permissionAction: authorization.ManageCluster, testDescription: manageDesc, policyVerb: manageVerb},
	}
	collectionsTests = []innerTest{
		{permissionAction: authorization.CreateCollections, testDescription: createDesc, policyVerb: createVerb},
		{permissionAction: authorization.ReadCollections, testDescription: readDesc, policyVerb: readVerb},
		{permissionAction: authorization.UpdateCollections, testDescription: updateDesc, policyVerb: updateVerb},
		{permissionAction: authorization.DeleteCollections, testDescription: deleteDesc, policyVerb: deleteVerb},
	}
	tenantsTests = []innerTest{
		{permissionAction: authorization.CreateTenants, testDescription: createDesc, policyVerb: createVerb},
		{permissionAction: authorization.ReadTenants, testDescription: readDesc, policyVerb: readVerb},
		{permissionAction: authorization.UpdateTenants, testDescription: updateDesc, policyVerb: updateVerb},
		{permissionAction: authorization.DeleteTenants, testDescription: deleteDesc, policyVerb: deleteVerb},
	}
	objectsCollectionTests = []innerTest{
		{permissionAction: authorization.CreateObjectsCollection, testDescription: createDesc, policyVerb: createVerb},
		{permissionAction: authorization.ReadObjectsCollection, testDescription: readDesc, policyVerb: readVerb},
		{permissionAction: authorization.UpdateObjectsCollection, testDescription: updateDesc, policyVerb: updateVerb},
		{permissionAction: authorization.DeleteObjectsCollection, testDescription: deleteDesc, policyVerb: deleteVerb},
	}
	objectsTenantTests = []innerTest{
		{permissionAction: authorization.CreateObjectsTenant, testDescription: createDesc, policyVerb: createVerb},
		{permissionAction: authorization.ReadObjectsTenant, testDescription: readDesc, policyVerb: readVerb},
		{permissionAction: authorization.UpdateObjectsTenant, testDescription: updateDesc, policyVerb: updateVerb},
		{permissionAction: authorization.DeleteObjectsTenant, testDescription: deleteDesc, policyVerb: deleteVerb},
	}
)

func Test_policy(t *testing.T) {
	tests := []struct {
		name       string
		permission *models.Permission
		policy     *authorization.Policy
		tests      []innerTest
	}{
		{
			name:       "all users",
			permission: &models.Permission{},
			policy: &authorization.Policy{
				Resource: CasbinUsers("*"),
				Domain:   "users",
			},
			tests: usersTests,
		},
		{
			name: "a user",
			permission: &models.Permission{
				User: authorization.String("user1"),
			},
			policy: &authorization.Policy{
				Resource: CasbinUsers("user1"),
				Domain:   "users",
			},
			tests: usersTests,
		},
		{
			name:       "all roles",
			permission: &models.Permission{},
			policy: &authorization.Policy{
				Resource: CasbinRoles("*"),
				Domain:   "roles",
			},
			tests: rolesTests,
		},
		{
			name: "a role",
			permission: &models.Permission{
				Role: authorization.String("admin"),
			},
			policy: &authorization.Policy{
				Resource: CasbinRoles("admin"),
				Domain:   "roles",
			},
			tests: rolesTests,
		},
		{
			name:       "manage cluster",
			permission: &models.Permission{},
			policy: &authorization.Policy{
				Resource: "cluster/*",
				Domain:   "cluster",
			},
			tests: clusterTests,
		},
		{
			name:       "all collections",
			permission: &models.Permission{},
			policy: &authorization.Policy{
				Resource: CasbinCollections("*"),
				Domain:   "meta_collections",
			},
			tests: collectionsTests,
		},
		{
			name: "a collection",
			permission: &models.Permission{
				Collection: foo,
			},
			policy: &authorization.Policy{
				Resource: CasbinCollections("foo"),
				Domain:   "meta_collections",
			},
			tests: collectionsTests,
		},
		{
			name:       "all tenants in all collections",
			permission: &models.Permission{},
			policy: &authorization.Policy{
				Resource: CasbinShards("*", "*"),
				Domain:   "meta_tenants",
			},
			tests: tenantsTests,
		},
		{
			name: "all tenants in a collection",
			permission: &models.Permission{
				Collection: foo,
			},
			policy: &authorization.Policy{
				Resource: CasbinShards("foo", "*"),
				Domain:   "meta_tenants",
			},
			tests: tenantsTests,
		},
		{
			name: "a tenant in all collections",
			permission: &models.Permission{
				Tenant: bar,
			},
			policy: &authorization.Policy{
				Resource: CasbinShards("*", "bar"),
				Domain:   "meta_tenants",
			},
			tests: tenantsTests,
		},
		{
			name: "a tenant in a collection",
			permission: &models.Permission{
				Collection: foo,
				Tenant:     bar,
			},
			policy: &authorization.Policy{
				Resource: CasbinShards("foo", "bar"),
				Domain:   "meta_tenants",
			},
			tests: tenantsTests,
		},
		{
			name:       "all objects in all collections ST",
			permission: &models.Permission{},
			policy: &authorization.Policy{
				Resource: CasbinObjects("*", "*", "*"),
				Domain:   "data_collection_objects",
			},
			tests: objectsCollectionTests,
		},
		{
			name: "all objects in a collection ST",
			permission: &models.Permission{
				Collection: foo,
			},
			policy: &authorization.Policy{
				Resource: CasbinObjects("foo", "*", "*"),
				Domain:   "data_collection_objects",
			},
			tests: objectsCollectionTests,
		},
		{
			name: "an object in all collections ST",
			permission: &models.Permission{
				Object: baz,
			},
			policy: &authorization.Policy{
				Resource: CasbinObjects("*", "*", "baz"),
				Domain:   "data_collection_objects",
			},
			tests: objectsCollectionTests,
		},
		{
			name: "an object in a collection ST",
			permission: &models.Permission{
				Collection: foo,
				Object:     baz,
			},
			policy: &authorization.Policy{
				Resource: CasbinObjects("foo", "*", "baz"),
				Domain:   "data_collection_objects",
			},
			tests: objectsCollectionTests,
		},
		{
			name: "all objects in all tenants in a collection MT",
			permission: &models.Permission{
				Collection: foo,
			},
			policy: &authorization.Policy{
				Resource: CasbinObjects("foo", "*", "*"),
				Domain:   "data_tenant_objects",
			},
			tests: objectsTenantTests,
		},
		{
			name: "all objects in a tenant in all collections MT",
			permission: &models.Permission{
				Tenant: bar,
			},
			policy: &authorization.Policy{
				Resource: CasbinObjects("*", "bar", "*"),
				Domain:   "data_tenant_objects",
			},
			tests: objectsTenantTests,
		},
		{
			name: "all objects in a tenant in a collection MT",
			permission: &models.Permission{
				Collection: foo,
				Tenant:     bar,
			},
			policy: &authorization.Policy{
				Resource: CasbinObjects("foo", "bar", "*"),
				Domain:   "data_tenant_objects",
			},
			tests: objectsTenantTests,
		},
		{
			name: "an object in all tenants in all collections MT",
			permission: &models.Permission{
				Object: baz,
			},
			policy: &authorization.Policy{
				Resource: CasbinObjects("*", "*", "baz"),
				Domain:   "data_tenant_objects",
			},
			tests: objectsTenantTests,
		},
		{
			name: "an object in all tenants in a collection MT",
			permission: &models.Permission{
				Collection: foo,
				Object:     baz,
			},
			policy: &authorization.Policy{
				Resource: CasbinObjects("foo", "*", "baz"),
				Domain:   "data_tenant_objects",
			},
			tests: objectsTenantTests,
		},
		{
			name: "an object in a tenant in all collections MT",
			permission: &models.Permission{
				Tenant: bar,
				Object: baz,
			},
			policy: &authorization.Policy{
				Resource: CasbinObjects("*", "bar", "baz"),
				Domain:   "data_tenant_objects",
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
			policy: &authorization.Policy{
				Resource: CasbinObjects("foo", "bar", "baz"),
				Domain:   "data_tenant_objects",
			},
			tests: objectsTenantTests,
		},
	}
	for _, tt := range tests {
		for _, ttt := range tt.tests {
			t.Run(fmt.Sprintf("%s %s", ttt.testDescription, tt.name), func(t *testing.T) {
				tt.permission.Action = authorization.String(ttt.permissionAction)
				tt.policy.Verb = ttt.policyVerb
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
			name:   "all users",
			policy: []string{"p", "users/*", "", "users"},
			permission: &models.Permission{
				User: authorization.All,
			},
			tests: usersTests,
		},
		{
			name:   "a role",
			policy: []string{"p", "users/user1", "", "users"},
			permission: &models.Permission{
				User: authorization.String("user1"),
			},
			tests: usersTests,
		},
		{
			name:   "all roles",
			policy: []string{"p", "roles/*", "", "roles"},
			permission: &models.Permission{
				Role: authorization.All,
			},
			tests: rolesTests,
		},
		{
			name:   "a role",
			policy: []string{"p", "roles/admin", "", "roles"},
			permission: &models.Permission{
				Role: authorization.String("admin"),
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
			policy: []string{"p", "meta/collections/*", "", "meta_collections"},
			permission: &models.Permission{
				Collection: authorization.All,
			},
			tests: collectionsTests,
		},
		{
			name:   "a collection",
			policy: []string{"p", "meta/collections/foo", "", "meta_collections"},
			permission: &models.Permission{
				Collection: foo,
			},
			tests: collectionsTests,
		},
		{
			name:   "all tenants in all collections",
			policy: []string{"p", "meta/collections/*/shards/*", "", "meta_tenants"},
			permission: &models.Permission{
				Collection: authorization.All,
				Tenant:     authorization.All,
			},
			tests: tenantsTests,
		},
		{
			name:   "all tenants in a collection",
			policy: []string{"p", "meta/collections/foo/shards/*", "", "meta_tenants"},
			permission: &models.Permission{
				Collection: foo,
				Tenant:     authorization.All,
			},
			tests: tenantsTests,
		},
		{
			name:   "a tenant in all collections",
			policy: []string{"p", "meta/collections/*/shards/bar", "", "meta_tenants"},
			permission: &models.Permission{
				Collection: authorization.All,
				Tenant:     bar,
			},
			tests: tenantsTests,
		},
		{
			name:   "a tenant in a collection",
			policy: []string{"p", "meta/collections/foo/shards/bar", "", "meta_tenants"},
			permission: &models.Permission{
				Collection: foo,
				Tenant:     bar,
			},
			tests: tenantsTests,
		},
		{
			name:   "all objects in all collections ST",
			policy: []string{"p", "data/collections/*/shards/*/objects/*", "", "data_collection_objects"},
			permission: &models.Permission{
				Collection: authorization.All,
				Tenant:     authorization.All,
				Object:     authorization.All,
			},
			tests: objectsCollectionTests,
		},
		{
			name:   "all objects in a collection ST",
			policy: []string{"p", "data/collections/foo/shards/*/objects/*", "", "data_collection_objects"},
			permission: &models.Permission{
				Collection: foo,
				Tenant:     authorization.All,
				Object:     authorization.All,
			},
			tests: objectsCollectionTests,
		},
		{
			name:   "an object in all collections ST",
			policy: []string{"p", "data/collections/*/shards/*/objects/baz", "", "data_collection_objects"},
			permission: &models.Permission{
				Collection: authorization.All,
				Tenant:     authorization.All,
				Object:     baz,
			},
			tests: objectsCollectionTests,
		},
		{
			name:   "an object in a collection ST",
			policy: []string{"p", "data/collections/foo/shards/*/objects/baz", "", "data_collection_objects"},
			permission: &models.Permission{
				Collection: foo,
				Tenant:     authorization.All,
				Object:     baz,
			},
			tests: objectsCollectionTests,
		},
		{
			name:   "all objects in all tenants in all collections MT",
			policy: []string{"p", "data/collections/*/shards/*/objects/*", "", "data_tenant_objects"},
			permission: &models.Permission{
				Collection: authorization.All,
				Tenant:     authorization.All,
				Object:     authorization.All,
			},
			tests: objectsTenantTests,
		},
		{
			name:   "all objects in all tenants in a collection MT",
			policy: []string{"p", "data/collections/foo/shards/*/objects/*", "", "data_tenant_objects"},
			permission: &models.Permission{
				Collection: foo,
				Tenant:     authorization.All,
				Object:     authorization.All,
			},
			tests: objectsTenantTests,
		},
		{
			name:   "all objects in a tenant in all collections MT",
			policy: []string{"p", "data/collections/*/shards/bar/objects/*", "", "data_tenant_objects"},
			permission: &models.Permission{
				Collection: authorization.All,
				Tenant:     bar,
				Object:     authorization.All,
			},
			tests: objectsTenantTests,
		},
		{
			name:   "all objects in a tenant in a collection MT",
			policy: []string{"p", "data/collections/foo/shards/bar/objects/*", "", "data_tenant_objects"},
			permission: &models.Permission{
				Collection: foo,
				Tenant:     bar,
				Object:     authorization.All,
			},
			tests: objectsTenantTests,
		},
		{
			name:   "an object in all tenants in all collections MT",
			policy: []string{"p", "data/collections/*/shards/*/objects/baz", "", "data_tenant_objects"},
			permission: &models.Permission{
				Collection: authorization.All,
				Tenant:     authorization.All,
				Object:     baz,
			},
			tests: objectsTenantTests,
		},
		{
			name:   "an object in all tenants in a collection MT",
			policy: []string{"p", "data/collections/foo/shards/*/objects/baz", "", "data_tenant_objects"},
			permission: &models.Permission{
				Collection: foo,
				Tenant:     authorization.All,
				Object:     baz,
			},
		},
		{
			name:   "an object in a tenant in all collections MT",
			policy: []string{"p", "data/collections/*/shards/bar/objects/baz", "", "data_tenant_objects"},
			permission: &models.Permission{
				Collection: authorization.All,
				Tenant:     bar,
				Object:     baz,
			},
			tests: objectsTenantTests,
		},
		{
			name:   "an object in a tenant in a collection MT",
			policy: []string{"p", "data/collections/foo/shards/bar/objects/baz", "", "data_tenant_objects"},
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
				tt.permission.Action = authorization.String(ttt.permissionAction)
				tt.policy[2] = ttt.policyVerb
				permission := permission(tt.policy)
				require.Equal(t, tt.permission, permission)
			})
		}
	}
}

func Test_pUsers(t *testing.T) {
	tests := []struct {
		user     string
		expected string
	}{
		{user: "", expected: "users/.*"},
		{user: "*", expected: "users/.*"},
		{user: "foo", expected: "users/foo"},
	}
	for _, tt := range tests {
		name := fmt.Sprintf("user: %s", tt.user)
		t.Run(name, func(t *testing.T) {
			p := CasbinUsers(tt.user)
			require.Equal(t, tt.expected, p)
		})
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
			p := CasbinRoles(tt.role)
			require.Equal(t, tt.expected, p)
		})
	}
}

func Test_pCollections(t *testing.T) {
	tests := []struct {
		collection string
		expected   string
	}{
		{collection: "", expected: "meta/collections/.*/*"},
		{collection: "*", expected: "meta/collections/.*/*"},
		{collection: "foo", expected: "meta/collections/foo/*"},
	}
	for _, tt := range tests {
		name := fmt.Sprintf("collection: %s", tt.collection)
		t.Run(name, func(t *testing.T) {
			p := CasbinCollections(tt.collection)
			require.Equal(t, tt.expected, p)
		})
	}
}

func Test_CasbinShards(t *testing.T) {
	tests := []struct {
		collection string
		shard      string
		expected   string
	}{
		{collection: "", shard: "", expected: "meta/collections/.*/shards/.*/*"},
		{collection: "*", shard: "*", expected: "meta/collections/.*/shards/.*/*"},
		{collection: "foo", shard: "", expected: "meta/collections/foo/shards/.*/*"},
		{collection: "foo", shard: "*", expected: "meta/collections/foo/shards/.*/*"},
		{collection: "", shard: "bar", expected: "meta/collections/.*/shards/bar/*"},
		{collection: "*", shard: "bar", expected: "meta/collections/.*/shards/bar/*"},
		{collection: "foo", shard: "bar", expected: "meta/collections/foo/shards/bar/*"},
	}
	for _, tt := range tests {
		name := fmt.Sprintf("collection: %s; shard: %s", tt.collection, tt.shard)
		t.Run(name, func(t *testing.T) {
			p := CasbinShards(tt.collection, tt.shard)
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
		{collection: "", shard: "", object: "", expected: "data/collections/.*/shards/.*/objects/.*"},
		{collection: "*", shard: "*", object: "*", expected: "data/collections/.*/shards/.*/objects/.*"},
		{collection: "foo", shard: "", object: "", expected: "data/collections/foo/shards/.*/objects/.*"},
		{collection: "foo", shard: "*", object: "*", expected: "data/collections/foo/shards/.*/objects/.*"},
		{collection: "", shard: "bar", object: "", expected: "data/collections/.*/shards/bar/objects/.*"},
		{collection: "*", shard: "bar", object: "*", expected: "data/collections/.*/shards/bar/objects/.*"},
		{collection: "", shard: "", object: "baz", expected: "data/collections/.*/shards/.*/objects/baz"},
		{collection: "*", shard: "*", object: "baz", expected: "data/collections/.*/shards/.*/objects/baz"},
		{collection: "foo", shard: "bar", object: "", expected: "data/collections/foo/shards/bar/objects/.*"},
		{collection: "foo", shard: "bar", object: "*", expected: "data/collections/foo/shards/bar/objects/.*"},
		{collection: "foo", shard: "", object: "baz", expected: "data/collections/foo/shards/.*/objects/baz"},
		{collection: "foo", shard: "*", object: "baz", expected: "data/collections/foo/shards/.*/objects/baz"},
		{collection: "", shard: "bar", object: "baz", expected: "data/collections/.*/shards/bar/objects/baz"},
		{collection: "*", shard: "bar", object: "baz", expected: "data/collections/.*/shards/bar/objects/baz"},
		{collection: "foo", shard: "bar", object: "baz", expected: "data/collections/foo/shards/bar/objects/baz"},
	}
	for _, tt := range tests {
		name := fmt.Sprintf("collection: %s; shard: %s; object: %s", tt.collection, tt.shard, tt.object)
		t.Run(name, func(t *testing.T) {
			p := CasbinObjects(tt.collection, tt.shard, tt.object)
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
		name := fmt.Sprintf("Resource: %s", tt.resource)
		t.Run(name, func(t *testing.T) {
			p := fromCasbinResource(tt.resource)
			require.Equal(t, tt.expected, p)
		})
	}
}
