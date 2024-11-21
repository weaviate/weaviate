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
		{permissionAction: authorization.CreateSchema, testDescription: createDesc, policyVerb: createVerb},
		{permissionAction: authorization.ReadSchema, testDescription: readDesc, policyVerb: readVerb},
		{permissionAction: authorization.UpdateSchema, testDescription: updateDesc, policyVerb: updateVerb},
		{permissionAction: authorization.DeleteSchema, testDescription: deleteDesc, policyVerb: deleteVerb},
	}
	tenantsTests = []innerTest{
		{permissionAction: authorization.CreateSchema, testDescription: createDesc, policyVerb: createVerb},
		{permissionAction: authorization.ReadSchema, testDescription: readDesc, policyVerb: readVerb},
		{permissionAction: authorization.UpdateSchema, testDescription: updateDesc, policyVerb: updateVerb},
		{permissionAction: authorization.DeleteSchema, testDescription: deleteDesc, policyVerb: deleteVerb},
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
				Domain:   authorization.UsersDomain,
				Verb:     *authorization.All,
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
				Domain:   authorization.UsersDomain,
			},
			tests: usersTests,
		},
		{
			name:       "all roles",
			permission: &models.Permission{},
			policy: &authorization.Policy{
				Resource: CasbinRoles("*"),
				Domain:   authorization.RolesDomain,
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
				Domain:   authorization.RolesDomain,
			},
			tests: rolesTests,
		},
		{
			name:       "manage cluster",
			permission: &models.Permission{},
			policy: &authorization.Policy{
				Resource: CasbinClusters(),
				Domain:   authorization.ClusterDomain,
			},
			tests: clusterTests,
		},
		{
			name:       "all collections",
			permission: &models.Permission{},
			policy: &authorization.Policy{
				Resource: CasbinSchema("*", ""),
				Domain:   authorization.SchemaDomain,
			},
			tests: collectionsTests,
		},
		{
			name: "a collection",
			permission: &models.Permission{
				Collection: foo,
			},
			policy: &authorization.Policy{
				Resource: CasbinSchema("foo", ""),
				Domain:   authorization.SchemaDomain,
			},
			tests: collectionsTests,
		},
		{
			name:       "all tenants in all collections",
			permission: &models.Permission{},
			policy: &authorization.Policy{
				Resource: CasbinSchema("*", "*"),
				Domain:   authorization.SchemaDomain,
			},
			tests: tenantsTests,
		},
		{
			name: "all tenants in a collection",
			permission: &models.Permission{
				Collection: foo,
			},
			policy: &authorization.Policy{
				Resource: CasbinSchema("foo", "*"),
				Domain:   authorization.SchemaDomain,
			},
			tests: tenantsTests,
		},
		{
			name: "a tenant in all collections",
			permission: &models.Permission{
				Tenant: bar,
			},
			policy: &authorization.Policy{
				Resource: CasbinSchema("*", "bar"),
				Domain:   authorization.SchemaDomain,
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
				Resource: CasbinSchema("foo", "bar"),
				Domain:   authorization.SchemaDomain,
			},
			tests: tenantsTests,
		},
		{
			name:       "all objects in all collections ST",
			permission: &models.Permission{},
			policy: &authorization.Policy{
				Resource: CasbinObjects("*", "*", "*"),
				Domain:   authorization.ObjectsCollectionsDomain,
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
				Domain:   authorization.ObjectsCollectionsDomain,
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
				Domain:   authorization.ObjectsCollectionsDomain,
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
				Domain:   authorization.ObjectsCollectionsDomain,
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
				Domain:   authorization.ObjectsTenantsDomain,
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
				Domain:   authorization.ObjectsTenantsDomain,
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
				Domain:   authorization.ObjectsTenantsDomain,
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
				Domain:   authorization.ObjectsTenantsDomain,
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
				Domain:   authorization.ObjectsTenantsDomain,
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
				Domain:   authorization.ObjectsTenantsDomain,
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
				Domain:   authorization.ObjectsTenantsDomain,
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
			policy: []string{"p", "meta/users/*", "", "users"},
			permission: &models.Permission{
				User: authorization.All,
			},
			tests: usersTests,
		},
		{
			name:   "a role",
			policy: []string{"p", "meta/users/user1", "", "users"},
			permission: &models.Permission{
				User: authorization.String("user1"),
			},
			tests: usersTests,
		},
		{
			name:   "all roles",
			policy: []string{"p", "meta/roles/*", "", "roles"},
			permission: &models.Permission{
				Role: authorization.All,
			},
			tests: rolesTests,
		},
		{
			name:   "a role",
			policy: []string{"p", "meta/roles/admin", "", "roles"},
			permission: &models.Permission{
				Role: authorization.String("admin"),
			},
			tests: rolesTests,
		},
		{
			name:       "cluster",
			policy:     []string{"p", "meta/cluster/*", "", "cluster"},
			permission: &models.Permission{},
			tests:      clusterTests,
		},
		{
			name:   "all collections",
			policy: []string{"p", "meta/collections/*/shards/*", "", authorization.SchemaDomain},
			permission: &models.Permission{
				Collection: authorization.All,
				Tenant:     authorization.All,
			},
			tests: collectionsTests,
		},
		{
			name:   "a collection",
			policy: []string{"p", "meta/collections/foo/shards/*", "", authorization.SchemaDomain},
			permission: &models.Permission{
				Collection: foo,
				Tenant:     authorization.All,
			},
			tests: collectionsTests,
		},
		{
			name:   "all tenants in all collections",
			policy: []string{"p", "meta/collections/*/shards/*", "", authorization.SchemaDomain},
			permission: &models.Permission{
				Collection: authorization.All,
				Tenant:     authorization.All,
			},
			tests: tenantsTests,
		},
		{
			name:   "all tenants in a collection",
			policy: []string{"p", "meta/collections/foo/shards/*", "", authorization.SchemaDomain},
			permission: &models.Permission{
				Collection: foo,
				Tenant:     authorization.All,
			},
			tests: tenantsTests,
		},
		{
			name:   "a tenant in all collections",
			policy: []string{"p", "meta/collections/*/shards/bar", "", authorization.SchemaDomain},
			permission: &models.Permission{
				Collection: authorization.All,
				Tenant:     bar,
			},
			tests: tenantsTests,
		},
		{
			name:   "a tenant in a collection",
			policy: []string{"p", "meta/collections/foo/shards/bar", "", authorization.SchemaDomain},
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
				permission, err := permission(tt.policy)
				require.Equal(t, tt.permission, permission)
				require.Nil(t, err)
			})
		}
	}
}

func Test_pUsers(t *testing.T) {
	tests := []struct {
		user     string
		expected string
	}{
		{user: "", expected: "meta/users/.*"},
		{user: "*", expected: "meta/users/.*"},
		{user: "foo", expected: "meta/users/foo"},
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
		{role: "", expected: "meta/roles/.*"},
		{role: "*", expected: "meta/roles/.*"},
		{role: "foo", expected: "meta/roles/foo"},
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
		{collection: "", expected: "meta/collections/.*/shards/.*"},
		{collection: "*", expected: "meta/collections/.*/shards/.*"},
		{collection: "foo", expected: "meta/collections/foo/shards/.*"},
	}
	for _, tt := range tests {
		name := fmt.Sprintf("collection: %s", tt.collection)
		t.Run(name, func(t *testing.T) {
			p := CasbinSchema(tt.collection, "")
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
		{collection: "", shard: "", expected: "meta/collections/.*/shards/.*"},
		{collection: "*", shard: "*", expected: "meta/collections/.*/shards/.*"},
		{collection: "foo", shard: "", expected: "meta/collections/foo/shards/.*"},
		{collection: "foo", shard: "*", expected: "meta/collections/foo/shards/.*"},
		{collection: "", shard: "bar", expected: "meta/collections/.*/shards/bar"},
		{collection: "*", shard: "bar", expected: "meta/collections/.*/shards/bar"},
		{collection: "foo", shard: "bar", expected: "meta/collections/foo/shards/bar"},
	}
	for _, tt := range tests {
		name := fmt.Sprintf("collection: %s; shard: %s", tt.collection, tt.shard)
		t.Run(name, func(t *testing.T) {
			p := CasbinSchema(tt.collection, tt.shard)
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

func TestValidResource(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{
			name:     "valid resource - users",
			input:    "meta/users/testUser",
			expected: true,
		},
		{
			name:     "valid resource - roles",
			input:    "meta/roles/testRole",
			expected: true,
		},
		{
			name:     "valid resource - collections",
			input:    "meta/collections/testCollection",
			expected: true,
		},
		{
			name:     "valid resource - objects",
			input:    "data/collections/testCollection/shards/testShard/objects/testObject",
			expected: true,
		},
		{
			name:     "invalid resource",
			input:    "invalid/resource",
			expected: false,
		},
		{
			name:     "invalid resource",
			input:    "some resource",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := validResource(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestValidVerb(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{
			name:     "valid verb - create",
			input:    "C",
			expected: true,
		},
		{
			name:     "valid verb - read",
			input:    "R",
			expected: true,
		},
		{
			name:     "valid verb - update",
			input:    "U",
			expected: true,
		},
		{
			name:     "valid verb - delete",
			input:    "D",
			expected: true,
		},
		{
			name:     "All",
			input:    "CRUD",
			expected: true,
		},
		{
			name:     "invalid verb",
			input:    "X",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := validVerb(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}
