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

package authorization

import (
	"fmt"
	"strings"
	"testing"

	"github.com/weaviate/weaviate/usecases/auth/authentication"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
)

func TestUsers(t *testing.T) {
	tests := []struct {
		name     string
		users    []string
		expected []string
	}{
		{"No users", []string{}, []string{fmt.Sprintf("%s/*", UsersDomain)}},
		{"Single user", []string{"user1"}, []string{fmt.Sprintf("%s/user1", UsersDomain)}},
		{"Multiple users", []string{"user1", "user2"}, []string{fmt.Sprintf("%s/user1", UsersDomain), fmt.Sprintf("%s/user2", UsersDomain)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Users(tt.users...)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGroups(t *testing.T) {
	tests := []struct {
		name     string
		groups   []string
		expected []string
	}{
		{"No groups", []string{}, []string{fmt.Sprintf("%s/%s/*", GroupsDomain, authentication.AuthTypeOIDC)}},
		{"Single group", []string{"group1"}, []string{fmt.Sprintf("%s/%s/group1", GroupsDomain, authentication.AuthTypeOIDC)}},
		{"Multiple groups", []string{"group1", "group2"}, []string{fmt.Sprintf("%s/%s/group1", GroupsDomain, authentication.AuthTypeOIDC), fmt.Sprintf("%s/%s/group2", GroupsDomain, authentication.AuthTypeOIDC)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Groups(authentication.AuthTypeOIDC, tt.groups...)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestRoles(t *testing.T) {
	tests := []struct {
		name     string
		roles    []string
		expected []string
	}{
		{"No roles", []string{}, []string{fmt.Sprintf("%s/*", RolesDomain)}},
		{"Single role", []string{"admin"}, []string{fmt.Sprintf("%s/admin", RolesDomain)}},
		{"Multiple roles", []string{"admin", "user"}, []string{fmt.Sprintf("%s/admin", RolesDomain), fmt.Sprintf("%s/user", RolesDomain)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Roles(tt.roles...)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCluster(t *testing.T) {
	expected := "cluster/*"
	result := Cluster()
	assert.Equal(t, expected, result)
}

func TestNodes(t *testing.T) {
	tests := []struct {
		name      string
		verbosity string
		classes   []string
		expected  []string
	}{
		{"Empty verbosity", "", []string{}, []string{fmt.Sprintf("%s/verbosity/minimal", NodesDomain)}},
		{"Minimal verbosity", "minimal", []string{}, []string{fmt.Sprintf("%s/verbosity/minimal", NodesDomain)}},
		{"Minimal verbosity with classes", "minimal", []string{"class1"}, []string{fmt.Sprintf("%s/verbosity/minimal", NodesDomain)}},
		{"Verbose verbosity with no classes", "verbose", []string{}, []string{fmt.Sprintf("%s/verbosity/verbose/collections/*", NodesDomain)}},
		{"Verbose verbosity with classes", "verbose", []string{"class1", "class2"}, []string{fmt.Sprintf("%s/verbosity/verbose/collections/Class1", NodesDomain), fmt.Sprintf("%s/verbosity/verbose/collections/Class2", NodesDomain)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Nodes(tt.verbosity, tt.classes...)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBackups(t *testing.T) {
	tests := []struct {
		name     string
		backend  string
		expected []string
	}{
		{"No collection", "", []string{fmt.Sprintf("%s/collections/*", BackupsDomain)}},
		{"Collection", "class1", []string{fmt.Sprintf("%s/collections/Class1", BackupsDomain)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Backups(tt.backend)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCollections(t *testing.T) {
	tests := []struct {
		name     string
		classes  []string
		expected []string
	}{
		{"No classes", []string{}, []string{fmt.Sprintf("%s/collections/*/shards/#", SchemaDomain)}},
		{"Single empty class", []string{""}, []string{fmt.Sprintf("%s/collections/*/shards/#", SchemaDomain)}},
		{"Single class", []string{"class1"}, []string{fmt.Sprintf("%s/collections/Class1/shards/#", SchemaDomain)}},
		{"Multiple classes", []string{"class1", "class2"}, []string{fmt.Sprintf("%s/collections/Class1/shards/#", SchemaDomain), fmt.Sprintf("%s/collections/Class2/shards/#", SchemaDomain)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CollectionsMetadata(tt.classes...)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestShards(t *testing.T) {
	tests := []struct {
		name     string
		class    string
		shards   []string
		expected []string
	}{
		{"No class, no shards", "", []string{}, []string{fmt.Sprintf("%s/collections/*/shards/*", SchemaDomain)}},
		{"Class, no shards", "class1", []string{}, []string{fmt.Sprintf("%s/collections/Class1/shards/*", SchemaDomain)}},
		{"No class, single shard", "", []string{"shard1"}, []string{fmt.Sprintf("%s/collections/*/shards/shard1", SchemaDomain)}},
		{"Class, single shard", "class1", []string{"shard1"}, []string{fmt.Sprintf("%s/collections/Class1/shards/shard1", SchemaDomain)}},
		{"Class, multiple shards", "class1", []string{"shard1", "shard2"}, []string{fmt.Sprintf("%s/collections/Class1/shards/shard1", SchemaDomain), fmt.Sprintf("%s/collections/Class1/shards/shard2", SchemaDomain)}},
		{"Class, empty shard", "class1", []string{"shard1", ""}, []string{fmt.Sprintf("%s/collections/Class1/shards/shard1", SchemaDomain), fmt.Sprintf("%s/collections/Class1/shards/*", SchemaDomain)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ShardsMetadata(tt.class, tt.shards...)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestObjects(t *testing.T) {
	tests := []struct {
		name     string
		class    string
		shard    string
		expected string
	}{
		{"No class, no shard", "", "", fmt.Sprintf("%s/collections/*/shards/*/objects/*", DataDomain)},
		{"Class, no shard", "class1", "", fmt.Sprintf("%s/collections/Class1/shards/*/objects/*", DataDomain)},
		{"No class, shard", "", "shard1", fmt.Sprintf("%s/collections/*/shards/shard1/objects/*", DataDomain)},
		{"Class, shard", "class1", "shard1", fmt.Sprintf("%s/collections/Class1/shards/shard1/objects/*", DataDomain)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Objects(tt.class, tt.shard)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestTenants(t *testing.T) {
	tests := []struct {
		name     string
		class    string
		shards   []string
		expected []string
	}{
		{"No class, no tenant", "", []string{}, []string{fmt.Sprintf("%s/collections/*/shards/*", SchemaDomain)}},
		{"Class, no tenant", "class1", []string{}, []string{fmt.Sprintf("%s/collections/Class1/shards/*", SchemaDomain)}},
		{"No class, single tenant", "", []string{"tenant1"}, []string{fmt.Sprintf("%s/collections/*/shards/tenant1", SchemaDomain)}},
		{"Class, single tenants", "class1", []string{"tenant1"}, []string{fmt.Sprintf("%s/collections/Class1/shards/tenant1", SchemaDomain)}},
		{"Class, multiple tenants", "class1", []string{"tenant1", "tenant2"}, []string{fmt.Sprintf("%s/collections/Class1/shards/tenant1", SchemaDomain), fmt.Sprintf("%s/collections/Class1/shards/tenant2", SchemaDomain)}},
		{"Class, empty tenant", "class1", []string{"tenant1", ""}, []string{fmt.Sprintf("%s/collections/Class1/shards/tenant1", SchemaDomain), fmt.Sprintf("%s/collections/Class1/shards/*", SchemaDomain)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ShardsMetadata(tt.class, tt.shards...)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestNamespaces(t *testing.T) {
	tests := []struct {
		name     string
		names    []string
		expected []string
	}{
		{"No names", []string{}, []string{fmt.Sprintf("%s/*", NamespacesDomain)}},
		{"Single empty string", []string{""}, []string{fmt.Sprintf("%s/*", NamespacesDomain)}},
		{"Single wildcard", []string{"*"}, []string{fmt.Sprintf("%s/*", NamespacesDomain)}},
		{"Single name", []string{"customer1"}, []string{fmt.Sprintf("%s/customer1", NamespacesDomain)}},
		{"Multiple names", []string{"customer1", "customer2"}, []string{fmt.Sprintf("%s/customer1", NamespacesDomain), fmt.Sprintf("%s/customer2", NamespacesDomain)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Namespaces(tt.names...)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestBuiltInPermissions_NamespaceManageOnly asserts that the manage_namespaces
// action is granted only to admin-level built-in roles. The viewer/read-only
// roles rely on the convention that availableWeaviateActions entries starting
// with a letter other than 'R' are filtered out of viewerPermissions(); this
// test locks in that the namespaces domain exposes no read-prefixed action,
// so viewers get none of it.
func TestBuiltInPermissions_NamespaceManageOnly(t *testing.T) {
	hasNamespaceAction := func(perms []*models.Permission) bool {
		for _, p := range perms {
			if p == nil || p.Action == nil {
				continue
			}
			if strings.HasSuffix(*p.Action, "_namespaces") {
				return true
			}
		}
		return false
	}

	hasBackupAction := func(perms []*models.Permission) bool {
		for _, p := range perms {
			if p == nil || p.Action == nil {
				continue
			}
			if strings.HasSuffix(*p.Action, "_backups") {
				return true
			}
		}
		return false
	}

	hasExactAction := func(perms []*models.Permission, action string) bool {
		for _, p := range perms {
			if p == nil || p.Action == nil {
				continue
			}
			if *p.Action == action {
				return true
			}
		}
		return false
	}

	builtIn := BuiltInPermissionsFor(false)
	admin := builtIn[Admin]
	root := builtIn[Root]
	viewer := builtIn[Viewer]
	readOnly := builtIn[ReadOnly]

	assert.True(t, hasExactAction(admin, ManageNamespaces), "Admin must include manage_namespaces")
	assert.True(t, hasExactAction(root, ManageNamespaces), "Root must include manage_namespaces")
	assert.False(t, hasNamespaceAction(viewer), "Viewer must not include any *_namespaces action")
	assert.False(t, hasNamespaceAction(readOnly), "ReadOnly must not include any *_namespaces action")
	// Regression guard for the "only R-prefixed actions survive viewer filter"
	// convention: backups has no read-prefixed action either, so viewers get none.
	assert.False(t, hasBackupAction(viewer), "Viewer must not include any *_backups action")
}

// TestBuiltInPermissions_NamespacesEnabled asserts the narrowed admin/viewer
// shape on NS-enabled clusters: CRUD/READ over collections/data/tenants/
// aliases plus MCP, no cluster-only domains. Root/read-only keep wildcard
// permissions regardless of NAMESPACES_ENABLED.
func TestBuiltInPermissions_NamespacesEnabled(t *testing.T) {
	builtIn := BuiltInPermissionsFor(true)
	admin := builtIn[Admin]
	viewer := builtIn[Viewer]
	root := builtIn[Root]
	readOnly := builtIn[ReadOnly]

	allowedAdminActions := map[string]struct{}{
		CreateCollections: {}, ReadCollections: {}, UpdateCollections: {}, DeleteCollections: {},
		CreateData: {}, ReadData: {}, UpdateData: {}, DeleteData: {},
		CreateTenants: {}, ReadTenants: {}, UpdateTenants: {}, DeleteTenants: {},
		CreateAliases: {}, ReadAliases: {}, UpdateAliases: {}, DeleteAliases: {},
		// MCP tools self-scope to principal.Namespace, so they are namespace-safe.
		CreateMcp: {}, ReadMcp: {}, UpdateMcp: {},
		CreateUsers: {}, ReadUsers: {}, UpdateUsers: {}, DeleteUsers: {},
	}
	allowedViewerActions := map[string]struct{}{
		ReadCollections: {}, ReadData: {}, ReadTenants: {}, ReadAliases: {},
		ReadMcp:   {},
		ReadUsers: {},
	}

	collectActions := func(perms []*models.Permission) map[string]struct{} {
		out := map[string]struct{}{}
		for _, p := range perms {
			if p == nil || p.Action == nil {
				continue
			}
			out[*p.Action] = struct{}{}
		}
		return out
	}

	gotAdmin := collectActions(admin)
	gotViewer := collectActions(viewer)
	gotRoot := collectActions(root)
	gotReadOnly := collectActions(readOnly)

	assert.Equal(t, allowedAdminActions, gotAdmin, "Admin (NS-enabled) must contain only CRUD over namespace-bearing domains")
	assert.Equal(t, allowedViewerActions, gotViewer, "Viewer (NS-enabled) must contain only READ over namespace-bearing domains")

	// Disallowed domains for narrowed admin/viewer. MCP and user CRUD are
	// intentionally absent — they are namespace-safe and covered by the
	// allowed maps above. AssignAndRevokeUsers remains excluded.
	for _, action := range []string{
		ManageBackups, ManageNamespaces,
		ReadNodes, ReadCluster,
		AssignAndRevokeUsers,
		AssignAndRevokeGroups, ReadGroups,
		ReadRoles, CreateRoles, UpdateRoles, DeleteRoles,
		CreateReplicate, ReadReplicate, UpdateReplicate, DeleteReplicate,
	} {
		_, hasAdmin := gotAdmin[action]
		_, hasViewer := gotViewer[action]
		assert.False(t, hasAdmin, "Admin (NS-enabled) must not include %s", action)
		assert.False(t, hasViewer, "Viewer (NS-enabled) must not include %s", action)
	}

	// Operator roles keep wildcard shape regardless of NAMESPACES_ENABLED.
	_, ok := gotRoot[ManageNamespaces]
	assert.True(t, ok, "Root must include manage_namespaces on NS-enabled")
	_, ok = gotRoot[ManageBackups]
	assert.True(t, ok, "Root must include manage_backups on NS-enabled")
	_, ok = gotReadOnly[ReadNodes]
	assert.True(t, ok, "ReadOnly must include read_nodes on NS-enabled")
}

func TestGetWildcardPath(t *testing.T) {
	tests := []struct {
		name     string
		resource string
		expected string
	}{
		// Data domain tests
		{
			name:     "data domain full path",
			resource: "data/collections/Class1/shards/Tenant1/objects/123",
			expected: "data/collections/Class1/shards/Tenant1/objects/*",
		},
		{
			name:     "data domain incomplete path",
			resource: "data/collections/Class1/shards/Tenant1",
			expected: "data/collections/Class1/shards/*",
		},

		// Schema domain tests
		{
			name:     "schema domain full path",
			resource: "schema/collections/Class1/shards/Tenant1",
			expected: "schema/collections/Class1/shards/*",
		},
		{
			name:     "schema domain full path",
			resource: "schema/collections/Class1/shards/Tenant1",
			expected: "schema/collections/Class1/shards/*",
		},
		{
			name:     "schema domain incomplete path",
			resource: "schema/collections/Class1",
			expected: "schema/collections/*",
		},

		// Backups domain tests
		{
			name:     "backups domain full path",
			resource: "backups/collections/Class1",
			expected: "backups/collections/*",
		},
		{
			name:     "backups domain incomplete path",
			resource: "backups/collections",
			expected: "backups/*",
		},

		// Users domain tests
		{
			name:     "users domain",
			resource: "users/user1",
			expected: "users/*",
		},

		// Roles domain tests
		{
			name:     "roles domain",
			resource: "roles/role1",
			expected: "roles/*",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := WildcardPath(tt.resource)
			assert.Equal(t, tt.expected, result, "WildcardPath(%q) = %q, want %q",
				tt.resource, result, tt.expected)
		})
	}
}
