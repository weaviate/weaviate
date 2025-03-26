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

package authorization

import (
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
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
		id       strfmt.UUID
		expected string
	}{
		{"No class, no shard, no id", "", "", "", fmt.Sprintf("%s/collections/*/shards/*/objects/*", DataDomain)},
		{"Class, no shard, no id", "class1", "", "", fmt.Sprintf("%s/collections/Class1/shards/*/objects/*", DataDomain)},
		{"No class, shard, no id", "", "shard1", "", fmt.Sprintf("%s/collections/*/shards/shard1/objects/*", DataDomain)},
		{"No class, no shard, id", "", "", "id1", fmt.Sprintf("%s/collections/*/shards/*/objects/id1", DataDomain)},
		{"Class, shard, no id", "class1", "shard1", "", fmt.Sprintf("%s/collections/Class1/shards/shard1/objects/*", DataDomain)},
		{"Class, no shard, id", "class1", "", "id1", fmt.Sprintf("%s/collections/Class1/shards/*/objects/id1", DataDomain)},
		{"No class, shard, id", "", "shard1", "id1", fmt.Sprintf("%s/collections/*/shards/shard1/objects/id1", DataDomain)},
		{"Class, shard, id", "class1", "shard1", "id1", fmt.Sprintf("%s/collections/Class1/shards/shard1/objects/id1", DataDomain)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Objects(tt.class, tt.shard, tt.id)
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
