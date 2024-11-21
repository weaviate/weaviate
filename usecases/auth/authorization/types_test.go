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
		{"No users", []string{}, []string{"meta/users/*"}},
		{"Single user", []string{"user1"}, []string{"meta/users/user1"}},
		{"Multiple users", []string{"user1", "user2"}, []string{"meta/users/user1", "meta/users/user2"}},
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
		{"No roles", []string{}, []string{"meta/roles/*"}},
		{"Single role", []string{"admin"}, []string{"meta/roles/admin"}},
		{"Multiple roles", []string{"admin", "user"}, []string{"meta/roles/admin", "meta/roles/user"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Roles(tt.roles...)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCluster(t *testing.T) {
	expected := "meta/cluster/*"
	result := Cluster()
	assert.Equal(t, expected, result)
}

func TestCollections(t *testing.T) {
	tests := []struct {
		name     string
		classes  []string
		expected []string
	}{
		{"No classes", []string{}, []string{"meta/collections/*/shards/*"}},
		{"Single empty class", []string{""}, []string{"meta/collections/*/shards/*"}},
		{"Single class", []string{"class1"}, []string{"meta/collections/class1/shards/*"}},
		{"Multiple classes", []string{"class1", "class2"}, []string{"meta/collections/class1/shards/*", "meta/collections/class2/shards/*"}},
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
		{"No class, no shards", "", []string{}, []string{"meta/collections/*/shards/*"}},
		{"Class, no shards", "class1", []string{}, []string{"meta/collections/class1/shards/*"}},
		{"No class, single shard", "", []string{"shard1"}, []string{"meta/collections/*/shards/shard1"}},
		{"Class, single shard", "class1", []string{"shard1"}, []string{"meta/collections/class1/shards/shard1"}},
		{"Class, multiple shards", "class1", []string{"shard1", "shard2"}, []string{"meta/collections/class1/shards/shard1", "meta/collections/class1/shards/shard2"}},
		{"Class, empty shard", "class1", []string{"shard1", ""}, []string{"meta/collections/class1/shards/shard1", "meta/collections/class1/shards/*"}},
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
		{"No class, no shard, no id", "", "", "", "data/collections/*/shards/*/objects/*"},
		{"Class, no shard, no id", "class1", "", "", "data/collections/class1/shards/*/objects/*"},
		{"No class, shard, no id", "", "shard1", "", "data/collections/*/shards/shard1/objects/*"},
		{"No class, no shard, id", "", "", "id1", "data/collections/*/shards/*/objects/id1"},
		{"Class, shard, no id", "class1", "shard1", "", "data/collections/class1/shards/shard1/objects/*"},
		{"Class, no shard, id", "class1", "", "id1", "data/collections/class1/shards/*/objects/id1"},
		{"No class, shard, id", "", "shard1", "id1", "data/collections/*/shards/shard1/objects/id1"},
		{"Class, shard, id", "class1", "shard1", "id1", "data/collections/class1/shards/shard1/objects/id1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Objects(tt.class, tt.shard, tt.id)
			assert.Equal(t, tt.expected, result)
		})
	}
}
