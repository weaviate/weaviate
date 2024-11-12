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

func TestRoles(t *testing.T) {
	tests := []struct {
		name     string
		roles    []string
		expected []string
	}{
		{"No roles", []string{}, []string{"roles/*"}},
		{"Single role", []string{"admin"}, []string{"roles/admin"}},
		{"Multiple roles", []string{"admin", "user"}, []string{"roles/admin", "roles/user"}},
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

func TestCollections(t *testing.T) {
	tests := []struct {
		name     string
		classes  []string
		expected []string
	}{
		{"No classes", []string{}, []string{"collections/*"}},
		{"Single empty class", []string{""}, []string{"collections/*"}},
		{"Single class", []string{"class1"}, []string{"collections/class1/*"}},
		{"Multiple classes", []string{"class1", "class2"}, []string{"collections/class1/*", "collections/class2/*"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Collections(tt.classes...)
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
		{"No class, no shards", "", []string{}, []string{"collections/*/shards/*/*"}},
		{"Class, no shards", "class1", []string{}, []string{"collections/class1/shards/*/*"}},
		{"No class, single shard", "", []string{"shard1"}, []string{"collections/*/shards/shard1/*"}},
		{"Class, single shard", "class1", []string{"shard1"}, []string{"collections/class1/shards/shard1/*"}},
		{"Class, multiple shards", "class1", []string{"shard1", "shard2"}, []string{"collections/class1/shards/shard1/*", "collections/class1/shards/shard2/*"}},
		{"Class, empty shard", "class1", []string{"shard1", ""}, []string{"collections/class1/shards/shard1/*", "collections/class1/shards/*/*"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Shards(tt.class, tt.shards...)
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
		{"No class, no shard, no id", "", "", "", "collections/*/shards/*/objects/*"},
		{"Class, no shard, no id", "class1", "", "", "collections/class1/shards/*/objects/*"},
		{"No class, shard, no id", "", "shard1", "", "collections/*/shards/shard1/objects/*"},
		{"No class, no shard, id", "", "", "id1", "collections/*/shards/*/objects/id1"},
		{"Class, shard, no id", "class1", "shard1", "", "collections/class1/shards/shard1/objects/*"},
		{"Class, no shard, id", "class1", "", "id1", "collections/class1/shards/*/objects/id1"},
		{"No class, shard, id", "", "shard1", "id1", "collections/*/shards/shard1/objects/id1"},
		{"Class, shard, id", "class1", "shard1", "id1", "collections/class1/shards/shard1/objects/id1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Objects(tt.class, tt.shard, tt.id)
			assert.Equal(t, tt.expected, result)
		})
	}
}
