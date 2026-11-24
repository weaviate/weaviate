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

package vectormanifest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/schema"
)

// The schema's collision check reads vector names only. It is complete
// because every file name a generated ID owns has a "-" and no file name a
// vector name owns has one, so the two can never share a file.
func TestGeneratedIDsNeverCollideWithNames(t *testing.T) {
	generated := []string{"3f2a9c1e-0b7d-4c1a-9e2f-5d6c7b8a9f00", "a-b", "-", "x_-_y"}
	for _, id := range generated {
		require.NoError(t, validateID(id), id)
		for _, n := range ownerOf(record{PhysicalID: id, QueueID: id}).names() {
			assert.Contains(t, n, "-", "generated ID %q owns %q", id, n)
		}
	}

	names := []string{"", "title", "_", "compressed", "a_centroids", "x", "Muvera_Vectors", "mv_mappings"}
	for _, name := range names {
		require.Regexp(t, `^(`+schema.TargetVectorNameRegex+`)?$`, name)
		for _, n := range ownerAt(helpers.VectorIndexIDForTarget(name)).names() {
			assert.NotContains(t, n, "-", "vector %q owns %q", name, n)
		}
	}

	// without a "-" an ID outside the naming rule would be one a vector name can reach
	for _, id := range []string{"title", "vectors_", "abc_def", "vectors_9x"} {
		assert.Error(t, validateID(id), id)
	}
}

// Records that name one file refuse the load when either has a generated ID;
// collisions between name-derived IDs predate the check and still load.
func TestRefuseGeneratedCollisions(t *testing.T) {
	rec := func(id string) record {
		return record{PhysicalID: id, IndexType: "hnsw", State: "ready", QueueID: id}
	}
	tests := []struct {
		name    string
		a, b    string
		wantErr string
	}{
		{"one storage ID", "a-b", "vectors_a-b", `both name "vectors_a-b"`},
		{"centroids of one, compressed of the other", "x-1", "x-1_centroids", `both name "vectors_compressed_x-1_centroids"`},
		{"compressed of one, vectors of the other", "a-b", "compressed_a-b", `both name "vectors_compressed_a-b"`},
		{"distinct generated IDs", "a-b", "c-d", ""},
		{"generated and name-derived", "a-b", "vectors_title", ""},
		{"name-derived only", "main", "vectors_compressed", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := refuseGeneratedCollisions(map[string]record{"a": rec(tc.a), "b": rec(tc.b)})
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
			require.ErrorContains(t, err, `vectors "a" and "b"`)
		})
	}
}

// TestCollisions pins which pairs of owners share a physical
// name, and that a newcomer is checked against the owners only.
func TestCollisions(t *testing.T) {
	tests := []struct {
		name   string
		owners map[string]string
		want   []string
	}{
		{name: "no collision", owners: map[string]string{"": "main", "title": "vectors_title"}},
		{
			name:   "compressed next to the legacy vector",
			owners: map[string]string{"": "main", "compressed": "vectors_compressed"},
			want:   []string{`vectors "" and "compressed" share "vectors_compressed"`},
		},
		{
			name:   "muvera next to its sibling",
			owners: map[string]string{"foo": "vectors_foo", "foo_muvera_vectors": "vectors_foo_muvera_vectors"},
			want:   []string{`vectors "foo" and "foo_muvera_vectors" share "vectors_foo_muvera_vectors"`},
		},
		{
			name:   "centroids next to its sibling",
			owners: map[string]string{"foo": "vectors_foo", "foo_centroids": "vectors_foo_centroids"},
			want:   []string{`vectors "foo" and "foo_centroids" share "vectors_compressed_foo_centroids"`},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, collisions(ownersAt(tt.owners)))
		})
	}

	// a newcomer is checked against the owners, not the owners against each
	// other: an existing collision does not block an unrelated addition
	owners := ownersAt(map[string]string{"foo": "vectors_foo", "foo_muvera_vectors": "vectors_foo_muvera_vectors"})
	assert.Empty(t, collisionsWith(owners, "bar", ownerAt("vectors_bar")))
	assert.Equal(t, []string{`vectors "foo" and "foo_mv_mappings" share "vectors_foo_mv_mappings"`},
		collisionsWith(owners, "foo_mv_mappings", ownerAt("vectors_foo_mv_mappings")))
}

// A queue that outlived its index is still its vector's: another vector
// naming it collides, and so does a newcomer whose ID the index took.
func TestCollisions_QueueOutlivedItsIndex(t *testing.T) {
	owners := map[string]owner{"foo": {physicalID: "vectors_foo_2", queueID: "vectors_foo"}}

	assert.Empty(t, collisionsWith(owners, "bar", ownerAt("vectors_bar")))
	assert.Equal(t, []string{`vectors "foo" and "bar" share "vectors_foo.queue.d"`},
		collisionsWith(owners, "bar", owner{physicalID: "vectors_bar", queueID: "vectors_foo"}))
	assert.Equal(t, []string{`vectors "foo" and "foo_2" share "vectors_foo_2"`},
		collisionsWith(owners, "foo_2", ownerAt("vectors_foo_2")))
}

// ownersAt maps each vector to an owner whose index and queue share its ID.
func ownersAt(ids map[string]string) map[string]owner {
	owners := make(map[string]owner, len(ids))
	for name, id := range ids {
		owners[name] = ownerAt(id)
	}
	return owners
}

// A name is refused when an existing vector, or an earlier one of the same
// update, owns one of its files.
func TestRefuseCollidingNames(t *testing.T) {
	require.NoError(t, RefuseCollidingNames([]string{"", "title"}, []string{"body", "summary"}))
	require.NoError(t, RefuseCollidingNames([]string{"foo", "foo_muvera_vectors"}, []string{"bar"}),
		"a collision between existing vectors does not block another one")
	require.ErrorContains(t, RefuseCollidingNames([]string{""}, []string{"compressed"}),
		`vector "compressed" cannot be created: vectors "" and "compressed" share "vectors_compressed"`)
	require.ErrorContains(t, RefuseCollidingNames(nil, []string{"foo", "foo_mv_mappings"}), `share "vectors_foo_mv_mappings"`)
}
