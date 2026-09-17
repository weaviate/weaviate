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

package helpers

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/schema"
)

// TestVectorIndexArtifactsFor_CoversEveryArtifact pins the full set a drop must
// remove. Each name is spelled out literally rather than rebuilt from the same
// helpers the implementation uses: the point is to fail when a name changes on
// the side that CREATES it, which a self-referential assertion cannot do.
func TestVectorIndexArtifactsFor_CoversEveryArtifact(t *testing.T) {
	got := VectorIndexArtifactsFor("vec", nil)

	assert.ElementsMatch(t, []string{
		"vectors_vec",                      // raw vectors
		"vectors_compressed_vec",           // BQ/PQ/SQ/RQ
		"vectors_vec_muvera_vectors",       // multivector + muvera (hnsw.New)
		"vectors_vec_mv_mappings",          // multivector without muvera (hnsw.New)
		"hfresh_postings_vectors_vec",      // hfresh postings
		"hfresh_shared_vectors_vec",        // hfresh shared metadata
		"vectors_compressed_vec_centroids", // hfresh's nested centroids HNSW
	}, got.LSMBuckets)

	assert.ElementsMatch(t, []string{
		"vectors_vec.hnsw.commitlog.d",
		"vectors_vec.hnsw.snapshot.d",
		"vectors_vec.hfresh.d",
		"vectors_vec.queue.d", // async-indexing queue
		"meta_vec.db",         // flat quantisation metadata (a FILE)
	}, got.ShardDirs)

	assert.Len(t, got.All(), len(got.LSMBuckets)+len(got.ShardDirs))
}

// TestVectorIndexArtifactsFor_NeverTakesASiblingsArtifact pins the guard
// against a name collision that would DELETE LIVE DATA, over every artifact a
// sibling owns rather than just its primary bucket.
//
// Target vector names are only constrained by TargetVectorNameRegex, which
// permits "<other>_muvera_vectors", "<other>_mv_mappings" and
// "<other>_centroids". Each makes one of this target's artifacts
// byte-identical to a bucket a live, unrelated vector owns — and the file
// sweep would re-remove it on every restart while the drop marker persists,
// surviving re-import.
func TestVectorIndexArtifactsFor_NeverTakesASiblingsArtifact(t *testing.T) {
	nameRe := regexp.MustCompile("^" + schema.TargetVectorNameRegex + "$")

	for _, tc := range []struct {
		name    string
		sibling string
		clash   string
	}{
		{"muvera bucket", "foo_muvera_vectors", "vectors_foo_muvera_vectors"},
		{"mv mappings bucket", "foo_mv_mappings", "vectors_foo_mv_mappings"},
		{"centroids compressed bucket", "foo_centroids", "vectors_compressed_foo_centroids"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.True(t, nameRe.MatchString(tc.sibling),
				"precondition: %q must be a legal vector name, or this collision is unreachable", tc.sibling)
			require.Contains(t, VectorIndexArtifactsFor(tc.sibling, nil).All(), tc.clash,
				"precondition: the colliding name must be one the sibling itself owns")

			unguarded := VectorIndexArtifactsFor("foo", nil)
			require.Contains(t, unguarded.LSMBuckets, tc.clash,
				"precondition: dropping foo does target this name when no siblings are declared")

			got := VectorIndexArtifactsFor("foo", []string{tc.sibling})
			assert.NotContains(t, got.LSMBuckets, tc.clash,
				"a live sibling's bucket must never be removed")

			// The target's own artifacts still go.
			assert.Contains(t, got.LSMBuckets, "vectors_foo")
			assert.Contains(t, got.LSMBuckets, "vectors_compressed_foo")
		})
	}
}

// TestVectorIndexArtifactsFor_KeepsItsOwnPrimaryBucket guards the guard: the
// target's own vectors bucket must survive the sibling filter even when the
// caller passes the target itself in the sibling list, which is easy to do by
// looping over the whole schema.
func TestVectorIndexArtifactsFor_KeepsItsOwnPrimaryBucket(t *testing.T) {
	got := VectorIndexArtifactsFor("vec", []string{"vec", "other"})
	assert.Contains(t, got.LSMBuckets, "vectors_vec",
		"the dropped vector's own bucket must still be removed")
	assert.Contains(t, got.LSMBuckets, "vectors_vec_mv_mappings")
}

// TestVectorIndexArtifactsFor_UnrelatedSiblingsChangeNothing pins that the
// guard is narrow: it drops only exact collisions, not anything sharing a
// prefix. "vec" prefixes "vec2", and treating that as a clash would silently
// stop cleaning real artifacts.
func TestVectorIndexArtifactsFor_UnrelatedSiblingsChangeNothing(t *testing.T) {
	plain := VectorIndexArtifactsFor("vec", nil)
	// Anchored, because comparing two calls to the function under test passes
	// just as happily when both return nothing.
	require.NotEmpty(t, plain.LSMBuckets)
	require.NotEmpty(t, plain.ShardDirs)

	withSiblings := VectorIndexArtifactsFor("vec", []string{"vec2", "other", "vec_extra"})
	assert.Equal(t, plain.LSMBuckets, withSiblings.LSMBuckets)
	assert.Equal(t, plain.ShardDirs, withSiblings.ShardDirs)
}
