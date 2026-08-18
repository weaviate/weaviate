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
	}, got.ShardDirs)

	assert.Len(t, got.All(), len(got.LSMBuckets)+len(got.ShardDirs))
}

// TestVectorIndexArtifactsFor_NeverTakesASiblingsPrimaryBucket pins the guard
// against a name collision that would DELETE LIVE DATA.
//
// Target vector names are only constrained by TargetVectorNameRegex, which
// permits "<other>_muvera_vectors" and "<other>_mv_mappings". Those make a
// sibling's PRIMARY vectors bucket byte-identical to one of this target's
// artifacts, so an unguarded drop of "foo" would remove the raw vectors of a
// live, unrelated vector — and the file sweep would re-remove the directory on
// every restart while the drop marker persists, surviving re-import.
func TestVectorIndexArtifactsFor_NeverTakesASiblingsPrimaryBucket(t *testing.T) {
	nameRe := regexp.MustCompile("^" + schema.TargetVectorNameRegex + "$")

	for _, tc := range []struct {
		name    string
		sibling string
		clash   string
	}{
		{"muvera bucket", "foo_muvera_vectors", "vectors_foo_muvera_vectors"},
		{"mv mappings bucket", "foo_mv_mappings", "vectors_foo_mv_mappings"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.True(t, nameRe.MatchString(tc.sibling),
				"precondition: %q must be a legal vector name, or this collision is unreachable", tc.sibling)
			require.Equal(t, tc.clash, GetVectorsBucketName(tc.sibling),
				"precondition: the sibling's primary bucket must be the colliding name")

			unguarded := VectorIndexArtifactsFor("foo", nil)
			require.Contains(t, unguarded.LSMBuckets, tc.clash,
				"precondition: dropping foo does target this name when no siblings are declared")

			got := VectorIndexArtifactsFor("foo", []string{tc.sibling})
			assert.NotContains(t, got.LSMBuckets, tc.clash,
				"a live sibling's primary vectors bucket must never be removed")

			// The target's own artifacts still go.
			assert.Contains(t, got.LSMBuckets, "vectors_foo")
			assert.Contains(t, got.LSMBuckets, "vectors_compressed_foo")
		})
	}
}

// TestVectorIndexArtifactsFor_NeverTakesASiblingsCompressedBucket pins that the
// guard covers every artifact a sibling owns, not just its primary bucket.
// Listing hfresh's nested centroids index makes this reachable: a vector named
// "<other>_centroids" owns "vectors_compressed_<other>_centroids", which is
// byte-identical to the centroids artifact of "<other>". Deleting it would take
// a live index's compressed vectors.
func TestVectorIndexArtifactsFor_NeverTakesASiblingsCompressedBucket(t *testing.T) {
	nameRe := regexp.MustCompile("^" + schema.TargetVectorNameRegex + "$")
	require.True(t, nameRe.MatchString("foo_centroids"),
		"precondition: the colliding name must be legal")

	const clash = "vectors_compressed_foo_centroids"
	require.Equal(t, clash, GetCompressedBucketName("foo_centroids"),
		"precondition: this is the sibling's own compressed bucket")

	unguarded := VectorIndexArtifactsFor("foo", nil)
	require.Contains(t, unguarded.LSMBuckets, clash,
		"precondition: dropping foo targets this name when no siblings are declared")

	got := VectorIndexArtifactsFor("foo", []string{"foo_centroids"})
	assert.NotContains(t, got.LSMBuckets, clash,
		"a live sibling's compressed bucket must never be removed")
	assert.Contains(t, got.LSMBuckets, "vectors_foo", "the target's own artifacts still go")
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
	withSiblings := VectorIndexArtifactsFor("vec", []string{"vec2", "other", "vec_extra"})
	assert.Equal(t, plain.LSMBuckets, withSiblings.LSMBuckets)
	assert.Equal(t, plain.ShardDirs, withSiblings.ShardDirs)
}
