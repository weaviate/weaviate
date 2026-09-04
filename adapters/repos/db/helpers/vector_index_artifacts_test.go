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

			got := VectorIndexArtifactsFor("foo", []SiblingVector{{Name: tc.sibling, Quantized: true}})
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
	got := VectorIndexArtifactsFor("vec", []SiblingVector{{Name: "vec", Quantized: true}, {Name: "other", Quantized: true}})
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

	withSiblings := VectorIndexArtifactsFor("vec", []SiblingVector{{Name: "vec2", Quantized: true}, {Name: "other"}, {Name: "vec_extra", Quantized: true}})
	assert.Equal(t, plain.LSMBuckets, withSiblings.LSMBuckets)
	assert.Equal(t, plain.ShardDirs, withSiblings.ShardDirs)
}

// The legacy vector's list is only ever used for protection (it cannot be
// dropped), so it has to name what the legacy index really writes: its ID is
// "main", not its raw bucket name "vectors".
func TestVectorIndexArtifactsFor_LegacyVectorNamesWhatItOwns(t *testing.T) {
	got := VectorIndexArtifactsFor("", nil)

	assert.Subset(t, got.LSMBuckets, []string{
		"vectors",              // raw vectors
		"vectors_compressed",   // BQ/PQ/SQ/RQ, and the centroid HNSW of a legacy hfresh
		"main_muvera_vectors",  // multivector + muvera
		"main_mv_mappings",     // multivector without muvera
		"hfresh_postings_main", // hfresh postings
		"hfresh_shared_main",   // hfresh shared metadata
	})
	assert.ElementsMatch(t, []string{
		"main.hnsw.commitlog.d",
		"main.hnsw.snapshot.d",
		"main.hfresh.d",
		"main.queue.d",
		"meta.db",
	}, got.ShardDirs)

	for _, never := range []string{
		"vectors_muvera_vectors", "vectors_mv_mappings", "hfresh_postings_vectors",
		"hfresh_shared_vectors", "vectors_compressed__centroids",
		"vectors.hfresh.d", "vectors.queue.d",
	} {
		assert.NotContains(t, got.All(), never, "no legacy index ever writes %q", never)
	}
}

func TestVectorIndexArtifactsFor_NeverTakesTheLegacyVectorsArtifact(t *testing.T) {
	// A named vector called "compressed" owns raw bucket "vectors_compressed",
	// byte-identical to the legacy vector's quantized bucket.
	got := VectorIndexArtifactsFor("compressed", []SiblingVector{{Name: "", Quantized: true}})
	assert.NotContains(t, got.LSMBuckets, "vectors_compressed",
		"the legacy vector's quantized bucket must survive dropping a named vector called compressed")
	assert.Contains(t, got.LSMBuckets, "vectors_compressed_compressed",
		"the dropped vector's own quantized bucket still goes")

	// Names that only collided with the misnamed legacy list keep their own
	// raw bucket: leaking it would hand a re-created vector of the same name
	// stale data.
	for _, name := range []string{"muvera_vectors", "mv_mappings", "compressed_centroids"} {
		got := VectorIndexArtifactsFor(name, []SiblingVector{{Name: "", Quantized: true}})
		assert.Contains(t, got.LSMBuckets, "vectors_"+name,
			"dropping %q next to the legacy vector must still remove its own raw bucket", name)
	}
}

// Only a quantized index ever writes a compressed bucket, so an unquantized
// sibling must not protect one: the raw bucket of a vector whose name
// collides with it would leak into a re-created vector of that name.
func TestVectorIndexArtifactsFor_ProtectsOnlyWhatASiblingCanOwn(t *testing.T) {
	tests := []struct {
		name      string
		target    string
		sibling   SiblingVector
		bucket    string // the target's artifact that collides with the sibling's
		protected bool
	}{
		{
			name:    "quantized named sibling keeps its compressed bucket",
			target:  "compressed_x",
			sibling: SiblingVector{Name: "x", Quantized: true},
			bucket:  "vectors_compressed_x", protected: true,
		},
		{
			name:    "unquantized named sibling never wrote a compressed bucket",
			target:  "compressed_x",
			sibling: SiblingVector{Name: "x"},
			bucket:  "vectors_compressed_x", protected: false,
		},
		{
			name:    "quantized legacy sibling keeps its compressed bucket",
			target:  "compressed",
			sibling: SiblingVector{Name: "", Quantized: true},
			bucket:  "vectors_compressed", protected: true,
		},
		{
			name:    "unquantized legacy sibling never wrote a compressed bucket",
			target:  "compressed",
			sibling: SiblingVector{Name: ""},
			bucket:  "vectors_compressed", protected: false,
		},
		{
			name:    "a raw bucket is protected whatever the sibling's compression",
			target:  "foo",
			sibling: SiblingVector{Name: "foo_muvera_vectors"},
			bucket:  "vectors_foo_muvera_vectors", protected: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Contains(t, VectorIndexArtifactsFor(tt.target, nil).LSMBuckets, tt.bucket,
				"precondition: dropping %s targets %s when nothing is protected", tt.target, tt.bucket)
			got := VectorIndexArtifactsFor(tt.target, []SiblingVector{tt.sibling})
			if tt.protected {
				assert.NotContains(t, got.LSMBuckets, tt.bucket)
			} else {
				assert.Contains(t, got.LSMBuckets, tt.bucket)
			}
		})
	}
}
