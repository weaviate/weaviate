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

package db

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hfresh"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/vectorindex"
)

func TestVectorDropIndexHelper_RemoveVectorIndexFiles(t *testing.T) {
	h := newVectorDropIndexHelper()

	setup := func(t *testing.T) (string, string) {
		t.Helper()
		indexPath := t.TempDir()
		shardName := "shard1"
		return indexPath, shardName
	}

	pathExists := func(p string) bool {
		_, err := os.Stat(p)
		return err == nil
	}

	t.Run("removes all flat vector artifacts", func(t *testing.T) {
		indexPath, shardName := setup(t)

		vectorsBucket := filepath.Join(indexPath, shardName, "lsm", fmt.Sprintf("%s_flat_bq", helpers.VectorsBucketLSM))
		compressedBucket := filepath.Join(indexPath, shardName, "lsm", helpers.GetCompressedBucketName("flat_bq"))

		require.NoError(t, os.MkdirAll(vectorsBucket, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(vectorsBucket, "data.db"), []byte("data"), 0o644))
		require.NoError(t, os.MkdirAll(compressedBucket, 0o755))

		err := h.removeVectorIndexFiles(indexPath, shardName, "flat_bq", nil)
		require.NoError(t, err)

		assert.False(t, pathExists(vectorsBucket))
		assert.False(t, pathExists(compressedBucket))
	})

	t.Run("removes the muvera bucket of a multi-vector index", func(t *testing.T) {
		// A muvera-encoded multi-vector index owns a bucket outside the
		// vectors/compressed pair. It was missing from this sweep, so the
		// encoded copy of every vector survived a completed drop — and nothing
		// collects it later: once the drop finishes, the vector's entry is gone
		// from the schema, so ensureFilesAreRemovedForDroppedVectorIndexes
		// never iterates over it again.
		indexPath, shardName := setup(t)

		const target = "multivector_muvera_bq"
		lsm := filepath.Join(indexPath, shardName, "lsm")
		vectorsBucket := filepath.Join(lsm, helpers.GetVectorsBucketName(target))
		compressedBucket := filepath.Join(lsm, helpers.GetCompressedBucketName(target))
		muveraBucket := filepath.Join(lsm, hnsw.MuveraBucketName(helpers.GetVectorsBucketName(target)))
		commitLog := filepath.Join(indexPath, shardName, hnsw.CommitLogDirName(helpers.GetVectorsBucketName(target)))

		for _, dir := range []string{vectorsBucket, compressedBucket, muveraBucket, commitLog} {
			require.NoError(t, os.MkdirAll(dir, 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(dir, "segment.db"), []byte("data"), 0o644))
		}
		require.Equal(t, filepath.Join(lsm, "vectors_multivector_muvera_bq_muvera_vectors"), muveraBucket,
			"the bucket name must match what hnsw.New creates")

		require.NoError(t, h.removeVectorIndexFiles(indexPath, shardName, target, nil))

		assert.False(t, pathExists(muveraBucket), "the muvera bucket must not survive the drop")
		assert.False(t, pathExists(vectorsBucket))
		assert.False(t, pathExists(compressedBucket))
		assert.False(t, pathExists(commitLog))
	})

	t.Run("removes the directory and buckets of an hfresh index", func(t *testing.T) {
		// hfresh keeps its state outside the vectors/compressed pair that every
		// index has, and its own Drop() removes none of it — so a per-vector
		// drop used to leave all of it behind. Nothing collects it afterwards:
		// once the drop completes the vector's schema entry is gone, so
		// ensureFilesAreRemovedForDroppedVectorIndexes never iterates over it.
		indexPath, shardName := setup(t)

		const target = "hfresh_vec"
		indexID := helpers.GetVectorsBucketName(target)
		lsm := filepath.Join(indexPath, shardName, "lsm")
		postings := filepath.Join(lsm, hfresh.HFreshPostingsBucketName(indexID))
		shared := filepath.Join(lsm, hfresh.HFreshSharedBucketName(indexID))
		hfreshDir := filepath.Join(indexPath, shardName, helpers.HFreshDirName(indexID))

		for _, dir := range []string{postings, shared, hfreshDir} {
			require.NoError(t, os.MkdirAll(dir, 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(dir, "segment.db"), []byte("data"), 0o644))
		}
		// Pinned literally: these are the names the running index creates, and
		// a rename on either side has to fail here rather than silently leave
		// the sweep looking for something that no longer exists.
		require.Equal(t, filepath.Join(lsm, "hfresh_postings_vectors_hfresh_vec"), postings)
		require.Equal(t, filepath.Join(lsm, "hfresh_shared_vectors_hfresh_vec"), shared)
		require.Equal(t, filepath.Join(indexPath, shardName, "vectors_hfresh_vec.hfresh.d"), hfreshDir)

		require.NoError(t, h.removeVectorIndexFiles(indexPath, shardName, target, nil))

		assert.False(t, pathExists(postings), "the hfresh postings bucket must not survive the drop")
		assert.False(t, pathExists(shared), "the hfresh shared bucket must not survive the drop")
		assert.False(t, pathExists(hfreshDir), "the hfresh directory must not survive the drop")
	})

	t.Run("a sibling whose bucket name collides is not deleted", func(t *testing.T) {
		// The only way this sweep can destroy live data: target vector names may
		// legally be "<other>_muvera_vectors" or "<other>_mv_mappings", which
		// makes that sibling's PRIMARY vectors bucket identical to one of the
		// dropped vector's artifacts. Deleting it takes a live vector's raw
		// vectors, and this sweep re-runs on every restart while the drop marker
		// persists, so a re-import would not survive either.
		indexPath, shardName := setup(t)
		lsm := filepath.Join(indexPath, shardName, "lsm")

		const dropped = "foo"
		for _, sibling := range []string{"foo_muvera_vectors", "foo_mv_mappings"} {
			siblingBucket := filepath.Join(lsm, helpers.GetVectorsBucketName(sibling))
			ownBucket := filepath.Join(lsm, helpers.GetVectorsBucketName(dropped))
			require.NoError(t, os.MkdirAll(siblingBucket, 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(siblingBucket, "segment.db"), []byte("live"), 0o644))
			require.NoError(t, os.MkdirAll(ownBucket, 0o755))

			require.NoError(t, h.removeVectorIndexFiles(indexPath, shardName, dropped, []string{sibling}))

			assert.True(t, pathExists(siblingBucket),
				"%s is %s's live vectors bucket and must survive dropping %s",
				siblingBucket, sibling, dropped)
			assert.False(t, pathExists(ownBucket), "the dropped vector's own bucket must still go")
		}
	})

	t.Run("removes all hnsw vector artifacts", func(t *testing.T) {
		indexPath, shardName := setup(t)

		vectorsBucket := filepath.Join(indexPath, shardName, "lsm", fmt.Sprintf("%s_hnsw_rq8", helpers.VectorsBucketLSM))
		compressedBucket := filepath.Join(indexPath, shardName, "lsm", helpers.GetCompressedBucketName("hnsw_rq8"))
		commitLog := filepath.Join(indexPath, shardName, "vectors_hnsw_rq8.hnsw.commitlog.d")
		snapshot := filepath.Join(indexPath, shardName, "vectors_hnsw_rq8.hnsw.snapshot.d")

		require.NoError(t, os.MkdirAll(vectorsBucket, 0o755))
		require.NoError(t, os.MkdirAll(compressedBucket, 0o755))
		require.NoError(t, os.MkdirAll(commitLog, 0o755))
		require.NoError(t, os.MkdirAll(snapshot, 0o755))

		err := h.removeVectorIndexFiles(indexPath, shardName, "hnsw_rq8", nil)
		require.NoError(t, err)

		assert.False(t, pathExists(vectorsBucket))
		assert.False(t, pathExists(compressedBucket))
		assert.False(t, pathExists(commitLog))
		assert.False(t, pathExists(snapshot))
	})

	t.Run("succeeds when files do not exist", func(t *testing.T) {
		indexPath, shardName := setup(t)
		require.NoError(t, os.MkdirAll(filepath.Join(indexPath, shardName, "lsm"), 0o755))

		err := h.removeVectorIndexFiles(indexPath, shardName, "nonexistent", nil)
		require.NoError(t, err)
	})
}

func TestVectorDropIndexHelper_EnsureFilesAreRemovedForDroppedVectorIndexes(t *testing.T) {
	h := newVectorDropIndexHelper()

	setup := func(t *testing.T) (string, string) {
		t.Helper()
		indexPath := t.TempDir()
		shardName := "shard1"
		return indexPath, shardName
	}

	createLSMBucket := func(t *testing.T, indexPath, shardName, bucketName string) {
		t.Helper()
		bucketPath := filepath.Join(indexPath, shardName, "lsm", bucketName)
		require.NoError(t, os.MkdirAll(bucketPath, 0o755))
	}

	createShardDir := func(t *testing.T, indexPath, shardName, dirName string) {
		t.Helper()
		dirPath := filepath.Join(indexPath, shardName, dirName)
		require.NoError(t, os.MkdirAll(dirPath, 0o755))
	}

	pathExists := func(p string) bool {
		_, err := os.Stat(p)
		return err == nil
	}

	t.Run("no vector config - no error", func(t *testing.T) {
		indexPath, shardName := setup(t)

		class := &models.Class{
			Class:        "TestClass",
			VectorConfig: nil,
		}

		err := h.ensureFilesAreRemovedForDroppedVectorIndexes(indexPath, shardName, class)
		require.NoError(t, err)
	})

	t.Run("all vectors active - files kept", func(t *testing.T) {
		indexPath, shardName := setup(t)
		createLSMBucket(t, indexPath, shardName, "vectors_flat_bq")
		createLSMBucket(t, indexPath, shardName, "vectors_compressed_flat_bq")
		createLSMBucket(t, indexPath, shardName, "vectors_hnsw_rq8")
		createShardDir(t, indexPath, shardName, "vectors_hnsw_rq8.hnsw.commitlog.d")

		class := &models.Class{
			Class: "TestClass",
			VectorConfig: map[string]models.VectorConfig{
				"flat_bq":  {VectorIndexType: "flat"},
				"hnsw_rq8": {VectorIndexType: "hnsw"},
			},
		}

		err := h.ensureFilesAreRemovedForDroppedVectorIndexes(indexPath, shardName, class)
		require.NoError(t, err)

		assert.True(t, pathExists(filepath.Join(indexPath, shardName, "lsm", "vectors_flat_bq")))
		assert.True(t, pathExists(filepath.Join(indexPath, shardName, "lsm", "vectors_compressed_flat_bq")))
		assert.True(t, pathExists(filepath.Join(indexPath, shardName, "lsm", "vectors_hnsw_rq8")))
		assert.True(t, pathExists(filepath.Join(indexPath, shardName, "vectors_hnsw_rq8.hnsw.commitlog.d")))
	})

	t.Run("dropped vector - files removed", func(t *testing.T) {
		indexPath, shardName := setup(t)

		createLSMBucket(t, indexPath, shardName, "vectors_flat_bq")
		createLSMBucket(t, indexPath, shardName, "vectors_compressed_flat_bq")
		createLSMBucket(t, indexPath, shardName, "vectors_hnsw_rq8")
		createShardDir(t, indexPath, shardName, "vectors_hnsw_rq8.hnsw.commitlog.d")

		class := &models.Class{
			Class: "TestClass",
			VectorConfig: map[string]models.VectorConfig{
				"flat_bq":  {VectorIndexType: vectorindex.VectorIndexTypeNone},
				"hnsw_rq8": {VectorIndexType: "hnsw"},
			},
		}

		err := h.ensureFilesAreRemovedForDroppedVectorIndexes(indexPath, shardName, class)
		require.NoError(t, err)

		assert.False(t, pathExists(filepath.Join(indexPath, shardName, "lsm", "vectors_flat_bq")))
		assert.False(t, pathExists(filepath.Join(indexPath, shardName, "lsm", "vectors_compressed_flat_bq")))

		assert.True(t, pathExists(filepath.Join(indexPath, shardName, "lsm", "vectors_hnsw_rq8")))
		assert.True(t, pathExists(filepath.Join(indexPath, shardName, "vectors_hnsw_rq8.hnsw.commitlog.d")))
	})

	t.Run("all vectors dropped - all files removed", func(t *testing.T) {
		indexPath, shardName := setup(t)

		createLSMBucket(t, indexPath, shardName, "vectors_flat_bq")
		createLSMBucket(t, indexPath, shardName, "vectors_compressed_flat_bq")
		createLSMBucket(t, indexPath, shardName, "vectors_hnsw_rq8")
		createShardDir(t, indexPath, shardName, "vectors_hnsw_rq8.hnsw.commitlog.d")
		createShardDir(t, indexPath, shardName, "vectors_hnsw_rq8.hnsw.snapshot.d")

		class := &models.Class{
			Class: "TestClass",
			VectorConfig: map[string]models.VectorConfig{
				"flat_bq":  {VectorIndexType: vectorindex.VectorIndexTypeNone},
				"hnsw_rq8": {VectorIndexType: vectorindex.VectorIndexTypeNone},
			},
		}

		err := h.ensureFilesAreRemovedForDroppedVectorIndexes(indexPath, shardName, class)
		require.NoError(t, err)

		assert.False(t, pathExists(filepath.Join(indexPath, shardName, "lsm", "vectors_flat_bq")))
		assert.False(t, pathExists(filepath.Join(indexPath, shardName, "lsm", "vectors_compressed_flat_bq")))
		assert.False(t, pathExists(filepath.Join(indexPath, shardName, "lsm", "vectors_hnsw_rq8")))
		assert.False(t, pathExists(filepath.Join(indexPath, shardName, "vectors_hnsw_rq8.hnsw.commitlog.d")))
		assert.False(t, pathExists(filepath.Join(indexPath, shardName, "vectors_hnsw_rq8.hnsw.snapshot.d")))
	})

	t.Run("a live sibling whose bucket name collides survives the sweep", func(t *testing.T) {
		// The sibling-guard case in removeVectorIndexFiles is handed a list
		// built by hand, so it proves the guard works given a correct list but
		// not that any caller supplies one — passing nil at every call site
		// leaves it green. This drives the real entry point, which derives the
		// list from the class itself.
		indexPath, shardName := setup(t)

		const (
			dropped = "foo"
			sibling = "foo_muvera_vectors"
		)
		require.Equal(t, helpers.GetVectorsBucketName(sibling),
			hnsw.MuveraBucketName(helpers.GetVectorsBucketName(dropped)),
			"precondition: the sibling's own bucket must be one of the dropped vector's artifact names")

		createLSMBucket(t, indexPath, shardName, helpers.GetVectorsBucketName(dropped))
		createLSMBucket(t, indexPath, shardName, helpers.GetVectorsBucketName(sibling))

		class := &models.Class{
			Class: "TestClass",
			VectorConfig: map[string]models.VectorConfig{
				dropped: {VectorIndexType: vectorindex.VectorIndexTypeNone},
				sibling: {VectorIndexType: "hnsw"},
			},
		}

		require.NoError(t, h.ensureFilesAreRemovedForDroppedVectorIndexes(indexPath, shardName, class))

		assert.True(t, pathExists(filepath.Join(indexPath, shardName, "lsm", helpers.GetVectorsBucketName(sibling))),
			"%s is a live vector's own bucket and must survive dropping %s", sibling, dropped)
		assert.False(t, pathExists(filepath.Join(indexPath, shardName, "lsm", helpers.GetVectorsBucketName(dropped))),
			"the dropped vector's own bucket must still go")
	})

	t.Run("dropped vector but no files on disk - no error", func(t *testing.T) {
		indexPath, shardName := setup(t)

		class := &models.Class{
			Class: "TestClass",
			VectorConfig: map[string]models.VectorConfig{
				"dropped": {VectorIndexType: vectorindex.VectorIndexTypeNone},
			},
		}

		err := h.ensureFilesAreRemovedForDroppedVectorIndexes(indexPath, shardName, class)
		require.NoError(t, err)
	})
}

// TestOtherTargetVectors_ProtectsTheLegacyVector pins that the legacy
// (unnamed, "") vector is treated as a sibling too: a mixed class (legacy
// vector plus named vectors, legal via a class update) can have a named
// vector whose artifacts collide with the legacy vector's — e.g. a named
// vector literally called "compressed" owns raw bucket "vectors_compressed",
// which is byte-identical to the legacy vector's quantized bucket. Before
// this fix, otherTargetVectors walked only class.VectorConfig, which never
// contains "", so the legacy vector was never in the protected set and
// dropping "compressed" would delete the legacy vector's quantized data.
// classWithCompressedSibling builds a TestClass whose VectorConfig always
// contains the named vector "compressed", optionally alongside a legacy
// (unnamed, "") vector and/or a second named vector "other" — the two axes
// the tests below vary independently.
func classWithCompressedSibling(legacy, withOther bool) *models.Class {
	vectorConfig := map[string]models.VectorConfig{
		"compressed": {VectorIndexType: "hnsw"},
	}
	if withOther {
		vectorConfig["other"] = models.VectorConfig{VectorIndexType: "hnsw"}
	}
	class := &models.Class{Class: "TestClass", VectorConfig: vectorConfig}
	if legacy {
		class.VectorIndexType = "hnsw"
	}
	return class
}

func TestOtherTargetVectors_ProtectsTheLegacyVector(t *testing.T) {
	t.Run("class with a legacy vector: \"\" is included as a sibling", func(t *testing.T) {
		class := classWithCompressedSibling(true, true)

		got := otherTargetVectors(class, "compressed")

		assert.Contains(t, got, "", "the legacy vector must be protected as a sibling")
		assert.Contains(t, got, "other")
		assert.NotContains(t, got, "compressed", "the excluded target itself must not be its own sibling")
	})

	t.Run("class with named vectors only: \"\" is not included", func(t *testing.T) {
		class := classWithCompressedSibling(false, true)

		got := otherTargetVectors(class, "compressed")

		assert.NotContains(t, got, "", "there is no legacy vector on this class")
		assert.Contains(t, got, "other")
	})
}

// TestOtherTargetVectors_ArtifactsForEndToEnd drives otherTargetVectors
// straight into VectorIndexArtifactsFor, the way the live drop and
// the file sweep both do, to prove the legacy vector's on-disk artifacts
// actually survive dropping a colliding named vector.
func TestOtherTargetVectors_ArtifactsForEndToEnd(t *testing.T) {
	t.Run("mixed class: dropping named vector \"compressed\" must not take the legacy vector's quantized bucket", func(t *testing.T) {
		class := classWithCompressedSibling(true, false)

		artifacts := VectorIndexArtifactsFor("compressed", otherTargetVectors(class, "compressed"))

		assert.NotContains(t, artifacts.LSMBuckets, "vectors_compressed",
			"vectors_compressed is the legacy vector's quantized bucket and must survive dropping the named vector \"compressed\"")
	})

	t.Run("named vectors only: no legacy sibling exists, so the named vector's own bucket is listed", func(t *testing.T) {
		class := classWithCompressedSibling(false, false)

		artifacts := VectorIndexArtifactsFor("compressed", otherTargetVectors(class, "compressed"))

		assert.Contains(t, artifacts.LSMBuckets, "vectors_compressed",
			"vectors_compressed is the named vector's own raw bucket and there is no legacy sibling to protect")
	})
}
