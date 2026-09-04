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
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/vectorindex"
	dynamicent "github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
	hfreshent "github.com/weaviate/weaviate/entities/vectorindex/hfresh"
	hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
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
		muveraBucket := filepath.Join(lsm, helpers.MuveraBucketName(helpers.GetVectorsBucketName(target)))
		commitLog := filepath.Join(indexPath, shardName, helpers.GetHNSWCommitLogDirName(target))

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
		postings := filepath.Join(lsm, helpers.HFreshPostingsBucketName(indexID))
		shared := filepath.Join(lsm, helpers.HFreshSharedBucketName(indexID))
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

			require.NoError(t, h.removeVectorIndexFiles(indexPath, shardName, dropped, []helpers.SiblingVector{{Name: sibling}}))

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
			helpers.MuveraBucketName(helpers.GetVectorsBucketName(dropped)),
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

func TestSiblingVectors(t *testing.T) {
	bq := hnswent.UserConfig{BQ: hnswent.BQConfig{Enabled: true}}
	named := map[string]models.VectorConfig{
		"a": {VectorIndexType: "hnsw", VectorIndexConfig: hnswent.UserConfig{}},
		"b": {VectorIndexType: "flat", VectorIndexConfig: flatent.UserConfig{BQ: flatent.CompressionUserConfig{Enabled: true}}},
	}
	tests := []struct {
		name    string
		class   *models.Class
		exclude string
		want    []helpers.SiblingVector
	}{
		{name: "nil class protects nothing", class: nil, exclude: "a", want: nil},
		{
			name:    "named vectors only",
			class:   &models.Class{VectorConfig: named},
			exclude: "a",
			want:    []helpers.SiblingVector{{Name: "b", Quantized: true}},
		},
		{
			name:    "legacy vector next to named vectors is a sibling",
			class:   &models.Class{VectorIndexType: "hnsw", VectorIndexConfig: bq, VectorConfig: named},
			exclude: "a",
			want:    []helpers.SiblingVector{{Name: "", Quantized: true}, {Name: "b", Quantized: true}},
		},
		{
			name:    "the legacy vector is never its own sibling",
			class:   &models.Class{VectorIndexType: "hnsw", VectorIndexConfig: bq, VectorConfig: named},
			exclude: "",
			want:    []helpers.SiblingVector{{Name: "a"}, {Name: "b", Quantized: true}},
		},
		{
			name:    "an uncompressed legacy hnsw cannot own a compressed bucket",
			class:   &models.Class{VectorIndexType: "hnsw", VectorIndexConfig: hnswent.UserConfig{}, VectorConfig: named},
			exclude: "b",
			want:    []helpers.SiblingVector{{Name: ""}, {Name: "a"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.ElementsMatch(t, tt.want, siblingVectors(tt.class, tt.exclude))
		})
	}
}

func TestCanOwnCompressedBucket(t *testing.T) {
	tests := []struct {
		name      string
		indexType string
		cfg       interface{}
		want      bool
	}{
		{"hnsw without a quantizer", "hnsw", hnswent.UserConfig{}, false},
		{"hnsw with pq", "hnsw", hnswent.UserConfig{PQ: hnswent.PQConfig{Enabled: true}}, true},
		{"hnsw with rq", "hnsw", hnswent.UserConfig{RQ: hnswent.RQConfig{Enabled: true}}, true},
		{"flat without a quantizer", "flat", flatent.UserConfig{}, false},
		{"flat with rq", "flat", flatent.UserConfig{RQ: flatent.RQUserConfig{Enabled: true}}, true},
		{"dynamic without a quantizer", "dynamic", dynamicent.UserConfig{}, false},
		{"dynamic whose hnsw stage has sq", "dynamic", dynamicent.UserConfig{HnswUC: hnswent.UserConfig{SQ: hnswent.SQConfig{Enabled: true}}}, true},
		{"dynamic whose flat stage has bq", "dynamic", dynamicent.UserConfig{FlatUC: flatent.UserConfig{BQ: flatent.CompressionUserConfig{Enabled: true}}}, true},
		{"hfresh always runs a quantized centroid graph", "hfresh", hfreshent.UserConfig{}, true},
		{"unreadable config is treated as quantized", "hnsw", map[string]interface{}{}, true},
		{"nil config is treated as quantized", "hnsw", nil, true},
		{"unknown index type is treated as quantized", "none", nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, canOwnCompressedBucket(tt.indexType, tt.cfg))
		})
	}
}

// TestVectorDropIndexHelper_SweepSparesTheLegacyVector pins the data-loss
// journey: a class with a legacy vector gains a named vector whose artifacts
// collide with the legacy vector's, then drops it. The sweep must take only
// what the named vector owns.
func TestVectorDropIndexHelper_SweepSparesTheLegacyVector(t *testing.T) {
	h := newVectorDropIndexHelper()

	mixedClass := func(dropped string, legacyConfig hnswent.UserConfig) *models.Class {
		return &models.Class{
			Class:             "Mixed",
			VectorIndexType:   "hnsw",
			VectorIndexConfig: legacyConfig,
			VectorConfig: map[string]models.VectorConfig{
				dropped: {VectorIndexType: vectorindex.VectorIndexTypeNone},
			},
		}
	}
	bq := hnswent.UserConfig{BQ: hnswent.BQConfig{Enabled: true}}
	pathExists := func(p string) bool {
		_, err := os.Stat(p)
		return err == nil
	}
	mkdirs := func(t *testing.T, base string, names ...string) []string {
		t.Helper()
		paths := make([]string, 0, len(names))
		for _, name := range names {
			p := filepath.Join(base, name)
			require.NoError(t, os.MkdirAll(p, 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(p, "segment.db"), []byte("live"), 0o644))
			paths = append(paths, p)
		}
		return paths
	}

	tests := []struct {
		name       string
		dropped    string
		legacy     hnswent.UserConfig
		legacyLSM  []string // buckets the legacy index owns that must survive
		droppedLSM []string // buckets the dropped vector owns that must go
	}{
		{
			// "vectors_compressed" is both the named vector's raw bucket and
			// the quantized legacy vector's compressed bucket: it stays, the
			// rest goes
			name:       "named vector called compressed next to a quantized legacy vector",
			dropped:    "compressed",
			legacy:     bq,
			legacyLSM:  []string{"vectors", "vectors_compressed"},
			droppedLSM: []string{"vectors_compressed_compressed"},
		},
		{
			// an uncompressed legacy hnsw never wrote "vectors_compressed", so
			// the bucket is the named vector's alone and has to go with it
			name:       "named vector called compressed next to an uncompressed legacy vector",
			dropped:    "compressed",
			legacyLSM:  []string{"vectors"},
			droppedLSM: []string{"vectors_compressed", "vectors_compressed_compressed"},
		},
		{
			// no real collision: the legacy muvera bucket is main_muvera_vectors
			name:       "named vector called muvera_vectors",
			dropped:    "muvera_vectors",
			legacy:     bq,
			legacyLSM:  []string{"vectors", "vectors_compressed", "main_muvera_vectors"},
			droppedLSM: []string{"vectors_muvera_vectors", "vectors_compressed_muvera_vectors"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			indexPath, shardName := t.TempDir(), "shard1"
			lsm := filepath.Join(indexPath, shardName, "lsm")
			legacy := mkdirs(t, lsm, tt.legacyLSM...)
			legacy = append(legacy, mkdirs(t, filepath.Join(indexPath, shardName), "main.hnsw.commitlog.d", "main.queue.d")...)
			gone := mkdirs(t, lsm, tt.droppedLSM...)
			gone = append(gone, mkdirs(t, filepath.Join(indexPath, shardName),
				"vectors_"+tt.dropped+".hnsw.commitlog.d", "vectors_"+tt.dropped+".queue.d")...)

			require.NoError(t, h.ensureFilesAreRemovedForDroppedVectorIndexes(indexPath, shardName, mixedClass(tt.dropped, tt.legacy)))

			for _, p := range legacy {
				assert.True(t, pathExists(p), "%s belongs to the legacy vector and must survive", p)
			}
			for _, p := range gone {
				assert.False(t, pathExists(p), "%s belongs to the dropped vector and must go", p)
			}
		})
	}
}
