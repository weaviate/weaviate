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

//go:build integrationTest

package db

import (
	"context"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/storobj"
	dynamicent "github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
	hfreshent "github.com/weaviate/weaviate/entities/vectorindex/hfresh"
	hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestPhysicalLayoutPin locks the on-disk names every vector index type
// produces. These names ARE the compatibility contract with already-shipped
// data: a failure here is an upgrade-breaking rename, never a test to update.
//
// Every path asserted below is a STRING LITERAL, never computed by calling
// the helpers under test (adapters/repos/db/helpers) - that would make the
// assertion tautological. A failure here means a real on-disk rename, not a
// test to "fix" by updating the literal.
func TestPhysicalLayoutPin(t *testing.T) {
	ctx := testCtx()

	t.Run("hnsw legacy", func(t *testing.T) {
		// BQ is enabled so an LSM bucket appears without needing to reach any
		// write-triggered threshold - see the "no raw bucket" note below.
		vic := hnswent.UserConfig{BQ: hnswent.BQConfig{Enabled: true}}
		shd, idx := testShardWithSettings(t, ctx, &models.Class{Class: "PinHNSWLegacy"}, vic, false, true, true)
		s := shd.(*Shard)
		defer removeRootPath(t, idx)

		putLegacyBatch(t, ctx, shd, "PinHNSWLegacy", 5, 4)
		drainQueue(t, shd, "")

		// shard-level dirs
		assertDirExists(t, filepath.Join(s.path(), "main.hnsw.commitlog.d"))
		assertDirExists(t, filepath.Join(s.path(), "main.queue.d"))

		// lsm dir: HNSW never stores a raw-vectors bucket of its own (unlike
		// flat/dynamic below) - object vectors live in the "objects" bucket and
		// are read back through VectorForIDThunk, so VectorsBucketNameForID's
		// "main" -> "vectors" mapping has no on-disk bucket to pin for pure
		// HNSW. What IS on disk, and what byte-compat depends on, is the
		// quantized bucket below, keyed off the same "main" -> "" suffix.
		assertDirExists(t, filepath.Join(s.pathLSM(), "vectors_compressed"))

		require.Nil(t, idx.drop())
	})

	t.Run("hnsw named", func(t *testing.T) {
		vic := hnswent.UserConfig{BQ: hnswent.BQConfig{Enabled: true}}
		shd, idx := testShardWithSettings(t, ctx, &models.Class{Class: "PinHNSWNamed"}, nil, false, true, true,
			func(i *Index) {
				i.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{"title": vic}
			},
		)
		s := shd.(*Shard)
		defer removeRootPath(t, idx)

		putNamedBatch(t, ctx, shd, "PinHNSWNamed", "title", 5, 4)
		drainQueue(t, shd, "title")

		assertDirExists(t, filepath.Join(s.path(), "vectors_title.hnsw.commitlog.d"))
		assertDirExists(t, filepath.Join(s.path(), "vectors_title.queue.d"))
		assertDirExists(t, filepath.Join(s.pathLSM(), "vectors_compressed_title"))

		require.Nil(t, idx.drop())
	})

	t.Run("flat legacy", func(t *testing.T) {
		fuc := flatent.UserConfig{}
		fuc.SetDefaults()
		shd, idx := testShardWithSettings(t, ctx, &models.Class{Class: "PinFlatLegacy"}, fuc, false, true, true)
		s := shd.(*Shard)
		defer removeRootPath(t, idx)

		putLegacyBatch(t, ctx, shd, "PinFlatLegacy", 5, 4)
		drainQueue(t, shd, "")

		// legacy asymmetry: the index id is "main" but the raw bucket is the
		// bare "vectors", and the metadata file has no suffix
		assertDirExists(t, filepath.Join(s.pathLSM(), "vectors"))
		assertDirExists(t, filepath.Join(s.path(), "main.queue.d"))
		assertFileExists(t, filepath.Join(s.path(), "meta.db"))

		require.Nil(t, idx.drop())
	})

	t.Run("flat named", func(t *testing.T) {
		fuc := flatent.UserConfig{}
		fuc.SetDefaults()
		shd, idx := testShardWithSettings(t, ctx, &models.Class{Class: "PinFlatNamed"}, nil, false, true, true,
			func(i *Index) {
				i.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{"title": fuc}
			},
		)
		s := shd.(*Shard)
		defer removeRootPath(t, idx)

		putNamedBatch(t, ctx, shd, "PinFlatNamed", "title", 5, 4)
		drainQueue(t, shd, "title")

		assertDirExists(t, filepath.Join(s.pathLSM(), "vectors_title"))
		assertDirExists(t, filepath.Join(s.path(), "vectors_title.queue.d"))
		assertFileExists(t, filepath.Join(s.path(), "meta_title.db"))

		require.Nil(t, idx.drop())
	})

	t.Run("dynamic legacy", func(t *testing.T) {
		duc := dynamicent.UserConfig{}
		duc.SetDefaults()
		shd, idx := testShardWithSettings(t, ctx, &models.Class{Class: "PinDynamicLegacy"}, duc, false, true, true)
		s := shd.(*Shard)
		defer removeRootPath(t, idx)

		putLegacyBatch(t, ctx, shd, "PinDynamicLegacy", 5, 4)
		drainQueue(t, shd, "")

		assertDirExists(t, filepath.Join(s.pathLSM(), "vectors"))
		assertDirExists(t, filepath.Join(s.path(), "main.queue.d"))
		assertFileExists(t, filepath.Join(s.path(), "meta.db"))

		require.Nil(t, idx.drop())
	})

	t.Run("dynamic named", func(t *testing.T) {
		duc := dynamicent.UserConfig{}
		duc.SetDefaults()
		shd, idx := testShardWithSettings(t, ctx, &models.Class{Class: "PinDynamicNamed"}, nil, false, true, true, /* async indexing required for dynamic */
			func(i *Index) {
				i.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{"title": duc}
			},
		)
		s := shd.(*Shard)
		defer removeRootPath(t, idx)

		putNamedBatch(t, ctx, shd, "PinDynamicNamed", "title", 5, 4)
		drainQueue(t, shd, "title")

		// dynamic starts out backed by flat until the upgrade threshold, so
		// the raw bucket it owns is the same one flat owns.
		assertDirExists(t, filepath.Join(s.pathLSM(), "vectors_title"))
		assertDirExists(t, filepath.Join(s.path(), "vectors_title.queue.d"))
		// the delegated flat stage's metadata file is dynamic's too
		assertFileExists(t, filepath.Join(s.path(), "meta_title.db"))

		require.Nil(t, idx.drop())
	})

	t.Run("hfresh legacy", func(t *testing.T) {
		huc := hfreshent.UserConfig{}
		huc.SetDefaults()
		shd, idx := testShardWithSettings(t, ctx, &models.Class{Class: "PinHFreshLegacy"}, huc, false, true, true)
		s := shd.(*Shard)
		defer removeRootPath(t, idx)

		putLegacyBatch(t, ctx, shd, "PinHFreshLegacy", 5, 4)
		drainQueue(t, shd, "")

		assertDirExists(t, filepath.Join(s.path(), "main.hfresh.d"))
		// the centroid graph is a nested hnsw with its own physical id
		assertDirExists(t, filepath.Join(s.path(), "main.hfresh.d", "main_centroids.hnsw.commitlog.d"))
		assertDirExists(t, filepath.Join(s.pathLSM(), "hfresh_postings_main"))
		assertDirExists(t, filepath.Join(s.pathLSM(), "hfresh_shared_main"))

		require.Nil(t, idx.drop())
	})

	t.Run("hfresh named", func(t *testing.T) {
		huc := hfreshent.UserConfig{}
		huc.SetDefaults()
		shd, idx := testShardWithSettings(t, ctx, &models.Class{Class: "PinHFreshNamed"}, nil, false, true, true,
			func(i *Index) {
				i.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{"title": huc}
			},
		)
		s := shd.(*Shard)
		defer removeRootPath(t, idx)

		putNamedBatch(t, ctx, shd, "PinHFreshNamed", "title", 5, 4)
		drainQueue(t, shd, "title")

		assertDirExists(t, filepath.Join(s.path(), "vectors_title.hfresh.d"))
		assertDirExists(t, filepath.Join(s.path(), "vectors_title.hfresh.d", "vectors_title_centroids.hnsw.commitlog.d"))
		// the centroid graph's RQ bucket lives in the shard's lsm dir under
		// the centroid id's suffix; it only exists once vectors reached hfresh
		assertDirExists(t, filepath.Join(s.pathLSM(), "vectors_compressed_title_centroids"))
		assertDirExists(t, filepath.Join(s.pathLSM(), "hfresh_postings_vectors_title"))
		assertDirExists(t, filepath.Join(s.pathLSM(), "hfresh_shared_vectors_title"))

		require.Nil(t, idx.drop())
	})
}

func putLegacyBatch(t *testing.T, ctx context.Context, shd ShardLike, className string, n, dim int) {
	t.Helper()
	r := rand.New(rand.NewSource(1))
	objs := createRandomObjects(r, className, n, dim)
	errs := shd.PutObjectBatch(ctx, objs)
	for _, err := range errs {
		require.Nil(t, err)
	}
}

func putNamedBatch(t *testing.T, ctx context.Context, shd ShardLike, className, targetVector string, n, dim int) {
	t.Helper()
	r := rand.New(rand.NewSource(1))
	objs := make([]*storobj.Object, n)
	for i := 0; i < n; i++ {
		vec := make([]float32, dim)
		for d := 0; d < dim; d++ {
			vec[d] = r.Float32()
		}
		objs[i] = &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:    strfmt.UUID(uuid.NewString()),
				Class: className,
			},
			Vectors: map[string][]float32{targetVector: vec},
		}
	}
	errs := shd.PutObjectBatch(ctx, objs)
	for _, err := range errs {
		require.Nil(t, err)
	}
}

// drainQueue waits for the async vector index queue of targetVector to hand
// every queued vector to its index: the write-triggered artifacts below only
// exist once the index has actually seen the vectors.
func drainQueue(t *testing.T, shd ShardLike, targetVector string) {
	t.Helper()
	q, release, ok := shd.AcquireVectorIndexQueue(targetVector)
	require.True(t, ok)
	defer release()
	require.True(t, ok, "no queue for target vector %q", targetVector)
	require.Eventually(t, func() bool { return q.Size() == 0 }, 30*time.Second, 50*time.Millisecond)
}

func assertDirExists(t *testing.T, path string) {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err, "expected on-disk artifact %s", path)
	assert.True(t, info.IsDir(), "expected %s to be a directory", path)
}

func assertFileExists(t *testing.T, path string) {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err, "expected on-disk artifact %s", path)
	assert.False(t, info.IsDir(), "expected %s to be a file", path)
}
