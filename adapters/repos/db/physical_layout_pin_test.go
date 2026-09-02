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
		shd, idx := testShardWithNamedVector(t, ctx, "PinHNSWNamed", vic)
		s := shd.(*Shard)
		defer removeRootPath(t, idx)

		putNamedBatch(t, ctx, shd, "PinHNSWNamed", "title", 5, 4)

		assertDirExists(t, filepath.Join(s.path(), "vectors_title.hnsw.commitlog.d"))
		assertDirExists(t, filepath.Join(s.path(), "vectors_title.queue.d"))
		assertDirExists(t, filepath.Join(s.pathLSM(), "vectors_compressed_title"))

		require.Nil(t, idx.drop())
	})

	t.Run("flat named", func(t *testing.T) {
		fuc := flatent.UserConfig{}
		fuc.SetDefaults()
		shd, idx := testShardWithNamedVector(t, ctx, "PinFlatNamed", fuc)
		s := shd.(*Shard)
		defer removeRootPath(t, idx)

		putNamedBatch(t, ctx, shd, "PinFlatNamed", "title", 5, 4)

		assertDirExists(t, filepath.Join(s.pathLSM(), "vectors_title"))
		assertDirExists(t, filepath.Join(s.path(), "vectors_title.queue.d"))
		assertFileExists(t, filepath.Join(s.path(), "meta_title.db"))

		require.Nil(t, idx.drop())
	})

	t.Run("dynamic named", func(t *testing.T) {
		duc := dynamicent.UserConfig{}
		duc.SetDefaults()
		shd, idx := testShardWithNamedVector(t, ctx, "PinDynamicNamed", duc)
		s := shd.(*Shard)
		defer removeRootPath(t, idx)

		putNamedBatch(t, ctx, shd, "PinDynamicNamed", "title", 5, 4)

		// dynamic starts out backed by flat until the upgrade threshold, so
		// the raw bucket it owns is the same one flat owns.
		assertDirExists(t, filepath.Join(s.pathLSM(), "vectors_title"))
		assertDirExists(t, filepath.Join(s.path(), "vectors_title.queue.d"))

		require.Nil(t, idx.drop())
	})

	t.Run("hfresh named", func(t *testing.T) {
		huc := hfreshent.UserConfig{}
		huc.SetDefaults()
		shd, idx := testShardWithNamedVector(t, ctx, "PinHFreshNamed", huc)
		s := shd.(*Shard)
		defer removeRootPath(t, idx)

		putNamedBatch(t, ctx, shd, "PinHFreshNamed", "title", 5, 4)

		assertDirExists(t, filepath.Join(s.path(), "vectors_title.hfresh.d"))
		assertDirExists(t, filepath.Join(s.pathLSM(), "hfresh_postings_vectors_title"))
		assertDirExists(t, filepath.Join(s.pathLSM(), "hfresh_shared_vectors_title"))

		require.Nil(t, idx.drop())
	})
}

// testShardWithNamedVector builds a shard whose only vector index is the
// named vector "title", configured with vic. All four "named" subtests above
// share this scaffolding; only the class name and the vector index's own
// settings differ between them.
func testShardWithNamedVector(t *testing.T, ctx context.Context, className string,
	vic schemaConfig.VectorIndexConfig,
) (ShardLike, *Index) {
	t.Helper()
	return testShardWithSettings(t, ctx, &models.Class{Class: className}, nil, false, true, true, /* async indexing required for dynamic */
		func(i *Index) {
			i.vectorIndexUserConfigs = map[string]schemaConfig.VectorIndexConfig{"title": vic}
		},
	)
}

func removeRootPath(t *testing.T, idx *Index) {
	t.Helper()
	require.Nil(t, os.RemoveAll(idx.Config.RootPath))
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
