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
	"path/filepath"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// crashImageVectorIndex takes the crash image once the vector index shut down, before the object store flushes.
type crashImageVectorIndex struct {
	VectorIndex
	image func()
}

func (c crashImageVectorIndex) Shutdown(ctx context.Context) error {
	err := c.VectorIndex.Shutdown(ctx)
	c.image()
	return err
}

type crashImageRepo struct {
	repo  *DB
	index *Index
	shard string
}

func openCrashImageRepo(t *testing.T, root string, state *sharding.State, class *models.Class) crashImageRepo {
	t.Helper()
	repo, migrator, schemaGetter, _ := newLazyLoadRepoWithConfig(t, state, true, -1,
		func(c *Config) { c.RootPath = root }, withShardUnloader(newFakeShardUnloader()))
	t.Cleanup(func() { require.NoError(t, repo.Shutdown(context.Background())) })
	schemaGetter.schema = schema.Schema{Objects: &models.Schema{Classes: []*models.Class{class}}}
	require.NoError(t, migrator.AddClass(testCtx(), class))
	index := repo.GetIndex(schema.ClassName(class.Class))
	require.NotNil(t, index)
	r := crashImageRepo{repo: repo, index: index}
	require.NoError(t, index.ForEachShard(func(name string, _ ShardLike) error { r.shard = name; return nil }))
	require.NotEmpty(t, r.shard)
	return r
}

func (r crashImageRepo) withShard(t *testing.T, fn func(ShardLike)) {
	t.Helper()
	shard, release, err := r.index.GetShard(testCtx(), r.shard)
	require.NoError(t, err)
	defer release()
	fn(shard)
}

func TestLazyLoadShard_CrashImageDuringUnloadKeepsAcknowledgedWrites(t *testing.T) {
	tests := []struct {
		name string
		arm  func(t *testing.T, lazy *LazyLoadShard, image func())
	}{
		{name: "shut marked before teardown", arm: func(t *testing.T, lazy *LazyLoadShard, image func()) {
			lazy.unloadMarkedHook = image
		}},
		{name: "vector index shut down before the store flush", arm: func(t *testing.T, lazy *LazyLoadShard, image func()) {
			replaceLegacyVectorIndex(t, lazy.currentShard(), func(v VectorIndex) VectorIndex {
				return crashImageVectorIndex{VectorIndex: v, image: image}
			})
		}},
		{name: "teardown finished", arm: func(t *testing.T, lazy *LazyLoadShard, image func()) {}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			class := newClassWithWarmProp("CrashImage")
			root, crashRoot, state := t.TempDir(), filepath.Join(t.TempDir(), "crash"), singleShardState()
			live := openCrashImageRepo(t, root, state, class)

			var kept, deleted []strfmt.UUID
			live.withShard(t, func(s ShardLike) {
				for i := range 40 {
					obj := testObject(class.Class)
					obj.Object.Properties = map[string]any{"warm": "acknowledged"}
					obj.Vector = []float32{rand.Float32(), rand.Float32(), rand.Float32(), rand.Float32()}
					require.NoError(t, s.PutObject(testCtx(), obj))
					if i%4 == 0 {
						require.NoError(t, s.DeleteObject(testCtx(), obj.ID(), time.Now()))
						deleted = append(deleted, obj.ID())
						continue
					}
					kept = append(kept, obj.ID())
				}
			})

			lazy, ok := live.index.shards.Load(live.shard).(*LazyLoadShard)
			require.True(t, ok)
			imaged := false
			image := func() {
				require.False(t, imaged, "one crash image per unload")
				imaged = true
				copyDirTree(t, root, crashRoot)
			}
			tt.arm(t, lazy, image)
			require.True(t, lazy.TryUnload(testCtx(), UnloadRequest{}))
			if !imaged {
				image()
			}

			restarted := openCrashImageRepo(t, crashRoot, state, class)
			restarted.withShard(t, func(s ShardLike) {
				for _, id := range kept {
					obj, err := s.ObjectByID(testCtx(), id, nil, additional.Properties{})
					require.NoError(t, err)
					require.NotNil(t, obj, "acknowledged write %s lost", id)
					requireWarmProp(t, obj)
				}
				for _, id := range deleted {
					ok, err := s.Exists(testCtx(), id)
					require.NoError(t, err)
					require.False(t, ok, "acknowledged delete %s resurrected", id)
				}
			})
		})
	}
}

func requireWarmProp(t *testing.T, obj *storobj.Object) {
	t.Helper()
	props, ok := obj.Object.Properties.(map[string]any)
	require.True(t, ok)
	require.Equal(t, "acknowledged", props["warm"])
}
