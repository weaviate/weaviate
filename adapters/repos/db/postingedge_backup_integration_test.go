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
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/objects"
)

// A replica movement copies the files of a posting-edge index that change in
// place, as a backup does: its meta, its journal, the slots of its vectors,
// built.u32 and its centroids are rewritten in place as the shard goes on
// writing, so a hardlink of one of them made while the shard was halted
// changes with it once the shard resumes, and the target would receive a meta
// and a journal of a later moment than the buckets it is sent. They are
// copied to the staging directory of the movement, which serves them.
func TestPostingEdge_ReplicaSnapshotCopiesWhatChangesInPlace(t *testing.T) {
	for _, tc := range []struct {
		name string
	}{
		{name: "hardlink mode"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const (
				dims  = 64
				base  = 300
				total = 500
			)
			ctx := context.Background()
			corpus := newPostingEdgeCorpus(total, dims, 3)
			class := postingEdgeTestClassOf(t, map[string]interface{}{
				"distance":         "cosine",
				"maxPostingSizeKB": float64(8),
			})
			repo := openTestDatabase(t, t.TempDir(), singleShardState(), monitoring.GetMetrics(), func(config *Config) {
				config.EnableLazyLoadShards = boolPtr(false)
				config.PostingEdgeBaseSize = base
			}, nil, class)
			defer func() { require.NoError(t, repo.Shutdown(context.Background())) }()

			batch := make(objects.BatchObjects, 0, total)
			for i := range total {
				batch = append(batch, objects.BatchObject{
					OriginalIndex: i,
					Object: &models.Object{
						Class:      postingEdgeTestClass,
						ID:         corpus.ids[i],
						Properties: map[string]interface{}{"name": fmt.Sprintf("object %d", i)},
						Vector:     corpus.vectors[i],
					},
					UUID: corpus.ids[i],
				})
			}
			res, err := repo.BatchPutObjects(ctx, batch, nil, 0)
			require.NoError(t, err)
			for _, object := range res {
				require.NoError(t, object.Err)
			}
			shard := postingEdgeTestShard(t, repo)
			dirs, err := filepath.Glob(filepath.Join(shard.path(), "*.postingedge.d"))
			require.NoError(t, err)
			require.Len(t, dirs, 1)
			dir, err := filepath.Rel(shard.path(), dirs[0])
			require.NoError(t, err)
			require.Eventually(t, func() bool {
				_, err := os.Stat(filepath.Join(dirs[0], "meta.json"))
				return err == nil
			}, 30*time.Second, 10*time.Millisecond, "the index builds its base")

			staging := t.TempDir()
			files, err := shard.CreateReplicaSnapshot(ctx, staging)
			require.NoError(t, err)
			for _, name := range []string{"meta.json", "journal.log", "slots-1.u32", "built.u32", "centroids.f32"} {
				rel := filepath.Join(dir, name)
				assert.Contains(t, files, rel)
				copied, err := os.Stat(filepath.Join(staging, rel))
				if !assert.NoError(t, err, "%s is in the staging directory", rel) {
					continue
				}
				live, err := os.Stat(filepath.Join(shard.path(), rel))
				require.NoError(t, err)
				assert.False(t, os.SameFile(copied, live), "%s is a copy, not a link", rel)
			}
		})
	}
}
