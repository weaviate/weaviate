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
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestEnableIndexSurvivesRepeatedLoadsBeforeSchemaFlip pins the invariant that
// a shard loaded any number of times between its local bucket swap and the
// cluster-wide schema flip must still serve the backfilled data once the flag
// flips. The flip only lands after every replica has swapped, so a node can be
// restarted arbitrarily often inside that window.
//
// The flip_before_loads row is the positive control: the identical sequence
// with the flag already flipped. It separates a real regression from a broken
// harness.
func TestEnableIndexSurvivesRepeatedLoadsBeforeSchemaFlip(t *testing.T) {
	const propName = "title"
	const numObjects = 25

	migrations := []struct {
		name        string
		bucketName  string
		newClass    func(className string) *models.Class
		newTask     func(t *testing.T, idx *Index, className string) *ShardReindexTaskGeneric
		fingerprint func(t *testing.T, b *lsmkv.Bucket) map[string][]uint64
		baseline    func(t *testing.T) map[string][]uint64
		flipSchema  func(class *models.Class)
	}{
		{
			name:       "enable-filterable",
			bucketName: helpers.BucketFromPropNameLSM(propName),
			newClass: func(className string) *models.Class {
				return newEnableFilterableTestClass(className, propName)
			},
			newTask: func(t *testing.T, idx *Index, className string) *ShardReindexTaskGeneric {
				task, _ := newEnableFilterableTask(t, idx, className, propName)
				return task
			},
			fingerprint: fingerprintRoaringSetBucket,
			baseline: func(t *testing.T) map[string][]uint64 {
				return computeEnableFilterableBaseline(t, propName, numObjects)
			},
			flipSchema: func(class *models.Class) {
				class.Properties[0].IndexFilterable = boolPtr(true)
			},
		},
		{
			name:       "enable-searchable",
			bucketName: helpers.BucketSearchableFromPropNameLSM(propName),
			newClass: func(className string) *models.Class {
				return newEnableSearchableTestClass(className, []string{propName})
			},
			newTask: func(t *testing.T, idx *Index, className string) *ShardReindexTaskGeneric {
				task, _ := newEnableSearchableTask(t, idx, className, propName,
					models.PropertyTokenizationWord)
				return task
			},
			fingerprint: fingerprintInvertedBucket,
			baseline: func(t *testing.T) map[string][]uint64 {
				return computeEnableSearchableBaseline(t, propName, numObjects)
			},
			flipSchema: func(class *models.Class) {
				class.Properties[0].IndexSearchable = boolPtr(true)
			},
		},
	}

	for _, mig := range migrations {
		t.Run(mig.name, func(t *testing.T) {
			baseline := mig.baseline(t)
			require.NotEmpty(t, baseline, "baseline fingerprint must be non-empty")

			for _, tc := range []struct {
				name            string
				flipBeforeLoads bool
			}{
				{name: "flip_after_loads"},
				{name: "flip_before_loads_control", flipBeforeLoads: true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					ctx := testCtx()
					className := "RepeatedLoadBeforeFlip_" + uuid.NewString()[:8]
					class := mig.newClass(className)

					shd, idx := testShardWithSettings(t, ctx, class,
						enthnsw.UserConfig{Skip: true}, false, false, false)
					shard := shd.(*Shard)
					shardName := shard.Name()
					lsmPath := shard.pathLSM()

					for _, obj := range makeConvergenceTestObjects(t, numObjects, className) {
						require.NoError(t, shard.PutObject(ctx, obj))
					}

					task := mig.newTask(t, idx, className)
					require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
					require.NoError(t, task.RunPrepareOnShard(ctx, shard))
					require.NoError(t, task.RunSwapOnShard(ctx, shard))
					require.NoError(t, shard.Shutdown(ctx))

					// No task is re-dispatched on load: this node's shard unit has already
					// completed and only the other replicas still hold up the flip.
					load := func() *Shard {
						idx.shardReindexer = NewShardReindexerV3Noop()
						loaded, err := idx.initShard(ctx, shardName, class, nil, true, true)
						require.NoError(t, err)
						idx.shards.Store(shardName, loaded)
						return loaded.(*Shard)
					}

					// The backfill lives either under the canonical name or under a
					// migration-suffixed variant of it, so match the whole family rather
					// than pin the moment one is promoted to the other.
					indexDirs := func() []string {
						entries, err := os.ReadDir(lsmPath)
						require.NoError(t, err)
						found := []string{}
						for _, e := range entries {
							if e.Name() == mig.bucketName ||
								strings.HasPrefix(e.Name(), mig.bucketName+"__") {
								found = append(found, e.Name())
							}
						}
						return found
					}

					if tc.flipBeforeLoads {
						mig.flipSchema(class)
					}

					require.NoError(t, load().Shutdown(ctx))
					require.NoError(t, load().Shutdown(ctx))

					require.NotEmptyf(t, indexDirs(),
						"the second load deleted the backfilled %q index from %s",
						mig.bucketName, lsmPath)

					if !tc.flipBeforeLoads {
						mig.flipSchema(class)
					}

					live := load()
					defer live.Shutdown(ctx)

					b := live.store.Bucket(mig.bucketName)
					require.NotNilf(t, b, "after the flip the %q bucket must be loaded", mig.bucketName)
					assert.Equal(t, baseline, mig.fingerprint(t, b),
						"after the flip the index must serve the backfilled data")
				})
			}
		})
	}
}
