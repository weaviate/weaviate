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
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestEnableIndexSurvivesRepeatedLoadsBeforeSchemaFlip pins the invariant that
// a shard loaded any number of times between its local bucket swap and the
// schema flag flip must still serve the backfilled data once the flag flips.
// Every load in that window must leave the index both on disk and open: a
// bucket that exists but is not loaded is invisible to reads, and to the
// active-shard backup path, which enumerates loaded buckets only.
//
// The flip_before_loads row is the positive control: the identical sequence
// with the flag already flipped. It separates a real regression from a broken
// harness.
func TestEnableIndexSurvivesRepeatedLoadsBeforeSchemaFlip(t *testing.T) {
	const numObjects = 25

	// enable-filterable and enable-searchable are dispatched as a task trio and
	// wait for a cluster-wide flip; the rangeable strategy runs inline at shard
	// init and flips from its own completion, which is a narrower window.
	runTrio := func(t *testing.T, ctx context.Context, shard *Shard, task *ShardReindexTaskGeneric) {
		require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
		require.NoError(t, task.RunPrepareOnShard(ctx, shard))
		require.NoError(t, task.RunSwapOnShard(ctx, shard))
	}
	runInline := func(t *testing.T, ctx context.Context, shard *Shard, task *ShardReindexTaskGeneric) {
		require.NoError(t, task.OnAfterLsmInit(ctx, shard))
		for {
			rerunAt, _, err := task.OnAfterLsmInitAsync(ctx, shard)
			require.NoError(t, err)
			if rerunAt.IsZero() {
				break
			}
		}
	}

	migrations := []struct {
		name            string
		bucketName      string
		newClass        func(className string) *models.Class
		makeObjects     func(t *testing.T, className string) []*storobj.Object
		newTask         func(t *testing.T, idx *Index, className string) *ShardReindexTaskGeneric
		driveToPostSwap func(t *testing.T, ctx context.Context, shard *Shard, task *ShardReindexTaskGeneric)
		fingerprint     func(t *testing.T, b *lsmkv.Bucket) any
		baseline        func(t *testing.T) any
		flipSchema      func(class *models.Class)
	}{
		{
			name:       "enable-filterable",
			bucketName: helpers.BucketFromPropNameLSM("title"),
			newClass: func(className string) *models.Class {
				return newEnableFilterableTestClass(className, "title")
			},
			makeObjects: func(t *testing.T, className string) []*storobj.Object {
				return makeConvergenceTestObjects(t, numObjects, className)
			},
			newTask: func(t *testing.T, idx *Index, className string) *ShardReindexTaskGeneric {
				task, _ := newEnableFilterableTask(t, idx, className, "title")
				return task
			},
			driveToPostSwap: runTrio,
			fingerprint: func(t *testing.T, b *lsmkv.Bucket) any {
				return fingerprintRoaringSetBucket(t, b)
			},
			baseline: func(t *testing.T) any {
				return computeEnableFilterableBaseline(t, "title", numObjects)
			},
			flipSchema: func(class *models.Class) {
				class.Properties[0].IndexFilterable = boolPtr(true)
			},
		},
		{
			name:       "enable-searchable",
			bucketName: helpers.BucketSearchableFromPropNameLSM("title"),
			newClass: func(className string) *models.Class {
				return newEnableSearchableTestClass(className, []string{"title"})
			},
			makeObjects: func(t *testing.T, className string) []*storobj.Object {
				return makeConvergenceTestObjects(t, numObjects, className)
			},
			newTask: func(t *testing.T, idx *Index, className string) *ShardReindexTaskGeneric {
				task, _ := newEnableSearchableTask(t, idx, className, "title",
					models.PropertyTokenizationWord)
				return task
			},
			driveToPostSwap: runTrio,
			fingerprint: func(t *testing.T, b *lsmkv.Bucket) any {
				return fingerprintInvertedBucket(t, b)
			},
			baseline: func(t *testing.T) any {
				return computeEnableSearchableBaseline(t, "title", numObjects)
			},
			flipSchema: func(class *models.Class) {
				class.Properties[0].IndexSearchable = boolPtr(true)
			},
		},
		{
			name:       "enable-rangeable",
			bucketName: helpers.BucketRangeableFromPropNameLSM(filterableToRangeablePropName),
			newClass: func(className string) *models.Class {
				class := newFilterableToRangeableTestClass(className)
				// Only an explicit false arms the init sweep; the fixture's nil would
				// leave this row green for the wrong reason.
				class.Properties[0].IndexRangeFilters = boolPtr(false)
				return class
			},
			makeObjects: func(t *testing.T, className string) []*storobj.Object {
				return makeFilterableToRangeableTestObjects(t, numObjects, className)
			},
			newTask: func(t *testing.T, idx *Index, className string) *ShardReindexTaskGeneric {
				task, _ := newFilterableToRangeableTask(t, idx, className,
					filterableToRangeablePropName)
				return task
			},
			driveToPostSwap: runInline,
			fingerprint: func(t *testing.T, b *lsmkv.Bucket) any {
				return filterableToRangeableFingerprint(t, b)
			},
			baseline: func(t *testing.T) any {
				return computeFilterableToRangeableBaseline(t,
					filterableToRangeablePropName, numObjects)
			},
			flipSchema: func(class *models.Class) {
				class.Properties[0].IndexRangeFilters = boolPtr(true)
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

					for _, obj := range mig.makeObjects(t, className) {
						require.NoError(t, shard.PutObject(ctx, obj))
					}

					mig.driveToPostSwap(t, ctx, shard, mig.newTask(t, idx, className))
					require.NoError(t, shard.Shutdown(ctx))

					// No task is re-dispatched on load: this node's shard unit has already
					// completed and only the flag flip is outstanding.
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

					loadAndCheck := func(label string) *Shard {
						live := load()
						require.NotEmptyf(t, indexDirs(),
							"%s deleted the backfilled %q index from %s",
							label, mig.bucketName, lsmPath)
						assert.NotNilf(t, live.store.Bucket(mig.bucketName),
							"%s: the %q bucket must be open, not merely present on disk",
							label, mig.bucketName)
						return live
					}

					if tc.flipBeforeLoads {
						mig.flipSchema(class)
					}

					// Three rounds rather than two: a fix that carries state across
					// restarts is most likely to mishandle it on the boot after the one
					// that wrote it, which only a third round can reach.
					for round := 1; round <= 3; round++ {
						require.NoError(t, loadAndCheck(fmt.Sprintf("load %d", round)).Shutdown(ctx))
					}

					if !tc.flipBeforeLoads {
						mig.flipSchema(class)
					}

					live := loadAndCheck("load 4 (after the flip)")
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
