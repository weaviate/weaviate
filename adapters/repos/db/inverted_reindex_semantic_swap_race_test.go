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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/entities/additional"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// A write is lost if its analysis spans the whole swap. Analysis time grows
// with the text, so a long text keeps writers inside that window often enough
// that a few swaps reliably hit it.
const (
	swapRaceFillerWords = 200_000
	swapRaceWriters     = 6
	swapRaceCycles      = 10
)

func swapRaceFiller() string {
	vocab := []string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"}
	words := make([]string, swapRaceFillerWords)
	for i := range words {
		words[i] = vocab[i%len(vocab)]
	}
	return strings.Join(words, " ")
}

func TestWritesRacingSemanticSwapStayIndexed(t *testing.T) {
	const propName = "title"
	filler := swapRaceFiller()

	tests := []struct {
		name      string
		newClass  func(className, propName string) *models.Class
		migration ReindexMigrationType
		newTask   func(*testing.T, *Index, string, string) *ShardReindexTaskGeneric
		flip      func(prop *models.Property)
		find      func(*testing.T, context.Context, *Shard, string, string, int) []*storobj.Object
		// Only the filterable bitmap can show a deleted docID; a search
		// drops it once the object itself is gone.
		checkDeletes bool
	}{
		{
			name:      "enable-filterable",
			newClass:  newEnableFilterableTestClass,
			migration: ReindexTypeEnableFilterable,
			newTask: func(t *testing.T, idx *Index, className, propName string) *ShardReindexTaskGeneric {
				task, _ := newEnableFilterableTask(t, idx, className, propName)
				return task
			},
			flip:         func(prop *models.Property) { prop.IndexFilterable = boolPtr(true) },
			find:         findAllByEqualFilter,
			checkDeletes: true,
		},
		{
			name: "enable-searchable",
			newClass: func(className, propName string) *models.Class {
				return newEnableSearchableTestClass(className, []string{propName})
			},
			migration: ReindexTypeEnableSearchable,
			newTask: func(t *testing.T, idx *Index, className, propName string) *ShardReindexTaskGeneric {
				task, _ := newEnableSearchableTask(t, idx, className, propName,
					models.PropertyTokenizationWord)
				return task
			},
			flip: func(prop *models.Property) { prop.IndexSearchable = boolPtr(true) },
			find: findAllByBM25,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for cycle := 0; cycle < swapRaceCycles; cycle++ {
				ctx := testCtx()
				className := "SwapRace_" + uuid.NewString()[:8]
				class := tc.newClass(className, propName)

				shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
					true, false, false)
				shard := shd.(*Shard)
				t.Cleanup(func() { shard.Shutdown(context.Background()) })

				for _, obj := range makeConvergenceTestObjects(t, semanticWindowSeedObjects, className) {
					require.NoError(t, shard.PutObject(ctx, obj))
				}
				task := tc.newTask(t, idx, className, propName)
				require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
				require.NoError(t, task.RunPrepareOnShard(ctx, shard))

				kept, deleted := runSwapUnderWriteLoad(t, ctx, shard, className, propName,
					semanticWindowToken+" "+filler, func() {
						logger, _ := logrustest.NewNullLogger()
						p := &ReindexProvider{logger: logger, localNode: "node1", serverCtx: ctx}
						res := p.runShardSwapPhase(ctx, &ReindexTaskPayload{
							MigrationType: tc.migration,
							Collection:    className,
							Properties:    []string{propName},
						}, "unit-1", shard.Name(), shard, []*ShardReindexTaskGeneric{task}, logger)
						require.Empty(t, res.Errs)
					})
				tc.flip(class.Properties[0])

				found := tc.find(t, ctx, shard, className, propName, len(kept)+len(deleted)+1)
				assert.ElementsMatch(t, objectIDs(kept), objectIDs(found),
					"cycle %d: an acknowledged write is missing from the migrated index", cycle)

				if tc.checkDeletes {
					bucket := shard.store.Bucket(helpers.BucketFromPropNameLSM(propName))
					require.NotNil(t, bucket)
					bm, release, err := bucket.RoaringSetGet(ctx, []byte(semanticWindowToken))
					require.NoError(t, err)
					for _, obj := range deleted {
						assert.False(t, bm.Contains(obj.DocID),
							"cycle %d: a deleted docID is still in the migrated index", cycle)
					}
					release()
				}
			}
		})
	}
}

// runSwapUnderWriteLoad runs swap while writers put objects and delete every
// other one, and returns the objects still stored and those deleted.
func runSwapUnderWriteLoad(t *testing.T, ctx context.Context, shard *Shard,
	className, propName, text string, swap func(),
) (kept, deleted []*storobj.Object) {
	t.Helper()

	var (
		mu      sync.Mutex
		stop    atomic.Bool
		started sync.WaitGroup
		done    sync.WaitGroup
		errs    = make(chan error, swapRaceWriters)
	)
	started.Add(swapRaceWriters)
	done.Add(swapRaceWriters)
	logger, _ := logrustest.NewNullLogger()
	for w := 0; w < swapRaceWriters; w++ {
		enterrors.GoWrapper(func() {
			defer done.Done()
			first := true
			for i := 0; !stop.Load(); i++ {
				obj := createTestObjectWithText(className, text)
				if err := shard.PutObject(ctx, obj); err != nil {
					errs <- err
					return
				}
				// Staggered so deletes as well as puts are in flight at the swap.
				if (i+w)%2 == 0 {
					if err := shard.DeleteObject(ctx, obj.ID(), time.Now()); err != nil {
						errs <- err
						return
					}
				}
				mu.Lock()
				if (i+w)%2 == 0 {
					deleted = append(deleted, obj)
				} else {
					kept = append(kept, obj)
				}
				mu.Unlock()
				if first {
					started.Done()
					first = false
				}
			}
		}, logger)
	}

	started.Wait()
	swap()
	stop.Store(true)
	done.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	return kept, deleted
}

func findAllByEqualFilter(t *testing.T, ctx context.Context, shard *Shard,
	className, propName string, limit int,
) []*storobj.Object {
	t.Helper()
	found, _, err := shard.ObjectSearch(ctx, limit,
		propEqualsFilter(className, propName, semanticWindowToken), nil, nil, nil,
		additional.Properties{}, nil)
	require.NoError(t, err)
	return found
}

func findAllByBM25(t *testing.T, ctx context.Context, shard *Shard,
	_, propName string, limit int,
) []*storobj.Object {
	t.Helper()
	found, _, err := shard.ObjectSearch(ctx, limit, nil,
		&searchparams.KeywordRanking{
			Type:       "bm25",
			Properties: []string{propName},
			Query:      semanticWindowToken,
		}, nil, nil, additional.Properties{}, []string{propName})
	require.NoError(t, err)
	return found
}
