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
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Pins the invariant that lets the backfill scan analyze every object
// unconditionally (weaviate/weaviate#11692): a reindex-bucket posting is
// always shadowed by the ingest bucket's newer mirror write/tombstone for
// the same key, because runtimeSwap prepends reindex segments before ingest
// segments and ingest buckets keep tombstones until merged
// (loadIngestBuckets, keepTombstones=!isMerged). One case per
// migration-target bucket strategy, since each has its own tombstone
// semantics. Companion: TestSegmentGroup_PrependedAddShadowedByNewerDelete
// (lsmkv), same invariant in isolation with explicit compaction.

// deleteConvergenceHarness abstracts one strategy's setup and probes.
type deleteConvergenceHarness struct {
	// propName is the migrated property; used to resolve the ingest bucket.
	propName string
	// prepare creates class+shard+corpus+victim and the migration task.
	prepare func(t *testing.T, ctx context.Context) (shard *Shard, task *ShardReindexTaskGeneric,
		victimID strfmt.UUID, update func(t *testing.T), probe func(t *testing.T, marker string) bool)
}

func runDeleteConvergence(t *testing.T, h deleteConvergenceHarness, op string) {
	ctx := testCtx()
	shard, task, victimID, update, probe := h.prepare(t, ctx)

	// Scan completes with the victim's OLD value in the reindex bucket;
	// double-write callbacks remain armed until RunSwapOnShard.
	require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))

	switch op {
	case "delete":
		require.NoError(t, shard.DeleteObject(ctx, victimID, time.Now()),
			"mid-migration delete must succeed")
	case "update":
		update(t)
	}

	// Flush so the tombstone must survive on disk, not just in the memtable.
	ingest := shard.store.Bucket(task.ingestBucketName(h.propName))
	require.NotNil(t, ingest, "ingest bucket must exist while callbacks are armed")
	require.NoError(t, ingest.FlushAndSwitch())

	require.NoError(t, task.RunPrepareOnShard(ctx, shard))
	require.NoError(t, task.RunSwapOnShard(ctx, shard))

	require.Truef(t, probe(t, "corpus"),
		"positive control: the scanned corpus must be present in the migrated bucket")

	assert.Falsef(t, probe(t, "old"),
		"resurrection: the victim was scanned into the reindex bucket and then %sd "+
			"while callbacks were armed, but its OLD posting is still readable in the "+
			"migrated bucket — the ingest tombstone did not shadow the prepended "+
			"reindex segment (keepTombstones lifetime or prepend order broke)", op)
	if op == "update" {
		assert.Truef(t, probe(t, "new"),
			"the victim's NEW value must be present in the migrated bucket via the "+
				"double-write mirror")
	}
}

// TestReindexDeleteConvergence_IngestTombstoneShadowsScannedPosting runs the
// delete and update journeys against every migration-target bucket strategy.
func TestReindexDeleteConvergence_IngestTombstoneShadowsScannedPosting(t *testing.T) {
	const numCorpus = 25

	harnesses := map[string]deleteConvergenceHarness{
		"rangeable (RoaringSetRange target)": {
			propName: filterableToRangeablePropName,
			prepare: func(t *testing.T, ctx context.Context) (*Shard, *ShardReindexTaskGeneric,
				strfmt.UUID, func(t *testing.T), func(t *testing.T, marker string) bool,
			) {
				const oldValue, newValue = int64(4242), int64(4343)
				propName := filterableToRangeablePropName
				className := "DelConvRange_" + uuid.NewString()[:8]
				class := newFilterableToRangeableTestClass(className)

				shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
					false, false, false)
				shard := shd.(*Shard)
				t.Cleanup(func() { shard.Shutdown(context.Background()) })

				for _, obj := range makeFilterableToRangeableTestObjects(t, numCorpus, className) {
					require.NoError(t, shard.PutObject(ctx, obj))
				}
				victimID := strfmt.UUID(uuid.NewString())
				putValue := func(v int64) {
					require.NoError(t, shard.PutObject(ctx, &storobj.Object{
						MarshallerVersion: 1,
						Object: models.Object{
							ID:         victimID,
							Class:      className,
							Properties: map[string]interface{}{propName: v},
						},
					}))
				}
				putValue(oldValue)

				task, _ := newFilterableToRangeableTask(t, idx, className, propName)
				probe := func(t *testing.T, marker string) bool {
					b := shard.store.Bucket(helpers.BucketRangeableFromPropNameLSM(propName))
					require.NotNil(t, b, "post-swap rangeable bucket must exist")
					switch marker {
					case "corpus":
						return len(readRangeableDocIDs(t, b, 0)) > 0
					case "old":
						return len(readRangeableDocIDs(t, b, oldValue)) > 0
					default:
						return len(readRangeableDocIDs(t, b, newValue)) > 0
					}
				}
				return shard, task, victimID, func(t *testing.T) { putValue(newValue) }, probe
			},
		},

		"roaringset refresh (RoaringSet target)": {
			propName: "title",
			prepare: func(t *testing.T, ctx context.Context) (*Shard, *ShardReindexTaskGeneric,
				strfmt.UUID, func(t *testing.T), func(t *testing.T, marker string) bool,
			) {
				return prepareTitleTokenHarness(t, ctx, "DelConvRoaring_",
					func(t *testing.T, _ *Shard, idx *Index, className string) *ShardReindexTaskGeneric {
						task, _ := newRoaringSetRefreshTask(t, idx)
						return task
					},
					helpers.BucketFromPropNameLSM("title"), roaringSetHasTerm)
			},
		},

		"searchable retokenize (MapCollection target)": {
			propName: "title",
			prepare: func(t *testing.T, ctx context.Context) (*Shard, *ShardReindexTaskGeneric,
				strfmt.UUID, func(t *testing.T), func(t *testing.T, marker string) bool,
			) {
				return prepareTitleTokenHarness(t, ctx, "DelConvMap_",
					func(t *testing.T, shard *Shard, idx *Index, className string) *ShardReindexTaskGeneric {
						// WORD→WORD: the case under test is tombstone shadowing, not retokenization.
						preStrategy := shard.store.Bucket(helpers.BucketSearchableFromPropNameLSM("title")).Strategy()
						task, _ := newSearchableRetokenizeTask(t, idx, className, "title",
							models.PropertyTokenizationWord, preStrategy)
						return task
					},
					helpers.BucketSearchableFromPropNameLSM("title"), mapBucketHasTerm)
			},
		},

		"map to blockmax (Inverted target)": {
			propName: "title",
			prepare: func(t *testing.T, ctx context.Context) (*Shard, *ShardReindexTaskGeneric,
				strfmt.UUID, func(t *testing.T), func(t *testing.T, marker string) bool,
			) {
				return prepareTitleTokenHarness(t, ctx, "DelConvBlockmax_",
					func(t *testing.T, _ *Shard, idx *Index, className string) *ShardReindexTaskGeneric {
						strategy := &testMigrationStrategy{MapToBlockmaxStrategy: MapToBlockmaxStrategy{generation: 1}}
						return newTestTask(idx.logger, strategy)
					},
					helpers.BucketSearchableFromPropNameLSM("title"), mapBucketHasTerm)
			},
		},
	}

	for name, h := range harnesses {
		for _, op := range []string{"delete", "update"} {
			t.Run(name+"/"+op, func(t *testing.T) {
				runDeleteConvergence(t, h, op)
			})
		}
	}
}

// mapBucketHasTerm uses MapList, not the raw MapCursor: the cursor doesn't
// apply cross-segment tombstones, so it would report a shadowed posting as
// present.
func mapBucketHasTerm(t *testing.T, b *lsmkv.Bucket, term string) bool {
	t.Helper()
	pairs, err := b.MapList(context.Background(), []byte(term))
	require.NoError(t, err)
	return len(pairs) > 0
}

// roaringSetHasTerm uses the roaringset cursor, which merges deletions
// across segments (RoaringSetGet would be the query path, but needs a
// bitmap buffer pool the test shard doesn't wire up).
func roaringSetHasTerm(t *testing.T, b *lsmkv.Bucket, term string) bool {
	t.Helper()
	fp := fingerprintRoaringSetBucket(t, b)
	return len(fp[term]) > 0
}

// prepareTitleTokenHarness is the shared setup for the token-bucket strategies
// (roaringset refresh, searchable retokenize, map→blockmax): a "title" class,
// a scanned corpus, and a victim whose unique old/new tokens are probed in the
// post-swap bucket named bucketName via hasTerm.
func prepareTitleTokenHarness(t *testing.T, ctx context.Context, classPrefix string,
	makeTask func(t *testing.T, shard *Shard, idx *Index, className string) *ShardReindexTaskGeneric,
	bucketName string, hasTerm func(t *testing.T, b *lsmkv.Bucket, term string) bool,
) (*Shard, *ShardReindexTaskGeneric, strfmt.UUID, func(t *testing.T), func(t *testing.T, marker string) bool) {
	const oldToken, newToken = "victimoldtoken", "victimnewtoken"

	className := classPrefix + uuid.NewString()[:8]
	class := newTestClassWithProps(className, []string{"title"})

	shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	t.Cleanup(func() { shard.Shutdown(context.Background()) })

	for _, obj := range makeConvergenceTestObjects(t, 25, className) {
		require.NoError(t, shard.PutObject(ctx, obj))
	}
	victimID := strfmt.UUID(uuid.NewString())
	putText := func(text string) {
		require.NoError(t, shard.PutObject(ctx, &storobj.Object{
			MarshallerVersion: 1,
			Object: models.Object{
				ID:         victimID,
				Class:      className,
				Properties: map[string]interface{}{"title": text},
			},
		}))
	}
	putText(oldToken)

	task := makeTask(t, shard, idx, className)
	probe := func(t *testing.T, marker string) bool {
		b := shard.store.Bucket(bucketName)
		require.NotNil(t, b, "post-swap bucket %q must exist", bucketName)
		switch marker {
		case "corpus":
			// "victor" is a makeConvergenceTestObjects dictionary token.
			return hasTerm(t, b, "victor")
		case "old":
			return hasTerm(t, b, oldToken)
		default:
			return hasTerm(t, b, newToken)
		}
	}
	return shard, task, victimID, func(t *testing.T) { putText(newToken) }, probe
}
