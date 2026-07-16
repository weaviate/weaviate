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

// This file pins the convergence invariant that lets the backfill scan analyze
// EVERY object unconditionally (the skew-immune fix for
// weaviate/weaviate#11692): a posting the scan writes into the reindex bucket
// is always shadowed by a newer ingest-bucket mirror write for the same key.
// That holds because
//
//  1. runtimeSwap prepends reindex segments BEFORE ingest segments
//     ([lsmkv.SegmentGroup.PrependSegmentsFromBucket]), so ingest writes are
//     strictly newer in LSM merge order, and
//  2. ingest buckets keep tombstones until the merge
//     (loadIngestBuckets is called with keepTombstones=!isMerged), so a
//     mirror-side delete survives as a tombstone that kills the prepended
//     posting at read time and across compaction.
//
// The journey per strategy: the victim object is scanned into the reindex
// bucket (RunReindexOnlyOnShard completes the iteration), THEN deleted or
// updated while the double-write callbacks are still armed, then the swap
// runs. If either invariant breaks, the victim's stale posting resurrects in
// the migrated bucket: a filter/search on the old value false-positives. Each
// migration-target bucket strategy (RoaringSetRange, RoaringSet,
// MapCollection, Inverted) has its own tombstone semantics, so each gets a
// case.
//
// The lsmkv-level companion (TestSegmentGroup_PrependedAddShadowedByNewerDelete)
// pins the same ordering in isolation, including explicit compaction.

// deleteConvergenceHarness abstracts one strategy's setup and probes.
type deleteConvergenceHarness struct {
	// propName is the migrated property; used to resolve the ingest bucket.
	propName string
	// prepare creates class+shard+corpus+victim and the migration task.
	// oldMarker/newMarker presence is checked via probe after the swap.
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

	// Force the mirror write out of the ingest memtable into a segment so
	// the tombstone must survive a flush — this makes the ingest bucket's
	// keepTombstones=!isMerged option (loadIngestBuckets) load-bearing in
	// this test rather than incidentally covered by memtable reads.
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
						// WORD→WORD keeps tokens comparable across scan and
						// mirror; the case under test is tombstone shadowing,
						// not retokenization.
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

// mapBucketHasTerm probes term presence via MapList — the tombstone-applying
// read path shared by MapCollection and Inverted buckets. Deliberately NOT the
// raw MapCursor (fingerprintInvertedBucket): that cursor does not apply
// inverted docID tombstones across segments, so it would report a
// tombstone-shadowed posting from a prepended reindex segment as present even
// though every query path (MapList, WAND, BlockMax) correctly hides it.
func mapBucketHasTerm(t *testing.T, b *lsmkv.Bucket, term string) bool {
	t.Helper()
	pairs, err := b.MapList(context.Background(), []byte(term))
	require.NoError(t, err)
	return len(pairs) > 0
}

// roaringSetHasTerm probes term presence via the roaringset cursor
// (fingerprintRoaringSetBucket), which merges additions and deletions across
// segments — unlike the raw inverted MapCursor, it is deletion-accurate.
// (RoaringSetGet would be the query path but requires the shard's bitmap
// buffer pool, which the test shard does not wire up.)
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
