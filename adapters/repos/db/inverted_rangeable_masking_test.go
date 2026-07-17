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
	"encoding/binary"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	entinverted "github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// rangeableDocIDsAtLeast returns every docID whose value is >= v in a
// RoaringSetRange bucket, used to count the whole index (v=0 with all-positive
// data) without per-value cursor iteration.
func rangeableDocIDsAtLeast(t *testing.T, b *lsmkv.Bucket, v int64) []uint64 {
	t.Helper()
	reader := b.ReaderRoaringSetRange()
	defer reader.Close()
	lex, err := entinverted.LexicographicallySortableInt64(v)
	require.NoError(t, err)
	key := binary.BigEndian.Uint64(lex)
	bm, release, err := reader.Read(context.Background(), key, filters.OperatorGreaterThanEqual)
	require.NoError(t, err)
	if release != nil {
		defer release()
	}
	if bm == nil {
		return nil
	}
	return bm.ToArray()
}

// readRangeableIDs returns docIDs for one int64 value in a RoaringSetRange
// bucket via an OperatorEqual read — the production range-query read shape.
func readRangeableIDs(t *testing.T, b *lsmkv.Bucket, v int64) []uint64 {
	t.Helper()
	require.Equal(t, lsmkv.StrategyRoaringSetRange, b.Strategy(),
		"readRangeableIDs requires a RoaringSetRange bucket")
	reader := b.ReaderRoaringSetRange()
	defer reader.Close()
	lex, err := entinverted.LexicographicallySortableInt64(v)
	require.NoError(t, err)
	key := binary.BigEndian.Uint64(lex)
	bm, release, err := reader.Read(context.Background(), key, filters.OperatorEqual)
	require.NoError(t, err)
	if release != nil {
		defer release()
	}
	if bm == nil {
		return nil
	}
	return bm.ToArray()
}

// rangeableMaskingValue is a single in-range int value shared by every test
// object. A shared value keeps the induced gap trivially addressable (one
// RoaringSetRange key) while a `>= value` range filter still selects every
// object — the point under test is the read-SOURCE switch, not range width.
const rangeableMaskingValue = int64(500)

// newRangeableAndFilterableMaskingClass builds the POST-migration steady
// state: a numeric property carrying BOTH a filterable and a rangeable index.
// This is the shape a property has once enable-rangeable has FINISHED — the
// filterable bucket that predated the migration still exists, and the
// rangeable bucket built by the migration sits beside it. Both buckets are
// created at shard init, so a normal write lands a posting in each.
func newRangeableAndFilterableMaskingClass(className string) *models.Class {
	c := newFilterableToRangeableTestClass(className)
	trueVal := true
	// IndexFilterable is nil in the base helper → defaults to true (bucket
	// created). Pin it explicit-true for clarity, then add the rangeable
	// index the migration would have enabled.
	c.Properties[0].IndexFilterable = &trueVal
	c.Properties[0].IndexRangeFilters = &trueVal
	return c
}

// rangeFilterGTE builds a `prop >= value` range filter on an int property.
// Inlined instead of reusing filters_integration_test.go's buildFilter,
// which lives behind the integrationTest build tag; this test is a plain
// unit test so it can run under `go test -race` without docker.
func rangeFilterGTE(className, propName string, value int) *filters.LocalFilter {
	return &filters.LocalFilter{
		Root: &filters.Clause{
			Operator: filters.OperatorGreaterThanEqual,
			On: &filters.Path{
				Class:    schema.ClassName(className),
				Property: schema.PropertyName(propName),
			},
			Value: &filters.Value{
				Value: value,
				Type:  schema.DataTypeInt,
			},
		},
	}
}

// putMaskingObject imports one object carrying rangeableMaskingValue. Every
// object shares the value so the whole corpus is selected by `>= value`.
func putMaskingObject(t *testing.T, ctx context.Context, shard *Shard, className, propName string) {
	t.Helper()
	require.NoError(t, shard.PutObject(ctx, &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:                 strfmt.UUID(uuid.NewString()),
			Class:              className,
			Properties:         map[string]interface{}{propName: rangeableMaskingValue},
			CreationTimeUnix:   time.Now().UnixMilli(),
			LastUpdateTimeUnix: time.Now().UnixMilli(),
		},
	}))
}

// TestRangeableMasking_ReadSourceSwitch_CountFlipsOnReadiness is the
// deterministic falsification test for the masking hypothesis on
// weaviate/0-weaviate-issues#335: a shard whose rangeable bucket is short by
// K postings vs its filterable bucket answers a where-filtered range query
// with the FULL count while IsRangeableLocallyReady is false (filterable
// walk) and the SHORT count while it is true/default (rangeable-only). This
// is the mechanism by which a hidden index gap converts into the CI
// pre-restart 1500 -> post-restart 1494 signature: the read SOURCE switches
// across the restart because the in-memory readiness map resets and defaults
// to bucket-existence.
//
// Read-source switch under test:
//   - hasUsableRangeableIndex = HasRangeableIndex && IsRangeableLocallyReady.
//     When false, propValuePair.hasRangeableIndex is dropped, so getBucketName
//     routes a range operator to the FILTERABLE bucket. When true, it routes to
//     the RANGEABLE bucket.
func TestRangeableMasking_ReadSourceSwitch_CountFlipsOnReadiness(t *testing.T) {
	ctx := testCtx()
	const (
		numObjs = 30
		gapK    = 6 // mirrors the CI 1500 -> 1494 delta (6 postings)
	)
	propName := filterableToRangeablePropName
	className := "RangeableMask_" + uuid.NewString()[:8]

	class := newRangeableAndFilterableMaskingClass(className)
	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	t.Cleanup(func() { shard.Shutdown(ctx) })

	for i := 0; i < numObjs; i++ {
		putMaskingObject(t, ctx, shard, className, propName)
	}

	filterBucket := shard.store.Bucket(helpers.BucketFromPropNameLSM(propName))
	require.NotNil(t, filterBucket, "filterable bucket must exist for a filterable prop")
	rangeBucket := shard.store.Bucket(helpers.BucketRangeableFromPropNameLSM(propName))
	require.NotNil(t, rangeBucket, "rangeable bucket must exist for a rangeable prop")

	// Precondition: the normal write path populated BOTH buckets fully.
	require.Lenf(t, rangeableDocIDsAtLeast(t, rangeBucket, rangeableMaskingValue), numObjs,
		"rangeable bucket must hold all %d postings before we induce the gap", numObjs)

	// Induce the index gap: remove K postings from the RANGEABLE bucket
	// ONLY. The filterable bucket keeps all N. This is the honest
	// construction of "rangeable short by K vs filterable" — the identical
	// divergence an incomplete backfill or a dropped replica-side posting
	// would leave behind. Reads never see the removal directly; they see
	// only whichever bucket the readiness gate routes them to.
	allIDs := rangeableDocIDsAtLeast(t, rangeBucket, rangeableMaskingValue)
	lex, err := entinverted.LexicographicallySortableInt64(rangeableMaskingValue)
	require.NoError(t, err)
	key := binary.BigEndian.Uint64(lex)
	for _, id := range allIDs[:gapK] {
		require.NoError(t, rangeBucket.RoaringSetRangeRemove(key, id))
	}
	require.Lenf(t, rangeableDocIDsAtLeast(t, rangeBucket, rangeableMaskingValue), numObjs-gapK,
		"rangeable bucket must now be short by exactly K=%d", gapK)

	filter := rangeFilterGTE(className, propName, int(rangeableMaskingValue))
	countVia := func() int {
		objs, _, err := shard.ObjectSearch(ctx, numObjs+10, filter,
			nil, nil, nil, additional.Properties{}, nil)
		require.NoError(t, err)
		return len(objs)
	}

	// (1) DEFAULT readiness (no explicit map entry). The rangeable bucket
	// exists, so IsRangeableLocallyReady falls back to true
	// (bucket-existence default). This is the POST-restart state: the
	// in-memory readiness map has reset, reads route to the SHORT rangeable
	// bucket. Count = N-K — the 1494 signature.
	require.Truef(t, shard.IsRangeableLocallyReady(propName),
		"default readiness must be true when the rangeable bucket exists (bucket-existence fallback)")
	assert.Equalf(t, numObjs-gapK, countVia(),
		"post-restart (default-ready) read routes to the SHORT rangeable bucket and MUST expose the gap")

	// (2) Readiness explicit FALSE. The PRE-restart state: the migration's
	// PreReindexHook set the prop not-locally-ready, so the range read falls
	// back to the FILTERABLE walk. The gap is MASKED. Count = N — the 1500
	// signature. The count FLIP between (1) and (2) over identical on-disk
	// data is the masking mechanism, confirmed.
	shard.setRangeableLocallyReady(propName, false)
	assert.Equalf(t, numObjs, countVia(),
		"masked (readiness=false) read routes to the COMPLETE filterable bucket and hides the gap")

	// (3) Readiness explicit TRUE — exposed again, back to the short count.
	shard.setRangeableLocallyReady(propName, true)
	assert.Equalf(t, numObjs-gapK, countVia(),
		"exposed (readiness=true) read routes back to the SHORT rangeable bucket")
}

// TestRangeableMasking_WriteSide_NotReadinessGated pins the write-side half
// of the masking question: post-flip writes are NOT readiness-gated. The
// rangeable write in addToPropertyValueIndex gates on
// property.HasRangeableIndex (the schema/analysis view) and bucket existence,
// NOT on IsRangeableLocallyReady. Consequence for the incident: a masked
// replica (readiness=false, filterable serving reads) keeps writing NEW
// postings into the rangeable bucket, so it does not fall further behind on new
// writes — the deficit is the fixed historical gap, not a widening one.
func TestRangeableMasking_WriteSide_NotReadinessGated(t *testing.T) {
	ctx := testCtx()
	propName := filterableToRangeablePropName
	className := "RangeableMaskWrite_" + uuid.NewString()[:8]

	class := newRangeableAndFilterableMaskingClass(className)
	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	t.Cleanup(func() { shard.Shutdown(ctx) })

	// Force the MASKED state: readiness=false, reads would use filterable.
	shard.setRangeableLocallyReady(propName, false)
	require.False(t, shard.IsRangeableLocallyReady(propName))

	rangeBucket := shard.store.Bucket(helpers.BucketRangeableFromPropNameLSM(propName))
	require.NotNil(t, rangeBucket)
	require.Empty(t, readRangeableIDs(t, rangeBucket, rangeableMaskingValue),
		"rangeable bucket starts empty for this value")

	// Write while masked. If writes were readiness-gated this posting would
	// be skipped; they are not, so it lands in the rangeable bucket.
	putMaskingObject(t, ctx, shard, className, propName)

	assert.Lenf(t, readRangeableIDs(t, rangeBucket, rangeableMaskingValue), 1,
		"a write under readiness=false MUST still populate the rangeable bucket "+
			"(the write path is schema+bucket gated, not readiness gated)")
}
