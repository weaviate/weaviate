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
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/filters"
	entinverted "github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// rangeableDocIDsAtLeast and readRangeableIDs are shared RoaringSetRange read
// helpers defined in the reindex write-during-* test files in this package
// (inverted_reindex_write_during_swap_window_test.go and
// inverted_reindex_write_during_enable_rangeable_test.go). The masking tests
// below reuse them rather than redeclaring them.

// rangeableMaskingValue is shared by every test object so a `>= value` filter
// selects the whole corpus via one key.
const rangeableMaskingValue = int64(500)

// newRangeableAndFilterableMaskingClass builds a property with both filterable
// and rangeable indexes enabled — the shape after enable-rangeable completes.
func newRangeableAndFilterableMaskingClass(className string) *models.Class {
	c := newFilterableToRangeableTestClass(className)
	trueVal := true
	c.Properties[0].IndexFilterable = &trueVal
	c.Properties[0].IndexRangeFilters = &trueVal
	return c
}

// rangeFilterGTE builds a `prop >= value` filter. Inlined instead of reusing
// filters_integration_test.go's buildFilter, which lives behind the
// integrationTest tag; this test must run under `go test -race` without docker.
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

// putMaskingObject imports one object carrying rangeableMaskingValue.
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

// TestRangeableMasking_ReadSourceSwitch_CountFlipsOnReadiness pins
// weaviate/0-weaviate-issues#335: with the rangeable bucket short by K
// postings vs filterable, range-query count depends on IsRangeableLocallyReady
// (which bucket getBucketName routes to), not just on-disk state.
func TestRangeableMasking_ReadSourceSwitch_CountFlipsOnReadiness(t *testing.T) {
	ctx := testCtx()
	const (
		numObjs = 30
		gapK    = 6
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

	// Induce the gap: remove K postings from the RANGEABLE bucket only
	// (filterable keeps all N).
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

	// (1) Default readiness (bucket exists → ready): routes to the SHORT
	// rangeable bucket.
	require.Truef(t, shard.IsRangeableLocallyReady(propName),
		"default readiness must be true when the rangeable bucket exists (bucket-existence fallback)")
	assert.Equalf(t, numObjs-gapK, countVia(),
		"post-restart (default-ready) read routes to the SHORT rangeable bucket and MUST expose the gap")

	// (2) Readiness explicit false: routes to the COMPLETE filterable
	// bucket, masking the gap.
	shard.setRangeableLocallyReady(propName, false)
	assert.Equalf(t, numObjs, countVia(),
		"masked (readiness=false) read routes to the COMPLETE filterable bucket and hides the gap")

	// (3) Readiness explicit TRUE — exposed again, back to the short count.
	shard.setRangeableLocallyReady(propName, true)
	assert.Equalf(t, numObjs-gapK, countVia(),
		"exposed (readiness=true) read routes back to the SHORT rangeable bucket")
}

// TestRangeableMasking_WriteSide_NotReadinessGated pins
// weaviate/0-weaviate-issues#335: writes gate on HasRangeableIndex + bucket
// existence, not on IsRangeableLocallyReady, so a masked replica's gap never widens.
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

	// Write while masked; writes are not readiness-gated, so it still lands
	// in the rangeable bucket.
	putMaskingObject(t, ctx, shard, className, propName)

	assert.Lenf(t, readRangeableIDs(t, rangeBucket, rangeableMaskingValue), 1,
		"a write under readiness=false MUST still populate the rangeable bucket "+
			"(the write path is schema+bucket gated, not readiness gated)")
}
