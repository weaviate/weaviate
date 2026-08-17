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
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestArmedWritesFillEveryBucketTheFlippedSchemaWould is the data-type sweep
// behind the routing.
//
// A property whose every index flag is off is dropped by the analyzer, so the
// migration that builds its first index only ever needed one bucket and its
// hook only creates one. Routing writes into that bucket brings back two more
// arms of the write path the migration never exercised, and neither is gated
// the way one would guess: the null-state arm has no data-type gate at all,
// and the property-length arm is gated on a value rather than a type, so it
// fires for every array type and for scalar text — the shard-init deny list
// that decides which properties get a length bucket covers eight scalar types
// and no array. Which is why this sweeps data types rather than picking an
// example: each type reaches a different subset of the three arms, and the
// property has to hold for all of them.
//
// The property, per type: an armed write leaves every bucket in the state the
// same write leaves on a shard whose flag was on from the start. Both objects
// matter — one carries the property and one omits it, because the null and
// length state of an absent property is recorded on a different path than
// that of a present one.
//
// Geo and nested properties are absent because no migration promotes one:
// their value index is not the bucket this builds (a geo index and per-nested
// buckets respectively), so there is nothing here to route a write into.
func TestArmedWritesFillEveryBucketTheFlippedSchemaWould(t *testing.T) {
	for _, dt := range promotedIndexDataTypes() {
		t.Run(dt.name, func(t *testing.T) {
			armed := writeUnderPromotedFilterableIndex(t, dt, false)
			flipped := writeUnderPromotedFilterableIndex(t, dt, true)

			assert.Equal(t, flipped, armed,
				"a write routed into the promoted %s index left different buckets behind "+
					"than the same write on a shard whose flag was already on", dt.name)
		})
	}
}

// promotedIndexDataType is one property shape an enable-filterable migration
// can target, with a value for the object that carries it.
type promotedIndexDataType struct {
	name     string
	dataType schema.DataType
	value    any
}

func promotedIndexDataTypes() []promotedIndexDataType {
	when := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)
	return []promotedIndexDataType{
		{"text", schema.DataTypeText, "alpha bravo charlie"},
		{"text[]", schema.DataTypeTextArray, []string{"alpha bravo", "charlie"}},
		{"int", schema.DataTypeInt, int64(42)},
		{"int[]", schema.DataTypeIntArray, []int{7, 42, 99}},
		{"number", schema.DataTypeNumber, float64(4.25)},
		{"number[]", schema.DataTypeNumberArray, []float64{4.25, 8.5}},
		{"boolean", schema.DataTypeBoolean, true},
		{"boolean[]", schema.DataTypeBooleanArray, []bool{true, false}},
		{"date", schema.DataTypeDate, when},
		{"date[]", schema.DataTypeDateArray, []string{when}},
		{"uuid", schema.DataTypeUUID, "5f8a1c22-0000-4000-8000-000000000001"},
		{"uuid[]", schema.DataTypeUUIDArray, []string{"5f8a1c22-0000-4000-8000-000000000002"}},
		// The analyzer indexes neither, so the routing has nothing to carry
		// and both shards must still agree on that.
		{"blob", schema.DataTypeBlob, "aGVsbG8="},
		{"phoneNumber", schema.DataTypePhoneNumber, "+49 171 1234567"},
	}
}

// writeUnderPromotedFilterableIndex writes one object carrying the property
// and one omitting it, and returns every bucket of that property that the
// shard holds afterwards.
//
// flagAlreadyOn picks which shard this is: the control, whose schema
// advertises the index from the start, or the one in the window, whose schema
// still says the property has no index at all and which gets there through
// the migration's own bucket creation plus the routing this exercises.
func writeUnderPromotedFilterableIndex(t *testing.T, dt promotedIndexDataType,
	flagAlreadyOn bool,
) map[string]map[string][]uint64 {
	t.Helper()
	ctx := testCtx()
	const propName = "target"
	className := "PromotedDataType_" + uuid.NewString()[:8]

	off, on := false, true
	indexFilterable := &off
	if flagAlreadyOn {
		indexFilterable = &on
	}
	class := &models.Class{
		Class:             className,
		VectorIndexConfig: enthnsw.NewDefaultUserConfig(),
		InvertedIndexConfig: &models.InvertedIndexConfig{
			CleanupIntervalSeconds: 60,
			Stopwords:              &models.StopwordConfig{Preset: "none"},
			IndexNullState:         true,
			IndexPropertyLength:    true,
			UsingBlockMaxWAND:      false,
		},
		Properties: []*models.Property{{
			Name:         propName,
			DataType:     dt.dataType.PropString(),
			Tokenization: tokenizationFor(dt.dataType),
			// Searchable and rangeable stay off in both shards: this is about
			// the one index the migration promotes.
			IndexFilterable:   indexFilterable,
			IndexSearchable:   &off,
			IndexRangeFilters: &off,
		}},
	}

	shd, _ := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
		false, false, false)
	shard := shd.(*Shard)
	defer shard.Shutdown(context.Background())

	if !flagAlreadyOn {
		// What the migration itself leaves behind: the value bucket its
		// backfill filled, and nothing else.
		(&EnableFilterableStrategy{propNames: []string{propName}}).
			PreReindexHook(shard, []string{propName})
		require.NotNil(t, shard.store.Bucket(helpers.BucketFromPropNameLSM(propName)),
			"the migration's own hook must have built the value bucket")
		require.NoError(t, shard.armPromotedIndex(ctx, propName, "filterable"))
	}

	require.NoError(t, shard.PutObject(ctx, &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:         strfmt.UUID("aaaaaaaa-0000-4000-8000-000000000001"),
			Class:      className,
			Properties: map[string]interface{}{propName: dt.value},
		},
	}))
	require.NoError(t, shard.PutObject(ctx, &storobj.Object{
		MarshallerVersion: 1,
		Object: models.Object{
			ID:         strfmt.UUID("aaaaaaaa-0000-4000-8000-000000000002"),
			Class:      className,
			Properties: map[string]interface{}{},
		},
	}))

	out := map[string]map[string][]uint64{}
	for label, bucketName := range map[string]string{
		"value":  helpers.BucketFromPropNameLSM(propName),
		"null":   helpers.BucketFromPropNameNullLSM(propName),
		"length": helpers.BucketFromPropNameLengthLSM(propName),
	} {
		if b := shard.store.Bucket(bucketName); b != nil {
			out[label] = fingerprintRoaringSetBucket(t, b)
		}
	}
	return out
}

func tokenizationFor(dt schema.DataType) string {
	switch dt {
	case schema.DataTypeText, schema.DataTypeTextArray:
		return models.PropertyTokenizationWord
	default:
		return ""
	}
}
