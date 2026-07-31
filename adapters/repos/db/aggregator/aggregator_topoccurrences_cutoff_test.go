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

package aggregator

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	entinverted "github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

func TestPropertyValuesFromInvertedCutoff(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	store, err := lsmkv.New(dir, dir, logger, nil, nil,
		cyclemanager.NewCallbackGroup("compactionObjects", logger, 1),
		cyclemanager.NewCallbackGroup("compactionNonObjects", logger, 1),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Shutdown(ctx) })

	newBucket := func(t *testing.T, prop string) *lsmkv.Bucket {
		t.Helper()
		name := helpers.BucketFromPropNameLSM(prop)
		require.NoError(t, store.CreateOrLoadBucket(ctx, name,
			lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
			lsmkv.WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
			lsmkv.WithUseBloomFilter(true)))
		return store.Bucket(name)
	}
	intKey := func(v int64) []byte {
		k, err := entinverted.LexicographicallySortableInt64(v)
		require.NoError(t, err)
		return k
	}
	floatKey := func(v float64) []byte {
		k, err := entinverted.LexicographicallySortableFloat64(v)
		require.NoError(t, err)
		return k
	}
	uuidKey := func(s string) []byte {
		b, err := uuid.MustParse(s).MarshalBinary()
		require.NoError(t, err)
		return b
	}

	limit := 5
	topOccs := []aggregation.Aggregator{aggregation.NewTopOccurrencesAggregator(&limit)}
	ua := newUnfilteredAggregator(&Aggregator{store: store, logger: logger})

	t.Run("text: exact values and live counts, deleted value excluded", func(t *testing.T) {
		b := newBucket(t, "category")
		require.NoError(t, b.RoaringSetAddList([]byte("red"), []uint64{1, 2, 3}))
		require.NoError(t, b.RoaringSetAddList([]byte("green"), []uint64{4, 5}))
		require.NoError(t, b.RoaringSetAddList([]byte("blue"), []uint64{6}))
		require.NoError(t, b.RoaringSetAddList([]byte("dead"), []uint64{7}))
		require.NoError(t, b.RoaringSetRemoveOne([]byte("dead"), 7))
		require.NoError(t, b.FlushAndSwitch())

		res, err := ua.propertyValuesFromInverted(ctx, aggregation.ParamProperty{
			Name: "category", Aggregators: topOccs, TopOccurrencesCutoff: 10,
		}, schema.DataTypeText)
		require.NoError(t, err)
		require.False(t, res.TextAggregation.CutoffExceeded)
		assert.Equal(t, string(schema.DataTypeText), res.SchemaType)
		assert.Equal(t, 6, res.TextAggregation.Count)
		assert.Equal(t, []aggregation.TextOccurrence{
			{Value: "red", Occurs: 3},
			{Value: "green", Occurs: 2},
			{Value: "blue", Occurs: 1},
		}, res.TextAggregation.Items)
	})

	t.Run("cutoff below the cardinality: sentinel instead of values", func(t *testing.T) {
		res, err := ua.propertyValuesFromInverted(ctx, aggregation.ParamProperty{
			Name: "category", Aggregators: topOccs, TopOccurrencesCutoff: 2,
		}, schema.DataTypeText)
		require.NoError(t, err)
		require.True(t, res.TextAggregation.CutoffExceeded)
		assert.Empty(t, res.TextAggregation.Items)
		assert.Zero(t, res.TextAggregation.Count)
	})

	date1 := time.Date(2026, 7, 29, 10, 0, 0, 0, time.UTC)
	date2 := date1.Add(time.Hour)

	typed := []struct {
		prop string
		dt   schema.DataType
		// two values: the first with two docs, the second with one
		keys [2][]byte
		want [2]string
	}{
		{
			prop: "count", dt: schema.DataTypeInt,
			keys: [2][]byte{intKey(42), intKey(7)},
			want: [2]string{"42", "7"},
		},
		{
			prop: "price", dt: schema.DataTypeNumber,
			keys: [2][]byte{floatKey(1.5), floatKey(2.25)},
			want: [2]string{"1.5", "2.25"},
		},
		{
			prop: "inStock", dt: schema.DataTypeBoolean,
			keys: [2][]byte{{1}, {0}},
			want: [2]string{"true", "false"},
		},
		{
			prop: "publishedAt", dt: schema.DataTypeDate,
			keys: [2][]byte{intKey(date1.UnixNano()), intKey(date2.UnixNano())},
			want: [2]string{"2026-07-29T10:00:00Z", "2026-07-29T11:00:00Z"},
		},
		{
			prop: "articleId", dt: schema.DataTypeUUID,
			keys: [2][]byte{
				uuidKey("11111111-2222-3333-4444-555555555555"),
				uuidKey("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
			},
			want: [2]string{
				"11111111-2222-3333-4444-555555555555",
				"aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
			},
		},
	}

	for _, tt := range typed {
		t.Run("typed values decode: "+string(tt.dt), func(t *testing.T) {
			b := newBucket(t, tt.prop)
			require.NoError(t, b.RoaringSetAddList(tt.keys[0], []uint64{1, 2}))
			require.NoError(t, b.RoaringSetAddList(tt.keys[1], []uint64{3}))
			require.NoError(t, b.FlushAndSwitch())

			res, err := ua.propertyValuesFromInverted(ctx, aggregation.ParamProperty{
				Name: schema.PropertyName(tt.prop), Aggregators: topOccs, TopOccurrencesCutoff: 10,
			}, tt.dt)
			require.NoError(t, err)
			require.False(t, res.TextAggregation.CutoffExceeded)
			assert.Equal(t, string(tt.dt), res.SchemaType)
			assert.Equal(t, []aggregation.TextOccurrence{
				{Value: tt.want[0], Occurs: 2},
				{Value: tt.want[1], Occurs: 1},
			}, res.TextAggregation.Items)
		})
	}

	t.Run("property dispatch routes typed cutoff requests to the inverted path", func(t *testing.T) {
		class := &models.Class{
			Class:      "MyClass",
			Properties: []*models.Property{{Name: "count", DataType: []string{"int"}}},
		}
		getSchema := schemaUC.NewMockSchemaGetter(t)
		getSchema.EXPECT().ReadOnlyClass("MyClass").Return(class).Once()

		uaTyped := newUnfilteredAggregator(&Aggregator{
			store: store, logger: logger, getSchema: getSchema,
			params: aggregation.Params{ClassName: "MyClass"},
		})
		res, err := uaTyped.property(ctx, aggregation.ParamProperty{
			Name: "count", Aggregators: topOccs, TopOccurrencesCutoff: 10,
		})
		require.NoError(t, err)
		assert.Equal(t, aggregation.PropertyTypeText, res.Type)
		assert.Equal(t, "int", res.SchemaType)
		assert.Equal(t, []aggregation.TextOccurrence{
			{Value: "42", Occurs: 2},
			{Value: "7", Occurs: 1},
		}, res.TextAggregation.Items)
	})

	t.Run("searchable-only property served from stored doc counts", func(t *testing.T) {
		sbName := helpers.BucketSearchableFromPropNameLSM("tag")
		require.NoError(t, store.CreateOrLoadBucket(ctx, sbName,
			lsmkv.WithStrategy(lsmkv.StrategyInverted),
			lsmkv.WithUseBloomFilter(true)))
		sb := store.Bucket(sbName)
		mapPair := func(docID uint64) lsmkv.MapPair {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, docID)
			return lsmkv.MapPair{Key: key, Value: make([]byte, 8)}
		}
		require.NoError(t, sb.MapSet([]byte("go"), mapPair(1)))
		require.NoError(t, sb.MapSet([]byte("go"), mapPair(2)))
		require.NoError(t, sb.MapSet([]byte("rust"), mapPair(3)))
		require.NoError(t, sb.FlushAndSwitch())

		res, err := ua.propertyValuesFromInverted(ctx, aggregation.ParamProperty{
			Name: "tag", Aggregators: topOccs, TopOccurrencesCutoff: 10,
		}, schema.DataTypeText)
		require.NoError(t, err)
		require.False(t, res.TextAggregation.CutoffExceeded)
		assert.Equal(t, []aggregation.TextOccurrence{
			{Value: "go", Occurs: 2},
			{Value: "rust", Occurs: 1},
		}, res.TextAggregation.Items)
	})

	t.Run("filterable preferred over a clean searchable bucket", func(t *testing.T) {
		mapPair := func(docID uint64) lsmkv.MapPair {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, docID)
			return lsmkv.MapPair{Key: key, Value: make([]byte, 8)}
		}

		// both indexes exist and the searchable one is churn-free; its
		// stored counts (go=2, rust=1) diverge from the filterable state,
		// so the result pins which bucket answered
		sbName := helpers.BucketSearchableFromPropNameLSM("lang")
		require.NoError(t, store.CreateOrLoadBucket(ctx, sbName,
			lsmkv.WithStrategy(lsmkv.StrategyInverted),
			lsmkv.WithUseBloomFilter(true)))
		sb := store.Bucket(sbName)
		require.NoError(t, sb.MapSet([]byte("go"), mapPair(1)))
		require.NoError(t, sb.MapSet([]byte("go"), mapPair(2)))
		require.NoError(t, sb.MapSet([]byte("rust"), mapPair(3)))
		require.NoError(t, sb.FlushAndSwitch())

		fb := newBucket(t, "lang")
		require.NoError(t, fb.RoaringSetAddList([]byte("go"), []uint64{1, 2, 4}))
		require.NoError(t, fb.RoaringSetAddList([]byte("rust"), []uint64{3}))
		require.NoError(t, fb.RoaringSetAddList([]byte("zig"), []uint64{5}))
		require.NoError(t, fb.FlushAndSwitch())

		res, err := ua.propertyValuesFromInverted(ctx, aggregation.ParamProperty{
			Name: "lang", Aggregators: topOccs, TopOccurrencesCutoff: 10,
		}, schema.DataTypeText)
		require.NoError(t, err)
		require.False(t, res.TextAggregation.CutoffExceeded)
		assert.ElementsMatch(t, []aggregation.TextOccurrence{
			{Value: "go", Occurs: 3},
			{Value: "rust", Occurs: 1},
			{Value: "zig", Occurs: 1},
		}, res.TextAggregation.Items, "must be the filterable bucket's counts")
	})

	t.Run("churned searchable without filterable falls back to the object scan", func(t *testing.T) {
		mapPair := func(docID uint64) lsmkv.MapPair {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, docID)
			return lsmkv.MapPair{Key: key, Value: make([]byte, 8)}
		}

		sbName := helpers.BucketSearchableFromPropNameLSM("desc")
		require.NoError(t, store.CreateOrLoadBucket(ctx, sbName,
			lsmkv.WithStrategy(lsmkv.StrategyInverted),
			lsmkv.WithUseBloomFilter(true)))
		sb := store.Bucket(sbName)
		require.NoError(t, sb.MapSet([]byte("go"), mapPair(1)))
		require.NoError(t, sb.MapSet([]byte("go"), mapPair(2)))
		require.NoError(t, sb.FlushAndSwitch())
		require.NoError(t, sb.MapDeleteKey([]byte("go"), mapPair(2).Key))
		require.NoError(t, sb.FlushAndSwitch())

		// the store has no objects bucket, so reaching the scan errors —
		// proving the churned stored counts were refused
		schemaGetter := schemaUC.NewMockSchemaGetter(t)
		schemaGetter.EXPECT().ReadOnlyClass("Things").Return(&models.Class{
			Class: "Things",
			Properties: []*models.Property{
				{Name: "desc", DataType: schema.DataTypeText.PropString()},
			},
		})
		uaScan := newUnfilteredAggregator(&Aggregator{
			store: store, logger: logger, getSchema: schemaGetter,
			params: aggregation.Params{ClassName: "Things"},
		})
		_, err := uaScan.propertyValuesFromInverted(ctx, aggregation.ParamProperty{
			Name: "desc", Aggregators: topOccs, TopOccurrencesCutoff: 10,
		}, schema.DataTypeText)
		require.Error(t, err)
	})

	t.Run("churned searchable bucket falls back to exact filterable counts", func(t *testing.T) {
		mapPair := func(docID uint64) lsmkv.MapPair {
			key := make([]byte, 8)
			binary.BigEndian.PutUint64(key, docID)
			return lsmkv.MapPair{Key: key, Value: make([]byte, 8)}
		}

		// searchable holds stale postings plus a tombstone: raw stored counts
		// would report go=2 and keep the fully-deleted value old alive
		sbName := helpers.BucketSearchableFromPropNameLSM("status")
		require.NoError(t, store.CreateOrLoadBucket(ctx, sbName,
			lsmkv.WithStrategy(lsmkv.StrategyInverted),
			lsmkv.WithUseBloomFilter(true)))
		sb := store.Bucket(sbName)
		require.NoError(t, sb.MapSet([]byte("go"), mapPair(1)))
		require.NoError(t, sb.MapSet([]byte("go"), mapPair(2)))
		require.NoError(t, sb.MapSet([]byte("rust"), mapPair(3)))
		require.NoError(t, sb.MapSet([]byte("old"), mapPair(4)))
		require.NoError(t, sb.FlushAndSwitch())
		require.NoError(t, sb.MapDeleteKey([]byte("go"), mapPair(2).Key))
		require.NoError(t, sb.MapDeleteKey([]byte("old"), mapPair(4).Key))
		require.NoError(t, sb.FlushAndSwitch())

		// filterable holds the live state
		fbName := helpers.BucketFromPropNameLSM("status")
		require.NoError(t, store.CreateOrLoadBucket(ctx, fbName,
			lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
			lsmkv.WithBitmapBufPool(roaringset.NewBitmapBufPoolNoop()),
			lsmkv.WithUseBloomFilter(true)))
		fb := store.Bucket(fbName)
		require.NoError(t, fb.RoaringSetAddList([]byte("go"), []uint64{1}))
		require.NoError(t, fb.RoaringSetAddList([]byte("rust"), []uint64{3}))
		require.NoError(t, fb.FlushAndSwitch())

		res, err := ua.propertyValuesFromInverted(ctx, aggregation.ParamProperty{
			Name: "status", Aggregators: topOccs, TopOccurrencesCutoff: 10,
		}, schema.DataTypeText)
		require.NoError(t, err)
		require.False(t, res.TextAggregation.CutoffExceeded)
		assert.ElementsMatch(t, []aggregation.TextOccurrence{
			{Value: "go", Occurs: 1},
			{Value: "rust", Occurs: 1},
		}, res.TextAggregation.Items, "must be live filterable counts, not stale stored ones")
	})

	t.Run("no cutoff: text takes the classic object scan", func(t *testing.T) {
		// no objects bucket exists in this store, so the classic path errors —
		// proving cutoff=0 does not take the inverted path
		_, err := ua.textProperty(ctx, aggregation.ParamProperty{
			Name: "category", Aggregators: topOccs,
		})
		require.Error(t, err)
	})
}
