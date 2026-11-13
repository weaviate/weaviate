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

package aggregator

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	entinverted "github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/monitoring"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

const (
	lengthTestClass   = "LengthIndexFixture"
	lengthTestObjects = 60
)

var lengthTestProps = map[schema.DataType]string{
	schema.DataTypeTextArray:    "texts",
	schema.DataTypeIntArray:     "ints",
	schema.DataTypeNumberArray:  "numbers",
	schema.DataTypeDateArray:    "dates",
	schema.DataTypeBooleanArray: "bools",
}

// lengthTestValue returns the array of dt that object i holds: i%4 elements,
// so some arrays are empty, and none for every fifth object.
func lengthTestValue(dt schema.DataType, i int) (interface{}, bool) {
	if i%5 == 0 {
		return nil, false
	}
	n := i % 4
	switch dt {
	case schema.DataTypeTextArray:
		out := make([]string, n)
		for j := range out {
			out[j] = fmt.Sprintf("v%d", j)
		}
		return out, true
	case schema.DataTypeBooleanArray:
		out := make([]bool, n)
		for j := range out {
			out[j] = j%2 == 0
		}
		return out, true
	case schema.DataTypeDateArray:
		out := make([]string, n)
		for j := range out {
			out[j] = time.Date(2020, 1, 1+j, 0, 0, 0, 0, time.UTC).Format(time.RFC3339)
		}
		return out, true
	default:
		out := make([]float64, n)
		for j := range out {
			out[j] = float64(j)
		}
		return out, true
	}
}

// newLengthTestStore writes the objects and, for every property, a length
// index that records each object in skipIndexOf under no length, the way an
// object written before the property had an index is recorded.
func newLengthTestStore(t *testing.T, ctx context.Context, skipIndexOf map[int]bool) *lsmkv.Store {
	t.Helper()

	metrics, err := lsmkv.NewMetrics(monitoring.GetMetrics(), lengthTestClass, "shard")
	require.NoError(t, err)
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()
	store, err := lsmkv.New(dir, dir, logger, metrics, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	t.Cleanup(func() { store.Shutdown(ctx) })

	require.NoError(t, store.CreateOrLoadBucket(ctx, helpers.ObjectsBucketLSM,
		lsmkv.WithStrategy(lsmkv.StrategyReplace),
		lsmkv.WithSecondaryIndices(1),
		lsmkv.WithCalcCountNetAdditions(true),
		lsmkv.WithClassName(lengthTestClass),
	))
	objects := store.Bucket(helpers.ObjectsBucketLSM)
	for _, prop := range lengthTestProps {
		require.NoError(t, store.CreateOrLoadBucket(ctx, helpers.BucketFromPropNameLengthLSM(prop),
			lsmkv.WithStrategy(lsmkv.StrategyRoaringSet)))
	}

	for i := range lengthTestObjects {
		docID := uint64(i)
		props := map[string]interface{}{}
		for dt, prop := range lengthTestProps {
			value, ok := lengthTestValue(dt, i)
			length := 0
			if ok {
				props[prop] = value
				length = arrayLen(t, value)
			}
			if skipIndexOf[i] {
				continue
			}
			key, err := entinverted.LexicographicallySortableInt64(int64(length))
			require.NoError(t, err)
			require.NoError(t, store.Bucket(helpers.BucketFromPropNameLengthLSM(prop)).RoaringSetAddOne(key, docID))
		}

		obj := storobj.FromObject(&models.Object{
			Class:      lengthTestClass,
			ID:         strfmt.UUID(uuid.NewSHA1(uuid.Nil, []byte(fmt.Sprint(i))).String()),
			Properties: props,
		}, nil, nil, nil)
		obj.DocID = docID
		data, err := obj.MarshalBinary()
		require.NoError(t, err)
		docIDKey := make([]byte, 8)
		for j := range 8 {
			docIDKey[j] = byte(docID >> (8 * j))
		}
		require.NoError(t, objects.Put([]byte(obj.ID()), data, lsmkv.WithSecondaryKey(0, docIDKey)))
	}
	return store
}

func arrayLen(t *testing.T, v interface{}) int {
	switch a := v.(type) {
	case []string:
		return len(a)
	case []bool:
		return len(a)
	case []float64:
		return len(a)
	default:
		t.Fatalf("unexpected array %T", v)
		return 0
	}
}

func lengthTestSchema(t *testing.T, indexPropertyLength bool) schemaUC.SchemaGetter {
	class := &models.Class{
		Class:               lengthTestClass,
		InvertedIndexConfig: &models.InvertedIndexConfig{IndexPropertyLength: indexPropertyLength},
	}
	for dt, prop := range lengthTestProps {
		class.Properties = append(class.Properties, &models.Property{Name: prop, DataType: dt.PropString()})
	}
	getter := schemaUC.NewMockSchemaGetter(t)
	getter.EXPECT().ReadOnlyClass(lengthTestClass).Return(class).Maybe()
	return getter
}

// TestUnfilteredCount_FromLengthIndex pins that a count-only aggregation over
// an array property is answered from the property length index without reading
// the objects, that it gives the result the object scan gives, and that it
// reads the objects whenever the index cannot answer exactly.
func TestUnfilteredCount_FromLengthIndex(t *testing.T) {
	ctx := context.Background()
	countOnly := []aggregation.Aggregator{aggregation.CountAggregator, aggregation.TypeAggregator}

	testCases := []struct {
		name                string
		dt                  schema.DataType
		aggs                []aggregation.Aggregator
		indexPropertyLength bool
		skipIndexOf         map[int]bool
		readsUnrequested    bool
		wantIndex           bool
	}{
		{name: "text[]", dt: schema.DataTypeTextArray, aggs: countOnly, indexPropertyLength: true, wantIndex: true},
		{name: "int[]", dt: schema.DataTypeIntArray, aggs: countOnly, indexPropertyLength: true, wantIndex: true},
		{name: "number[]", dt: schema.DataTypeNumberArray, aggs: countOnly, indexPropertyLength: true, wantIndex: true},
		{name: "date[]", dt: schema.DataTypeDateArray, aggs: countOnly, indexPropertyLength: true, wantIndex: true},
		{name: "no aggregators", dt: schema.DataTypeTextArray, indexPropertyLength: true, wantIndex: true},
		{name: "boolean[] needs its totals", dt: schema.DataTypeBooleanArray, aggs: countOnly, indexPropertyLength: true},
		{
			name: "more than the count", dt: schema.DataTypeNumberArray, indexPropertyLength: true,
			aggs: []aggregation.Aggregator{aggregation.CountAggregator, aggregation.SumAggregator},
		},
		{
			name: "text[] top occurrences", dt: schema.DataTypeTextArray, indexPropertyLength: true,
			aggs: []aggregation.Aggregator{aggregation.CountAggregator, aggregation.NewTopOccurrencesAggregator(nil)},
		},
		{name: "length index disabled", dt: schema.DataTypeTextArray, aggs: countOnly},
		{
			name: "text[] for a caller that reads unrequested results", dt: schema.DataTypeTextArray, aggs: countOnly,
			indexPropertyLength: true, readsUnrequested: true,
		},
		{
			name: "int[] for a caller that reads unrequested results", dt: schema.DataTypeIntArray, aggs: countOnly,
			indexPropertyLength: true, readsUnrequested: true, wantIndex: true,
		},
		{
			name: "objects missing from the index", dt: schema.DataTypeTextArray, aggs: countOnly,
			indexPropertyLength: true, skipIndexOf: map[int]bool{3: true, 7: true},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			store := newLengthTestStore(t, ctx, tc.skipIndexOf)
			prop := aggregation.ParamProperty{Name: schema.PropertyName(lengthTestProps[tc.dt]), Aggregators: tc.aggs}
			aggregate := func(indexPropertyLength bool) (aggregation.Property, float64) {
				before := openedReplaceCursors(t)
				logger, _ := test.NewNullLogger()
				ua := unfilteredAggregator{Aggregator: &Aggregator{
					store:     store,
					logger:    logger,
					getSchema: lengthTestSchema(t, indexPropertyLength),
					params: aggregation.Params{
						ClassName:                 lengthTestClass,
						Properties:                []aggregation.ParamProperty{prop},
						OnlyRequestedAggregations: !tc.readsUnrequested,
					},
				}}
				got, err := ua.properties(ctx)
				require.NoError(t, err)
				return got[prop.Name.String()], openedReplaceCursors(t) - before
			}

			got, scans := aggregate(tc.indexPropertyLength)
			if tc.wantIndex {
				require.Zero(t, scans, "the objects must not be read")
			} else {
				require.Positive(t, scans, "the objects must be read")
			}

			fromObjects, scans := aggregate(false)
			require.Positive(t, scans)
			require.Equal(t, fromObjects, got)
			if tc.aggs != nil {
				require.Equal(t, lengthTestCount(tc.dt), lengthTestCountOf(t, got))
			}
		})
	}
}

func lengthTestCount(dt schema.DataType) int {
	n := 0
	for i := range lengthTestObjects {
		if v, ok := lengthTestValue(dt, i); ok {
			switch a := v.(type) {
			case []string:
				n += len(a)
			case []bool:
				n += len(a)
			case []float64:
				n += len(a)
			}
		}
	}
	return n
}

func lengthTestCountOf(t *testing.T, p aggregation.Property) int {
	switch p.Type {
	case aggregation.PropertyTypeText:
		return p.TextAggregation.Count
	case aggregation.PropertyTypeBoolean:
		return p.BooleanAggregation.Count
	case aggregation.PropertyTypeNumerical:
		return int(p.NumericalAggregations["count"].(float64))
	case aggregation.PropertyTypeDate:
		return int(p.DateAggregations["count"].(int64))
	default:
		t.Fatalf("unexpected property type %s", p.Type)
		return 0
	}
}
