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
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/aggregation"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/monitoring"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

const (
	propsTestClass     = "PropsFixture"
	propsTestText      = "text"
	propsTestTextArray = "textArray"
	propsTestNumbers   = "numbers"
	propsTestInts      = "ints"
	propsTestBools     = "bools"
	propsTestDates     = "dates"
	propsTestRef       = "ref"
	propsTestObjects   = 400
)

type propsTestObject struct {
	text      string
	textArray []string
	numbers   []float64
	ints      []float64
	bools     []bool
	dates     []string
}

func newPropsTestObject(i int) propsTestObject {
	return propsTestObject{
		text:      fmt.Sprintf("t%d", i%7),
		textArray: []string{fmt.Sprintf("a%d", i%5), fmt.Sprintf("b%d", i%3)},
		numbers:   []float64{float64(i) + 0.5, -float64(i)},
		ints:      []float64{float64(i % 10)},
		bools:     []bool{i%2 == 0, i%3 == 0},
		dates: []string{time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).
			Add(time.Duration(i) * time.Hour).Format(time.RFC3339)},
	}
}

// TestUnfilteredProperties_ReadObjectsOnceForAllProperties pins that the
// properties read from the stored objects share one scan of the objects
// bucket, and that each still gets only its own values.
func TestUnfilteredProperties_ReadObjectsOnceForAllProperties(t *testing.T) {
	ctx := context.Background()
	store, objects := propsTestStore(t, ctx)

	countAggs := []aggregation.Aggregator{
		aggregation.CountAggregator, aggregation.SumAggregator,
		aggregation.MinimumAggregator, aggregation.MaximumAggregator,
	}
	dateAggs := []aggregation.Aggregator{
		aggregation.CountAggregator, aggregation.MinimumAggregator, aggregation.MaximumAggregator,
	}
	topAll := propsTestObjects // lists every distinct value, so none is cut off
	textAggs := []aggregation.Aggregator{{Type: aggregation.TopOccurrencesType, Limit: &topAll}}

	allProps := []aggregation.ParamProperty{
		{Name: propsTestText, Aggregators: textAggs},
		{Name: propsTestTextArray, Aggregators: textAggs},
		{Name: propsTestNumbers, Aggregators: countAggs},
		{Name: propsTestInts, Aggregators: countAggs},
		{Name: propsTestBools},
		{Name: propsTestDates, Aggregators: dateAggs},
		{Name: propsTestRef},
	}

	aggregate := func(t *testing.T, props []aggregation.ParamProperty) (map[string]aggregation.Property, float64) {
		t.Helper()
		before := openedReplaceCursors(t)
		logger, _ := test.NewNullLogger()
		ua := unfilteredAggregator{Aggregator: &Aggregator{
			store:     store,
			logger:    logger,
			getSchema: propsTestSchema(t),
			params:    aggregation.Params{ClassName: propsTestClass, Properties: props},
		}}
		got, err := ua.properties(ctx)
		require.NoError(t, err)
		return got, openedReplaceCursors(t) - before
	}

	_, oneScan := aggregate(t, allProps[:1])
	require.Positive(t, oneScan)

	got, cursors := aggregate(t, allProps)
	require.Equal(t, oneScan, cursors, "every object-read property must share one scan")

	// references are aggregated outside the repo
	require.NotContains(t, got, propsTestRef)
	require.Len(t, got, len(allProps)-1)

	wantText := func(values func(o propsTestObject) []string) aggregation.Text {
		agg := newTextAggregator(topAll)
		for _, o := range objects {
			for _, v := range values(o) {
				require.NoError(t, agg.AddText(v))
			}
		}
		return agg.Res()
	}
	for name, want := range map[string]aggregation.Text{
		propsTestText:      wantText(func(o propsTestObject) []string { return []string{o.text} }),
		propsTestTextArray: wantText(func(o propsTestObject) []string { return o.textArray }),
	} {
		require.Equal(t, aggregation.PropertyTypeText, got[name].Type, name)
		require.Equal(t, want.Count, got[name].TextAggregation.Count, name)
		// Res breaks ties by first byte only, so the order of equal counts is not stable
		require.ElementsMatch(t, want.Items, got[name].TextAggregation.Items, name)
	}

	wantNumerical := func(values func(o propsTestObject) []float64) aggregation.Property {
		agg := newNumericalAggregator()
		for _, o := range objects {
			for _, v := range values(o) {
				require.NoError(t, agg.AddNumberRow(v, 1))
			}
		}
		out := aggregation.Property{Type: aggregation.PropertyTypeNumerical}
		addNumericalAggregations(&out, countAggs, agg)
		return out
	}
	require.Equal(t, wantNumerical(func(o propsTestObject) []float64 { return o.numbers }), got[propsTestNumbers])
	require.Equal(t, wantNumerical(func(o propsTestObject) []float64 { return o.ints }), got[propsTestInts])

	boolAgg := newBoolAggregator()
	dateAgg := newDateAggregator()
	for _, o := range objects {
		for _, v := range o.bools {
			require.NoError(t, boolAgg.AddBool(v))
		}
		for _, v := range o.dates {
			require.NoError(t, dateAgg.AddTimestamp(v))
		}
	}
	require.Equal(t, aggregation.Property{
		Type: aggregation.PropertyTypeBoolean, BooleanAggregation: boolAgg.Res(),
	}, got[propsTestBools])

	wantDates := aggregation.Property{Type: aggregation.PropertyTypeDate}
	addDateAggregations(&wantDates, dateAggs, dateAgg)
	require.Equal(t, wantDates, got[propsTestDates])
}

func propsTestSchema(t *testing.T) schemaUC.SchemaGetter {
	class := &models.Class{
		Class: propsTestClass,
		Properties: []*models.Property{
			{Name: propsTestText, DataType: schema.DataTypeText.PropString()},
			{Name: propsTestTextArray, DataType: schema.DataTypeTextArray.PropString()},
			{Name: propsTestNumbers, DataType: schema.DataTypeNumberArray.PropString()},
			{Name: propsTestInts, DataType: schema.DataTypeIntArray.PropString()},
			{Name: propsTestBools, DataType: schema.DataTypeBooleanArray.PropString()},
			{Name: propsTestDates, DataType: schema.DataTypeDateArray.PropString()},
			{Name: propsTestRef, DataType: []string{propsTestClass}},
		},
	}
	getter := schemaUC.NewMockSchemaGetter(t)
	getter.EXPECT().ReadOnlyClass(propsTestClass).Return(class).Maybe()
	return getter
}

// propsTestStore writes the objects to one segment plus the memtable, metered
// so that opened cursors can be counted.
func propsTestStore(t *testing.T, ctx context.Context) (*lsmkv.Store, []propsTestObject) {
	t.Helper()

	metrics, err := lsmkv.NewMetrics(monitoring.GetMetrics(), propsTestClass, "shard")
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
		lsmkv.WithClassName(propsTestClass),
	))
	b := store.Bucket(helpers.ObjectsBucketLSM)
	require.NotNil(t, b)

	objects := make([]propsTestObject, propsTestObjects)
	for i := range objects {
		o := newPropsTestObject(i)
		objects[i] = o
		obj := storobj.FromObject(&models.Object{
			Class: propsTestClass,
			ID:    strfmt.UUID(uuid.NewSHA1(uuid.Nil, []byte(fmt.Sprint(i))).String()),
			Properties: map[string]interface{}{
				propsTestText:      o.text,
				propsTestTextArray: o.textArray,
				propsTestNumbers:   o.numbers,
				propsTestInts:      o.ints,
				propsTestBools:     o.bools,
				propsTestDates:     o.dates,
			},
		}, nil, nil, nil)
		obj.DocID = uint64(i)

		value, err := obj.MarshalBinary()
		require.NoError(t, err)
		key, err := uuid.MustParse(obj.ID().String()).MarshalBinary()
		require.NoError(t, err)
		docIDBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(docIDBytes, obj.DocID)
		require.NoError(t, b.Put(key, value,
			lsmkv.WithSecondaryKey(helpers.ObjectsBucketLSMDocIDSecondaryIndex, docIDBytes)))

		if i == propsTestObjects/2 {
			// a segment gives the scan seed keys, so it runs on several cursors
			require.NoError(t, b.FlushAndSwitch())
		}
	}

	return store, objects
}

// openedReplaceCursors reads a process-wide counter, so callers compare it
// before and after; no test in this package runs in parallel.
func openedReplaceCursors(t *testing.T) float64 {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, f := range families {
		if f.GetName() != "weaviate_lsm_bucket_opened_cursors" {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "strategy" && l.GetValue() == lsmkv.StrategyReplace {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}
