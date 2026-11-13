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
	"path/filepath"
	"strings"
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
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

const (
	scanTextProp   = "text"
	scanNumberProp = "numbers"
	scanBoolProp   = "bools"
	scanDateProp   = "dates"
	scanBulkProp   = "bulk"

	// Larger than the reusable cursor's 8 KiB read buffer, so some nodes span refills.
	scanMaxBulkLen = 10_000
)

// scanObject is what one live object holds for the properties under test.
type scanObject struct {
	text    string
	numbers []float64
	bools   []bool
	dates   []string
}

func newScanObject(i, version int) scanObject {
	return scanObject{
		// i%11 repeats values across objects, so occurrence counts exceed 1
		text:    fmt.Sprintf("t%d-v%d", i%11, version),
		numbers: []float64{float64(i), float64(version)*1000 + 0.5},
		bools:   []bool{(i+version)%2 == 0, i%3 == 0},
		dates: []string{time.Date(2020+version, 1, 1, 0, 0, 0, 0, time.UTC).
			Add(time.Duration(i) * time.Hour).Format(time.RFC3339)},
	}
}

// scanFixture writes objects to an objects bucket and tracks what is live, so
// an aggregation is checked against the final state rather than another scan.
type scanFixture struct {
	tb    testing.TB
	store *lsmkv.Store
	b     *lsmkv.Bucket
	dir   string
	live  map[int]scanObject
	// docIDs holds each stored key's current doc ID, which a delete must name
	docIDs map[int]uint64
	docID  uint64
}

func newScanFixture(tb testing.TB, ctx context.Context, pread bool) *scanFixture {
	tb.Helper()

	dir := tb.TempDir()
	logger, _ := test.NewNullLogger()

	store, err := lsmkv.New(dir, dir, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(tb, err)
	tb.Cleanup(func() { store.Shutdown(ctx) })

	require.NoError(tb, store.CreateOrLoadBucket(ctx, helpers.ObjectsBucketLSM,
		lsmkv.WithStrategy(lsmkv.StrategyReplace),
		lsmkv.WithSecondaryIndices(1),
		lsmkv.WithKeepTombstones(true),
		lsmkv.WithCalcCountNetAdditions(true),
		lsmkv.WithClassName(fixtureClassName),
		lsmkv.WithPread(pread),
		lsmkv.WithMinMMapSize(0),
	))

	b := store.Bucket(helpers.ObjectsBucketLSM)
	require.NotNil(tb, b)
	b.SetMemtableThreshold(1 << 40)

	return &scanFixture{tb: tb, store: store, b: b, dir: dir, live: map[int]scanObject{}, docIDs: map[int]uint64{}}
}

func (f *scanFixture) key(i int) []byte {
	key, err := uuid.NewSHA1(uuid.Nil, []byte(fmt.Sprint(i))).MarshalBinary()
	require.NoError(f.tb, err)
	return key
}

func (f *scanFixture) put(i, version int) {
	o := newScanObject(i, version)
	obj := storobj.FromObject(&models.Object{
		Class: fixtureClassName,
		ID:    strfmt.UUID(uuid.NewSHA1(uuid.Nil, []byte(fmt.Sprint(i))).String()),
		Properties: map[string]interface{}{
			scanTextProp:   o.text,
			scanNumberProp: o.numbers,
			scanBoolProp:   o.bools,
			scanDateProp:   o.dates,
			scanBulkProp:   strings.Repeat("x", (i*37+version*101)%scanMaxBulkLen),
		},
	}, nil, nil, nil)
	f.docID++
	obj.DocID = f.docID

	value, err := obj.MarshalBinary()
	require.NoError(f.tb, err)

	require.NoError(f.tb, f.b.Put(f.key(i), value, f.docIDKey(obj.DocID)))
	f.live[i] = o
	f.docIDs[i] = obj.DocID
}

func (f *scanFixture) docIDKey(docID uint64) lsmkv.SecondaryKeyOption {
	docIDBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(docIDBytes, docID)
	return lsmkv.WithSecondaryKey(helpers.ObjectsBucketLSMDocIDSecondaryIndex, docIDBytes)
}

func (f *scanFixture) delete(i int) {
	docID, ok := f.docIDs[i]
	require.True(f.tb, ok, "delete of a key that was never stored")
	require.NoError(f.tb, f.b.Delete(f.key(i), f.docIDKey(docID)))
	delete(f.live, i)
}

// aggregate runs prop through the unfiltered aggregator the way a query does.
func (f *scanFixture) aggregate(ctx context.Context, prop aggregation.ParamProperty) aggregation.Property {
	f.tb.Helper()

	class := &models.Class{
		Class: fixtureClassName,
		Properties: []*models.Property{
			{Name: scanTextProp, DataType: schema.DataTypeText.PropString()},
			{Name: scanNumberProp, DataType: schema.DataTypeNumberArray.PropString()},
			{Name: scanBoolProp, DataType: schema.DataTypeBooleanArray.PropString()},
			{Name: scanDateProp, DataType: schema.DataTypeDateArray.PropString()},
			{Name: scanBulkProp, DataType: schema.DataTypeText.PropString()},
		},
	}
	getter := schemaUC.NewMockSchemaGetter(f.tb)
	getter.EXPECT().ReadOnlyClass(fixtureClassName).Return(class).Maybe()

	logger, _ := test.NewNullLogger()
	ua := unfilteredAggregator{Aggregator: &Aggregator{
		store:     f.store,
		logger:    logger,
		getSchema: getter,
		params: aggregation.Params{
			ClassName:  fixtureClassName,
			Properties: []aggregation.ParamProperty{prop},
		},
	}}
	got, err := ua.properties(ctx)
	require.NoError(f.tb, err)
	require.Contains(f.tb, got, prop.Name.String())
	return got[prop.Name.String()]
}

func (f *scanFixture) flush() {
	require.NoError(f.tb, f.b.FlushAndSwitch())
}

// buildLayered spreads the objects over three segments and the memtable, with
// later layers overwriting and deleting keys of earlier ones. The scan's seed
// keys come from every segment, so some land on keys a newer layer replaced or
// deleted.
func buildLayered(f *scanFixture) {
	const initial = 600
	for i := 0; i < initial; i++ {
		f.put(i, 0)
	}
	f.flush()

	for i := 0; i < initial; i += 3 {
		f.put(i, 1)
	}
	for i := 0; i < initial; i += 5 {
		f.delete(i)
	}
	f.flush()

	for i := initial; i < initial+200; i++ {
		f.put(i, 0)
	}
	for i := 0; i < initial; i += 9 {
		f.put(i, 2) // revives some keys the previous segment deleted
	}
	for i := 1; i < initial; i += 7 {
		f.delete(i)
	}
	f.flush()

	for i := 2; i < initial+200; i += 13 {
		f.put(i, 3)
	}
	for i := 4; i < initial+200; i += 17 {
		f.delete(i)
	}
	for i := initial + 200; i < initial+250; i++ {
		f.put(i, 0)
	}

	require.Equal(f.tb, 3, countSegmentFiles(f.tb, filepath.Join(f.dir, helpers.ObjectsBucketLSM)))
}

// buildMemtableOnly leaves every object unflushed, so there are no seed keys
// and the scan runs on a single cursor from First.
func buildMemtableOnly(f *scanFixture) {
	for i := 0; i < 300; i++ {
		f.put(i, 0)
	}
	for i := 0; i < 300; i += 4 {
		f.put(i, 1)
	}
	for i := 0; i < 300; i += 6 {
		f.delete(i)
	}

	require.Zero(f.tb, countSegmentFiles(f.tb, filepath.Join(f.dir, helpers.ObjectsBucketLSM)))
}

// TestUnfilteredObjectScans_AggregateEveryLiveObjectOnce pins that the
// aggregations reading the objects bucket count each live object exactly once,
// with its newest value, in both access modes.
func TestUnfilteredObjectScans_AggregateEveryLiveObjectOnce(t *testing.T) {
	layouts := []struct {
		name  string
		build func(f *scanFixture)
	}{
		{name: "segments with overwrites, deletes and a memtable", build: buildLayered},
		{name: "memtable only", build: buildMemtableOnly},
	}

	for _, pread := range []bool{true, false} {
		for _, layout := range layouts {
			name := fmt.Sprintf("pread=%t/%s", pread, layout.name)
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				f := newScanFixture(t, ctx, pread)
				layout.build(f)
				require.NotEmpty(t, f.live)

				t.Run("text", func(t *testing.T) {
					limit := len(f.live) // every distinct value is listed, so none is cut off
					got := f.aggregate(ctx, aggregation.ParamProperty{
						Name:        scanTextProp,
						Aggregators: []aggregation.Aggregator{{Type: aggregation.TopOccurrencesType, Limit: &limit}},
					})

					want := newTextAggregator(limit)
					for _, o := range f.live {
						require.NoError(t, want.AddText(o.text))
					}
					wantRes := want.Res()
					require.Equal(t, wantRes.Count, got.TextAggregation.Count)
					// Res breaks ties by first byte only, so the order of equal counts is not stable
					require.ElementsMatch(t, wantRes.Items, got.TextAggregation.Items)
				})

				t.Run("number array", func(t *testing.T) {
					aggs := []aggregation.Aggregator{
						aggregation.CountAggregator, aggregation.SumAggregator,
						aggregation.MinimumAggregator, aggregation.MaximumAggregator,
					}
					got := f.aggregate(ctx, aggregation.ParamProperty{Name: scanNumberProp, Aggregators: aggs})

					agg := newNumericalAggregator()
					for _, o := range f.live {
						for _, n := range o.numbers {
							require.NoError(t, agg.AddNumberRow(n, 1))
						}
					}
					want := aggregation.Property{Type: aggregation.PropertyTypeNumerical}
					addNumericalAggregations(&want, aggs, agg)
					require.Equal(t, want, got)
				})

				t.Run("bool array", func(t *testing.T) {
					got := f.aggregate(ctx, aggregation.ParamProperty{Name: scanBoolProp})

					agg := newBoolAggregator()
					for _, o := range f.live {
						for _, v := range o.bools {
							require.NoError(t, agg.AddBool(v))
						}
					}
					require.Equal(t, agg.Res(), got.BooleanAggregation)
				})

				t.Run("date array", func(t *testing.T) {
					aggs := []aggregation.Aggregator{
						aggregation.CountAggregator, aggregation.MinimumAggregator, aggregation.MaximumAggregator,
					}
					got := f.aggregate(ctx, aggregation.ParamProperty{Name: scanDateProp, Aggregators: aggs})

					agg := newDateAggregator()
					for _, o := range f.live {
						for _, d := range o.dates {
							require.NoError(t, agg.AddTimestamp(d))
						}
					}
					want := aggregation.Property{Type: aggregation.PropertyTypeDate}
					addDateAggregations(&want, aggs, agg)
					require.Equal(t, want, got)
				})
			})
		}
	}
}
