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

package sorter

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"runtime"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/inverted"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/test/helper"
)

// this test is invoked through inverted_sorter_race_test.go and
// inverted_sorter_no_race_test.go respectively
func TestInvertedSorter(t *testing.T) {
	forceFlush := []bool{false, true}
	propNames := []string{"int", "int2", "number", "date"}
	limits := []int{1, 2, 5, 10, 100, 373, 500, 1000, 2000}
	order := []string{"asc", "desc"}
	objectCounts := []int{87, 100, 133, 500, 1000, 10000}
	matchers := []func(t *testing.T, count int) helpers.AllowList{
		matchAllBitmap,
		matchEveryOtherBitmap,
		match10PercentBitmap,
		matchRandomBitmap,
		matchSingleBitmap,
		nilBitmap,
	}

	if helper.RaceDetectorEnabled {
		t.Log("race detector is on, reduce scope of test to avoid timeouts")
		propNames = []string{"int"}
		limits = []int{5}
		order = []string{"asc", "desc"}
		objectCounts = []int{500}
		matchers = matchers[:1]
	} else {
		t.Log("race detector is off, run full test suite")
	}

	for _, objectCount := range objectCounts {
		var (
			dirName   = t.TempDir()
			logger, _ = test.NewNullLogger()
			ctx       = context.Background()
		)

		t.Run(fmt.Sprintf("object count %d", objectCount), func(t *testing.T) {
			store := createStoreAndInitWithObjects(t, ctx, objectCount, dirName, logger)
			defer store.Shutdown(ctx)

			props := generateRandomProps(objectCount)
			dummyInvertedIndex(t, ctx, store, props)

			for _, flush := range forceFlush {
				t.Run(fmt.Sprintf("force flush %t", flush), func(t *testing.T) {
					if flush {
						err := store.Bucket(helpers.ObjectsBucketLSM).FlushAndSwitch()
						require.Nil(t, err)

						for _, propName := range propNames {
							err := store.Bucket(helpers.BucketFromPropNameLSM(propName)).FlushAndSwitch()
							require.Nil(t, err)
						}
					}

					for _, propName := range propNames {
						for _, limit := range limits {
							for _, ord := range order {
								for _, matcher := range matchers {
									fullFuncName := runtime.FuncForPC(reflect.ValueOf(matcher).Pointer()).Name()
									parts := strings.Split(fullFuncName, ".")
									matcherStr := parts[len(parts)-1]

									t.Run(fmt.Sprintf("prop=%s, order=%s limit=%d matcher %s", propName, ord, limit, matcherStr), func(t *testing.T) {
										sortParams := []filters.Sort{{Path: []string{propName}, Order: ord}}
										assertSorting(t, ctx, store, props, objectCount, limit, sortParams, matcher)
									})
								}
							}
						}
					}
				})
			}
		})
	}
}

func TestInvertedSorterMultiOrder(t *testing.T) {
	sortPlans := [][]filters.Sort{
		{{Path: []string{"int"}, Order: "desc"}, {Path: []string{"number"}, Order: "desc"}},
		{{Path: []string{"int"}, Order: "desc"}, {Path: []string{"number"}, Order: "asc"}},
		{{Path: []string{"int"}, Order: "asc"}, {Path: []string{"number"}, Order: "asc"}},
		{{Path: []string{"int"}, Order: "asc"}, {Path: []string{"number"}, Order: "desc"}},
		{{Path: []string{"int"}, Order: "asc"}, {Path: []string{"int2"}, Order: "desc"}, {Path: []string{"number"}, Order: "desc"}},
	}

	forceFlush := []bool{false, true}
	limits := []int{1, 2, 5, 10, 100, 500, 1000, 2000}
	objectCounts := []int{87, 100, 133, 500, 1000, 2000}

	matchers := []func(t *testing.T, count int) helpers.AllowList{
		matchAllBitmap,
		matchEveryOtherBitmap,
		match10PercentBitmap,
		matchRandomBitmap,
		matchSingleBitmap,
		nilBitmap,
	}

	if helper.RaceDetectorEnabled {
		t.Log("race detector is on, reduce scope of test to avoid timeouts")
		limits = []int{5}
		objectCounts = []int{500}
		matchers = matchers[:1]
	} else {
		t.Log("race detector is off, run full test suite")
	}

	for _, objectCount := range objectCounts {
		t.Run(fmt.Sprintf("object count %d", objectCount), func(t *testing.T) {
			var (
				dirName   = t.TempDir()
				logger, _ = test.NewNullLogger()
				ctx       = context.Background()
			)

			store := createStoreAndInitWithObjects(t, ctx, objectCount, dirName, logger)
			defer store.Shutdown(ctx)

			props := generateRandomProps(objectCount)
			dummyInvertedIndex(t, ctx, store, props)

			for _, flush := range forceFlush {
				t.Run(fmt.Sprintf("force flush %t", flush), func(t *testing.T) {
					if flush {
						err := store.Bucket(helpers.ObjectsBucketLSM).FlushAndSwitch()
						require.Nil(t, err)

						for _, propName := range []string{"int", "number", "date"} {
							err := store.Bucket(helpers.BucketFromPropNameLSM(propName)).FlushAndSwitch()
							require.Nil(t, err)
						}
					}

					for _, sortParam := range sortPlans {
						sortPlanStrings := make([]string, 0, len(sortParam))
						for _, sp := range sortParam {
							sortPlanStrings = append(sortPlanStrings, fmt.Sprintf("%s %s", sp.Path[0], sp.Order))
						}
						sortPlanString := strings.Join(sortPlanStrings, " -> ")
						for _, limit := range limits {
							for _, matcher := range matchers {
								fullFuncName := runtime.FuncForPC(reflect.ValueOf(matcher).Pointer()).Name()
								parts := strings.Split(fullFuncName, ".")
								matcherStr := parts[len(parts)-1]

								t.Run(fmt.Sprintf("sort=%s limit=%d matcher=%s", sortPlanString, limit, matcherStr), func(t *testing.T) {
									assertSorting(t, ctx, store, props, objectCount, limit, sortParam, matcher)
								})
							}
						}
					}
				})
			}
		})
	}
}

func createStoreAndInitWithObjects(t *testing.T, ctx context.Context, objectCount int,
	dirName string, logger logrus.FieldLogger,
) *lsmkv.Store {
	store := newSortTestStore(t, ctx, dirName, logger)
	objectsB := store.Bucket(helpers.ObjectsBucketLSM)
	for i := 0; i < objectCount; i++ {
		objBytes, docID := createDummyObject(t, i)
		require.NoError(t, objectsB.Put([]byte(fmt.Sprintf("%08d", docID)), objBytes))
	}
	return store
}

func newSortTestStore(t testing.TB, ctx context.Context, dirName string, logger logrus.FieldLogger,
	objectsOpts ...lsmkv.BucketOption,
) *lsmkv.Store {
	store, err := lsmkv.New(dirName, dirName, logger, nil, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)

	require.NoError(t, store.CreateOrLoadBucket(ctx, helpers.ObjectsBucketLSM,
		append([]lsmkv.BucketOption{lsmkv.WithStrategy(lsmkv.StrategyReplace)}, objectsOpts...)...))
	for _, propName := range []string{"int", "int2", "number", "date"} {
		require.NoError(t, store.CreateOrLoadBucket(ctx, helpers.BucketFromPropNameLSM(propName),
			lsmkv.WithStrategy(lsmkv.StrategyRoaringSet)))
	}
	return store
}

func assertSorting(t *testing.T, ctx context.Context, store *lsmkv.Store,
	props []dummyProps, objectCount int, limit int, sortParams []filters.Sort,
	matcher func(t *testing.T, count int) helpers.AllowList,
) {
	ctx = helpers.InitSlowQueryDetails(ctx)
	sorter := NewInvertedSorter(store, newDataTypesHelper(dummyClass()))
	bm := matcher(t, objectCount)

	actual, err := sorter.SortDocIDs(ctx, limit, sortParams, bm)
	require.Nil(t, err)

	sortedProps := filterAndSortControl(t, props, bm, sortParams)

	expectedLength := min(len(sortedProps), limit)
	assert.Len(t, actual, expectedLength)

	for i, docID := range actual {
		assert.Equal(t, int(sortedProps[i].docID), int(docID))
	}

	// enable below for debugging
	// sl := helpers.ExtractSlowQueryDetails(ctx)
	// t.Log(sl)
}

func createDummyObject(t *testing.T, i int) ([]byte, uint64) {
	docID := uint64(i)
	// we will never read those objects, so we don't actually have to store any
	// props, empty objects are fine
	obj := storobj.New(docID)
	obj.SetID(strfmt.UUID(uuid.New().String()))
	objBytes, err := obj.MarshalBinary()
	require.Nil(t, err)

	return objBytes, docID
}

type dummyProps struct {
	docID  uint64
	int    int64
	int2   int64
	number float64
	date   time.Time
}

func generateRandomProps(count int) []dummyProps {
	props := make([]dummyProps, count)
	for i := 0; i < count; i++ {
		props[i] = dummyProps{
			docID:  uint64(i),
			int:    rand.Int63n(10),                              // few values, many collisions
			int2:   rand.Int63n(100),                             // still some collisions, but not as many
			number: rand.Float64() * 100,                         // many values, few collisions
			date:   time.Now().Add(time.Duration(i) * time.Hour), // guaranteed to be unique
		}
	}
	return props
}

func dummyInvertedIndex(t *testing.T, ctx context.Context, store *lsmkv.Store, props []dummyProps) {
	for _, p := range props {
		r := sortRow{int: &p.int, int2: p.int2, number: p.number, date: p.date}
		for propName, key := range sortRowIndexKeys(t, r) {
			require.NoError(t, store.Bucket(helpers.BucketFromPropNameLSM(propName)).RoaringSetAddOne(key, p.docID))
		}
	}
}

func dummyClass() *models.Class {
	f := false
	t := true
	return &models.Class{
		Class: "DummyClass",
		Properties: []*models.Property{
			{
				Name:          "int",
				DataType:      []string{string(schema.DataTypeInt)},
				IndexInverted: &t,
			},
			{
				Name:          "int2",
				DataType:      []string{string(schema.DataTypeInt)},
				IndexInverted: &t,
			},
			{
				Name:          "number",
				DataType:      []string{string(schema.DataTypeNumber)},
				IndexInverted: &t,
			},
			{
				Name:            "date",
				DataType:        []string{string(schema.DataTypeDate)},
				IndexInverted:   &t,
				IndexFilterable: &t,
			},
			{
				Name:          "int_not_indexed",
				DataType:      []string{string(schema.DataTypeInt)},
				IndexInverted: &f,
			},
			{
				Name:          "int_corrupt_index",
				DataType:      []string{string(schema.DataTypeInt)},
				IndexInverted: &t,
			},
			{
				Name:            "int_reindexing",
				DataType:        []string{string(schema.DataTypeInt)},
				IndexFilterable: &f,
			},
			{
				Name:          "text",
				DataType:      []string{string(schema.DataTypeText)},
				IndexInverted: &t,
			},
		},
	}
}

func filterAndSortControl(t *testing.T, input []dummyProps, bm helpers.AllowList,
	sortParams []filters.Sort,
) []dummyProps {
	// sort props as control, always first by doc id, then by the prop,
	// this way we have a consistent tie breaker
	sortedProps := make([]dummyProps, 0, len(input))
	for _, p := range input {
		if bm == nil || bm.Contains(p.docID) {
			sortedProps = append(sortedProps, p)
		}
	}

	sort.Slice(sortedProps, func(i, j int) bool {
		return sortedProps[i].docID < sortedProps[j].docID
	})

	for sortIndex := len(sortParams) - 1; sortIndex >= 0; sortIndex-- {
		var sortFn func(i, j int) bool
		sortParam := sortParams[sortIndex]
		switch sortParam.Path[0] {
		case "int":
			sortFn = func(i, j int) bool {
				if sortParam.Order == "desc" {
					return sortedProps[j].int < sortedProps[i].int
				}
				return sortedProps[i].int < sortedProps[j].int
			}
		case "int2":
			sortFn = func(i, j int) bool {
				if sortParam.Order == "desc" {
					return sortedProps[j].int2 < sortedProps[i].int2
				}
				return sortedProps[i].int2 < sortedProps[j].int2
			}
		case "number":
			sortFn = func(i, j int) bool {
				if sortParam.Order == "desc" {
					return sortedProps[i].number > sortedProps[j].number
				}
				return sortedProps[i].number < sortedProps[j].number
			}
		case "date":
			sortFn = func(i, j int) bool {
				if sortParam.Order == "desc" {
					return sortedProps[i].date.After(sortedProps[j].date)
				}
				return sortedProps[i].date.Before(sortedProps[j].date)
			}
		}
		sort.SliceStable(sortedProps, sortFn)
	}

	return sortedProps
}

func matchAllBitmap(t *testing.T, count int) helpers.AllowList {
	ids := make([]uint64, count)
	for i := 0; i < count; i++ {
		ids[i] = uint64(i)
	}
	return helpers.NewAllowList(ids...)
}

func matchEveryOtherBitmap(t *testing.T, count int) helpers.AllowList {
	ids := make([]uint64, count/2)
	for i := 0; i < count; i += 2 {
		if i/2 >= len(ids)-1 {
			break
		}
		ids[i/2] = uint64(i)
	}
	return helpers.NewAllowList(ids...)
}

func match10PercentBitmap(t *testing.T, count int) helpers.AllowList {
	ids := make([]uint64, count/10)
	for i := 0; i < count; i += 10 {
		if i/10 >= len(ids)-1 {
			break
		}

		ids[i/10] = uint64(i)
	}
	return helpers.NewAllowList(ids...)
}

func matchRandomBitmap(t *testing.T, count int) helpers.AllowList {
	ids := make([]uint64, 0, count)
	for len(ids) < count/2 {
		id := uint64(rand.Intn(count))
		if !slices.Contains(ids, id) {
			ids = append(ids, id)
		}
	}
	return helpers.NewAllowList(ids...)
}

func matchSingleBitmap(t *testing.T, count int) helpers.AllowList {
	id := uint64(rand.Intn(count))
	return helpers.NewAllowList(id)
}

func nilBitmap(t *testing.T, count int) helpers.AllowList {
	return nil
}

// sortRow is one object for the candidate tests. A nil int leaves the object
// with no int value and no index entry for it.
type sortRow struct {
	docID  uint64
	int    *int64
	int2   int64
	number float64
	date   time.Time
	text   string
}

func sortRows(n int, row func(i int) sortRow) []sortRow {
	rows := make([]sortRow, n)
	for i := range rows {
		rows[i] = row(i)
		rows[i].docID = uint64(i)
	}
	return rows
}

func groupedRow(groupSize int) func(i int) sortRow {
	return func(i int) sortRow {
		group := int64(i / groupSize)
		return sortRow{
			int:    &group,
			int2:   int64(i % 3),
			number: float64((i * 37) % 101),
			date:   time.Unix(1_700_000_000+int64(i), 0).UTC(),
			text:   fmt.Sprintf("t%03d", (i*53)%97),
		}
	}
}

func docIDKey(docID uint64) []byte {
	return binary.LittleEndian.AppendUint64(nil, docID)
}

func newCandidateTestSorter(t testing.TB) (*lsmSorter, *lsmkv.Store) {
	logger, _ := test.NewNullLogger()
	ctx := context.Background()
	store := newSortTestStore(t, ctx, t.TempDir(), logger,
		lsmkv.WithSecondaryIndices(1), lsmkv.WithCalcCountNetAdditions(true))
	t.Cleanup(func() { store.Shutdown(ctx) })

	s, err := NewLSMSorter(store, func(string) *models.Class { return dummyClass() }, "DummyClass", nil)
	require.NoError(t, err)
	return s.(*lsmSorter), store
}

func putSortRow(t testing.TB, store *lsmkv.Store, r sortRow) {
	props := map[string]interface{}{
		"int2":   float64(r.int2),
		"number": r.number,
		"date":   r.date.Format(time.RFC3339Nano),
		"text":   r.text,
	}
	if r.int != nil {
		props["int"] = float64(*r.int)
	}
	obj := storobj.New(r.docID)
	obj.SetID(strfmt.UUID(uuid.New().String()))
	obj.SetProperties(props)
	objBytes, err := obj.MarshalBinary()
	require.NoError(t, err)
	require.NoError(t, store.Bucket(helpers.ObjectsBucketLSM).Put(docIDKey(r.docID), objBytes,
		lsmkv.WithSecondaryKey(0, docIDKey(r.docID))))

	for propName, key := range sortRowIndexKeys(t, r) {
		require.NoError(t, store.Bucket(helpers.BucketFromPropNameLSM(propName)).RoaringSetAddOne(key, r.docID))
	}
}

func sortRowIndexKeys(t testing.TB, r sortRow) map[string][]byte {
	keys := map[string][]byte{}
	var err error
	if r.int != nil {
		keys["int"], err = inverted.LexicographicallySortableInt64(*r.int)
		require.NoError(t, err)
	}
	keys["int2"], err = inverted.LexicographicallySortableInt64(r.int2)
	require.NoError(t, err)
	keys["number"], err = inverted.LexicographicallySortableFloat64(r.number)
	require.NoError(t, err)
	keys["date"], err = inverted.LexicographicallySortableInt64(r.date.UnixNano())
	require.NoError(t, err)
	return keys
}

func flushSortBuckets(t testing.TB, store *lsmkv.Store) {
	require.NoError(t, store.Bucket(helpers.ObjectsBucketLSM).FlushAndSwitch())
	for _, propName := range []string{"int", "int2", "number", "date"} {
		require.NoError(t, store.Bucket(helpers.BucketFromPropNameLSM(propName)).FlushAndSwitch())
	}
}

// sortByObjects is the objects-bucket strategy of Sort (nil ids) and SortDocIDs.
func sortByObjects(t testing.TB, s *lsmSorter, limit int, sort []filters.Sort, ids helpers.AllowList) []uint64 {
	ctx := context.Background()
	if ids == nil {
		sorted, err := s.sortAllByObjects(ctx, limit, sort)
		require.NoError(t, err)
		return sorted
	}
	helper, err := s.createHelper(sort, validateLimit(limit, ids.Len()))
	require.NoError(t, err)
	sorted, err := helper.getSortedDocIDs(ctx, ids)
	require.NoError(t, err)
	return sorted
}

func everyNth(rows []sortRow, n int) helpers.AllowList {
	ids := make([]uint64, 0, len(rows)/n+1)
	for i := 0; i < len(rows); i += n {
		ids = append(ids, rows[i].docID)
	}
	return helpers.NewAllowList(ids...)
}

func TestLSMSorterMultiPropertyCandidatesMatchObjectsPath(t *testing.T) {
	missingEvery4th := func(i int) sortRow {
		r := groupedRow(7)(i)
		if i%4 == 0 {
			r.int = nil
		}
		return r
	}
	sameFirst := func(i int) sortRow {
		r := groupedRow(1)(i)
		five := int64(5)
		r.int = &five
		return r
	}

	tests := []struct {
		name       string
		rows       []sortRow
		sort       []filters.Sort
		limit      int
		unfiltered bool
		flush      bool
		fallsBack  bool
	}{
		{
			name: "asc first, missing first value omitted", rows: sortRows(100, missingEvery4th),
			sort: sort2("int", "asc", "text", "desc"), limit: 10,
		},
		{
			// the limit reaches past the 75 objects with a value
			name: "desc first, missing first value omitted", rows: sortRows(100, missingEvery4th),
			sort: sort2("int", "desc", "text", "desc"), limit: 90,
		},
		{
			name: "unfiltered, every object ties", rows: sortRows(100, sameFirst),
			sort: sort2("int", "asc", "text", "desc"), limit: 10, unfiltered: true, flush: true, fallsBack: true,
		},
		{
			name: "unfiltered, small tie group", rows: sortRows(100, groupedRow(7)),
			sort: sort2("int", "asc", "text", "desc"), limit: 10, unfiltered: true,
		},
		{
			name: "filtered, every object ties", rows: sortRows(100, sameFirst),
			sort: sort2("int", "asc", "text", "desc"), limit: 10, flush: true,
		},
		{
			// GraphQL leaves order empty when the query omits it
			name: "empty first order sorts ascending", rows: sortRows(100, groupedRow(7)),
			sort: sort2("int", "", "text", ""), limit: 10,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, store := newCandidateTestSorter(t)
			for _, r := range tc.rows {
				putSortRow(t, store, r)
			}
			if tc.flush {
				flushSortBuckets(t, store)
			}

			// objects without a first value have no index entry, so the candidate
			// path omits them
			var withFirst []uint64
			for _, r := range tc.rows {
				if r.int != nil {
					withFirst = append(withFirst, r.docID)
				}
			}
			ids, expectedIDs := everyNth(tc.rows, 1), helpers.NewAllowList(withFirst...)
			if tc.unfiltered {
				ids, expectedIDs = nil, nil
			}
			expected := sortByObjects(t, s, tc.limit, tc.sort, expectedIDs)
			require.NotEmpty(t, expected)

			ctx := helpers.InitSlowQueryDetails(context.Background())
			actual, err := s.sortInverted(ctx, tc.limit, tc.sort, ids)
			require.NoError(t, err)
			assert.Equal(t, expected, actual)
			fallback := strings.Contains(fmt.Sprint(helpers.ExtractSlowQueryDetails(ctx)["sort_query_planner"]),
				"sorting all objects from objects bucket")
			assert.Equal(t, tc.fallsBack, fallback)
		})
	}
}

func TestLSMSorterRejectsNegativeLimit(t *testing.T) {
	s, store := newCandidateTestSorter(t)
	for _, r := range sortRows(20, groupedRow(1)) {
		putSortRow(t, store, r)
	}

	for _, sort := range [][]filters.Sort{sort1("int", "asc"), sort2("int", "asc", "text", "asc")} {
		_, err := s.Sort(context.Background(), -1, sort)
		assert.ErrorContains(t, err, "negative sort limit")
		_, err = s.SortDocIDs(context.Background(), -1, sort, helpers.NewAllowList(1, 2, 3))
		assert.ErrorContains(t, err, "negative sort limit")
	}
}

func TestLSMSorterRoutesMultiPropertySortThroughCandidates(t *testing.T) {
	s, store := newCandidateTestSorter(t)
	rows := sortRows(200, groupedRow(7))
	for _, r := range rows {
		putSortRow(t, store, r)
	}
	flushSortBuckets(t, store)
	multi := sort2("int", "asc", "text", "desc")
	sortAll := func(ctx context.Context, limit int, sort []filters.Sort, _ helpers.AllowList) ([]uint64, error) {
		return s.Sort(ctx, limit, sort)
	}
	sortDocIDs := func(ctx context.Context, limit int, sort []filters.Sort, ids helpers.AllowList) ([]uint64, error) {
		return s.SortDocIDs(ctx, limit, sort, ids)
	}

	tests := []struct {
		name     string
		ids      helpers.AllowList
		sort     []filters.Sort
		limit    int
		strategy string
		run      func(ctx context.Context, limit int, sort []filters.Sort, ids helpers.AllowList) ([]uint64, error)
	}{
		{name: "Sort", sort: multi, limit: 10, strategy: "inverted index candidates", run: sortAll},
		{
			name: "SortDocIDs", ids: everyNth(rows, 2), sort: multi, limit: 10,
			strategy: "inverted index candidates", run: sortDocIDs,
		},
		{
			name: "Sort, first property not indexable", sort: sort2("text", "asc", "int", "asc"), limit: 10,
			strategy: "objects bucket", run: sortAll,
		},
		{
			name: "SortDocIDs, single property", ids: everyNth(rows, 2), sort: sort1("int", "asc"), limit: 10,
			strategy: "inverted index", run: sortDocIDs,
		},
		// limit 0 sorts every match, so the candidate walk would leave none unread
		{name: "Sort, limit 0", sort: multi, strategy: "objects bucket", run: sortAll},
		{name: "SortDocIDs, limit 0", ids: everyNth(rows, 2), sort: multi, strategy: "objects bucket", run: sortDocIDs},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := helpers.InitSlowQueryDetails(context.Background())
			actual, err := tc.run(ctx, tc.limit, tc.sort, tc.ids)
			require.NoError(t, err)
			assert.Equal(t, sortByObjects(t, s, tc.limit, tc.sort, tc.ids), actual)
			assert.Contains(t, fmt.Sprint(helpers.ExtractSlowQueryDetails(ctx)["sort_query_planner"]),
				fmt.Sprintf("START EXECUTING with %s strategy", tc.strategy))
		})
	}
}

func TestLSMSorterSortInvertedReturnsCandidatesError(t *testing.T) {
	s, _ := newCandidateTestSorter(t)
	_, err := s.sortInverted(context.Background(), 10, sort2("int_not_indexed", "asc", "text", "asc"), nil)
	require.ErrorIs(t, err, lsmkv.ErrBucketNotFound)
}

// BenchmarkMultiPropertySort compares the objects path with the candidate path
// on a keyset filter, date > X. The matches sit at the high end of the index, so
// the ascending candidate walk passes every non-matching row first.
func BenchmarkMultiPropertySort(b *testing.B) {
	const objectCount = 50_000
	ctx := context.Background()
	s, store := newCandidateTestSorter(b)
	rows := sortRows(objectCount, groupedRow(1))
	for _, r := range rows {
		putSortRow(b, store, r)
	}
	flushSortBuckets(b, store)
	sort := sort2("date", "asc", "text", "asc")
	const limit = 101

	for _, matchCount := range []int{500, 5_000, 25_000} {
		ids := everyNth(rows[objectCount-matchCount:], 1)
		b.Run(fmt.Sprintf("matches=%d/objects", matchCount), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				helper, err := s.createHelper(sort, limit)
				require.NoError(b, err)
				_, err = helper.getSortedDocIDs(ctx, ids)
				require.NoError(b, err)
			}
		})
		b.Run(fmt.Sprintf("matches=%d/candidates", matchCount), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, err := s.sortInverted(ctx, limit, sort, ids)
				require.NoError(b, err)
			}
		})
	}
}

func TestInvertedSorterCandidates(t *testing.T) {
	rowsWhere := func(keep func(i int) bool) func([]sortRow) []uint64 {
		return func(rows []sortRow) []uint64 {
			var ids []uint64
			for i, r := range rows {
				if keep(i) {
					ids = append(ids, r.docID)
				}
			}
			return ids
		}
	}
	tests := []struct {
		name    string
		rows    []sortRow
		ids     func(rows []sortRow) []uint64 // nil walks unfiltered
		order   string
		limit   int
		maxIDs  int // 0 sets no cap
		tooMany bool
	}{
		{
			name: "asc, limit on a group boundary", rows: sortRows(100, groupedRow(7)),
			ids: rowsWhere(func(i int) bool { return true }), order: "asc", limit: 14,
		},
		{
			// the top group holds 6, so 13 ends on a boundary inside a window
			name: "desc, limit on a group boundary", rows: sortRows(1000, groupedRow(7)),
			ids: rowsWhere(func(i int) bool { return true }), order: "desc", limit: 13,
		},
		{
			// one match far above a dense low range makes the walk take several
			// quantile windows, with rows of different sizes
			name: "desc, uneven rows over several windows", rows: sortRows(1000, groupedRow(10)),
			ids: rowsWhere(func(i int) bool {
				key := i / 10
				return i == 605 || (key <= 20 && i%10 < key%4+1)
			}),
			order: "desc", limit: 5,
		},
		{
			// the candidates are the 14 IDs of the first two groups
			name: "asc, cap at the candidate count", rows: sortRows(100, groupedRow(7)),
			ids: rowsWhere(func(i int) bool { return true }), order: "asc", limit: 10, maxIDs: 14,
		},
		{
			name: "asc, cap below the candidate count", rows: sortRows(100, groupedRow(7)),
			ids: rowsWhere(func(i int) bool { return true }), order: "asc", limit: 10, maxIDs: 13, tooMany: true,
		},
		{
			// the candidates are the top group's 2 IDs and the next group's 7
			name: "desc, cap at the candidate count", rows: sortRows(100, groupedRow(7)),
			ids: rowsWhere(func(i int) bool { return true }), order: "desc", limit: 5, maxIDs: 9,
		},
		{
			name: "desc, cap below the candidate count", rows: sortRows(100, groupedRow(7)),
			ids: rowsWhere(func(i int) bool { return true }), order: "desc", limit: 5, maxIDs: 8, tooMany: true,
		},
		{
			name: "desc unfiltered, limit on a group boundary", rows: sortRows(1000, groupedRow(7)),
			order: "desc", limit: 13,
		},
		{
			name: "desc unfiltered, cap at the candidate count", rows: sortRows(100, groupedRow(7)),
			order: "desc", limit: 5, maxIDs: 9,
		},
		{
			name: "desc unfiltered, cap below the candidate count", rows: sortRows(100, groupedRow(7)),
			order: "desc", limit: 5, maxIDs: 8, tooMany: true,
		},
		{
			// fewer than 2*limit objects make one window, so the row of 0s is read with the 6 above it
			name: "desc unfiltered, rows below the cut-off do not count toward the cap",
			rows: sortRows(11, func(i int) sortRow {
				r := groupedRow(1)(i)
				*r.int = max(int64(i)-4, 0)
				return r
			}),
			order: "desc", limit: 6, maxIDs: 6,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s, store := newCandidateTestSorter(t)
			for _, r := range tc.rows {
				putSortRow(t, store, r)
			}
			flushSortBuckets(t, store)
			var allowList helpers.AllowList
			ids := rowsWhere(func(i int) bool { return true })(tc.rows)
			if tc.ids != nil {
				ids = tc.ids(tc.rows)
				allowList = helpers.NewAllowList(ids...)
			}
			maxIDs := tc.maxIDs
			if maxIDs == 0 {
				maxIDs = math.MaxInt
			}

			is := NewInvertedSorter(store, s.dataTypesHelper)
			actual, err := is.Candidates(context.Background(), tc.limit,
				filters.Sort{Path: []string{"int"}, Order: tc.order}, allowList, maxIDs)
			if tc.tooMany {
				require.ErrorIs(t, err, errTooManyCandidates)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, firstLimitWithTies(tc.rows, ids, tc.order, tc.limit), actual)
		})
	}
}

// An unfiltered descending walk must fail its cap before copying a large tie row's IDs out.
func TestInvertedSorterCandidatesDescCapCopiesNoIDs(t *testing.T) {
	const tieRow = 1 << 18
	s, store := newCandidateTestSorter(t)
	bucket := store.Bucket(helpers.BucketFromPropNameLSM("int"))
	key, err := inverted.LexicographicallySortableInt64(1)
	require.NoError(t, err)
	docIDs := make([]uint64, tieRow)
	for i := range docIDs {
		docIDs[i] = uint64(i)
	}
	require.NoError(t, bucket.RoaringSetAddList(key, docIDs))
	require.NoError(t, bucket.FlushAndSwitch())

	is := NewInvertedSorter(store, s.dataTypesHelper)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err = is.Candidates(context.Background(), 10, filters.Sort{Path: []string{"int"}, Order: "desc"},
		nil, tieRow-1)
	runtime.ReadMemStats(&after)

	require.ErrorIs(t, err, errTooManyCandidates)
	assert.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(tieRow*8/4),
		"copying the row's IDs alone allocates 8 bytes per ID")
}

// firstLimitWithTies orders the rows in ids by int, then by doc ID, and keeps
// the first limit plus every row tying with the last of them.
func firstLimitWithTies(rows []sortRow, ids []uint64, order string, limit int) []uint64 {
	var matches []sortRow
	for _, r := range rows {
		if slices.Contains(ids, r.docID) {
			matches = append(matches, r)
		}
	}
	slices.SortStableFunc(matches, func(a, b sortRow) int {
		if order == "desc" {
			return int(*b.int - *a.int)
		}
		return int(*a.int - *b.int)
	})
	out := make([]uint64, 0, len(matches))
	for i, r := range matches {
		if i >= limit && *r.int != *matches[limit-1].int {
			break
		}
		out = append(out, r.docID)
	}
	return out
}

// BenchmarkMultiPropertySortTieGroups compares, without a filter, reading all
// objects with a cursor against looking up every candidate, as the candidates'
// share of the objects grows. FullScanCandidateShare is where the two should cross.
func BenchmarkMultiPropertySortTieGroups(b *testing.B) {
	const objectCount = 20_000
	ctx := context.Background()
	s, store := newCandidateTestSorter(b)
	for _, r := range sortRows(objectCount, groupedRow(objectCount/10)) {
		putSortRow(b, store, r)
	}
	flushSortBuckets(b, store)
	const limit = 101

	// a limit inside the k-th of 10 groups makes the first k groups, k/10 of
	// the objects, candidates
	for _, groups := range []int{1, 3, 5, 9} {
		sort := sort2("int", "asc", "text", "asc")
		limit := groups*objectCount/10 - objectCount/20 + limit
		b.Run(fmt.Sprintf("share=%d0%%/objects", groups), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, err := s.sortAllByObjects(ctx, limit, sort)
				require.NoError(b, err)
			}
		})
		b.Run(fmt.Sprintf("share=%d0%%/candidates", groups), func(b *testing.B) {
			is := NewInvertedSorter(store, s.dataTypesHelper)
			for i := 0; i < b.N; i++ {
				candidates, err := is.Candidates(ctx, limit, sort[0], nil, math.MaxInt)
				require.NoError(b, err)
				helper, err := s.createHelper(sort, validateLimit(limit, len(candidates)))
				require.NoError(b, err)
				_, err = helper.getSortedDocIDs(ctx, helpers.NewAllowList(candidates...))
				require.NoError(b, err)
			}
		})
	}
}
