//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package sorter

import (
	"context"
	"fmt"
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
	store, err := lsmkv.New(dirName, dirName, logger, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)

	err = store.CreateOrLoadBucket(ctx, helpers.ObjectsBucketLSM)
	require.Nil(t, err)

	objectsB := store.Bucket(helpers.ObjectsBucketLSM)
	for i := 0; i < objectCount; i++ {
		objBytes, docID := createDummyObject(t, i)
		objectsB.Put([]byte(fmt.Sprintf("%08d", docID)), objBytes)
		require.Nil(t, err)
	}

	for _, propName := range []string{"int", "int2", "number", "date"} {
		err = store.CreateOrLoadBucket(ctx, helpers.BucketFromPropNameLSM(propName),
			lsmkv.WithStrategy(lsmkv.StrategyRoaringSet))
		require.Nil(t, err)
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
	for _, propName := range []string{"int", "int2", "number", "date"} {
		bucket := store.Bucket(helpers.BucketFromPropNameLSM(propName))
		for _, p := range props {
			var key []byte
			var err error
			switch propName {
			case "int":
				key, err = inverted.LexicographicallySortableInt64(p.int)
				require.Nil(t, err)
			case "int2":
				key, err = inverted.LexicographicallySortableInt64(p.int2)
				require.Nil(t, err)
			case "number":
				key, err = inverted.LexicographicallySortableFloat64(p.number)
				require.Nil(t, err)
			case "date":
				key, err = inverted.LexicographicallySortableInt64(p.date.UnixNano())
				require.Nil(t, err)
			}
			err = bucket.RoaringSetAddOne(key, p.docID)
			require.Nil(t, err)
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
				Name:          "date",
				DataType:      []string{string(schema.DataTypeDate)},
				IndexInverted: &t,
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
