package sorter

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
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
)

func TestInvertedSorter(t *testing.T) {
	var (
		dirName     = t.TempDir()
		logger, _   = test.NewNullLogger()
		objectCount = 1000
	)

	ctx := context.Background()

	store, err := lsmkv.New(dirName, dirName, logger, nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop())
	require.Nil(t, err)

	defer store.Shutdown(ctx)

	err = store.CreateOrLoadBucket(ctx, helpers.ObjectsBucketLSM)
	require.Nil(t, err)

	objectsB := store.Bucket(helpers.ObjectsBucketLSM)
	for i := 0; i < objectCount; i++ {
		objBytes, docID := createDummyObject(t, i)
		objectsB.Put([]byte(fmt.Sprintf("%08d", docID)), objBytes)
		require.Nil(t, err)
	}

	for _, propName := range []string{"int", "number", "date"} {
		err = store.CreateOrLoadBucket(ctx, helpers.BucketFromPropNameLSM(propName),
			lsmkv.WithStrategy(lsmkv.StrategyRoaringSet))
		require.Nil(t, err)
	}

	props := generateRandomProps(objectCount)
	dummyInvertedIndex(t, ctx, store, props)

	sorter := NewInvertedSorter(store, newDataTypesHelper(dummyClass()))

	forceFlush := []bool{false, true}
	propNames := []string{"int", "number", "date"}
	limits := []int{1, 2, 5, 10, 100, 500, 1000, 2000}
	order := []string{"asc", "desc"}

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
				t.Run(fmt.Sprintf("sort by %s", propName), func(t *testing.T) {
					for _, limit := range limits {
						t.Run(fmt.Sprintf("limit %d", limit), func(t *testing.T) {
							for _, ord := range order {
								t.Run(fmt.Sprintf("order %s", ord), func(t *testing.T) {
									actual, err := sorter.SortDocIDs(ctx, 100, []filters.Sort{{Path: []string{propName}, Order: ord}}, matchAllBitmap(t, objectCount))
									require.Nil(t, err)

									// sort props as control, always first by doc id, then by the prop,
									// this way we have a consistent tie breaker
									sortedProps := make([]dummyProps, len(props))
									copy(sortedProps, props)

									sort.Slice(sortedProps, func(i, j int) bool {
										return sortedProps[i].docID < sortedProps[j].docID
									})

									var sortFn func(i, j int) bool
									switch propName {
									case "int":
										sortFn = func(i, j int) bool {
											if ord == "desc" {
												return sortedProps[j].int < sortedProps[i].int
											}
											return sortedProps[i].int < sortedProps[j].int
										}
									case "number":
										sortFn = func(i, j int) bool {
											if ord == "desc" {
												return sortedProps[i].number > sortedProps[j].number
											}
											return sortedProps[i].number < sortedProps[j].number
										}
									case "date":
										sortFn = func(i, j int) bool {
											if ord == "desc" {
												return sortedProps[i].date.After(sortedProps[j].date)
											}
											return sortedProps[i].date.Before(sortedProps[j].date)
										}
									}

									sort.SliceStable(sortedProps, sortFn)

									for i, docID := range actual {
										assert.Equal(t, int(sortedProps[i].docID), int(docID))
									}
								})
							}
						})
					}
				})
			}
		})
	}
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
	number float64
	date   time.Time
}

func generateRandomProps(count int) []dummyProps {
	props := make([]dummyProps, count)
	for i := 0; i < count; i++ {
		props[i] = dummyProps{
			docID:  uint64(i),
			int:    rand.Int63n(10),                              // few values, many collisions
			number: rand.Float64() * 100,                         // many values, few collisions
			date:   time.Now().Add(time.Duration(i) * time.Hour), // guaranteed to be unique
		}
	}
	return props
}

func dummyInvertedIndex(t *testing.T, ctx context.Context, store *lsmkv.Store, props []dummyProps) {
	for _, propName := range []string{"int", "number", "date"} {
		bucket := store.Bucket(helpers.BucketFromPropNameLSM(propName))
		for _, p := range props {
			var key []byte
			var err error
			switch propName {
			case "int":
				key, err = inverted.LexicographicallySortableInt64(p.int)
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
	return &models.Class{
		Class: "DummyClass",
		Properties: []*models.Property{
			{
				Name:     "int",
				DataType: []string{string(schema.DataTypeInt)},
			},
			{
				Name:     "number",
				DataType: []string{string(schema.DataTypeNumber)},
			},
			{
				Name:     "date",
				DataType: []string{string(schema.DataTypeDate)},
			},
		},
	}
}

func matchAllBitmap(t *testing.T, count int) helpers.AllowList {
	ids := make([]uint64, count)
	for i := 0; i < count; i++ {
		ids[i] = uint64(i)
	}
	return helpers.NewAllowList(ids...)
}
