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

package filters_tests

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate-go-client/v6"
	"github.com/weaviate/weaviate-go-client/v6/batch"
	"github.com/weaviate/weaviate-go-client/v6/collections"
	"github.com/weaviate/weaviate-go-client/v6/data"
	"github.com/weaviate/weaviate-go-client/v6/query"
	"github.com/weaviate/weaviate-go-client/v6/query/filter"
)

func testNumericalFilters(c *weaviate.Client) func(t *testing.T) {
	return func(t *testing.T) {
		require.NoError(t, c.Collections.DeleteAll(t.Context()))
		t.Cleanup(func() {
			require.NoError(t, c.Collections.DeleteAll(context.Background()))
		})

		collectionName := "NumericalClass"
		randPool := 100
		randInts := make([]int64, randPool)
		randNumbers := make([]float64, randPool)
		randDates := make([]time.Time, randPool)
		selectedRandIds := make([]int, 0, 6)
		r := rand.New(rand.NewSource(time.Now().UnixNano()))

		batches := 50
		perBatch := 200

		var h *collections.Handle
		var err error

		t.Run("create schema", func(t *testing.T) {
			h, err = c.Collections.Create(t.Context(), collections.Collection{
				Name: collectionName,
				Properties: []collections.Property{
					{
						Name:            "filterable_int",
						DataType:        collections.DataTypeInt,
						IndexFilterable: new(true),
						IndexRangeable:  new(false),
					},
					{
						Name:            "rangeable_int",
						DataType:        collections.DataTypeInt,
						IndexFilterable: new(false),
						IndexRangeable:  new(true),
					},
					{
						Name:            "filterable_number",
						DataType:        collections.DataTypeNumber,
						IndexFilterable: new(true),
						IndexRangeable:  new(false),
					},
					{
						Name:            "rangeable_number",
						DataType:        collections.DataTypeNumber,
						IndexFilterable: new(false),
						IndexRangeable:  new(true),
					},
					{
						Name:            "filterable_date",
						DataType:        collections.DataTypeDate,
						IndexFilterable: new(true),
						IndexRangeable:  new(false),
					},
					{
						Name:            "rangeable_date",
						DataType:        collections.DataTypeDate,
						IndexFilterable: new(false),
						IndexRangeable:  new(true),
					},
					{
						Name:     "delete",
						DataType: collections.DataTypeBool,
					},
				},
			})
			require.NoError(t, err)
			require.NotNilf(t, h, "%h collection handle", collectionName)
		})

		t.Run("generate random data", func(t *testing.T) {
			for i := 0; i < randPool; i++ {
				randInts[i] = int64(r.Uint32())
				randNumbers[i] = r.Float64()
				randDates[i] = time.Unix(0, 0).Add(time.Hour * time.Duration(r.Int63n(1_000_000)))
			}

			uniqueSelectedRandIds := map[int]struct{}{}
			b := h.Batch(t.Context())
			var tasks []*batch.Task
			for range batches {
				for i := 0; i < perBatch; i++ {
					randId := r.Intn(randPool)
					// collect only the ones not to be deleted later on
					toDelete := i%2 == 0
					if !toDelete {
						uniqueSelectedRandIds[randId] = struct{}{}
					}
					id := uuid.New()
					task, err := b.Object(t.Context(), &data.Object{
						UUID: &id,
						Properties: map[string]any{
							"filterable_int":    randInts[randId],
							"rangeable_int":     randInts[randId],
							"filterable_number": randNumbers[randId],
							"rangeable_number":  randNumbers[randId],
							"filterable_date":   randDates[randId],
							"rangeable_date":    randDates[randId],
							"delete":            toDelete,
						},
					})
					if assert.NoErrorf(t, err, "add object %q", id) {
						require.NotNil(t, task, "task %q", id)
						tasks = append(tasks, task)
					}
				}
			}
			require.NoError(t, b.Close(), "close batch")
			for _, task := range tasks {
				require.NoError(t, task.Wait(), "task %q", task.ID())
			}

			i, c := 0, cap(selectedRandIds)
			for id := range uniqueSelectedRandIds {
				if i == c {
					break
				}
				selectedRandIds = append(selectedRandIds, id)
				i++
			}
			for j := i; j < c; j++ {
				selectedRandIds = append(selectedRandIds, selectedRandIds[j-i])
			}
		})

		get := func(f filter.Expr) []uuid.UUID {
			r, err := h.Query.OverAll(t.Context(), query.OverAll{
				Filter: f,
				Limit:  batches * perBatch,
			})
			require.NoError(t, err)
			require.NotNil(t, r, "query response")

			var got []uuid.UUID
			for i := range r.Objects {
				got = append(got, r.Objects[i].UUID)
			}
			return got
		}

		queryEqual := func(propName string, val any) []uuid.UUID {
			return get(filter.Cond{
				Target:   propName,
				Operator: filter.Equal,
				Value:    val,
			})
		}
		queryUnion := func(propName string, lt, gte any) []uuid.UUID {
			return get(filter.Or{
				filter.Cond{
					Target:   propName,
					Operator: filter.LessThan,
					Value:    lt,
				},
				filter.Cond{
					Target:   propName,
					Operator: filter.GreaterThanEqual,
					Value:    gte,
				},
			})
		}
		queryIntersection := func(propName string, lte, gte any) []uuid.UUID {
			return get(filter.And{
				filter.Cond{
					Target:   propName,
					Operator: filter.LessThanEqual,
					Value:    lte,
				},
				filter.Cond{
					Target:   propName,
					Operator: filter.GreaterThanEqual,
					Value:    gte,
				},
			})
		}

		randId1 := selectedRandIds[0]
		randId2 := selectedRandIds[1]
		randId3 := selectedRandIds[2]
		randId4 := selectedRandIds[3]
		randId5 := selectedRandIds[4]
		randId6 := selectedRandIds[5]

		runQueries := func(t *testing.T) {
			t.Run("equal int", func(t *testing.T) {
				int1, int2 := randInts[randId1], randInts[randId2]

				filterableUuids1 := queryEqual("filterable_int", int1)
				rangeableUuids1 := queryEqual("rangeable_int", int1)
				assert.GreaterOrEqual(t, len(filterableUuids1), 1)
				assert.ElementsMatch(t, filterableUuids1, rangeableUuids1)

				filterableUuids2 := queryEqual("filterable_int", int2)
				rangeableUuids2 := queryEqual("rangeable_int", int2)
				assert.GreaterOrEqual(t, len(filterableUuids2), 1)
				assert.ElementsMatch(t, filterableUuids2, rangeableUuids2)
			})

			t.Run("equal number", func(t *testing.T) {
				number1, number2 := randNumbers[randId1], randNumbers[randId2]

				filterableUuids1 := queryEqual("filterable_number", number1)
				rangeableUuids1 := queryEqual("rangeable_number", number1)
				assert.GreaterOrEqual(t, len(filterableUuids1), 1)
				assert.ElementsMatch(t, filterableUuids1, rangeableUuids1)

				filterableUuids2 := queryEqual("filterable_number", number2)
				rangeableUuids2 := queryEqual("rangeable_number", number2)
				assert.GreaterOrEqual(t, len(filterableUuids2), 1)
				assert.ElementsMatch(t, filterableUuids2, rangeableUuids2)
			})

			t.Run("equal date", func(t *testing.T) {
				date1, date2 := randDates[randId1], randDates[randId2]

				filterableUuids1 := queryEqual("filterable_date", date1)
				rangeableUuids1 := queryEqual("rangeable_date", date1)
				assert.GreaterOrEqual(t, len(filterableUuids1), 1)
				assert.ElementsMatch(t, filterableUuids1, rangeableUuids1)

				filterableUuids2 := queryEqual("filterable_date", date2)
				rangeableUuids2 := queryEqual("rangeable_date", date2)
				assert.GreaterOrEqual(t, len(filterableUuids2), 1)
				assert.ElementsMatch(t, filterableUuids2, rangeableUuids2)
			})

			t.Run("union int", func(t *testing.T) {
				int1, int2 := randInts[randId3], randInts[randId4]
				if int1 > int2 {
					int1, int2 = int2, int1
				}

				filterableUuids := queryUnion("filterable_int", int1, int2)
				rangeableUuids := queryUnion("rangeable_int", int1, int2)
				assert.GreaterOrEqual(t, len(filterableUuids), 1)
				assert.ElementsMatch(t, filterableUuids, rangeableUuids)
			})

			t.Run("union number", func(t *testing.T) {
				number1, number2 := randNumbers[randId3], randNumbers[randId4]
				if number1 > number2 {
					number1, number2 = number2, number1
				}

				filterableUuids := queryUnion("filterable_number", number1, number2)
				rangeableUuids := queryUnion("rangeable_number", number1, number2)
				assert.GreaterOrEqual(t, len(filterableUuids), 1)
				assert.ElementsMatch(t, filterableUuids, rangeableUuids)
			})

			t.Run("union date", func(t *testing.T) {
				date1, date2 := randDates[randId3], randDates[randId4]
				if date1.After(date2) {
					date1, date2 = date2, date1
				}

				filterableUuids := queryUnion("filterable_date", date1, date2)
				rangeableUuids := queryUnion("rangeable_date", date1, date2)
				assert.GreaterOrEqual(t, len(filterableUuids), 1)
				assert.ElementsMatch(t, filterableUuids, rangeableUuids)
			})

			t.Run("intersection int", func(t *testing.T) {
				int1, int2 := randInts[randId5], randInts[randId6]
				if int1 > int2 {
					int1, int2 = int2, int1
				}

				filterableUuids := queryIntersection("filterable_int", int2, int1)
				rangeableUuids := queryIntersection("rangeable_int", int2, int1)
				assert.GreaterOrEqual(t, len(filterableUuids), 1)
				assert.ElementsMatch(t, filterableUuids, rangeableUuids)
			})

			t.Run("intersection number", func(t *testing.T) {
				number1, number2 := randNumbers[randId5], randNumbers[randId6]
				if number1 > number2 {
					number1, number2 = number2, number1
				}

				filterableUuids := queryIntersection("filterable_number", number2, number1)
				rangeableUuids := queryIntersection("rangeable_number", number2, number1)
				assert.GreaterOrEqual(t, len(filterableUuids), 1)
				assert.ElementsMatch(t, filterableUuids, rangeableUuids)
			})

			t.Run("intersection date", func(t *testing.T) {
				date1, date2 := randDates[randId5], randDates[randId6]
				if date1.After(date2) {
					date1, date2 = date2, date1
				}

				filterableUuids := queryIntersection("filterable_date", date2, date1)
				rangeableUuids := queryIntersection("rangeable_date", date2, date1)
				assert.GreaterOrEqual(t, len(filterableUuids), 1)
				assert.ElementsMatch(t, filterableUuids, rangeableUuids)
			})
		}

		t.Run("queries before delete", runQueries)

		t.Run("delete some data", func(t *testing.T) {
			r, err := h.Data.DeleteSelected(t.Context(), data.DeleteSelected{
				Filter: filter.Cond{
					Target:   "delete",
					Operator: filter.Equal,
					Value:    true,
				},
			})
			require.NoError(t, err)
			require.EqualValues(t, r.Matches, batches*perBatch/2, "values matched by delete filter")

			count, err := h.Count(t.Context())
			require.NoError(t, err)
			require.EqualValues(t, batches*perBatch/2, count, "remaining items after deletion")
		})

		t.Run("queries after delete", runQueries)
	}
}
