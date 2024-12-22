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

package filters_tests

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wvt "github.com/liutizhong/weaviate-go-client/v4/weaviate"
	"github.com/liutizhong/weaviate-go-client/v4/weaviate/filters"
	"github.com/liutizhong/weaviate-go-client/v4/weaviate/graphql"
	"github.com/liutizhong/weaviate/entities/models"
	"github.com/liutizhong/weaviate/entities/schema"
)

func testNumericalFilters(host string) func(t *testing.T) {
	return func(t *testing.T) {
		className := "NumericalClass"

		client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: host})
		require.NoError(t, err)

		cleanup := func() {
			err := client.Schema().AllDeleter().Do(context.Background())
			require.Nil(t, err)
		}
		cleanup()
		defer cleanup()

		randPool := 100
		randInts := make([]int64, randPool)
		randNumbers := make([]float64, randPool)
		randDates := make([]time.Time, randPool)
		selectedRandIds := make([]int, 0, 6)
		r := rand.New(rand.NewSource(time.Now().UnixNano()))

		batches := 50
		perBatch := 200

		t.Run("create schema", func(t *testing.T) {
			vTrue := true
			vFalse := false

			class := &models.Class{
				Class:      className,
				Vectorizer: "none",
				Properties: []*models.Property{
					{
						Name:              "int_filterable",
						DataType:          schema.DataTypeInt.PropString(),
						IndexFilterable:   &vTrue,
						IndexRangeFilters: &vFalse,
					},
					{
						Name:              "int_rangeable",
						DataType:          schema.DataTypeInt.PropString(),
						IndexFilterable:   &vFalse,
						IndexRangeFilters: &vTrue,
					},
					{
						Name:              "number_filterable",
						DataType:          schema.DataTypeNumber.PropString(),
						IndexFilterable:   &vTrue,
						IndexRangeFilters: &vFalse,
					},
					{
						Name:              "number_rangeable",
						DataType:          schema.DataTypeNumber.PropString(),
						IndexFilterable:   &vFalse,
						IndexRangeFilters: &vTrue,
					},
					{
						Name:              "date_filterable",
						DataType:          schema.DataTypeDate.PropString(),
						IndexFilterable:   &vTrue,
						IndexRangeFilters: &vFalse,
					},
					{
						Name:              "date_rangeable",
						DataType:          schema.DataTypeDate.PropString(),
						IndexFilterable:   &vFalse,
						IndexRangeFilters: &vTrue,
					},
					{
						Name:     "delete",
						DataType: schema.DataTypeBoolean.PropString(),
					},
				},
			}

			err = client.Schema().ClassCreator().
				WithClass(class).
				Do(context.Background())
			require.NoError(t, err)
		})

		t.Run("generate random data", func(t *testing.T) {
			for i := 0; i < randPool; i++ {
				randInts[i] = int64(r.Uint32())
				randNumbers[i] = r.Float64()
				randDates[i] = time.Unix(0, 0).Add(time.Hour * time.Duration(r.Int63n(1_000_000)))
			}

			uniqueSelectedRandIds := map[int]struct{}{}
			objects := make([]*models.Object, 0, perBatch)
			for batch := 0; batch < batches; batch++ {
				objects = objects[:0]

				for i := 0; i < perBatch; i++ {
					randId := r.Intn(randPool)
					toDelete := i%2 == 0
					// collect only the ones not to be deleted later on
					if !toDelete {
						uniqueSelectedRandIds[randId] = struct{}{}
					}
					uuid_, err := uuid.NewRandom()
					require.NoError(t, err)

					objects = append(objects, &models.Object{
						Class: className,
						ID:    strfmt.UUID(uuid_.String()),
						Properties: map[string]interface{}{
							"int_filterable":    randInts[randId],
							"int_rangeable":     randInts[randId],
							"number_filterable": randNumbers[randId],
							"number_rangeable":  randNumbers[randId],
							"date_filterable":   randDates[randId],
							"date_rangeable":    randDates[randId],
							"delete":            toDelete,
						},
					})
				}

				responses, err := client.Batch().ObjectsBatcher().
					WithObjects(objects...).
					Do(context.Background())
				require.NoError(t, err)
				require.Len(t, responses, perBatch)

				for _, response := range responses {
					s := response.Result.Status
					require.Equal(t, "SUCCESS", *s)
				}
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

		gqlGet := func(where *filters.WhereBuilder) []string {
			resp, err := client.GraphQL().Get().
				WithClassName(className).
				WithLimit(batches * perBatch).
				WithWhere(where).
				WithFields(graphql.Field{
					Name: "_additional", Fields: []graphql.Field{{Name: "id"}},
				}).
				Do(context.Background())
			require.NoError(t, err)
			require.Empty(t, resp.Errors)

			results := resp.Data["Get"].(map[string]interface{})[className].([]interface{})
			ids := make([]string, len(results))
			for i := range ids {
				ids[i] = results[i].(map[string]interface{})["_additional"].(map[string]interface{})["id"].(string)
			}
			return ids
		}
		equalInt := func(propName string, val int64) []string {
			return gqlGet(filters.Where().
				WithPath([]string{propName}).
				WithOperator(filters.Equal).
				WithValueInt(val))
		}
		equalNumber := func(propName string, val float64) []string {
			return gqlGet(filters.Where().
				WithPath([]string{propName}).
				WithOperator(filters.Equal).
				WithValueNumber(val))
		}
		equalDate := func(propName string, val time.Time) []string {
			return gqlGet(filters.Where().
				WithPath([]string{propName}).
				WithOperator(filters.Equal).
				WithValueDate(val))
		}
		unionInt := func(propName string, val1, val2 int64) []string {
			return gqlGet(filters.Where().
				WithOperator(filters.Or).
				WithOperands(
					[]*filters.WhereBuilder{
						filters.Where().
							WithPath([]string{propName}).
							WithOperator(filters.LessThan).
							WithValueInt(val1),
						filters.Where().
							WithPath([]string{propName}).
							WithOperator(filters.GreaterThanEqual).
							WithValueInt(val2),
					}))
		}
		unionNumber := func(propName string, val1, val2 float64) []string {
			return gqlGet(filters.Where().
				WithOperator(filters.Or).
				WithOperands(
					[]*filters.WhereBuilder{
						filters.Where().
							WithPath([]string{propName}).
							WithOperator(filters.LessThan).
							WithValueNumber(val1),
						filters.Where().
							WithPath([]string{propName}).
							WithOperator(filters.GreaterThanEqual).
							WithValueNumber(val2),
					}))
		}
		unionDate := func(propName string, val1, val2 time.Time) []string {
			return gqlGet(filters.Where().
				WithOperator(filters.Or).
				WithOperands(
					[]*filters.WhereBuilder{
						filters.Where().
							WithPath([]string{propName}).
							WithOperator(filters.LessThan).
							WithValueDate(val1),
						filters.Where().
							WithPath([]string{propName}).
							WithOperator(filters.GreaterThanEqual).
							WithValueDate(val2),
					}))
		}
		intersectionInt := func(propName string, val1, val2 int64) []string {
			return gqlGet(filters.Where().
				WithOperator(filters.And).
				WithOperands(
					[]*filters.WhereBuilder{
						filters.Where().
							WithPath([]string{propName}).
							WithOperator(filters.GreaterThanEqual).
							WithValueInt(val1),
						filters.Where().
							WithPath([]string{propName}).
							WithOperator(filters.LessThanEqual).
							WithValueInt(val2),
					}))
		}
		intersectionNumber := func(propName string, val1, val2 float64) []string {
			return gqlGet(filters.Where().
				WithOperator(filters.And).
				WithOperands(
					[]*filters.WhereBuilder{
						filters.Where().
							WithPath([]string{propName}).
							WithOperator(filters.GreaterThanEqual).
							WithValueNumber(val1),
						filters.Where().
							WithPath([]string{propName}).
							WithOperator(filters.LessThanEqual).
							WithValueNumber(val2),
					}))
		}
		intersectionDate := func(propName string, val1, val2 time.Time) []string {
			return gqlGet(filters.Where().
				WithOperator(filters.And).
				WithOperands(
					[]*filters.WhereBuilder{
						filters.Where().
							WithPath([]string{propName}).
							WithOperator(filters.GreaterThanEqual).
							WithValueDate(val1),
						filters.Where().
							WithPath([]string{propName}).
							WithOperator(filters.LessThanEqual).
							WithValueDate(val2),
					}))
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

				filterableUuids1 := equalInt("int_filterable", int1)
				rangeableUuids1 := equalInt("int_rangeable", int1)
				assert.GreaterOrEqual(t, len(filterableUuids1), 1)
				assert.ElementsMatch(t, filterableUuids1, rangeableUuids1)

				filterableUuids2 := equalInt("int_filterable", int2)
				rangeableUuids2 := equalInt("int_rangeable", int2)
				assert.GreaterOrEqual(t, len(filterableUuids2), 1)
				assert.ElementsMatch(t, filterableUuids2, rangeableUuids2)
			})

			t.Run("equal number", func(t *testing.T) {
				number1, number2 := randNumbers[randId1], randNumbers[randId2]

				filterableUuids1 := equalNumber("number_filterable", number1)
				rangeableUuids1 := equalNumber("number_rangeable", number1)
				assert.GreaterOrEqual(t, len(filterableUuids1), 1)
				assert.ElementsMatch(t, filterableUuids1, rangeableUuids1)

				filterableUuids2 := equalNumber("number_filterable", number2)
				rangeableUuids2 := equalNumber("number_rangeable", number2)
				assert.GreaterOrEqual(t, len(filterableUuids2), 1)
				assert.ElementsMatch(t, filterableUuids2, rangeableUuids2)
			})

			t.Run("equal date", func(t *testing.T) {
				date1, date2 := randDates[randId1], randDates[randId2]

				filterableUuids1 := equalDate("date_filterable", date1)
				rangeableUuids1 := equalDate("date_rangeable", date1)
				assert.GreaterOrEqual(t, len(filterableUuids1), 1)
				assert.ElementsMatch(t, filterableUuids1, rangeableUuids1)

				filterableUuids2 := equalDate("date_filterable", date2)
				rangeableUuids2 := equalDate("date_rangeable", date2)
				assert.GreaterOrEqual(t, len(filterableUuids2), 1)
				assert.ElementsMatch(t, filterableUuids2, rangeableUuids2)
			})

			t.Run("union int", func(t *testing.T) {
				int1, int2 := randInts[randId3], randInts[randId4]
				if int1 > int2 {
					int1, int2 = int2, int1
				}

				filterableUuids := unionInt("int_filterable", int1, int2)
				rangeableUuids := unionInt("int_rangeable", int1, int2)
				assert.GreaterOrEqual(t, len(filterableUuids), 1)
				assert.ElementsMatch(t, filterableUuids, rangeableUuids)
			})

			t.Run("union number", func(t *testing.T) {
				number1, number2 := randNumbers[randId3], randNumbers[randId4]
				if number1 > number2 {
					number1, number2 = number2, number1
				}

				filterableUuids := unionNumber("number_filterable", number1, number2)
				rangeableUuids := unionNumber("number_rangeable", number1, number2)
				assert.GreaterOrEqual(t, len(filterableUuids), 1)
				assert.ElementsMatch(t, filterableUuids, rangeableUuids)
			})

			t.Run("union date", func(t *testing.T) {
				date1, date2 := randDates[randId3], randDates[randId4]
				if date1.After(date2) {
					date1, date2 = date2, date1
				}

				filterableUuids := unionDate("date_filterable", date1, date2)
				rangeableUuids := unionDate("date_rangeable", date1, date2)
				assert.GreaterOrEqual(t, len(filterableUuids), 1)
				assert.ElementsMatch(t, filterableUuids, rangeableUuids)
			})

			t.Run("intersection int", func(t *testing.T) {
				int1, int2 := randInts[randId5], randInts[randId6]
				if int1 > int2 {
					int1, int2 = int2, int1
				}

				filterableUuids := intersectionInt("int_filterable", int1, int2)
				rangeableUuids := intersectionInt("int_rangeable", int1, int2)
				assert.GreaterOrEqual(t, len(filterableUuids), 1)
				assert.ElementsMatch(t, filterableUuids, rangeableUuids)
			})

			t.Run("intersection number", func(t *testing.T) {
				number1, number2 := randNumbers[randId5], randNumbers[randId6]
				if number1 > number2 {
					number1, number2 = number2, number1
				}

				filterableUuids := intersectionNumber("number_filterable", number1, number2)
				rangeableUuids := intersectionNumber("number_rangeable", number1, number2)
				assert.GreaterOrEqual(t, len(filterableUuids), 1)
				assert.ElementsMatch(t, filterableUuids, rangeableUuids)
			})

			t.Run("intersection date", func(t *testing.T) {
				date1, date2 := randDates[randId5], randDates[randId6]
				if date1.After(date2) {
					date1, date2 = date2, date1
				}

				filterableUuids := intersectionDate("date_filterable", date1, date2)
				rangeableUuids := intersectionDate("date_rangeable", date1, date2)
				assert.GreaterOrEqual(t, len(filterableUuids), 1)
				assert.ElementsMatch(t, filterableUuids, rangeableUuids)
			})
		}

		t.Run("queries before delete", runQueries)

		t.Run("delete some data", func(t *testing.T) {
			resp, err := client.Batch().ObjectsBatchDeleter().
				WithClassName(className).
				WithWhere(filters.Where().
					WithOperator(filters.Equal).
					WithPath([]string{"delete"}).
					WithValueBoolean(true)).
				Do(context.Background())
			require.NoError(t, err)
			require.Equal(t, int64(0), resp.Results.Failed)
			require.Equal(t, int64(batches*perBatch/2), resp.Results.Successful)
		})

		t.Run("queries after delete", runQueries)
	}
}
