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
	"acceptance_tests_with_client/internal/wvhost"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate-go-client/v6/collections"
	"github.com/weaviate/weaviate-go-client/v6/data"
	"github.com/weaviate/weaviate-go-client/v6/query"
	"github.com/weaviate/weaviate-go-client/v6/query/filter"
)

// FIXME(dyma): connect to host passed in the function
func testContains(string) func(t *testing.T) {
	return func(t *testing.T) {
		c := wvhost.NewClient(t)

		collectionName := "WhereTest"
		id1 := uuid.MustParse("00000000-0000-0000-0000-000000000001")
		id2 := uuid.MustParse("00000000-0000-0000-0000-000000000002")
		id3 := uuid.MustParse("00000000-0000-0000-0000-000000000003")
		ids := []uuid.UUID{id1, id2, id3}

		var h *collections.Handle
		var err error

		require.NoError(t, c.Collections.DeleteAll(t.Context()))
		t.Cleanup(func() {
			require.NoError(t, c.Collections.DeleteAll(context.Background()))
		})
		t.Run("create class", func(t *testing.T) {
			h, err = c.Collections.Create(t.Context(), collections.Collection{
				Name: collectionName,
				Properties: []collections.Property{
					{
						Name:     "author",
						DataType: collections.DataTypeText,
					},
					{
						Name:     "authors",
						DataType: collections.DataTypeTextArray,
					},
					{
						Name:     "number",
						DataType: collections.DataTypeNumber,
					},
					{
						Name:     "numbers",
						DataType: collections.DataTypeNumberArray,
					},
					{
						Name:     "int",
						DataType: collections.DataTypeInt,
					},
					{
						Name:     "ints",
						DataType: collections.DataTypeIntArray,
					},
					{
						Name:     "date",
						DataType: collections.DataTypeDate,
					},
					{
						Name:     "dates",
						DataType: collections.DataTypeDateArray,
					},
					{
						Name:     "bool",
						DataType: collections.DataTypeBool,
					},
					{
						Name:     "bools",
						DataType: collections.DataTypeBoolArray,
					},
					{
						Name:     "uuid",
						DataType: collections.DataTypeUUID,
					},
					{
						Name:     "uuids",
						DataType: collections.DataTypeUUIDArray,
					},
				},
			})
			require.NoError(t, err)

			// Give time for the schema to replicate and graphql to rebuild it for it's queries
			time.Sleep(3 * time.Second)
		})

		t.Run("where with bm25 without data", func(t *testing.T) {
			mustGetTime := func(date string) time.Time {
				result, err := time.Parse(time.RFC3339Nano, date)
				if err != nil {
					panic(fmt.Sprintf("can't parse date: %v", date))
				}
				return result
			}
			tests := []struct {
				name     string
				where    filter.Expr
				property string
			}{
				// Contains operator with array types
				{
					name: "contains any authors with text array",
					where: filter.Cond{
						Target:   "authors",
						Operator: filter.ContainsAny,
						Value:    []string{"John", "Jenny", "Joseph"},
					},
					property: "authors",
				},
				{
					name: "contains any numbers with number array",
					where: filter.Cond{
						Target:   "numbers",
						Operator: filter.ContainsAny,
						Value:    []float64{1.1, 2.2, 3.3},
					},
					property: "numbers",
				},
				{
					name: "contains any ints with int array",
					where: filter.Cond{
						Target:   "ints",
						Operator: filter.ContainsAny,
						Value:    []int{1, 2, 3},
					},
					property: "ints",
				},
				{
					name: "contains any bools with bool array",
					where: filter.Cond{
						Target:   "bools",
						Operator: filter.ContainsAny,
						Value:    []bool{true, false},
					},
					property: "bools",
				},
				{
					name: "contains any uuids with uuid array",
					where: filter.Cond{
						Target:   "uuids",
						Operator: filter.ContainsAny,
						Value:    ids,
					},
					property: "uuids",
				},
				{
					name: "contains any dates with dates array",
					where: filter.Cond{
						Target:   "dates",
						Operator: filter.ContainsAny,
						Value: []time.Time{
							mustGetTime("2009-11-01T23:00:00Z"),
							mustGetTime("2009-11-02T23:00:00Z"),
							mustGetTime("2009-11-03T23:00:00Z"),
						},
					},
					property: "dates",
				},
				// Contains operator with primitives
				{
					name: "contains any author with text",
					where: filter.Cond{
						Target:   "author",
						Operator: filter.ContainsAny,
						Value:    []string{"John", "Jenny", "Joseph"},
					},
					property: "author",
				},
				{
					name: "contains any number with number",
					where: filter.Cond{
						Target:   "number",
						Operator: filter.ContainsAny,
						Value:    []float64{1.1, 2.2, 3.3},
					},
					property: "number",
				},
				{
					name: "contains any int with int",
					where: filter.Cond{
						Target:   "ints",
						Operator: filter.ContainsAny,
						Value:    []int{1, 2, 3},
					},
					property: "int",
				},
				{
					name: "contains any bool with bool",
					where: filter.Cond{
						Target:   "bool",
						Operator: filter.ContainsAny,
						Value:    []bool{true, false, true},
					},
					property: "bool",
				},
				{
					name: "contains any uuid with uuid",
					where: filter.Cond{
						Target:   "uuid",
						Operator: filter.ContainsAny,
						Value:    ids,
					},
					property: "uuid",
				},
				{
					name: "contains any uuid with id",
					where: filter.Cond{
						Target:   filter.UUID,
						Operator: filter.ContainsAny,
						Value:    ids,
					},
					property: "uuid",
				},
				{
					name: "contains any date with date",
					where: filter.Cond{
						Target:   "date",
						Operator: filter.ContainsAny,
						Value: []time.Time{
							mustGetTime("2009-11-01T23:00:00Z"),
							mustGetTime("2009-11-02T23:00:00Z"),
							mustGetTime("2009-11-03T23:00:00Z"),
						},
					},
					property: "date",
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					_, err := h.Query.BM25(t.Context(), query.BM25{
						Query:            "Pit Vipers",
						ReturnProperties: []string{tt.property},
						Filter:           tt.where,
					})
					require.NoError(t, err)
				})
			}
		})

		t.Run("with data", func(t *testing.T) {
			t.Run("insert data", func(t *testing.T) {
				authors := []string{"John", "Jenny", "Joseph"}
				authorsArray := [][]string{
					{"John", "Jenny", "Joseph"},
					{"John", "Jenny"},
					{"John"},
				}
				numbers := []float64{1.1, 2.2, 3.3}
				numbersArray := [][]float64{
					{1.1, 2.2, 3.3},
					{1.1, 2.2},
					{1.1},
				}
				ints := []int64{1, 2, 3}
				intsArray := [][]int64{
					{1, 2, 3},
					{1, 2},
					{1},
				}
				uuids := []uuid.UUID{id1, id2, id3}
				uuidsArray := [][]uuid.UUID{
					{id1, id2, id3},
					{id1, id2},
					{id1},
				}
				dates := []string{"2009-11-01T23:00:00Z", "2009-11-02T23:00:00Z", "2009-11-03T23:00:00Z"}
				datesArray := [][]string{
					{"2009-11-01T23:00:00Z", "2009-11-02T23:00:00Z", "2009-11-03T23:00:00Z"},
					{"2009-11-01T23:00:00Z", "2009-11-02T23:00:00Z"},
					{"2009-11-01T23:00:00Z"},
				}
				bools := []bool{true, false, true}
				boolsArray := [][]bool{
					{true, false, true},
					{true, false},
					{true},
				}
				for i, id := range ids {
					_, err := h.Data.Insert(t.Context(), &data.Object{
						UUID: &id,
						Properties: map[string]interface{}{
							"author":  authors[i],
							"authors": authorsArray[i],
							"number":  numbers[i],
							"numbers": numbersArray[i],
							"int":     ints[i],
							"ints":    intsArray[i],
							"uuid":    uuids[i],
							"uuids":   uuidsArray[i],
							"date":    dates[i],
							"dates":   datesArray[i],
							"bool":    bools[i],
							"bools":   boolsArray[i],
						},
					})
					require.NoError(t, err)
				}
			})

			t.Run("where", func(t *testing.T) {
				mustGetTime := func(date string) time.Time {
					result, err := time.Parse(time.RFC3339Nano, date)
					if err != nil {
						panic(fmt.Sprintf("can't parse date: %v", date))
					}
					return result
				}
				tests := []struct {
					name        string
					where       filter.Expr
					property    string
					nearText    *query.NearText
					expectedIDs []uuid.UUID
				}{
					// Contains operator with array types
					{
						name: "contains all authors with text array",
						where: filter.Cond{
							Target:   "authors",
							Operator: filter.ContainsAll,
							Value:    []string{"John", "Jenny", "Joseph"},
						},
						property:    "authors",
						expectedIDs: []uuid.UUID{id1},
					},
					{
						name: "contains any authors with text array",
						where: filter.Cond{
							Target:   "authors",
							Operator: filter.ContainsAny,
							Value:    []string{"John", "Jenny", "Joseph"},
						},
						property:    "authors",
						expectedIDs: []uuid.UUID{id1, id2, id3},
					},
					{
						name: "contains none authors with text array",
						where: filter.Cond{
							Target:   "authors",
							Operator: filter.ContainsNone,
							Value:    []string{"Missing", "Joseph"},
						},
						property:    "authors",
						expectedIDs: []uuid.UUID{id2, id3},
					},
					{
						name: "contains all numbers with number array",
						where: filter.Cond{
							Target:   "numbers",
							Operator: filter.ContainsAll,
							Value:    []float64{1.1, 2.2, 3.3},
						},
						property:    "numbers",
						expectedIDs: []uuid.UUID{id1},
					},
					{
						name: "contains any numbers with number array",
						where: filter.Cond{
							Target:   "numbers",
							Operator: filter.ContainsAny,
							Value:    []float64{1.1, 2.2, 3.3},
						},
						property:    "numbers",
						expectedIDs: []uuid.UUID{id1, id2, id3},
					},
					{
						name: "contains none numbers with number array",
						where: filter.Cond{
							Target:   "numbers",
							Operator: filter.ContainsNone,
							Value:    []float64{3.3, 0},
						},
						property:    "numbers",
						expectedIDs: []uuid.UUID{id2, id3},
					},
					{
						name: "contains all ints with int array",
						where: filter.Cond{
							Target:   "ints",
							Operator: filter.ContainsAll,
							Value:    []int{1, 2, 3},
						},
						property:    "ints",
						expectedIDs: []uuid.UUID{id1},
					},
					{
						name: "contains any ints with int array",
						where: filter.Cond{
							Target:   "ints",
							Operator: filter.ContainsAny,
							Value:    []int{1, 2, 3},
						},
						property:    "ints",
						expectedIDs: []uuid.UUID{id1, id2, id3},
					},
					{
						name: "contains none ints with int array",
						where: filter.Cond{
							Target:   "ints",
							Operator: filter.ContainsNone,
							Value:    []int{3, 0},
						},
						property:    "ints",
						expectedIDs: []uuid.UUID{id2, id3},
					},
					{
						name: "contains all bools with bool array",
						where: filter.Cond{
							Target:   "bools",
							Operator: filter.ContainsAll,
							Value:    []bool{true, false},
						},
						property:    "bools",
						expectedIDs: []uuid.UUID{id1, id2},
					},
					{
						name: "contains any bools with bool array",
						where: filter.Cond{
							Target:   "bools",
							Operator: filter.ContainsAny,
							Value:    []bool{true, false},
						},
						property:    "bools",
						expectedIDs: []uuid.UUID{id1, id2, id3},
					},
					{
						name: "contains none bools with bool array",
						where: filter.Cond{
							Target:   "bools",
							Operator: filter.ContainsNone,
							Value:    []bool{false},
						},
						property:    "bools",
						expectedIDs: []uuid.UUID{id3},
					},
					{
						name: "contains all uuids with uuid array",
						where: filter.Cond{
							Target:   "uuids",
							Operator: filter.ContainsAll,
							Value:    ids,
						},
						property:    "uuids",
						expectedIDs: []uuid.UUID{id1},
					},
					{
						name: "contains any uuids with uuid array",
						where: filter.Cond{
							Target:   "uuids",
							Operator: filter.ContainsAny,
							Value:    ids,
						},
						property:    "uuids",
						expectedIDs: []uuid.UUID{id1, id2, id3},
					},
					{
						name: "contains none uuids with uuid array",
						where: filter.Cond{
							Target:   "uuids",
							Operator: filter.ContainsNone,
							Value: []uuid.UUID{
								id3,
								uuid.MustParse("FFFFFFFF-FFFF-0000-0000-000000000000"),
							},
						},
						property:    "uuids",
						expectedIDs: []uuid.UUID{id2, id3},
					},
					{
						name: "contains all dates with dates array",
						where: filter.Cond{
							Target:   "dates",
							Operator: filter.ContainsAll,
							Value: []time.Time{
								mustGetTime("2009-11-01T23:00:00Z"),
								mustGetTime("2009-11-02T23:00:00Z"),
								mustGetTime("2009-11-03T23:00:00Z"),
							},
						},
						property:    "dates",
						expectedIDs: []uuid.UUID{id1},
					},
					{
						name: "contains any dates with dates array",
						where: filter.Cond{
							Target:   "dates",
							Operator: filter.ContainsAny,
							Value: []time.Time{
								mustGetTime("2009-11-01T23:00:00Z"),
								mustGetTime("2009-11-02T23:00:00Z"),
								mustGetTime("2009-11-03T23:00:00Z"),
							},
						},
						property:    "dates",
						expectedIDs: []uuid.UUID{id1, id2, id3},
					},
					{
						name: "contains none dates with dates array",
						where: filter.Cond{
							Target:   "dates",
							Operator: filter.ContainsNone,
							Value: []time.Time{
								mustGetTime("2009-11-03T23:00:00Z"),
								mustGetTime("1970-01-01T00:00:00Z"),
							},
						},
						property:    "dates",
						expectedIDs: []uuid.UUID{id2, id3},
					},
					{
						name: "complex contains all ints and all numbers with AND on int array",
						where: filter.And{
							filter.Cond{
								Target:   "numbers",
								Operator: filter.ContainsAll,
								Value:    []float64{1.1, 2.2, 3.3},
							},
							filter.Cond{
								Target:   "ints",
								Operator: filter.ContainsAll,
								Value:    []int{1, 2, 3},
							},
						},
						property:    "ints",
						expectedIDs: []uuid.UUID{id1},
					},
					{
						name: "complex contains any ints and all numbers and none texts with OR",
						where: filter.Or{
							filter.Cond{
								Target:   "numbers",
								Operator: filter.ContainsAll,
								Value:    []float64{1.1, 2.2, 3.3},
							},
							filter.Cond{
								Target:   "ints",
								Operator: filter.ContainsAny,
								Value:    []int{3},
							},
							filter.Cond{
								Target:   "authors",
								Operator: filter.ContainsNone,
								Value:    []string{"Jenny", "Missing"},
							},
						},
						property:    "ints",
						expectedIDs: []uuid.UUID{id1, id3},
					},
					// Contains operator with primitives
					{
						name: "contains any author with text",
						where: filter.Cond{
							Target:   "author",
							Operator: filter.ContainsAny,
							Value:    []string{"John", "Jenny", "Joseph"},
						},
						property:    "author",
						expectedIDs: []uuid.UUID{id1, id2, id3},
					},
					{
						name: "contains any number with number",
						where: filter.Cond{
							Target:   "number",
							Operator: filter.ContainsAny,
							Value:    []float64{1.1, 2.2, 3.3},
						},
						property:    "number",
						expectedIDs: []uuid.UUID{id1, id2, id3},
					},
					{
						name: "contains any int with int",
						where: filter.Cond{
							Target:   "int",
							Operator: filter.ContainsAny,
							Value:    []int{1, 2, 3},
						},
						property:    "int",
						expectedIDs: []uuid.UUID{id1, id2, id3},
					},
					{
						name: "contains any bool with bool",
						where: filter.Cond{
							Target:   "bool",
							Operator: filter.ContainsAny,
							Value:    []bool{true, false, true},
						},
						property:    "bool",
						expectedIDs: []uuid.UUID{id1, id2, id3},
					},
					{
						name: "contains any uuid with uuid",
						where: filter.Cond{
							Target:   "uuid",
							Operator: filter.ContainsAny,
							Value:    ids,
						},
						property:    "uuid",
						expectedIDs: []uuid.UUID{id1, id2, id3},
					},
					{
						name: "contains any uuid with id",
						where: filter.Cond{
							Target:   filter.UUID,
							Operator: filter.ContainsAny,
							Value:    ids,
						},
						property:    "uuid",
						expectedIDs: []uuid.UUID{id1, id2, id3},
					},
					{
						name: "contains any date with date",
						where: filter.Cond{
							Target:   "date",
							Operator: filter.ContainsAny,
							Value: []time.Time{
								mustGetTime("2009-11-01T23:00:00Z"),
								mustGetTime("2009-11-02T23:00:00Z"),
								mustGetTime("2009-11-03T23:00:00Z"),
							},
						},
						property:    "date",
						expectedIDs: []uuid.UUID{id1, id2, id3},
					},
					{
						name: "contains all author with text",
						where: filter.Cond{
							Target:   "author",
							Operator: filter.ContainsAll,
							Value:    []string{"Jenny"},
						},
						property:    "author",
						expectedIDs: []uuid.UUID{id2},
					},
					{
						name: "contains all number with number",
						where: filter.Cond{
							Target:   "number",
							Operator: filter.ContainsAll,
							Value:    []float64{2.2},
						},
						property:    "number",
						expectedIDs: []uuid.UUID{id2},
					},
					{
						name: "contains all int with int",
						where: filter.Cond{
							Target:   "int",
							Operator: filter.ContainsAll,
							Value:    []int{2},
						},
						property:    "int",
						expectedIDs: []uuid.UUID{id2},
					},
					{
						name: "contains all bool with bool",
						where: filter.Cond{
							Target:   "bool",
							Operator: filter.ContainsAll,
							Value:    []bool{false},
						},
						property:    "bool",
						expectedIDs: []uuid.UUID{id2},
					},
					{
						name: "contains all uuid with uuid",
						where: filter.Cond{
							Target:   "uuid",
							Operator: filter.ContainsAll,
							Value:    []uuid.UUID{id2},
						},
						property:    "uuid",
						expectedIDs: []uuid.UUID{id2},
					},
					{
						name: "contains all date with date",
						where: filter.Cond{
							Target:   "date",
							Operator: filter.ContainsAll,
							Value: []time.Time{
								mustGetTime("2009-11-02T23:00:00Z"),
							},
						},
						property:    "date",
						expectedIDs: []uuid.UUID{id2},
					},

					{
						name: "contains none author with text",
						where: filter.Cond{
							Target:   "author",
							Operator: filter.ContainsNone,
							Value:    []string{"Jenny", "Joseph"},
						},
						property:    "author",
						expectedIDs: []uuid.UUID{id1},
					},
					{
						name: "contains none number with number",
						where: filter.Cond{
							Target:   "number",
							Operator: filter.ContainsNone,
							Value:    []float64{2.2, 3.3},
						},
						property:    "number",
						expectedIDs: []uuid.UUID{id1},
					},
					{
						name: "contains none int with int",
						where: filter.Cond{
							Target:   "int",
							Operator: filter.ContainsNone,
							Value:    []int{2, 3},
						},
						property:    "int",
						expectedIDs: []uuid.UUID{id1},
					},
					{
						name: "contains none bool with bool",
						where: filter.Cond{
							Target:   "bool",
							Operator: filter.ContainsNone,
							Value:    []bool{false},
						},
						property:    "bool",
						expectedIDs: []uuid.UUID{id1, id3},
					},
					{
						name: "contains none uuid with uuid",
						where: filter.Cond{
							Target:   "uuid",
							Operator: filter.ContainsNone,
							Value:    []uuid.UUID{id2, id3},
						},
						property:    "uuid",
						expectedIDs: []uuid.UUID{id1},
					},
					{
						name: "contains none date with date",
						where: filter.Cond{
							Target:   "date",
							Operator: filter.ContainsNone,
							Value: []time.Time{
								mustGetTime("2009-11-02T23:00:00Z"),
								mustGetTime("2009-11-03T23:00:00Z"),
							},
						},
						property:    "date",
						expectedIDs: []uuid.UUID{id1},
					},

					{
						name: "contains all authors with text array and nearText",
						where: filter.Cond{
							Target:   "authors",
							Operator: filter.ContainsAll,
							Value:    []string{"John", "Jenny", "Joseph"},
						},
						property: "authors",
						nearText: &query.NearText{
							Concepts: []string{"John"},
						},
						expectedIDs: []uuid.UUID{id1},
					},
				}

				h := c.Collections.Use(collectionName)
				require.NotNilf(t, h, "%q collection handle", collectionName)
				for _, tt := range tests {
					t.Run(tt.name, func(t *testing.T) {
						nt := query.NearText{Concepts: []string{"Pit Vipers"}}
						if tt.nearText != nil {
							nt = *tt.nearText
						}
						nt.ReturnProperties = append(nt.ReturnProperties, tt.property)
						nt.Filter = tt.where

						r, err := h.Query.NearText(t.Context(), nt)
						require.NoError(t, err)
						require.NotNil(t, r, "query response")

						var got []uuid.UUID
						for i := range r.Objects {
							got = append(got, r.Objects[i].UUID)
						}
						require.ElementsMatch(t, tt.expectedIDs, got)
					})
				}
			})
		})
	}
}

// FIXME(dyma): connect to host passed in the function
func testContainsMovies(string) func(t *testing.T) {
	return func(t *testing.T) {
		c := wvhost.NewClient(t)

		collectionName := "Movies"
		movies := []struct {
			id        uuid.UUID
			title     string
			director  string
			languages []string
		}{
			{
				id:        uuid.MustParse("00000000-0000-0000-0000-000000000000"),
				title:     "Braveheart",
				director:  "Mel Gibson",
				languages: []string{"English", "French", "Latin", "Scottish Gaelic"},
			},
			{
				id:        uuid.MustParse("00000000-0000-0000-0000-000000000001"),
				title:     "A Prophet",
				director:  "Mel Gibson",
				languages: []string{"French", "Arabic", "Corsican"},
			},
			{
				id:        uuid.MustParse("00000000-0000-0000-0000-000000000002"),
				title:     "Avatar",
				director:  "James Cameron",
				languages: []string{"Portugese", "Czech", "Romanian", "German"},
			},
			{
				id:        uuid.MustParse("00000000-0000-0000-0000-000000000003"),
				title:     "Spectre",
				director:  "Sam Mendes",
				languages: []string{"Spanish", "Finnish", "Polish"},
			},
			{
				id:        uuid.MustParse("00000000-0000-0000-0000-000000000004"),
				title:     "The Dark Knight Rises",
				director:  "Christopher Nolan",
				languages: []string{"English", "German", "Dutch", "Swedish"},
			},
			{
				id:        uuid.MustParse("00000000-0000-0000-0000-000000000005"),
				title:     "Incendies",
				director:  "Denis Villeneuve",
				languages: []string{"English", "French", "Polish", "Arabic"},
			},
		}

		var h *collections.Handle
		var err error

		require.NoError(t, c.Collections.DeleteAll(t.Context()))
		t.Cleanup(func() {
			require.NoError(t, c.Collections.DeleteAll(context.Background()))
		})
		t.Run("create and populate collection", func(t *testing.T) {
			h, err = c.Collections.Create(t.Context(), collections.Collection{
				Name: collectionName,
				Properties: []collections.Property{
					{
						Name:         "title",
						DataType:     collections.DataTypeText,
						Tokenization: collections.TokenizationField,
					},
					{
						Name:         "director",
						DataType:     collections.DataTypeText,
						Tokenization: collections.TokenizationField,
					},
					{
						Name:         "languages",
						DataType:     collections.DataTypeTextArray,
						Tokenization: collections.TokenizationField,
					},
				},
			})
			require.NoError(t, err)
			require.NotNilf(t, "%q collection handle", h.CollectionName())

			// Give time for the schema to replicate and graphql to rebuild it for it's queries
			time.Sleep(3 * time.Second)

			for i := range movies {
				_, err := h.Data.Insert(t.Context(), &data.Object{
					UUID: &movies[i].id,
					Properties: map[string]interface{}{
						"title":     movies[i].title,
						"director":  movies[i].director,
						"languages": movies[i].languages,
					},
				})
				require.NoError(t, err)
			}
		})

		t.Run("contains", func(t *testing.T) {
			tests := []struct {
				name        string
				where       filter.Expr
				property    string
				expectedIDs []uuid.UUID
			}{
				{
					name: "contains any languages (1)",
					where: filter.Cond{
						Target:   "languages",
						Operator: filter.ContainsAny,
						Value:    []string{"English", "German"},
					},
					property:    "languages",
					expectedIDs: []uuid.UUID{movies[0].id, movies[2].id, movies[4].id, movies[5].id},
				},
				{
					name: "contains all languages (1)",
					where: filter.Cond{
						Target:   "languages",
						Operator: filter.ContainsAll,
						Value:    []string{"English", "German"},
					},
					property:    "languages",
					expectedIDs: []uuid.UUID{movies[4].id},
				},
				{
					name: "contains none languages (1)",
					where: filter.Cond{
						Target:   "languages",
						Operator: filter.ContainsNone,
						Value:    []string{"English", "German"},
					},
					property:    "languages",
					expectedIDs: []uuid.UUID{movies[1].id, movies[3].id},
				},

				{
					name: "contains any languages (2)",
					where: filter.Cond{
						Target:   "languages",
						Operator: filter.ContainsAny,
						Value:    []string{"French", "Polish"},
					},
					property:    "languages",
					expectedIDs: []uuid.UUID{movies[0].id, movies[1].id, movies[3].id, movies[5].id},
				},
				{
					name: "contains all languages (2)",
					where: filter.Cond{
						Target:   "languages",
						Operator: filter.ContainsAll,
						Value:    []string{"French", "Polish"},
					},
					property:    "languages",
					expectedIDs: []uuid.UUID{movies[5].id},
				},
				{
					name: "contains none languages (2)",
					where: filter.Cond{
						Target:   "languages",
						Operator: filter.ContainsNone,
						Value:    []string{"French", "Polish"},
					},
					property:    "languages",
					expectedIDs: []uuid.UUID{movies[2].id, movies[4].id},
				},

				{
					name: "contains any languages (3)",
					where: filter.Cond{
						Target:   "languages",
						Operator: filter.ContainsAny,
						Value:    []string{"Portugese", "Czech", "Romanian", "German"},
					},
					property:    "languages",
					expectedIDs: []uuid.UUID{movies[2].id, movies[4].id},
				},
				{
					name: "contains all languages (3)",
					where: filter.Cond{
						Target:   "languages",
						Operator: filter.ContainsAll,
						Value:    []string{"Portugese", "Czech", "Romanian", "German"},
					},
					property:    "languages",
					expectedIDs: []uuid.UUID{movies[2].id},
				},
				{
					name: "contains none languages (3)",
					where: filter.Cond{
						Target:   "languages",
						Operator: filter.ContainsNone,
						Value:    []string{"Portugese", "Czech", "Romanian", "German"},
					},
					property:    "languages",
					expectedIDs: []uuid.UUID{movies[0].id, movies[1].id, movies[3].id, movies[5].id},
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					r, err := h.Query.OverAll(t.Context(), query.OverAll{
						Filter: tt.where,
					})
					require.NoError(t, err)
					require.NotNil(t, r, "query response")

					var got []uuid.UUID
					for i := range r.Objects {
						got = append(got, r.Objects[i].UUID)
					}
					require.ElementsMatch(t, tt.expectedIDs, got)
				})
			}
		})
	}
}
