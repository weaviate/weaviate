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
	"fmt"
	"testing"
	"time"

	acceptance_with_go_client "acceptance_tests_with_client"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/filters"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func testContainsAnyAll(t *testing.T, host string) func(t *testing.T) {
	return func(t *testing.T) {
		client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: host})
		require.Nil(t, err)

		className := "WhereTest"
		id1 := "00000000-0000-0000-0000-000000000001"
		id2 := "00000000-0000-0000-0000-000000000002"
		id3 := "00000000-0000-0000-0000-000000000003"
		ids := []string{id1, id2, id3}

		cleanup := func() {
			err := client.Schema().AllDeleter().Do(context.Background())
			require.Nil(t, err)
		}

		t.Run("create class", func(t *testing.T) {
			cleanup()
			class := &models.Class{
				Class: className,
				Properties: []*models.Property{
					{
						Name:     "color",
						DataType: schema.DataTypeText.PropString(),
					},
					{
						Name:     "colors",
						DataType: schema.DataTypeTextArray.PropString(),
					},
					{
						Name:     "author",
						DataType: schema.DataTypeString.PropString(),
					},
					{
						Name:     "authors",
						DataType: schema.DataTypeStringArray.PropString(),
					},
					{
						Name:     "number",
						DataType: schema.DataTypeNumber.PropString(),
					},
					{
						Name:     "numbers",
						DataType: schema.DataTypeNumberArray.PropString(),
					},
					{
						Name:     "int",
						DataType: schema.DataTypeInt.PropString(),
					},
					{
						Name:     "ints",
						DataType: schema.DataTypeIntArray.PropString(),
					},
					{
						Name:     "date",
						DataType: schema.DataTypeDate.PropString(),
					},
					{
						Name:     "dates",
						DataType: schema.DataTypeDateArray.PropString(),
					},
					{
						Name:     "bool",
						DataType: schema.DataTypeBoolean.PropString(),
					},
					{
						Name:     "bools",
						DataType: schema.DataTypeBooleanArray.PropString(),
					},
					{
						Name:     "uuid",
						DataType: schema.DataTypeUUID.PropString(),
					},
					{
						Name:     "uuids",
						DataType: schema.DataTypeUUIDArray.PropString(),
					},
				},
			}
			err := client.Schema().ClassCreator().WithClass(class).Do(context.TODO())
			require.Nil(t, err)
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
				where    *filters.WhereBuilder
				property string
			}{
				// Contains operator with array types
				{
					name: "contains any authors with string array",
					where: filters.Where().
						WithPath([]string{"authors"}).
						WithOperator(filters.ContainsAny).
						WithValueString("John", "Jenny", "Joseph"),
					property: "authors",
				},
				{
					name: "contains any colors with text array",
					where: filters.Where().
						WithPath([]string{"colors"}).
						WithOperator(filters.ContainsAny).
						WithValueText("red", "blue", "green"),
					property: "colors",
				},
				{
					name: "contains any numbers with number array",
					where: filters.Where().
						WithPath([]string{"numbers"}).
						WithOperator(filters.ContainsAny).
						WithValueNumber(1.1, 2.2, 3.3),
					property: "numbers",
				},
				{
					name: "contains any ints with int array",
					where: filters.Where().
						WithPath([]string{"ints"}).
						WithOperator(filters.ContainsAny).
						WithValueInt(1, 2, 3),
					property: "ints",
				},
				{
					name: "contains any bools with bool array",
					where: filters.Where().
						WithPath([]string{"bools"}).
						WithOperator(filters.ContainsAny).
						WithValueBoolean(true, false),
					property: "bools",
				},
				{
					name: "contains any uuids with uuid array",
					where: filters.Where().
						WithPath([]string{"uuids"}).
						WithOperator(filters.ContainsAny).
						WithValueText(id1, id2, id3),
					property: "uuids",
				},
				{
					name: "contains any dates with dates array",
					where: filters.Where().
						WithPath([]string{"dates"}).
						WithOperator(filters.ContainsAny).
						WithValueDate(
							mustGetTime("2009-11-01T23:00:00Z"), mustGetTime("2009-11-02T23:00:00Z"), mustGetTime("2009-11-03T23:00:00Z"),
						),
					property: "dates",
				},
				// Contains operator with primitives
				{
					name: "contains any author with string",
					where: filters.Where().
						WithPath([]string{"author"}).
						WithOperator(filters.ContainsAny).
						WithValueString("John", "Jenny", "Joseph"),
					property: "author",
				},
				{
					name: "contains any color with text",
					where: filters.Where().
						WithPath([]string{"color"}).
						WithOperator(filters.ContainsAny).
						WithValueText("red", "blue", "green"),
					property: "color",
				},
				{
					name: "contains any number with number",
					where: filters.Where().
						WithPath([]string{"number"}).
						WithOperator(filters.ContainsAny).
						WithValueNumber(1.1, 2.2, 3.3),
					property: "number",
				},
				{
					name: "contains any int with int",
					where: filters.Where().
						WithPath([]string{"int"}).
						WithOperator(filters.ContainsAny).
						WithValueInt(1, 2, 3),
					property: "int",
				},
				{
					name: "contains any bool with bool",
					where: filters.Where().
						WithPath([]string{"bool"}).
						WithOperator(filters.ContainsAny).
						WithValueBoolean(true, false, true),
					property: "bool",
				},
				{
					name: "contains any uuid with uuid",
					where: filters.Where().
						WithPath([]string{"uuid"}).
						WithOperator(filters.ContainsAny).
						WithValueText(id1, id2, id3),
					property: "uuid",
				},
				{
					name: "contains any uuid with id",
					where: filters.Where().
						WithPath([]string{"id"}).
						WithOperator(filters.ContainsAny).
						WithValueText(id1, id2, id3),
					property: "uuid",
				},
				{
					name: "contains any date with date",
					where: filters.Where().
						WithPath([]string{"date"}).
						WithOperator(filters.ContainsAny).
						WithValueDate(
							mustGetTime("2009-11-01T23:00:00Z"), mustGetTime("2009-11-02T23:00:00Z"), mustGetTime("2009-11-03T23:00:00Z"),
						),
					property: "date",
				},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					fields := []graphql.Field{
						{Name: tt.property},
						{Name: "_additional", Fields: []graphql.Field{{Name: "id"}}},
					}
					resp, err := client.GraphQL().Get().
						WithClassName(className).
						WithWhere(tt.where).
						WithBM25(client.GraphQL().
							Bm25ArgBuilder().
							WithQuery(tt.property),
						).
						WithFields(fields...).
						Do(context.TODO())
					require.Nil(t, err)
					require.Empty(t, resp.Errors)
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
				colors := []string{"red", "blue", "green"}
				colorssArray := [][]string{
					{"red", "blue", "green"},
					{"red", "blue"},
					{"red"},
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
				uuids := []string{id1, id2, id3}
				uuidsArray := [][]string{
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
					_, err := client.Data().Creator().
						WithClassName(className).
						WithID(id).
						WithProperties(map[string]interface{}{
							"color":   colors[i],
							"colors":  colorssArray[i],
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
						}).
						Do(context.TODO())
					require.Nil(t, err)
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
					where       *filters.WhereBuilder
					property    string
					nearText    *graphql.NearTextArgumentBuilder
					expectedIds []string
				}{
					// Contains operator with array types
					{
						name: "contains all authors with string array",
						where: filters.Where().
							WithPath([]string{"authors"}).
							WithOperator(filters.ContainsAll).
							WithValueString("John", "Jenny", "Joseph"),
						property:    "authors",
						expectedIds: []string{id1},
					},
					{
						name: "contains any authors with string array",
						where: filters.Where().
							WithPath([]string{"authors"}).
							WithOperator(filters.ContainsAny).
							WithValueString("John", "Jenny", "Joseph"),
						property:    "authors",
						expectedIds: []string{id1, id2, id3},
					},
					{
						name: "contains all colors with text array",
						where: filters.Where().
							WithPath([]string{"colors"}).
							WithOperator(filters.ContainsAll).
							WithValueText("red", "blue", "green"),
						property:    "colors",
						expectedIds: []string{id1},
					},
					{
						name: "contains any colors with text array",
						where: filters.Where().
							WithPath([]string{"colors"}).
							WithOperator(filters.ContainsAny).
							WithValueText("red", "blue", "green"),
						property:    "colors",
						expectedIds: []string{id1, id2, id3},
					},
					{
						name: "contains all numbers with number array",
						where: filters.Where().
							WithPath([]string{"numbers"}).
							WithOperator(filters.ContainsAll).
							WithValueNumber(1.1, 2.2, 3.3),
						property:    "numbers",
						expectedIds: []string{id1},
					},
					{
						name: "contains any numbers with number array",
						where: filters.Where().
							WithPath([]string{"numbers"}).
							WithOperator(filters.ContainsAny).
							WithValueNumber(1.1, 2.2, 3.3),
						property:    "numbers",
						expectedIds: []string{id1, id2, id3},
					},
					{
						name: "contains all ints with int array",
						where: filters.Where().
							WithPath([]string{"ints"}).
							WithOperator(filters.ContainsAll).
							WithValueInt(1, 2, 3),
						property:    "ints",
						expectedIds: []string{id1},
					},
					{
						name: "contains any ints with int array",
						where: filters.Where().
							WithPath([]string{"ints"}).
							WithOperator(filters.ContainsAny).
							WithValueInt(1, 2, 3),
						property:    "ints",
						expectedIds: []string{id1, id2, id3},
					},
					{
						name: "contains all bools with bool array",
						where: filters.Where().
							WithPath([]string{"bools"}).
							WithOperator(filters.ContainsAll).
							WithValueBoolean(true, false, true),
						property:    "bools",
						expectedIds: []string{id1, id2},
					},
					{
						name: "contains any bools with bool array",
						where: filters.Where().
							WithPath([]string{"bools"}).
							WithOperator(filters.ContainsAny).
							WithValueBoolean(true, false),
						property:    "bools",
						expectedIds: []string{id1, id2, id3},
					},
					{
						name: "contains all uuids with uuid array",
						where: filters.Where().
							WithPath([]string{"uuids"}).
							WithOperator(filters.ContainsAll).
							WithValueText(id1, id2, id3),
						property:    "uuids",
						expectedIds: []string{id1},
					},
					{
						name: "contains any uuids with uuid array",
						where: filters.Where().
							WithPath([]string{"uuids"}).
							WithOperator(filters.ContainsAny).
							WithValueText(id1, id2, id3),
						property:    "uuids",
						expectedIds: []string{id1, id2, id3},
					},
					{
						name: "contains all dates with dates array",
						where: filters.Where().
							WithPath([]string{"dates"}).
							WithOperator(filters.ContainsAll).
							WithValueDate(
								mustGetTime("2009-11-01T23:00:00Z"), mustGetTime("2009-11-02T23:00:00Z"), mustGetTime("2009-11-03T23:00:00Z"),
							),
						property:    "dates",
						expectedIds: []string{id1},
					},
					{
						name: "contains any dates with dates array",
						where: filters.Where().
							WithPath([]string{"dates"}).
							WithOperator(filters.ContainsAny).
							WithValueDate(
								mustGetTime("2009-11-01T23:00:00Z"), mustGetTime("2009-11-02T23:00:00Z"), mustGetTime("2009-11-03T23:00:00Z"),
							),
						property:    "dates",
						expectedIds: []string{id1, id2, id3},
					},
					{
						name: "complex contains all ints and all numbers with AND on int array",
						where: filters.Where().
							WithOperator(filters.And).
							WithOperands([]*filters.WhereBuilder{
								filters.Where().
									WithPath([]string{"numbers"}).
									WithOperator(filters.ContainsAll).
									WithValueNumber(1.1, 2.2, 3.3),
								filters.Where().
									WithPath([]string{"ints"}).
									WithOperator(filters.ContainsAll).
									WithValueInt(1, 2, 3),
							}),
						property:    "ints",
						expectedIds: []string{id1},
					},
					{
						name: "complex contains any ints and all numbers with OR on int array",
						where: filters.Where().
							WithOperator(filters.Or).
							WithOperands([]*filters.WhereBuilder{
								filters.Where().
									WithPath([]string{"numbers"}).
									WithOperator(filters.ContainsAll).
									WithValueNumber(1.1, 2.2, 3.3),
								filters.Where().
									WithPath([]string{"ints"}).
									WithOperator(filters.ContainsAny).
									WithValueInt(1, 2, 3),
							}),
						property:    "ints",
						expectedIds: []string{id1, id2, id3},
					},
					// Contains operator with primitives
					{
						name: "contains any author with string",
						where: filters.Where().
							WithPath([]string{"author"}).
							WithOperator(filters.ContainsAny).
							WithValueString("John", "Jenny", "Joseph"),
						property:    "author",
						expectedIds: []string{id1, id2, id3},
					},
					{
						name: "contains any color with text",
						where: filters.Where().
							WithPath([]string{"color"}).
							WithOperator(filters.ContainsAny).
							WithValueText("red", "blue", "green"),
						property:    "color",
						expectedIds: []string{id1, id2, id3},
					},
					{
						name: "contains any number with number",
						where: filters.Where().
							WithPath([]string{"number"}).
							WithOperator(filters.ContainsAny).
							WithValueNumber(1.1, 2.2, 3.3),
						property:    "number",
						expectedIds: []string{id1, id2, id3},
					},
					{
						name: "contains any int with int",
						where: filters.Where().
							WithPath([]string{"int"}).
							WithOperator(filters.ContainsAny).
							WithValueInt(1, 2, 3),
						property:    "int",
						expectedIds: []string{id1, id2, id3},
					},
					{
						name: "contains any bool with bool",
						where: filters.Where().
							WithPath([]string{"bool"}).
							WithOperator(filters.ContainsAny).
							WithValueBoolean(true, false, true),
						property:    "bool",
						expectedIds: []string{id1, id2, id3},
					},
					{
						name: "contains any uuid with uuid",
						where: filters.Where().
							WithPath([]string{"uuid"}).
							WithOperator(filters.ContainsAny).
							WithValueText(id1, id2, id3),
						property:    "uuid",
						expectedIds: []string{id1, id2, id3},
					},
					{
						name: "contains any uuid with id",
						where: filters.Where().
							WithPath([]string{"id"}).
							WithOperator(filters.ContainsAny).
							WithValueText(id1, id2, id3),
						property:    "uuid",
						expectedIds: []string{id1, id2, id3},
					},
					{
						name: "contains any date with date",
						where: filters.Where().
							WithPath([]string{"date"}).
							WithOperator(filters.ContainsAny).
							WithValueDate(
								mustGetTime("2009-11-01T23:00:00Z"), mustGetTime("2009-11-02T23:00:00Z"), mustGetTime("2009-11-03T23:00:00Z"),
							),
						property:    "date",
						expectedIds: []string{id1, id2, id3},
					},
					{
						name: "contains all authors with string array and nearText",
						where: filters.Where().
							WithPath([]string{"authors"}).
							WithOperator(filters.ContainsAll).
							WithValueString("John", "Jenny", "Joseph"),
						property: "authors",
						nearText: client.GraphQL().NearTextArgBuilder().
							WithConcepts([]string{"John"}),
						expectedIds: []string{id1},
					},
				}
				for _, tt := range tests {
					t.Run(tt.name, func(t *testing.T) {
						fields := []graphql.Field{
							{Name: tt.property},
							{Name: "_additional", Fields: []graphql.Field{{Name: "id"}}},
						}
						resp, err := client.GraphQL().Get().
							WithClassName(className).
							WithWhere(tt.where).
							WithFields(fields...).
							WithNearText(tt.nearText).
							Do(context.TODO())
						require.Nil(t, err)
						resultIds := acceptance_with_go_client.GetIds(t, resp, className)
						assert.Len(t, resultIds, len(tt.expectedIds))
						assert.ElementsMatch(t, resultIds, tt.expectedIds)
					})
				}
			})
		})
	}
}
