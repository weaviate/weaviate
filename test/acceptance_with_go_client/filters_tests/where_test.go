//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package filters_tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/filters"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func TestWhereFilter(t *testing.T) {
	client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: "localhost:8080"})
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
					DataType: []string{schema.DataTypeText.String()},
				},
				{
					Name:     "colors",
					DataType: []string{schema.DataTypeTextArray.String()},
				},
				{
					Name:     "author",
					DataType: []string{schema.DataTypeString.String()},
				},
				{
					Name:     "authors",
					DataType: []string{schema.DataTypeStringArray.String()},
				},
				{
					Name:     "number",
					DataType: []string{schema.DataTypeNumber.String()},
				},
				{
					Name:     "numbers",
					DataType: []string{schema.DataTypeNumberArray.String()},
				},
				{
					Name:     "int",
					DataType: []string{schema.DataTypeInt.String()},
				},
				{
					Name:     "ints",
					DataType: []string{schema.DataTypeIntArray.String()},
				},
				{
					Name:     "date",
					DataType: []string{schema.DataTypeDate.String()},
				},
				{
					Name:     "dates",
					DataType: []string{schema.DataTypeDateArray.String()},
				},
				{
					Name:     "bool",
					DataType: []string{schema.DataTypeBoolean.String()},
				},
				{
					Name:     "bools",
					DataType: []string{schema.DataTypeBooleanArray.String()},
				},
				{
					Name:     "uuid",
					DataType: []string{schema.DataTypeUUID.String()},
				},
				{
					Name:     "uuids",
					DataType: []string{schema.DataTypeUUIDArray.String()},
				},
			},
		}
		err := client.Schema().ClassCreator().WithClass(class).Do(context.TODO())
		require.Nil(t, err)
	})

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
		getIds := func(data map[string]models.JSONObject) []string {
			classMap, ok := data["Get"].(map[string]interface{})
			require.True(t, ok)
			class, ok := classMap[className].([]interface{})
			require.True(t, ok)
			ids := make([]string, len(class))
			for i := range class {
				resultMap, ok := class[i].(map[string]interface{})
				require.True(t, ok)
				additional, ok := resultMap["_additional"].(map[string]interface{})
				require.True(t, ok)
				ids[i] = additional["id"].(string)
			}
			return ids
		}
		tests := []struct {
			name        string
			where       *filters.WhereBuilder
			property    string
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
					Do(context.TODO())
				require.Nil(t, err)
				require.Empty(t, resp.Errors)
				resultIds := getIds(resp.Data)
				assert.Len(t, resultIds, len(tt.expectedIds))
				assert.ElementsMatch(t, resultIds, tt.expectedIds)
			})
		}
	})
}

func mustGetTime(date string) time.Time {
	result, err := time.Parse(time.RFC3339Nano, date)
	if err != nil {
		panic(fmt.Sprintf("can't parse date: %v", date))
	}
	return result
}
