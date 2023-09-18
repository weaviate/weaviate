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

	acceptance_with_go_client "acceptance_tests_with_client"

	"github.com/go-openapi/strfmt"
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
				resultIds := acceptance_with_go_client.GetIds(t, resp, className)
				assert.Len(t, resultIds, len(tt.expectedIds))
				assert.ElementsMatch(t, resultIds, tt.expectedIds)
			})
		}
	})
}

func TestWhereFilter_Contains_Text(t *testing.T) {
	client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: "localhost:8080"})
	require.NoError(t, err)

	defer func() {
		err := client.Schema().AllDeleter().Do(context.Background())
		require.NoError(t, err)
	}()

	ctx := context.Background()
	className := "ContainsText"
	id := "be6452f4-5db6-4a41-bfef-ff5dffd4ab16"
	texts := []string{
		" Hello You*-beautiful_world?!",
		"HoW yOU_DOin? ",
	}

	t.Run("init data", func(t *testing.T) {
		class := &models.Class{
			Class: className,
			Properties: []*models.Property{
				{
					Name:         "textField",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationField,
				},
				{
					Name:         "textWhitespace",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationWhitespace,
				},
				{
					Name:         "textLowercase",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationLowercase,
				},
				{
					Name:         "textWord",
					DataType:     schema.DataTypeText.PropString(),
					Tokenization: models.PropertyTokenizationWord,
				},

				{
					Name:         "textsField",
					DataType:     schema.DataTypeTextArray.PropString(),
					Tokenization: models.PropertyTokenizationField,
				},
				{
					Name:         "textsWhitespace",
					DataType:     schema.DataTypeTextArray.PropString(),
					Tokenization: models.PropertyTokenizationWhitespace,
				},
				{
					Name:         "textsLowercase",
					DataType:     schema.DataTypeTextArray.PropString(),
					Tokenization: models.PropertyTokenizationLowercase,
				},
				{
					Name:         "textsWord",
					DataType:     schema.DataTypeTextArray.PropString(),
					Tokenization: models.PropertyTokenizationWord,
				},
			},
		}

		err := client.Schema().ClassCreator().
			WithClass(class).
			Do(ctx)
		require.NoError(t, err)

		wrap, err := client.Data().Creator().
			WithClassName(className).
			WithID(id).
			WithProperties(map[string]interface{}{
				"textField":       texts[0],
				"textWhitespace":  texts[0],
				"textLowercase":   texts[0],
				"textWord":        texts[0],
				"textsField":      texts,
				"textsWhitespace": texts,
				"textsLowercase":  texts,
				"textsWord":       texts,
			}).
			Do(ctx)
		require.NoError(t, err)
		require.NotNil(t, wrap)
		require.NotNil(t, wrap.Object)
		require.Equal(t, strfmt.UUID(id), wrap.Object.ID)
	})

	t.Run("search using contains", func(t *testing.T) {
		type testCase struct {
			propName      string
			operator      filters.WhereOperator
			values        []string
			expectedFound bool
		}

		testCases := []testCase{}
		testCases = append(testCases,
			testCase{
				propName:      "textField",
				operator:      filters.ContainsAny,
				values:        []string{"Hello You*-beautiful_world?!", "HoW yOU_DOin?"},
				expectedFound: true,
			},
			testCase{
				propName:      "textField",
				operator:      filters.ContainsAll,
				values:        []string{"Hello You*-beautiful_world?!", "HoW yOU_DOin?"},
				expectedFound: false,
			},
			testCase{
				propName:      "textsField",
				operator:      filters.ContainsAny,
				values:        []string{"Hello You*-beautiful_world?!", "HoW yOU_DOin?"},
				expectedFound: true,
			},
			testCase{
				propName:      "textsField",
				operator:      filters.ContainsAll,
				values:        []string{"Hello You*-beautiful_world?!", "HoW yOU_DOin?"},
				expectedFound: true,
			},

			testCase{
				propName:      "textWord",
				operator:      filters.ContainsAny,
				values:        []string{"HELLO", "doin"},
				expectedFound: true,
			},
			testCase{
				propName:      "textWord",
				operator:      filters.ContainsAll,
				values:        []string{"HELLO", "doin"},
				expectedFound: false,
			},
			testCase{
				propName:      "textsWord",
				operator:      filters.ContainsAny,
				values:        []string{"HELLO", "doin"},
				expectedFound: true,
			},
			testCase{
				propName:      "textsWord",
				operator:      filters.ContainsAll,
				values:        []string{"HELLO", "doin"},
				expectedFound: true,
			},

			testCase{
				propName:      "textField",
				operator:      filters.ContainsAny,
				values:        []string{"Hello", "HoW"},
				expectedFound: false,
			},
			testCase{
				propName:      "textField",
				operator:      filters.ContainsAll,
				values:        []string{"Hello", "HoW"},
				expectedFound: false,
			},
			testCase{
				propName:      "textsField",
				operator:      filters.ContainsAny,
				values:        []string{"Hello", "HoW"},
				expectedFound: false,
			},
			testCase{
				propName:      "textsField",
				operator:      filters.ContainsAll,
				values:        []string{"Hello", "HoW"},
				expectedFound: false,
			},
			testCase{
				propName:      "textWhitespace",
				operator:      filters.ContainsAny,
				values:        []string{"Hello", "HoW"},
				expectedFound: true,
			},
			testCase{
				propName:      "textWhitespace",
				operator:      filters.ContainsAll,
				values:        []string{"Hello", "HoW"},
				expectedFound: false,
			},
			testCase{
				propName:      "textsWhitespace",
				operator:      filters.ContainsAny,
				values:        []string{"Hello", "HoW"},
				expectedFound: true,
			},
			testCase{
				propName:      "textsWhitespace",
				operator:      filters.ContainsAll,
				values:        []string{"Hello", "HoW"},
				expectedFound: true,
			},
			testCase{
				propName:      "textLowercase",
				operator:      filters.ContainsAny,
				values:        []string{"Hello", "HoW"},
				expectedFound: true,
			},
			testCase{
				propName:      "textLowercase",
				operator:      filters.ContainsAll,
				values:        []string{"Hello", "HoW"},
				expectedFound: false,
			},
			testCase{
				propName:      "textsLowercase",
				operator:      filters.ContainsAny,
				values:        []string{"Hello", "HoW"},
				expectedFound: true,
			},
			testCase{
				propName:      "textsLowercase",
				operator:      filters.ContainsAll,
				values:        []string{"Hello", "HoW"},
				expectedFound: true,
			},
			testCase{
				propName:      "textWord",
				operator:      filters.ContainsAny,
				values:        []string{"Hello", "HoW"},
				expectedFound: true,
			},
			testCase{
				propName:      "textWord",
				operator:      filters.ContainsAll,
				values:        []string{"Hello", "HoW"},
				expectedFound: false,
			},
			testCase{
				propName:      "textsWord",
				operator:      filters.ContainsAny,
				values:        []string{"Hello", "HoW"},
				expectedFound: true,
			},
			testCase{
				propName:      "textsWord",
				operator:      filters.ContainsAll,
				values:        []string{"Hello", "HoW"},
				expectedFound: true,
			},
		)

		for _, propName := range []string{"textField", "textsField"} {
			testCases = append(testCases,
				testCase{
					propName:      propName,
					operator:      filters.ContainsAny,
					values:        []string{"hello", "world"},
					expectedFound: false,
				},
				testCase{
					propName:      propName,
					operator:      filters.ContainsAll,
					values:        []string{"hello", "world"},
					expectedFound: false,
				},
			)
		}
		for _, propName := range []string{"textWhitespace", "textsWhitespace"} {
			testCases = append(testCases,
				testCase{
					propName:      propName,
					operator:      filters.ContainsAny,
					values:        []string{"hello", "world"},
					expectedFound: false,
				},
				testCase{
					propName:      propName,
					operator:      filters.ContainsAll,
					values:        []string{"hello", "world"},
					expectedFound: false,
				},
			)
		}
		for _, propName := range []string{"textLowercase", "textsLowercase"} {
			testCases = append(testCases,
				testCase{
					propName:      propName,
					operator:      filters.ContainsAny,
					values:        []string{"hello", "world"},
					expectedFound: true,
				},
				testCase{
					propName:      propName,
					operator:      filters.ContainsAll,
					values:        []string{"hello", "world"},
					expectedFound: false,
				},
			)
		}
		for _, propName := range []string{"textWord", "textsWord"} {
			testCases = append(testCases,
				testCase{
					propName:      propName,
					operator:      filters.ContainsAny,
					values:        []string{"hello", "world"},
					expectedFound: true,
				},
				testCase{
					propName:      propName,
					operator:      filters.ContainsAll,
					values:        []string{"hello", "world"},
					expectedFound: true,
				},
			)
		}

		for _, tc := range testCases {
			t.Run(fmt.Sprintf("%+v", tc), func(t *testing.T) {
				where := filters.Where().
					WithPath([]string{tc.propName}).
					WithOperator(tc.operator).
					WithValueText(tc.values...)
				field := graphql.Field{
					Name:   "_additional",
					Fields: []graphql.Field{{Name: "id"}},
				}

				resp, err := client.GraphQL().Get().
					WithClassName(className).
					WithWhere(where).
					WithFields(field).
					Do(ctx)
				require.NoError(t, err)

				ids := acceptance_with_go_client.GetIds(t, resp, className)
				if tc.expectedFound {
					require.ElementsMatch(t, ids, []string{id})
				} else {
					require.Empty(t, ids)
				}
			})
		}
	})
}
