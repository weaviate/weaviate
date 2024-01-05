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

	acceptance_with_go_client "acceptance_tests_with_client"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/filters"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

func testContainsText(t *testing.T, host string) func(t *testing.T) {
	return func(t *testing.T) {
		client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: host})
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
}
