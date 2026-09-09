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
func testContainsText(host string) func(t *testing.T) {
	return func(t *testing.T) {
		c := wvhost.NewClient(t)

		require.NoError(t, c.Collections.DeleteAll(t.Context()))
		t.Cleanup(func() {
			require.NoError(t, c.Collections.DeleteAll(context.Background()))
		})

		ctx := context.Background()
		collectionName := "ContainsText"
		id := uuid.MustParse("be6452f4-5db6-4a41-bfef-ff5dffd4ab16")
		texts := []string{
			" Hello You*-beautiful_world?!",
			"HoW yOU_DOin? ",
		}

		var h *collections.Handle
		var err error

		t.Run("init data", func(t *testing.T) {
			h, err = c.Collections.Create(t.Context(), collections.Collection{
				Name: collectionName,
				Properties: []collections.Property{
					{
						Name:         "textField",
						DataType:     collections.DataTypeText,
						Tokenization: collections.TokenizationField,
					},
					{
						Name:         "textWhitespace",
						DataType:     collections.DataTypeText,
						Tokenization: collections.TokenizationWhitespace,
					},
					{
						Name:         "textLowercase",
						DataType:     collections.DataTypeText,
						Tokenization: collections.TokenizationLowercase,
					},
					{
						Name:         "textWord",
						DataType:     collections.DataTypeText,
						Tokenization: collections.TokenizationWord,
					},

					{
						Name:         "textsField",
						DataType:     collections.DataTypeTextArray,
						Tokenization: collections.TokenizationField,
					},
					{
						Name:         "textsWhitespace",
						DataType:     collections.DataTypeTextArray,
						Tokenization: collections.TokenizationWhitespace,
					},
					{
						Name:         "textsLowercase",
						DataType:     collections.DataTypeTextArray,
						Tokenization: collections.TokenizationLowercase,
					},
					{
						Name:         "textsWord",
						DataType:     collections.DataTypeTextArray,
						Tokenization: collections.TokenizationWord,
					},
				},
			})
			require.NoError(t, err)
			require.NotNilf(t, h, "%q collection handle", collectionName)

			_, err = h.Data.Insert(ctx, &data.Object{
				UUID: &id,
				Properties: map[string]any{
					"textField":       texts[0],
					"textWhitespace":  texts[0],
					"textLowercase":   texts[0],
					"textWord":        texts[0],
					"textsField":      texts,
					"textsWhitespace": texts,
					"textsLowercase":  texts,
					"textsWord":       texts,
				},
			})
			require.NoError(t, err)

			// Give time for graphql to receive and rebuild the schema internally
			time.Sleep(3 * time.Second)
		})

		t.Run("search using contains", func(t *testing.T) {
			type testCase struct {
				propName      string
				operator      filter.Operator
				values        []string
				expectedFound bool
			}

			testCases := []testCase{}
			testCases = append(testCases,
				testCase{
					propName:      "textField",
					operator:      filter.ContainsAny,
					values:        []string{"Hello You*-beautiful_world?!", "HoW yOU_DOin?"},
					expectedFound: true,
				},
				testCase{
					propName:      "textField",
					operator:      filter.ContainsAll,
					values:        []string{"Hello You*-beautiful_world?!", "HoW yOU_DOin?"},
					expectedFound: false,
				},
				testCase{
					propName:      "textField",
					operator:      filter.ContainsNone,
					values:        []string{"Hello You*-beautiful_world?!", "HoW yOU_DOin?"},
					expectedFound: false,
				},
				testCase{
					propName:      "textsField",
					operator:      filter.ContainsAny,
					values:        []string{"Hello You*-beautiful_world?!", "HoW yOU_DOin?"},
					expectedFound: true,
				},
				testCase{
					propName:      "textsField",
					operator:      filter.ContainsAll,
					values:        []string{"Hello You*-beautiful_world?!", "HoW yOU_DOin?"},
					expectedFound: true,
				},
				testCase{
					propName:      "textsField",
					operator:      filter.ContainsNone,
					values:        []string{"Hello You*-beautiful_world?!", "HoW yOU_DOin?"},
					expectedFound: false,
				},

				testCase{
					propName:      "textWord",
					operator:      filter.ContainsAny,
					values:        []string{"HELLO", "doin"},
					expectedFound: true,
				},
				testCase{
					propName:      "textWord",
					operator:      filter.ContainsAll,
					values:        []string{"HELLO", "doin"},
					expectedFound: false,
				},
				testCase{
					propName:      "textWord",
					operator:      filter.ContainsNone,
					values:        []string{"HELLO", "doin"},
					expectedFound: false,
				},
				testCase{
					propName:      "textsWord",
					operator:      filter.ContainsAny,
					values:        []string{"HELLO", "doin"},
					expectedFound: true,
				},
				testCase{
					propName:      "textsWord",
					operator:      filter.ContainsAll,
					values:        []string{"HELLO", "doin"},
					expectedFound: true,
				},
				testCase{
					propName:      "textsWord",
					operator:      filter.ContainsNone,
					values:        []string{"HELLO", "doin"},
					expectedFound: false,
				},

				testCase{
					propName:      "textField",
					operator:      filter.ContainsAny,
					values:        []string{"Hello", "HoW"},
					expectedFound: false,
				},
				testCase{
					propName:      "textField",
					operator:      filter.ContainsAll,
					values:        []string{"Hello", "HoW"},
					expectedFound: false,
				},
				testCase{
					propName:      "textField",
					operator:      filter.ContainsNone,
					values:        []string{"Hello", "HoW"},
					expectedFound: true,
				},
				testCase{
					propName:      "textsField",
					operator:      filter.ContainsAny,
					values:        []string{"Hello", "HoW"},
					expectedFound: false,
				},
				testCase{
					propName:      "textsField",
					operator:      filter.ContainsAll,
					values:        []string{"Hello", "HoW"},
					expectedFound: false,
				},
				testCase{
					propName:      "textsField",
					operator:      filter.ContainsNone,
					values:        []string{"Hello", "HoW"},
					expectedFound: true,
				},
				testCase{
					propName:      "textWhitespace",
					operator:      filter.ContainsAny,
					values:        []string{"Hello", "HoW"},
					expectedFound: true,
				},
				testCase{
					propName:      "textWhitespace",
					operator:      filter.ContainsAll,
					values:        []string{"Hello", "HoW"},
					expectedFound: false,
				},
				testCase{
					propName:      "textWhitespace",
					operator:      filter.ContainsNone,
					values:        []string{"Hello", "HoW"},
					expectedFound: false,
				},
				testCase{
					propName:      "textsWhitespace",
					operator:      filter.ContainsAny,
					values:        []string{"Hello", "HoW"},
					expectedFound: true,
				},
				testCase{
					propName:      "textsWhitespace",
					operator:      filter.ContainsAll,
					values:        []string{"Hello", "HoW"},
					expectedFound: true,
				},
				testCase{
					propName:      "textsWhitespace",
					operator:      filter.ContainsNone,
					values:        []string{"Hello", "HoW"},
					expectedFound: false,
				},
				testCase{
					propName:      "textLowercase",
					operator:      filter.ContainsAny,
					values:        []string{"Hello", "HoW"},
					expectedFound: true,
				},
				testCase{
					propName:      "textLowercase",
					operator:      filter.ContainsAll,
					values:        []string{"Hello", "HoW"},
					expectedFound: false,
				},
				testCase{
					propName:      "textLowercase",
					operator:      filter.ContainsNone,
					values:        []string{"Hello", "HoW"},
					expectedFound: false,
				},
				testCase{
					propName:      "textsLowercase",
					operator:      filter.ContainsAny,
					values:        []string{"Hello", "HoW"},
					expectedFound: true,
				},
				testCase{
					propName:      "textsLowercase",
					operator:      filter.ContainsAll,
					values:        []string{"Hello", "HoW"},
					expectedFound: true,
				},
				testCase{
					propName:      "textsLowercase",
					operator:      filter.ContainsNone,
					values:        []string{"Hello", "HoW"},
					expectedFound: false,
				},
				testCase{
					propName:      "textWord",
					operator:      filter.ContainsAny,
					values:        []string{"Hello", "HoW"},
					expectedFound: true,
				},
				testCase{
					propName:      "textWord",
					operator:      filter.ContainsAll,
					values:        []string{"Hello", "HoW"},
					expectedFound: false,
				},
				testCase{
					propName:      "textWord",
					operator:      filter.ContainsNone,
					values:        []string{"Hello", "HoW"},
					expectedFound: false,
				},
				testCase{
					propName:      "textsWord",
					operator:      filter.ContainsAny,
					values:        []string{"Hello", "HoW"},
					expectedFound: true,
				},
				testCase{
					propName:      "textsWord",
					operator:      filter.ContainsAll,
					values:        []string{"Hello", "HoW"},
					expectedFound: true,
				},
				testCase{
					propName:      "textsWord",
					operator:      filter.ContainsNone,
					values:        []string{"Hello", "HoW"},
					expectedFound: false,
				},
			)

			for _, propName := range []string{"textField", "textsField"} {
				testCases = append(testCases,
					testCase{
						propName:      propName,
						operator:      filter.ContainsAny,
						values:        []string{"hello", "world"},
						expectedFound: false,
					},
					testCase{
						propName:      propName,
						operator:      filter.ContainsAll,
						values:        []string{"hello", "world"},
						expectedFound: false,
					},
					testCase{
						propName:      propName,
						operator:      filter.ContainsNone,
						values:        []string{"hello", "world"},
						expectedFound: true,
					},
				)
			}
			for _, propName := range []string{"textWhitespace", "textsWhitespace"} {
				testCases = append(testCases,
					testCase{
						propName:      propName,
						operator:      filter.ContainsAny,
						values:        []string{"hello", "world"},
						expectedFound: false,
					},
					testCase{
						propName:      propName,
						operator:      filter.ContainsAll,
						values:        []string{"hello", "world"},
						expectedFound: false,
					},
					testCase{
						propName:      propName,
						operator:      filter.ContainsNone,
						values:        []string{"hello", "world"},
						expectedFound: true,
					},
				)
			}
			for _, propName := range []string{"textLowercase", "textsLowercase"} {
				testCases = append(testCases,
					testCase{
						propName:      propName,
						operator:      filter.ContainsAny,
						values:        []string{"hello", "world"},
						expectedFound: true,
					},
					testCase{
						propName:      propName,
						operator:      filter.ContainsAll,
						values:        []string{"hello", "world"},
						expectedFound: false,
					},
					testCase{
						propName:      propName,
						operator:      filter.ContainsNone,
						values:        []string{"hello", "world"},
						expectedFound: false,
					},
				)
			}
			for _, propName := range []string{"textWord", "textsWord"} {
				testCases = append(testCases,
					testCase{
						propName:      propName,
						operator:      filter.ContainsAny,
						values:        []string{"hello", "world"},
						expectedFound: true,
					},
					testCase{
						propName:      propName,
						operator:      filter.ContainsAll,
						values:        []string{"hello", "world"},
						expectedFound: true,
					},
					testCase{
						propName:      propName,
						operator:      filter.ContainsNone,
						values:        []string{"hello", "world"},
						expectedFound: false,
					},
				)
			}

			for _, tc := range testCases {
				t.Run(fmt.Sprintf("%+v", tc), func(t *testing.T) {
					r, err := h.Query.OverAll(t.Context(), query.OverAll{
						Filter: filter.Cond{
							Target:   tc.propName,
							Operator: tc.operator,
							Value:    tc.values,
						},
					})
					require.NoError(t, err)
					require.NotNil(t, r, "query response")

					var got []uuid.UUID
					for i := range r.Objects {
						got = append(got, r.Objects[i].UUID)
					}
					if tc.expectedFound {
						require.ElementsMatch(t, []uuid.UUID{id}, got)
					} else {
						require.Empty(t, got)
					}
				})
			}
		})
	}
}
