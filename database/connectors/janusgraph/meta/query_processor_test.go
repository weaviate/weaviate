/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */
package meta

import (
	"testing"

	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/getmeta"
	"github.com/creativesoftwarefdn/weaviate/gremlin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_QueryProcessor(t *testing.T) {
	t.Run("when bool count and groupCount are requested", func(t *testing.T) {
		janusResponse := &gremlin.Response{
			Data: []gremlin.Datum{
				gremlin.Datum{
					Datum: map[string]interface{}{
						"myBoolProp": map[string]interface{}{
							"count": 8,
							"true":  2.0,
							"false": 6.0,
						},
					},
				},
			},
		}
		executor := &fakeExecutor{result: janusResponse}
		expectedResult := map[string]interface{}{
			"myBoolProp": map[string]interface{}{
				"count":           8,
				"totalTrue":       2.0,
				"totalFalse":      6.0,
				"percentageTrue":  0.25,
				"percentageFalse": 0.75,
			},
		}

		params := &getmeta.Params{
			Properties: []getmeta.MetaProperty{
				getmeta.MetaProperty{
					Name: "myBoolProp",
					StatisticalAnalyses: []getmeta.StatisticalAnalysis{
						getmeta.Count,
						getmeta.PercentageTrue,
						getmeta.PercentageFalse,
						getmeta.TotalTrue,
						getmeta.TotalFalse,
					},
				}},
		}
		result, err := NewProcessor(executor, nil, nil).Process(gremlin.New(), nil, params)

		require.Nil(t, err, "should not error")
		assert.Equal(t, expectedResult, result, "result should be merged and post-processed")
	})

	t.Run("when bool groupCount but all results are 'true'", func(t *testing.T) {
		// this happens either when the user specifies a filter or when every
		// single vertices has the same value for a boolean prop. We have to make
		// sure that we don't error because of the missing counter prop, i.e. true
		// for false, or false for true.
		janusResponse := &gremlin.Response{
			Data: []gremlin.Datum{
				gremlin.Datum{
					Datum: map[string]interface{}{
						"myBoolProp": map[string]interface{}{
							"count": 8,
							"true":  8.0,
						},
					},
				},
			},
		}
		executor := &fakeExecutor{result: janusResponse}
		expectedResult := map[string]interface{}{
			"myBoolProp": map[string]interface{}{
				"count":           8,
				"totalTrue":       8.0,
				"totalFalse":      0.0,
				"percentageTrue":  1.0,
				"percentageFalse": 0.0,
			},
		}

		params := &getmeta.Params{
			Properties: []getmeta.MetaProperty{
				getmeta.MetaProperty{
					Name: "myBoolProp",
					StatisticalAnalyses: []getmeta.StatisticalAnalysis{
						getmeta.Count,
						getmeta.PercentageTrue,
						getmeta.PercentageFalse,
						getmeta.TotalTrue,
						getmeta.TotalFalse,
					},
				}},
		}
		result, err := NewProcessor(executor, nil, nil).Process(gremlin.New(), nil, params)

		require.Nil(t, err, "should not error")
		assert.Equal(t, expectedResult, result, "result should be merged and post-processed")
	})

	t.Run("when bool groupCount but all results are 'false'", func(t *testing.T) {
		// this happens either when the user specifies a filter or when every
		// single vertices has the same value for a boolean prop. We have to make
		// sure that we don't error because of the missing counter prop, i.e. true
		// for false, or false for true.
		janusResponse := &gremlin.Response{
			Data: []gremlin.Datum{
				gremlin.Datum{
					Datum: map[string]interface{}{
						"myBoolProp": map[string]interface{}{
							"count": 8,
							"false": 8.0,
						},
					},
				},
			},
		}
		executor := &fakeExecutor{result: janusResponse}
		expectedResult := map[string]interface{}{
			"myBoolProp": map[string]interface{}{
				"count":           8,
				"totalFalse":      8.0,
				"totalTrue":       0.0,
				"percentageFalse": 1.0,
				"percentageTrue":  0.0,
			},
		}

		params := &getmeta.Params{
			Properties: []getmeta.MetaProperty{
				getmeta.MetaProperty{
					Name: "myBoolProp",
					StatisticalAnalyses: []getmeta.StatisticalAnalysis{
						getmeta.Count,
						getmeta.PercentageTrue,
						getmeta.PercentageFalse,
						getmeta.TotalTrue,
						getmeta.TotalFalse,
					},
				}},
		}
		result, err := NewProcessor(executor, nil, nil).Process(gremlin.New(), nil, params)

		require.Nil(t, err, "should not error")
		assert.Equal(t, expectedResult, result, "result should be merged and post-processed")
	})

	t.Run("when int count is requested", func(t *testing.T) {
		janusResponse := &gremlin.Response{
			Data: []gremlin.Datum{
				gremlin.Datum{
					Datum: map[string]interface{}{
						"myIntProp": map[string]interface{}{
							"count": 8,
						},
					},
				},
			},
		}
		executor := &fakeExecutor{result: janusResponse}
		expectedResult := map[string]interface{}{
			"myIntProp": map[string]interface{}{
				"count": 8,
			},
		}

		result, err := NewProcessor(executor, nil, nil).Process(gremlin.New(), nil, &getmeta.Params{})

		require.Nil(t, err, "should not error")
		assert.Equal(t, expectedResult, result, "result should be merged and post-processed")
	})

	t.Run("when there are more than 1 gremlin result",
		func(t *testing.T) {
			janusResponse := &gremlin.Response{
				Data: []gremlin.Datum{
					gremlin.Datum{
						Datum: map[string]interface{}{
							"myStringProp": map[string]interface{}{
								"topOccurrences": map[string]interface{}{
									"rare string":          1.0,
									"common string":        7.0,
									"not so common string": 3.0,
								},
							},
						},
					},
					gremlin.Datum{
						Datum: map[string]interface{}{
							"myStringProp": map[string]interface{}{
								"topOccurrences": map[string]interface{}{
									"rare string":          1.0,
									"common string":        7.0,
									"not so common string": 3.0,
								},
							},
						},
					},
				},
			}

			executor := &fakeExecutor{result: janusResponse}
			_, err := NewProcessor(executor, nil, nil).Process(gremlin.New(), nil, &getmeta.Params{})
			require.NotNil(t, err, "should error")
		})

	t.Run("when int count is requested and there are types to be merged in from a different prop",
		func(t *testing.T) {
			janusResponse := &gremlin.Response{
				Data: []gremlin.Datum{
					gremlin.Datum{
						Datum: map[string]interface{}{
							"myIntProp": map[string]interface{}{
								"count": 8,
							},
						},
					},
				},
			}
			executor := &fakeExecutor{result: janusResponse}
			typeInput := map[string]interface{}{
				"MyRefProp": map[string]interface{}{
					"pointingTo": []interface{}{"ClassA", "ClassB"},
				},
			}
			expectedResult := map[string]interface{}{
				"myIntProp": map[string]interface{}{
					"count": 8,
				},
				"MyRefProp": map[string]interface{}{
					"pointingTo": []interface{}{"ClassA", "ClassB"},
				},
			}

			result, err := NewProcessor(executor, nil, nil).Process(gremlin.New(), typeInput, &getmeta.Params{})

			require.Nil(t, err, "should not error")
			assert.Equal(t, expectedResult, result, "result should be merged and post-processed")
		})

	t.Run("when int count is requested and there are types to be merged in from the same int prop",
		func(t *testing.T) {
			janusResponse := &gremlin.Response{
				Data: []gremlin.Datum{
					gremlin.Datum{
						Datum: map[string]interface{}{
							"myIntProp": map[string]interface{}{
								"count": 8,
							},
						},
					},
				},
			}
			executor := &fakeExecutor{result: janusResponse}
			typeInput := map[string]interface{}{
				"myIntProp": map[string]interface{}{
					"type": "int",
				},
			}
			expectedResult := map[string]interface{}{
				"myIntProp": map[string]interface{}{
					"count": 8,
					"type":  "int",
				},
			}

			result, err := NewProcessor(executor, nil, nil).Process(gremlin.New(), typeInput, &getmeta.Params{})

			require.Nil(t, err, "should not error")
			assert.Equal(t, expectedResult, result, "result should be merged and post-processed")
		})

	t.Run("when int count is requested and there are only types, but nothing else",
		func(t *testing.T) {
			janusResponse := &gremlin.Response{
				Data: nil,
			}
			executor := &fakeExecutor{result: janusResponse}
			typeInput := map[string]interface{}{
				"myIntProp": map[string]interface{}{
					"type": "int",
				},
			}
			expectedResult := map[string]interface{}{
				"myIntProp": map[string]interface{}{
					"type": "int",
				},
			}

			result, err := NewProcessor(executor, nil, nil).Process(gremlin.New(), typeInput, &getmeta.Params{})
			require.Nil(t, err, "should not error")
			assert.Equal(t, expectedResult, result, "result should be merged and post-processed")
		})

	t.Run("when string top occurrences are requested", func(t *testing.T) {
		janusResponse := &gremlin.Response{
			Data: []gremlin.Datum{
				gremlin.Datum{
					Datum: map[string]interface{}{
						"myStringProp": map[string]interface{}{
							"topOccurrences": map[string]interface{}{
								"rare string":          1.0,
								"common string":        7.0,
								"not so common string": 3.0,
							},
						},
					},
				},
			},
		}
		executor := &fakeExecutor{result: janusResponse}
		expectedResult := map[string]interface{}{
			"myStringProp": map[string]interface{}{
				"topOccurrences": []interface{}{
					map[string]interface{}{
						"value":  "common string",
						"occurs": 7.0,
					},
					map[string]interface{}{
						"value":  "not so common string",
						"occurs": 3.0,
					},
					map[string]interface{}{
						"value":  "rare string",
						"occurs": 1.0,
					},
				},
			},
		}

		result, err := NewProcessor(executor, nil, nil).Process(gremlin.New(), nil, &getmeta.Params{})

		require.Nil(t, err, "should not error")
		assert.Equal(t, expectedResult, result, "result should be merged and post-processed")
	})

	t.Run("when string top occurrences are requested including count", func(t *testing.T) {
		janusResponse := &gremlin.Response{
			Data: []gremlin.Datum{
				gremlin.Datum{
					Datum: map[string]interface{}{
						"myStringProp": map[string]interface{}{
							"count": 11,
							"topOccurrences": map[string]interface{}{
								"rare string":          1.0,
								"common string":        7.0,
								"not so common string": 3.0,
							},
						},
					},
				},
			},
		}
		executor := &fakeExecutor{result: janusResponse}
		expectedResult := map[string]interface{}{
			"myStringProp": map[string]interface{}{
				"count": 11,
				"topOccurrences": []interface{}{
					map[string]interface{}{
						"value":  "common string",
						"occurs": 7.0,
					},
					map[string]interface{}{
						"value":  "not so common string",
						"occurs": 3.0,
					},
					map[string]interface{}{
						"value":  "rare string",
						"occurs": 1.0,
					},
				},
			},
		}

		result, err := NewProcessor(executor, nil, nil).Process(gremlin.New(), nil, &getmeta.Params{})

		require.Nil(t, err, "should not error")
		assert.Equal(t, expectedResult, result, "result should be merged and post-processed")
	})

}

type fakeExecutor struct {
	result *gremlin.Response
}

func (f *fakeExecutor) Execute(query gremlin.Gremlin) (*gremlin.Response, error) {
	return f.result, nil
}
