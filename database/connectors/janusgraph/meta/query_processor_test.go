package meta

import (
	"testing"

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
						},
					},
				},
				gremlin.Datum{
					Datum: map[string]interface{}{
						"myBoolProp": map[string]interface{}{
							BoolGroupCount: map[string]interface{}{
								"true":  2.0,
								"false": 6.0,
							},
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

		result, err := NewProcessor(executor).Process(gremlin.New(), nil)

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

		result, err := NewProcessor(executor).Process(gremlin.New(), nil)

		require.Nil(t, err, "should not error")
		assert.Equal(t, expectedResult, result, "result should be merged and post-processed")
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

			result, err := NewProcessor(executor).Process(gremlin.New(), typeInput)

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

			result, err := NewProcessor(executor).Process(gremlin.New(), typeInput)

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

			result, err := NewProcessor(executor).Process(gremlin.New(), typeInput)

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

		result, err := NewProcessor(executor).Process(gremlin.New(), nil)

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
