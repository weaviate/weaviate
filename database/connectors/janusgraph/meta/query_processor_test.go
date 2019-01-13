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

		result, err := NewProcessor(executor).Process(gremlin.New())

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
