//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
//  DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package aggregate

import (
	"context"
	"testing"

	"github.com/semi-technologies/weaviate/adapters/connectors/janusgraph/gremlin"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_QueryProcessor(t *testing.T) {
	t.Run("when city population is requested aggregated by 'isCapital'", func(t *testing.T) {
		janusResponse := &gremlin.Response{
			Data: []gremlin.Datum{
				gremlin.Datum{
					Datum: map[string]interface{}{
						"true": map[string]interface{}{
							"population": map[string]interface{}{
								"prop_18__count": 8,
							},
						},
						"false": map[string]interface{}{
							"population": map[string]interface{}{
								"prop_18__count": 3,
							},
						},
					},
				},
			},
		}
		executor := &fakeExecutor{result: janusResponse}
		groupBy := &filters.Path{
			Class:    schema.ClassName("City"),
			Property: schema.PropertyName("isCapital"),
		}

		expectedResult := []interface{}{
			map[string]interface{}{
				"groupedBy": map[string]interface{}{
					"path":  []interface{}{"isCapital"},
					"value": "true",
				},
				"population": map[string]interface{}{
					"count": 8,
				},
			},
			map[string]interface{}{
				"groupedBy": map[string]interface{}{
					"path":  []interface{}{"isCapital"},
					"value": "false",
				},
				"population": map[string]interface{}{
					"count": 3,
				},
			},
		}

		result, err := NewProcessor(executor, nil, nil).Process(context.Background(), gremlin.New(), groupBy, &traverser.AggregateParams{})

		require.Nil(t, err, "should not error")
		assert.ElementsMatch(t, expectedResult, result, "result should be merged and post-processed")
	})

}

type fakeExecutor struct {
	result *gremlin.Response
}

func (f *fakeExecutor) Execute(ctx context.Context, query gremlin.Gremlin) (*gremlin.Response, error) {
	return f.result, nil
}
