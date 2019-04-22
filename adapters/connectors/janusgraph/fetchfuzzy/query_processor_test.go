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
package fetchfuzzy

import (
	"context"
	"testing"

	"github.com/creativesoftwarefdn/weaviate/gremlin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_QueryProcessor(t *testing.T) {
	t.Run("with both a thing and action", func(t *testing.T) {

		janusResponse := &gremlin.Response{
			Data: []gremlin.Datum{
				gremlin.Datum{
					Datum: map[string]interface{}{
						"kind": []interface{}{
							"thing",
						},
						"uuid": []interface{}{
							"some-uuid",
						},
						"classId": []interface{}{
							"class_18",
						},
					},
				},
				gremlin.Datum{
					Datum: map[string]interface{}{
						"kind": []interface{}{
							"action",
						},
						"uuid": []interface{}{
							"some-other-uuid",
						},
						"classId": []interface{}{
							"class_18",
						},
					},
				},
			},
		}
		executor := &fakeExecutor{result: janusResponse}
		expectedResult := []interface{}{
			map[string]interface{}{
				"beacon":    "weaviate://my-super-peer/things/some-uuid",
				"className": "City",
			},
			map[string]interface{}{
				"beacon":    "weaviate://my-super-peer/actions/some-other-uuid",
				"className": "City",
			},
		}

		peerName := "my-super-peer"

		result, err := NewProcessor(executor, peerName, &fakeNameSource{}).Process(context.Background(), gremlin.New())

		require.Nil(t, err, "should not error")
		assert.Equal(t, expectedResult, result, "result should be merged and post-processed")
	})

}

type fakeExecutor struct {
	result *gremlin.Response
}

func (f *fakeExecutor) Execute(ctx context.Context, query gremlin.Gremlin) (*gremlin.Response, error) {
	return f.result, nil
}
