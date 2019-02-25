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
package get

import (
	"testing"

	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/local/get"
	"github.com/creativesoftwarefdn/weaviate/gremlin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_QueryProcessor(t *testing.T) {
	t.Run("only primtive fields, no cross refs", func(t *testing.T) {
		janusResponse := &gremlin.Response{
			Data: []gremlin.Datum{
				gremlin.Datum{
					Datum: map[string]interface{}{
						"objects": []interface{}{
							map[string]interface{}{
								"uuid": []interface{}{
									"someuuid",
								},
								"classId": []interface{}{
									"class_18",
								},
								"prop_1": []interface{}{
									"Amsterdam",
								},
								"prop_2": []interface{}{
									800000,
								},
							},
						},
					},
				},
				gremlin.Datum{
					Datum: map[string]interface{}{
						"objects": []interface{}{
							map[string]interface{}{
								"uuid": []interface{}{
									"someotheruuid",
								},
								"classId": []interface{}{
									"class_18",
								},
								"prop_1": []interface{}{
									"Dusseldorf",
								},
								"prop_2": []interface{}{
									600000,
								},
							},
						},
					},
				},
			},
		}
		executor := &fakeExecutor{result: janusResponse}
		expectedResult := []interface{}{
			map[string]interface{}{
				"uuid":       "someuuid",
				"name":       "Amsterdam",
				"population": 800000,
			},
			map[string]interface{}{
				"uuid":       "someotheruuid",
				"name":       "Dusseldorf",
				"population": 600000,
			},
		}

		result, err := NewProcessor(executor, &fakeNameSource{}, schema.ClassName("City")).
			Process(gremlin.New())

		require.Nil(t, err, "should not error")
		assert.Equal(t, expectedResult, result, "result should be merged and post-processed")
	})

	t.Run("single result, cross-ref one level deep", func(t *testing.T) {
		janusResponse := &gremlin.Response{
			Data: []gremlin.Datum{
				gremlin.Datum{
					Datum: map[string]interface{}{
						"objects": []interface{}{
							map[string]interface{}{
								"uuid": []interface{}{
									"someuuid",
								},
								"classId": []interface{}{
									"class_18",
								},
								"prop_1": []interface{}{
									"Amsterdam",
								},
								"prop_2": []interface{}{
									800000,
								},
							},
							map[string]interface{}{
								"refId": "prop_3",
							},
							map[string]interface{}{
								"uuid": []interface{}{
									"someotheruuid",
								},
								"classId": []interface{}{
									"class_19",
								},
								"prop_1": []interface{}{
									"Netherlands",
								},
								"prop_2": []interface{}{
									30000000,
								},
							},
						},
					},
				},
			},
		}
		executor := &fakeExecutor{result: janusResponse}
		expectedResult := []interface{}{
			map[string]interface{}{
				"uuid":       "someuuid",
				"name":       "Amsterdam",
				"population": 800000,
				"InCountry": []interface{}{
					get.LocalRef{
						Fields: map[string]interface{}{
							"name":       "Netherlands",
							"population": 30000000,
							"uuid":       "someotheruuid",
						},
						AtClass: "Country",
					},
				},
			},
		}

		result, err := NewProcessor(executor, &fakeNameSource{}, schema.ClassName("City")).
			Process(gremlin.New())

		require.Nil(t, err, "should not error")
		assert.Equal(t, expectedResult, result, "result should be merged and post-processed")
	})

	t.Run("single result, cross-refs several levels deep", func(t *testing.T) {
		janusResponse := &gremlin.Response{
			Data: []gremlin.Datum{
				gremlin.Datum{
					Datum: map[string]interface{}{
						"objects": []interface{}{
							map[string]interface{}{
								"uuid": []interface{}{
									"someuuid",
								},
								"classId": []interface{}{
									"class_18",
								},
								"prop_1": []interface{}{
									"Amsterdam",
								},
								"prop_2": []interface{}{
									800000,
								},
							},
							map[string]interface{}{
								"refId": "prop_3",
							},
							map[string]interface{}{
								"uuid": []interface{}{
									"someotheruuid",
								},
								"classId": []interface{}{
									"class_19",
								},
								"prop_1": []interface{}{
									"Netherlands",
								},
								"prop_2": []interface{}{
									30000000,
								},
							},
							map[string]interface{}{
								"refId": "prop_13",
							},
							map[string]interface{}{
								"uuid": []interface{}{
									"yetanotheruuid",
								},
								"classId": []interface{}{
									"class_20",
								},
							},
							map[string]interface{}{
								"refId": "prop_23",
							},
							map[string]interface{}{
								"uuid": []interface{}{
									"adifferentuuid",
								},
								"classId": []interface{}{
									"class_21",
								},
								"prop_1": []interface{}{
									"Earth",
								},
							},
						},
					},
				},
			},
		}
		executor := &fakeExecutor{result: janusResponse}
		expectedResult := []interface{}{
			map[string]interface{}{
				"uuid":       "someuuid",
				"name":       "Amsterdam",
				"population": 800000,
				"InCountry": []interface{}{
					get.LocalRef{
						AtClass: "Country",
						Fields: map[string]interface{}{
							"name":       "Netherlands",
							"population": 30000000,
							"uuid":       "someotheruuid",
							"InContinent": []interface{}{
								get.LocalRef{
									AtClass: "Continent",
									Fields: map[string]interface{}{
										"uuid": "yetanotheruuid",
										"OnPlanet": []interface{}{
											get.LocalRef{
												AtClass: "Planet",
												Fields: map[string]interface{}{
													"uuid": "adifferentuuid",
													"name": "Earth",
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		}

		result, err := NewProcessor(executor, &fakeNameSource{}, schema.ClassName("City")).
			Process(gremlin.New())

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
