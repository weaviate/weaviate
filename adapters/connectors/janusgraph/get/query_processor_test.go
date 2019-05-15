/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */
package get

import (
	"context"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/adapters/connectors/janusgraph/gremlin"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/local/get"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/usecases/network/crossrefs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	uuid1 = "b1330042-9667-4cf5-8236-40d625892feb"
	uuid2 = "a2bd5b33-6b02-467a-bc9c-cd240819b9af"
	uuid3 = "c86e91c5-6f6e-43b0-9dc8-f93728813407"
	uuid4 = "a026ab3e-db31-4da0-a855-de2afc2e963f"
	uuid5 = "69a76eba-ea37-4e31-ab27-6896d825098d"
	uuid6 = "abe6e9a7-14a3-4561-b031-35084508e3b0"
	uuid7 = "4a9061de-7685-4374-9563-85876f88bf29"
	uuid8 = "c45833bb-07a8-4bed-be02-c71bbcc16ddf"
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
									uuid1,
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
								"prop_9": []interface{}{
									map[string]interface{}{
										"type": "Point",
										// WARNING: Although all create queries in Janusgraph
										// always take the coordinates in the form of latitude,
										// longitude, they are actually stored (and therefore
										// returned) in the oppposite order! So this array is
										// longitude, latitude!
										"coordinates": []interface{}{float64(0.6), float64(0.5)},
									},
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
									uuid2,
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
				"uuid":       uuid1,
				"name":       "Amsterdam",
				"population": 800000,
				"location":   &models.GeoCoordinates{Latitude: 0.5, Longitude: 0.6},
			},
			map[string]interface{}{
				"uuid":       uuid2,
				"name":       "Dusseldorf",
				"population": 600000,
			},
		}

		result, err := NewProcessor(executor, &fakeNameSource{}, schema.ClassName("City")).
			Process(context.Background(), gremlin.New())

		require.Nil(t, err, "should not error")
		assert.ElementsMatch(t, expectedResult, result, "result should be merged and post-processed")
	})

	t.Run("single result, cross-ref one level deep", func(t *testing.T) {
		janusResponse := &gremlin.Response{
			Data: []gremlin.Datum{
				gremlin.Datum{
					Datum: map[string]interface{}{
						"objects": []interface{}{
							map[string]interface{}{
								"uuid": []interface{}{
									uuid1,
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
								"refId":       "prop_3",
								"$cref":       uuid2,
								"locationUrl": "localhost",
								"refType":     "thing",
							},
							map[string]interface{}{
								"uuid": []interface{}{
									uuid2,
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
				"uuid":       uuid1,
				"name":       "Amsterdam",
				"population": 800000,
				"InCountry": []interface{}{
					get.LocalRef{
						Fields: map[string]interface{}{
							"name":       "Netherlands",
							"population": 30000000,
							"uuid":       uuid2,
						},
						Class: "Country",
					},
				},
			},
		}

		result, err := NewProcessor(executor, &fakeNameSource{}, schema.ClassName("City")).
			Process(context.Background(), gremlin.New())

		require.Nil(t, err, "should not error")
		assert.ElementsMatch(t, expectedResult, result, "result should be merged and post-processed")
	})

	t.Run("two cross-refs for one class, cross-ref one level deep", func(t *testing.T) {
		janusResponse := &gremlin.Response{
			Data: []gremlin.Datum{
				gremlin.Datum{
					Datum: map[string]interface{}{
						"objects": []interface{}{
							map[string]interface{}{
								"uuid": []interface{}{
									uuid1,
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
								"refId":       "prop_3",
								"$cref":       uuid2,
								"locationUrl": "localhost",
								"refType":     "thing",
							},
							map[string]interface{}{
								"uuid": []interface{}{
									uuid2,
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
				gremlin.Datum{
					Datum: map[string]interface{}{
						"objects": []interface{}{
							map[string]interface{}{
								"uuid": []interface{}{
									uuid1,
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
								"refId":       "prop_3",
								"$cref":       uuid3,
								"locationUrl": "localhost",
								"refType":     "thing",
							},
							map[string]interface{}{
								"uuid": []interface{}{
									uuid3,
								},
								"classId": []interface{}{
									"class_19",
								},
								"prop_1": []interface{}{
									"Holland",
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
		expectedInnerRefs := []interface{}{
			get.LocalRef{
				Fields: map[string]interface{}{
					"name":       "Netherlands",
					"population": 30000000,
					"uuid":       uuid2,
				},
				Class: "Country",
			},
			get.LocalRef{
				Fields: map[string]interface{}{
					"name":       "Holland",
					"population": 30000000,
					"uuid":       uuid3,
				},
				Class: "Country",
			},
		}

		result, err := NewProcessor(executor, &fakeNameSource{}, schema.ClassName("City")).
			Process(context.Background(), gremlin.New())
		require.Nil(t, err, "should not error")

		subResult, err := extractResult(result, []interface{}{0, "InCountry"})
		require.Nil(t, err, "should not error")

		assert.ElementsMatch(t, expectedInnerRefs, subResult, "result should be merged and post-processed")
	})

	t.Run("single result, cross ref is a network ref", func(t *testing.T) {
		janusResponse := &gremlin.Response{
			Data: []gremlin.Datum{
				gremlin.Datum{
					Datum: map[string]interface{}{
						"objects": []interface{}{
							map[string]interface{}{
								"uuid": []interface{}{
									uuid1,
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
								"refId":       "prop_3",
								"$cref":       uuid3,
								"locationUrl": "other-peer",
								"refType":     "thing",
							},
							map[string]interface{}{
								// does not matter on cross-refs
							},
						},
					},
				},
			},
		}
		executor := &fakeExecutor{result: janusResponse}
		expectedResult := []interface{}{
			map[string]interface{}{
				"uuid":       uuid1,
				"name":       "Amsterdam",
				"population": 800000,
				"InCountry": []interface{}{
					get.NetworkRef{
						NetworkKind: crossrefs.NetworkKind{
							PeerName: "other-peer",
							ID:       strfmt.UUID(uuid3),
							Kind:     kind.Thing,
						},
					},
				},
			},
		}

		result, err := NewProcessor(executor, &fakeNameSource{}, schema.ClassName("City")).
			Process(context.Background(), gremlin.New())

		require.Nil(t, err, "should not error")
		assert.ElementsMatch(t, expectedResult, result, "result should be merged and post-processed")
	})

	t.Run("single result, cross-refs several levels deep", func(t *testing.T) {
		janusResponse := &gremlin.Response{
			Data: []gremlin.Datum{
				gremlin.Datum{
					Datum: map[string]interface{}{
						"objects": []interface{}{
							map[string]interface{}{
								"uuid": []interface{}{
									uuid1,
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
								"refId":       "prop_3",
								"$cref":       uuid2,
								"refType":     "thing",
								"locationUrl": "localhost",
							},
							map[string]interface{}{
								"uuid": []interface{}{
									uuid2,
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
								"refId":       "prop_13",
								"$cref":       uuid3,
								"refType":     "thing",
								"locationUrl": "localhost",
							},
							map[string]interface{}{
								"uuid": []interface{}{
									uuid3,
								},
								"classId": []interface{}{
									"class_20",
								},
							},
							map[string]interface{}{
								"refId":       "prop_23",
								"$cref":       uuid4,
								"refType":     "thing",
								"locationUrl": "localhost",
							},
							map[string]interface{}{
								"uuid": []interface{}{
									uuid4,
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
				"uuid":       uuid1,
				"name":       "Amsterdam",
				"population": 800000,
				"InCountry": []interface{}{
					get.LocalRef{
						Class: "Country",
						Fields: map[string]interface{}{
							"name":       "Netherlands",
							"population": 30000000,
							"uuid":       uuid2,
							"InContinent": []interface{}{
								get.LocalRef{
									Class: "Continent",
									Fields: map[string]interface{}{
										"uuid": uuid3,
										"OnPlanet": []interface{}{
											get.LocalRef{
												Class: "Planet",
												Fields: map[string]interface{}{
													"uuid": uuid4,
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
			Process(context.Background(), gremlin.New())

		require.Nil(t, err, "should not error")
		assert.ElementsMatch(t, expectedResult, result, "result should be merged and post-processed")
	})

	t.Run("multiple results for a nested cross-ref", func(t *testing.T) {
		janusResponse := &gremlin.Response{
			Data: []gremlin.Datum{
				gremlin.Datum{
					Datum: map[string]interface{}{
						"objects": []interface{}{
							map[string]interface{}{
								"uuid": []interface{}{
									uuid1,
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
								"refId":       "prop_3",
								"$cref":       uuid2,
								"refType":     "thing",
								"locationUrl": "localhost",
							},
							map[string]interface{}{
								"uuid": []interface{}{
									uuid2,
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
								"refId":       "prop_13",
								"$cref":       uuid3,
								"refType":     "thing",
								"locationUrl": "localhost",
							},
							map[string]interface{}{
								"uuid": []interface{}{
									uuid3,
								},
								"classId": []interface{}{
									"class_20",
								},
							},
							map[string]interface{}{
								"refId":       "prop_23",
								"$cref":       uuid4,
								"refType":     "thing",
								"locationUrl": "localhost",
							},
							map[string]interface{}{
								"uuid": []interface{}{
									uuid4,
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

				gremlin.Datum{
					Datum: map[string]interface{}{
						"objects": []interface{}{
							map[string]interface{}{
								"uuid": []interface{}{
									uuid1,
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
								"refId":       "prop_3",
								"$cref":       uuid2,
								"refType":     "thing",
								"locationUrl": "localhost",
							},
							map[string]interface{}{
								"uuid": []interface{}{
									uuid2,
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
								"refId":       "prop_13",
								"$cref":       uuid3,
								"refType":     "thing",
								"locationUrl": "localhost",
							},
							map[string]interface{}{
								"uuid": []interface{}{
									uuid3,
								},
								"classId": []interface{}{
									"class_20",
								},
							},
							map[string]interface{}{
								"refId":       "prop_23",
								"$cref":       uuid5,
								"refType":     "thing",
								"locationUrl": "localhost",
							},
							map[string]interface{}{
								"uuid": []interface{}{
									uuid5,
								},
								"classId": []interface{}{
									"class_21",
								},
								"prop_1": []interface{}{
									"FlatEarth",
								},
							},
						},
					},
				},
			},
		}
		executor := &fakeExecutor{result: janusResponse}

		innerMostRefs := []interface{}{
			get.LocalRef{
				Class: "Planet",
				Fields: map[string]interface{}{
					"uuid": uuid4,
					"name": "Earth",
				},
			},
			get.LocalRef{
				Class: "Planet",
				Fields: map[string]interface{}{
					"uuid": uuid5,
					"name": "FlatEarth",
				},
			},
		}

		result, err := NewProcessor(executor, &fakeNameSource{}, schema.ClassName("City")).
			Process(context.Background(), gremlin.New())
		require.Nil(t, err, "should not error")

		subResult, err := extractResult(result,
			[]interface{}{0, "InCountry", 0, "Fields", "InContinent", 0, "Fields", "OnPlanet"})
		require.Nil(t, err, "should not error")

		assert.ElementsMatch(t, innerMostRefs, subResult, "result should be merged and post-processed")
	})
}

type fakeExecutor struct {
	result *gremlin.Response
}

func (f *fakeExecutor) Execute(ctx context.Context, query gremlin.Gremlin) (*gremlin.Response, error) {
	return f.result, nil
}
