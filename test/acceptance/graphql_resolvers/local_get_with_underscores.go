//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package test

import (
	"testing"

	"github.com/semi-technologies/weaviate/test/acceptance/helper"
	"github.com/stretchr/testify/assert"
)

func gettingObjectsWithUnderscoreProps(t *testing.T) {
	t.Run("with _interpretation set", func(t *testing.T) {
		query := `
		{
				Get {
					Things {
						Company {
						  _interpretation{
							  source {
								  concept
								}
							}
							name
						}
					}
				}
		}
		`
		result := AssertGraphQL(t, helper.RootAuth, query)
		companies := result.Get("Get", "Things", "Company").AsSlice()

		expected := []interface{}{
			map[string]interface{}{
				"name": "Microsoft Inc.",
				"_interpretation": map[string]interface{}{
					"source": []interface{}{
						map[string]interface{}{
							"concept": "microsoft",
						},
						map[string]interface{}{
							"concept": "inc",
						},
					},
				},
			},
			map[string]interface{}{
				"name": "Microsoft Incorporated",
				"_interpretation": map[string]interface{}{
					"source": []interface{}{
						map[string]interface{}{
							"concept": "microsoft",
						},
						map[string]interface{}{
							"concept": "incorporated",
						},
					},
				},
			},
			map[string]interface{}{
				"name": "Microsoft",
				"_interpretation": map[string]interface{}{
					"source": []interface{}{
						map[string]interface{}{
							"concept": "microsoft",
						},
					},
				},
			},
			map[string]interface{}{
				"name": "Apple Inc.",
				"_interpretation": map[string]interface{}{
					"source": []interface{}{
						map[string]interface{}{
							"concept": "apple",
						},
						map[string]interface{}{
							"concept": "inc",
						},
					},
				},
			},
			map[string]interface{}{
				"name": "Apple Incorporated",
				"_interpretation": map[string]interface{}{
					"source": []interface{}{
						map[string]interface{}{
							"concept": "apple",
						},
						map[string]interface{}{
							"concept": "incorporated",
						},
					},
				},
			},
			map[string]interface{}{
				"name": "Apple",
				"_interpretation": map[string]interface{}{
					"source": []interface{}{
						map[string]interface{}{
							"concept": "apple",
						},
					},
				},
			},
			map[string]interface{}{
				"name": "Google Inc.",
				"_interpretation": map[string]interface{}{
					"source": []interface{}{
						map[string]interface{}{
							"concept": "google",
						},
						map[string]interface{}{
							"concept": "inc",
						},
					},
				},
			},
			map[string]interface{}{
				"name": "Google Incorporated",
				"_interpretation": map[string]interface{}{
					"source": []interface{}{
						map[string]interface{}{
							"concept": "google",
						},
						map[string]interface{}{
							"concept": "incorporated",
						},
					},
				},
			},
			map[string]interface{}{
				"name": "Google",
				"_interpretation": map[string]interface{}{
					"source": []interface{}{
						map[string]interface{}{
							"concept": "google",
						},
					},
				},
			},
		}

		assert.ElementsMatch(t, expected, companies)
	})

	t.Run("with _nearestNeighbors set", func(t *testing.T) {
		query := `
		{
				Get {
					Things {
						Company {
						  _nearestNeighbors{
							  neighbors {
								  concept
									distance
								}
							}
							name
						}
					}
				}
		}
		`
		result := AssertGraphQL(t, helper.RootAuth, query)
		companies := result.Get("Get", "Things", "Company").AsSlice()

		extractNeighbors := func(in interface{}) []interface{} {
			return in.(map[string]interface{})["_nearestNeighbors"].(map[string]interface{})["neighbors"].([]interface{})
		}

		neighbors0 := extractNeighbors(companies[0])
		neighbors1 := extractNeighbors(companies[1])
		neighbors2 := extractNeighbors(companies[2])

		validateNeighbors(t, neighbors0, neighbors1, neighbors2)
	})

	t.Run("with _featureProjection set", func(t *testing.T) {
		query := `
		{
				Get {
					Things {
						Company {
							_featureProjection(dimensions:3){
								vector
							}
							name
						}
					}
				}
		}
		`
		result := AssertGraphQL(t, helper.RootAuth, query)
		companies := result.Get("Get", "Things", "Company").AsSlice()

		extractProjections := func(in interface{}) []interface{} {
			return in.(map[string]interface{})["_featureProjection"].(map[string]interface{})["vector"].([]interface{})
		}

		projections0 := extractProjections(companies[0])
		projections1 := extractProjections(companies[1])
		projections2 := extractProjections(companies[2])

		validateProjections(t, 3, projections0, projections1, projections2)
	})
}

func validateNeighbors(t *testing.T, neighborsGroups ...[]interface{}) {
	for i, group := range neighborsGroups {
		if len(group) == 0 {
			t.Fatalf("group %d: length of neighbors is 0", i)
		}

		for j, neighbor := range group {
			asMap := neighbor.(map[string]interface{})
			if len(asMap["concept"].(string)) == 0 {
				t.Fatalf("group %d: element %d: concept has length 0", i, j)
			}
		}
	}
}

func validateProjections(t *testing.T, dims int, vectors ...[]interface{}) {
	for _, vector := range vectors {
		if len(vector) != dims {
			t.Fatalf("expected feature projection vector to have length 3, got: %d", len(vector))
		}
	}
}
