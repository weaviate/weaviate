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

package test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

func Test_Text2Vec_GraphQLAdditionalProps(t *testing.T) {
	helper.SetupClient(os.Getenv(weaviateNode1Endpoint))

	skipPropertyName := map[string]interface{}{
		"text2vec-contextionary": map[string]interface{}{"vectorizePropertyName": false},
	}
	companyClass := &models.Class{
		Class:      "Company",
		Vectorizer: "text2vec-contextionary",
		ModuleConfig: map[string]interface{}{
			"text2vec-contextionary": map[string]interface{}{"vectorizeClassName": false},
		},
		Properties: []*models.Property{
			{
				Name:         "name",
				DataType:     schema.DataTypeText.PropString(),
				Tokenization: models.PropertyTokenizationWhitespace,
				ModuleConfig: skipPropertyName,
			},
		},
	}
	helper.CreateClass(t, companyClass)
	defer helper.DeleteClass(t, companyClass.Class)

	names := []string{
		"Microsoft Inc.", "Microsoft Incorporated", "Microsoft",
		"Apple Inc.", "Apple Incorporated", "Apple",
		"Google Inc.", "Google Incorporated", "Google",
	}
	for i, name := range names {
		obj := &models.Object{
			Class:      companyClass.Class,
			ID:         helper.IntToUUID(uint64(i + 1)),
			Properties: map[string]interface{}{"name": name},
		}
		helper.CreateObject(t, obj)
		helper.AssertGetObjectEventually(t, obj.Class, obj.ID)
	}

	t.Run("with interpretation set", func(t *testing.T) {
		query := `
		{
			Get {
				Company {
					_additional {
						interpretation{
							source {
								concept
							}
						}
					}
					name
				}
			}
		}
		`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		companies := result.Get("Get", "Company").AsSlice()

		expected := []interface{}{
			map[string]interface{}{
				"name": "Microsoft Inc.",
				"_additional": map[string]interface{}{
					"interpretation": map[string]interface{}{
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
			},
			map[string]interface{}{
				"name": "Microsoft Incorporated",
				"_additional": map[string]interface{}{
					"interpretation": map[string]interface{}{
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
			},
			map[string]interface{}{
				"name": "Microsoft",
				"_additional": map[string]interface{}{
					"interpretation": map[string]interface{}{
						"source": []interface{}{
							map[string]interface{}{
								"concept": "microsoft",
							},
						},
					},
				},
			},
			map[string]interface{}{
				"name": "Apple Inc.",
				"_additional": map[string]interface{}{
					"interpretation": map[string]interface{}{
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
			},
			map[string]interface{}{
				"name": "Apple Incorporated",
				"_additional": map[string]interface{}{
					"interpretation": map[string]interface{}{
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
			},
			map[string]interface{}{
				"name": "Apple",
				"_additional": map[string]interface{}{
					"interpretation": map[string]interface{}{
						"source": []interface{}{
							map[string]interface{}{
								"concept": "apple",
							},
						},
					},
				},
			},
			map[string]interface{}{
				"name": "Google Inc.",
				"_additional": map[string]interface{}{
					"interpretation": map[string]interface{}{
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
			},
			map[string]interface{}{
				"name": "Google Incorporated",
				"_additional": map[string]interface{}{
					"interpretation": map[string]interface{}{
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
			},
			map[string]interface{}{
				"name": "Google",
				"_additional": map[string]interface{}{
					"interpretation": map[string]interface{}{
						"source": []interface{}{
							map[string]interface{}{
								"concept": "google",
							},
						},
					},
				},
			},
		}

		assert.ElementsMatch(t, expected, companies)
	})

	t.Run("with _additional nearestNeighbors set", func(t *testing.T) {
		query := `
		{
			Get {
				Company {
					_additional {
						nearestNeighbors{
							neighbors {
								concept
								distance
							}
						}
					}
					name
				}
			}
		}
		`
		result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
		companies := result.Get("Get", "Company").AsSlice()

		extractNeighbors := func(in interface{}) []interface{} {
			return in.(map[string]interface{})["_additional"].(map[string]interface{})["nearestNeighbors"].(map[string]interface{})["neighbors"].([]interface{})
		}

		neighbors0 := extractNeighbors(companies[0])
		neighbors1 := extractNeighbors(companies[1])
		neighbors2 := extractNeighbors(companies[2])

		validateNeighbors(t, neighbors0, neighbors1, neighbors2)
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
