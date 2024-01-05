//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package test

import (
	"fmt"
	"testing"

	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

// This test prevents a regression on
// https://github.com/weaviate/weaviate/issues/1410
func TestMultipleRefTypeIssues(t *testing.T) {
	className := func(suffix string) string {
		return "MultiRefTypeBug" + suffix
	}
	defer deleteObjectClass(t, className("TargetOne"))
	defer deleteObjectClass(t, className("TargetTwo"))
	defer deleteObjectClass(t, className("Source"))

	const (
		targetOneID strfmt.UUID = "155c5914-6594-4cde-b3ab-f8570b561965"
		targetTwoID strfmt.UUID = "ebf85a07-6b34-4e3b-b7c5-077f904fc955"
	)

	t.Run("import schema", func(t *testing.T) {
		createObjectClass(t, &models.Class{
			Class: className("TargetOne"),
			Properties: []*models.Property{
				{
					Name:     "name",
					DataType: []string{"text"},
				},
			},
		})

		createObjectClass(t, &models.Class{
			Class: className("TargetTwo"),
			Properties: []*models.Property{
				{
					Name:     "name",
					DataType: []string{"text"},
				},
			},
		})

		createObjectClass(t, &models.Class{
			Class: className("Source"),
			Properties: []*models.Property{
				{
					Name:     "name",
					DataType: []string{"text"},
				},
				{
					Name:     "toTargets",
					DataType: []string{className("TargetOne"), className("TargetTwo")},
				},
			},
		})
	})

	t.Run("import data", func(t *testing.T) {
		createObject(t, &models.Object{
			Class: className("TargetOne"),
			ID:    targetOneID,
			Properties: map[string]interface{}{
				"name": "target a",
			},
		})

		createObject(t, &models.Object{
			Class: className("TargetTwo"),
			ID:    targetTwoID,
			Properties: map[string]interface{}{
				"name": "target b",
			},
		})

		createObject(t, &models.Object{
			Class: className("Source"),
			Properties: map[string]interface{}{
				"name": "source without refs",
			},
		})

		createObject(t, &models.Object{
			Class: className("Source"),
			Properties: map[string]interface{}{
				"name": "source with ref to One",
				"toTargets": []interface{}{
					map[string]interface{}{
						"beacon": fmt.Sprintf("weaviate://localhost/%s", targetOneID),
					},
				},
			},
		})

		createObject(t, &models.Object{
			Class: className("Source"),
			Properties: map[string]interface{}{
				"name": "source with ref to Two",
				"toTargets": []interface{}{
					map[string]interface{}{
						"beacon": fmt.Sprintf("weaviate://localhost/%s", targetTwoID),
					},
				},
			},
		})

		createObject(t, &models.Object{
			Class: className("Source"),
			Properties: map[string]interface{}{
				"name": "source with ref to both",
				"toTargets": []interface{}{
					map[string]interface{}{
						"beacon": fmt.Sprintf("weaviate://localhost/%s", targetOneID),
					},
					map[string]interface{}{
						"beacon": fmt.Sprintf("weaviate://localhost/%s", targetTwoID),
					},
				},
			},
		})
	})

	t.Run("verify different scenarios through GraphQL", func(t *testing.T) {
		t.Run("requesting no references", func(t *testing.T) {
			query := fmt.Sprintf(`
		{
			Get {
				%s {
					name
				}
			}
		}
		`, className("Source"))
			result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
			actual := result.Get("Get", className("Source")).AsSlice()
			expected := []interface{}{
				map[string]interface{}{"name": "source with ref to One"},
				map[string]interface{}{"name": "source with ref to Two"},
				map[string]interface{}{"name": "source with ref to both"},
				map[string]interface{}{"name": "source without refs"},
			}

			assert.ElementsMatch(t, expected, actual)
		})

		t.Run("requesting references of type One without additional { id }", func(t *testing.T) {
			query := fmt.Sprintf(`
		{
			Get {
				%s {
					name
					toTargets {
					  ... on %s {
						  name
						}
					}
				}
			}
		}
		`, className("Source"), className("TargetOne"))
			result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
			actual := result.Get("Get", className("Source")).AsSlice()
			expected := []interface{}{
				map[string]interface{}{
					"name": "source with ref to One",
					"toTargets": []interface{}{
						map[string]interface{}{
							"name": "target a",
						},
					},
				},
				map[string]interface{}{
					"name":      "source with ref to Two",
					"toTargets": nil,
				},
				map[string]interface{}{
					"name": "source with ref to both",
					"toTargets": []interface{}{
						map[string]interface{}{
							"name": "target a",
						},
					},
				},
				map[string]interface{}{
					"name":      "source without refs",
					"toTargets": nil,
				},
			}

			assert.ElementsMatch(t, expected, actual)
		})

		t.Run("requesting references of type One with additional { id }", func(t *testing.T) {
			query := fmt.Sprintf(`
		{
			Get {
				%s {
					name
					toTargets {
					  ... on %s {
						  name
							_additional { id }
						}
					}
				}
			}
		}
		`, className("Source"), className("TargetOne"))
			result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
			actual := result.Get("Get", className("Source")).AsSlice()
			expected := []interface{}{
				map[string]interface{}{
					"name": "source with ref to One",
					"toTargets": []interface{}{
						map[string]interface{}{
							"name": "target a",
							"_additional": map[string]interface{}{
								"id": targetOneID.String(),
							},
						},
					},
				},
				map[string]interface{}{
					"name":      "source with ref to Two",
					"toTargets": nil,
				},
				map[string]interface{}{
					"name": "source with ref to both",
					"toTargets": []interface{}{
						map[string]interface{}{
							"name": "target a",
							"_additional": map[string]interface{}{
								"id": targetOneID.String(),
							},
						},
					},
				},
				map[string]interface{}{
					"name":      "source without refs",
					"toTargets": nil,
				},
			}

			assert.ElementsMatch(t, expected, actual)
		})

		t.Run("requesting references of type Two without additional { id }", func(t *testing.T) {
			query := fmt.Sprintf(`
		{
			Get {
				%s {
					name
					toTargets {
					  ... on %s {
						  name
						}
					}
				}
			}
		}
		`, className("Source"), className("TargetTwo"))
			result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
			actual := result.Get("Get", className("Source")).AsSlice()
			expected := []interface{}{
				map[string]interface{}{
					"name": "source with ref to Two",
					"toTargets": []interface{}{
						map[string]interface{}{
							"name": "target b",
						},
					},
				},
				map[string]interface{}{
					"name":      "source with ref to One",
					"toTargets": nil,
				},
				map[string]interface{}{
					"name": "source with ref to both",
					"toTargets": []interface{}{
						map[string]interface{}{
							"name": "target b",
						},
					},
				},
				map[string]interface{}{
					"name":      "source without refs",
					"toTargets": nil,
				},
			}

			assert.ElementsMatch(t, expected, actual)
		})

		t.Run("requesting references of type Two with additional { id }", func(t *testing.T) {
			query := fmt.Sprintf(`
		{
			Get {
				%s {
					name
					toTargets {
					  ... on %s {
						  name
							_additional { id }
						}
					}
				}
			}
		}
		`, className("Source"), className("TargetTwo"))
			result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
			actual := result.Get("Get", className("Source")).AsSlice()
			expected := []interface{}{
				map[string]interface{}{
					"name": "source with ref to Two",
					"toTargets": []interface{}{
						map[string]interface{}{
							"name": "target b",
							"_additional": map[string]interface{}{
								"id": targetTwoID.String(),
							},
						},
					},
				},
				map[string]interface{}{
					"name":      "source with ref to One",
					"toTargets": nil,
				},
				map[string]interface{}{
					"name": "source with ref to both",
					"toTargets": []interface{}{
						map[string]interface{}{
							"name": "target b",
							"_additional": map[string]interface{}{
								"id": targetTwoID.String(),
							},
						},
					},
				},
				map[string]interface{}{
					"name":      "source without refs",
					"toTargets": nil,
				},
			}

			assert.ElementsMatch(t, expected, actual)
		})

		t.Run("requesting references of both types without additional { id }",
			func(t *testing.T) {
				query := fmt.Sprintf(`
		{
			Get {
				%s {
					name
					toTargets {
					  ... on %s {
						  name
						}
					  ... on %s {
						  name
						}
					}
				}
			}
		}
		`, className("Source"), className("TargetOne"), className("TargetTwo"))
				result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
				actual := result.Get("Get", className("Source")).AsSlice()
				expected := []interface{}{
					map[string]interface{}{
						"name": "source with ref to Two",
						"toTargets": []interface{}{
							map[string]interface{}{
								"name": "target b",
							},
						},
					},
					map[string]interface{}{
						"name": "source with ref to One",
						"toTargets": []interface{}{
							map[string]interface{}{
								"name": "target a",
							},
						},
					},
					map[string]interface{}{
						"name": "source with ref to both",
						"toTargets": []interface{}{
							map[string]interface{}{
								"name": "target a",
							},
							map[string]interface{}{
								"name": "target b",
							},
						},
					},
					map[string]interface{}{
						"name":      "source without refs",
						"toTargets": nil,
					},
				}

				assert.ElementsMatch(t, expected, actual)
			})

		t.Run("requesting references of type Two with additional { id }", func(t *testing.T) {
			query := fmt.Sprintf(`
		{
			Get {
				%s {
					name
					toTargets {
					  ... on %s {
						  name
							_additional { id }
						}
					  ... on %s {
						  name
							_additional { id }
						}
					}
				}
			}
		}
		`, className("Source"), className("TargetOne"), className("TargetTwo"))
			result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
			actual := result.Get("Get", className("Source")).AsSlice()
			expected := []interface{}{
				map[string]interface{}{
					"name": "source with ref to Two",
					"toTargets": []interface{}{
						map[string]interface{}{
							"name": "target b",
							"_additional": map[string]interface{}{
								"id": targetTwoID.String(),
							},
						},
					},
				},
				map[string]interface{}{
					"name": "source with ref to One",
					"toTargets": []interface{}{
						map[string]interface{}{
							"name": "target a",
							"_additional": map[string]interface{}{
								"id": targetOneID.String(),
							},
						},
					},
				},
				map[string]interface{}{
					"name": "source with ref to both",
					"toTargets": []interface{}{
						map[string]interface{}{
							"name": "target a",
							"_additional": map[string]interface{}{
								"id": targetOneID.String(),
							},
						},
						map[string]interface{}{
							"name": "target b",
							"_additional": map[string]interface{}{
								"id": targetTwoID.String(),
							},
						},
					},
				},
				map[string]interface{}{
					"name":      "source without refs",
					"toTargets": nil,
				},
			}

			assert.ElementsMatch(t, expected, actual)
		})
	})

	t.Run("cleanup", func(t *testing.T) {
		deleteObjectClass(t, className("Source"))
		deleteObjectClass(t, className("TargetOne"))
		deleteObjectClass(t, className("TargetTwo"))
	})
}
