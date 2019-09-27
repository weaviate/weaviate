//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

// +build integrationTest

package esvector

import (
	"context"
	"testing"

	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/schema/kind"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// this test suite actually runs as part of the
// cache_multiple_reftypes_integration_test.go test suite

func testFilteringOnRefProps(repo *Repo) func(t *testing.T) {
	return func(t *testing.T) {
		t.Run("one level deep", func(t *testing.T) {
			t.Run("ref name matches", func(t *testing.T) {
				filter := filterCarParkedAtGarage(schema.DataTypeString,
					"name", filters.OperatorEqual, "Luxury Parking Garage")
				params := getParamsWithFilter("MultiRefCar", filter)

				res, err := repo.ClassSearch(context.Background(), params)
				require.Nil(t, err)
				require.Len(t, res, 2)
			})

			t.Run("ref name doesn't match", func(t *testing.T) {
				filter := filterCarParkedAtGarage(schema.DataTypeString,
					"name", filters.OperatorEqual, "There is no parking garage with this name")
				params := getParamsWithFilter("MultiRefCar", filter)

				res, err := repo.ClassSearch(context.Background(), params)
				require.Nil(t, err)
				require.Len(t, res, 0)
			})

			t.Run("within geo range", func(t *testing.T) {
				filter := filterCarParkedAtGarage(schema.DataTypeGeoCoordinates,
					"location", filters.OperatorWithinGeoRange, filters.GeoRange{
						GeoCoordinates: &models.GeoCoordinates{
							Latitude:  48.801407,
							Longitude: 2.130122,
						},
						Distance: 100000,
					})
				params := getParamsWithFilter("MultiRefCar", filter)

				res, err := repo.ClassSearch(context.Background(), params)
				require.Nil(t, err)
				require.Len(t, res, 2)

				names := extractNames(res)
				expectedNames := []string{
					"Car which is parked in a garage",
					"Car which is parked in two places at the same time (magic!)",
				}

				assert.ElementsMatch(t, names, expectedNames)
			})

			t.Run("outside of geo range", func(t *testing.T) {
				filter := filterCarParkedAtGarage(schema.DataTypeGeoCoordinates,
					"location", filters.OperatorWithinGeoRange, filters.GeoRange{
						GeoCoordinates: &models.GeoCoordinates{
							Latitude:  42.279594,
							Longitude: -83.732124,
						},
						Distance: 100000,
					})
				params := getParamsWithFilter("MultiRefCar", filter)

				res, err := repo.ClassSearch(context.Background(), params)
				require.Nil(t, err)
				require.Len(t, res, 0)
			})

			t.Run("combining ref filter with primitive root filter", func(t *testing.T) {
				parkedAtFilter := filterCarParkedAtGarage(schema.DataTypeGeoCoordinates,
					"location", filters.OperatorWithinGeoRange, filters.GeoRange{
						GeoCoordinates: &models.GeoCoordinates{
							Latitude:  48.801407,
							Longitude: 2.130122,
						},
						Distance: 100000,
					})

				filter := &filters.LocalFilter{
					Root: &filters.Clause{
						Operator: filters.OperatorAnd,
						Operands: []filters.Clause{
							*(parkedAtFilter.Root),
							filters.Clause{
								On: &filters.Path{
									Class:    schema.ClassName("MultiRefCar"),
									Property: schema.PropertyName("name"),
								},
								Value: &filters.Value{
									Value: "Car which is parked in a garage",
									Type:  schema.DataTypeString,
								},
								Operator: filters.OperatorEqual,
							},
						},
					},
				}
				params := getParamsWithFilter("MultiRefCar", filter)

				res, err := repo.ClassSearch(context.Background(), params)
				require.Nil(t, err)
				require.Len(t, res, 1)

				names := extractNames(res)
				expectedNames := []string{
					"Car which is parked in a garage",
				}

				assert.ElementsMatch(t, names, expectedNames)
			})

		})

		t.Run("multiple levels deep", func(t *testing.T) {
			t.Run("ref name matches", func(t *testing.T) {
				filter := filterDrivesCarParkedAtGarage(schema.DataTypeString,
					"name", filters.OperatorEqual, "Luxury Parking Garage")
				params := getParamsWithFilter("MultiRefDriver", filter)

				res, err := repo.ClassSearch(context.Background(), params)
				require.Nil(t, err)
				require.Len(t, res, 1)

				assert.Equal(t, "Johny Drivemuch", res[0].Schema.(map[string]interface{})["name"])
			})

			t.Run("ref name doesn't match", func(t *testing.T) {
				filter := filterDrivesCarParkedAtGarage(schema.DataTypeString,
					"name", filters.OperatorEqual, "There is no parking garage with this name")
				params := getParamsWithFilter("MultiRefDriver", filter)

				res, err := repo.ClassSearch(context.Background(), params)
				require.Nil(t, err)
				require.Len(t, res, 0)
			})

			t.Run("within geo range", func(t *testing.T) {
				filter := filterDrivesCarParkedAtGarage(schema.DataTypeGeoCoordinates,
					"location", filters.OperatorWithinGeoRange, filters.GeoRange{
						GeoCoordinates: &models.GeoCoordinates{
							Latitude:  48.801407,
							Longitude: 2.130122,
						},
						Distance: 100000,
					})
				params := getParamsWithFilter("MultiRefDriver", filter)

				res, err := repo.ClassSearch(context.Background(), params)
				require.Nil(t, err)
				require.Len(t, res, 1)

				assert.Equal(t, "Johny Drivemuch", res[0].Schema.(map[string]interface{})["name"])
			})

			t.Run("outside of geo range", func(t *testing.T) {
				filter := filterDrivesCarParkedAtGarage(schema.DataTypeGeoCoordinates,
					"location", filters.OperatorWithinGeoRange, filters.GeoRange{
						GeoCoordinates: &models.GeoCoordinates{
							Latitude:  42.279594,
							Longitude: -83.732124,
						},
						Distance: 100000,
					})
				params := getParamsWithFilter("MultiRefDriver", filter)

				res, err := repo.ClassSearch(context.Background(), params)
				require.Nil(t, err)
				require.Len(t, res, 0)
			})

		})
	}
}

func filterCarParkedAtGarage(dataType schema.DataType,
	prop string, operator filters.Operator, value interface{}) *filters.LocalFilter {
	return &filters.LocalFilter{
		Root: &filters.Clause{
			Operator: operator,
			On: &filters.Path{
				Class:    schema.ClassName("MultiRefCar"),
				Property: schema.PropertyName("parkedAt"),
				Child: &filters.Path{
					Class:    schema.ClassName("MultiRefParkingGarage"),
					Property: schema.PropertyName(prop),
				},
			},
			Value: &filters.Value{
				Value: value,
				Type:  dataType,
			},
		},
	}
}

func filterDrivesCarParkedAtGarage(dataType schema.DataType,
	prop string, operator filters.Operator, value interface{}) *filters.LocalFilter {
	return &filters.LocalFilter{
		Root: &filters.Clause{
			Operator: operator,
			On: &filters.Path{
				Class:    schema.ClassName("MultiRefDriver"),
				Property: schema.PropertyName("drives"),
				Child:    filterCarParkedAtGarage(dataType, prop, operator, value).Root.On,
			},
			Value: &filters.Value{
				Value: value,
				Type:  dataType,
			},
		},
	}
}

func getParamsWithFilter(className string, filter *filters.LocalFilter) traverser.GetParams {
	return traverser.GetParams{
		Filters: filter,
		// we don't care about actually resolving the ref as long as filtering
		// on it worked
		Properties: nil,
		Pagination: &filters.Pagination{
			Limit: 10,
		},
		ClassName: className,
		Kind:      kind.Thing,
	}
}

func extractNames(in []search.Result) []string {
	out := make([]string, len(in), len(in))
	for i, res := range in {
		out[i] = res.Schema.(map[string]interface{})["name"].(string)
	}

	return out
}
