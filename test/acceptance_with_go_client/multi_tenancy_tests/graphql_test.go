//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package multi_tenancy_tests

import (
	"context"
	"testing"

	"acceptance_tests_with_client/fixtures"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wvt "github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/filters"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
)

func TestGraphQL_MultiTenancy(t *testing.T) {
	client, err := wvt.NewClient(wvt.Config{Scheme: "http", Host: "localhost:8080"})
	require.Nil(t, err)

	cleanup := func() {
		err := client.Schema().AllDeleter().Do(context.Background())
		require.Nil(t, err)
	}

	t.Run("GraphQL Get", func(t *testing.T) {
		defer cleanup()

		tenant1 := "tenantNo1"
		tenant2 := "tenantNo2"

		assertGetContainsIds := func(t *testing.T, response *models.GraphQLResponse,
			className string, expectedIds []string,
		) {
			require.NotNil(t, response)
			assert.Nil(t, response.Errors)
			require.NotNil(t, response.Data)

			get := response.Data["Get"].(map[string]interface{})
			objects := get[className].([]interface{})
			require.Len(t, objects, len(expectedIds))

			ids := []string{}
			for i := range objects {
				ids = append(ids, objects[i].(map[string]interface{})["_additional"].(map[string]interface{})["id"].(string))
			}
			assert.ElementsMatch(t, expectedIds, ids)
		}

		t.Run("add data", func(t *testing.T) {
			fixtures.CreateSchemaPizzaForTenants(t, client)
			fixtures.CreateTenantsPizza(t, client, tenant1, tenant2)
			fixtures.CreateDataPizzaQuattroFormaggiForTenants(t, client, tenant1)
			fixtures.CreateDataPizzaFruttiDiMareForTenants(t, client, tenant1)
			fixtures.CreateDataPizzaHawaiiForTenants(t, client, tenant2)
			fixtures.CreateDataPizzaDoenerForTenants(t, client, tenant2)
		})

		t.Run("get all data for tenant", func(t *testing.T) {
			expectedIdsByTenant := map[string][]string{
				tenant1: {
					fixtures.PIZZA_QUATTRO_FORMAGGI_ID,
					fixtures.PIZZA_FRUTTI_DI_MARE_ID,
				},
				tenant2: {
					fixtures.PIZZA_HAWAII_ID,
					fixtures.PIZZA_DOENER_ID,
				},
			}

			for tenant, expectedIds := range expectedIdsByTenant {
				resp, err := client.GraphQL().Get().
					WithClassName("Pizza").
					WithTenant(tenant).
					WithFields(graphql.Field{
						Name:   "_additional",
						Fields: []graphql.Field{{Name: "id"}},
					}).
					Do(context.Background())

				assert.Nil(t, err)
				assertGetContainsIds(t, resp, "Pizza", expectedIds)
			}
		})

		t.Run("get limited data for tenant", func(t *testing.T) {
			expectedIdsByTenant := map[string][]string{
				tenant1: {
					fixtures.PIZZA_QUATTRO_FORMAGGI_ID,
				},
				tenant2: {
					fixtures.PIZZA_HAWAII_ID,
				},
			}

			for tenant, expectedIds := range expectedIdsByTenant {
				resp, err := client.GraphQL().Get().
					WithClassName("Pizza").
					WithTenant(tenant).
					WithLimit(1).
					WithFields(graphql.Field{
						Name:   "_additional",
						Fields: []graphql.Field{{Name: "id"}},
					}).
					Do(context.Background())

				assert.Nil(t, err)
				assertGetContainsIds(t, resp, "Pizza", expectedIds)
			}
		})

		t.Run("get filtered data for tenant", func(t *testing.T) {
			expectedIdsByTenant := map[string][]string{
				tenant1: {},
				tenant2: {
					fixtures.PIZZA_DOENER_ID,
				},
			}
			where := filters.Where().
				WithPath([]string{"price"}).
				WithOperator(filters.GreaterThan).
				WithValueNumber(1.3)

			for tenant, expectedIds := range expectedIdsByTenant {
				resp, err := client.GraphQL().Get().
					WithClassName("Pizza").
					WithTenant(tenant).
					WithWhere(where).
					WithFields(graphql.Field{
						Name:   "_additional",
						Fields: []graphql.Field{{Name: "id"}},
					}).
					Do(context.Background())

				assert.Nil(t, err)
				assertGetContainsIds(t, resp, "Pizza", expectedIds)
			}
		})
	})

	t.Run("GraphQL Aggregate", func(t *testing.T) {
		defer cleanup()

		tenant1 := "tenantNo1"
		tenant2 := "tenantNo2"

		assertAggregateNumFieldHasValues := func(t *testing.T, response *models.GraphQLResponse,
			className string, fieldName string, expectedAggValues map[string]*float64,
		) {
			require.NotNil(t, response)
			assert.Nil(t, response.Errors)
			require.NotNil(t, response.Data)

			agg := response.Data["Aggregate"].(map[string]interface{})
			objects := agg[className].([]interface{})
			require.Len(t, objects, 1)
			obj := objects[0].(map[string]interface{})[fieldName].(map[string]interface{})

			for name, value := range expectedAggValues {
				if value == nil {
					assert.Nil(t, obj[name])
				} else {
					assert.Equal(t, *value, obj[name])
				}
			}
		}
		ptr := func(f float64) *float64 {
			return &f
		}

		t.Run("add data", func(t *testing.T) {
			fixtures.CreateSchemaPizzaForTenants(t, client)
			fixtures.CreateTenantsPizza(t, client, tenant1, tenant2)
			fixtures.CreateDataPizzaQuattroFormaggiForTenants(t, client, tenant1)
			fixtures.CreateDataPizzaFruttiDiMareForTenants(t, client, tenant1)
			fixtures.CreateDataPizzaHawaiiForTenants(t, client, tenant2)
			fixtures.CreateDataPizzaDoenerForTenants(t, client, tenant2)
		})

		t.Run("aggregate all data for tenant", func(t *testing.T) {
			expectedAggValuesByTenant := map[string]map[string]*float64{
				tenant1: {
					"count":   ptr(2),
					"maximum": ptr(1.2),
					"minimum": ptr(1.1),
					"median":  ptr(1.15),
					"mean":    ptr(1.15),
					"mode":    ptr(1.1),
					"sum":     ptr(2.3),
				},
				tenant2: {
					"count":   ptr(2),
					"maximum": ptr(1.4),
					"minimum": ptr(1.3),
					"median":  ptr(1.35),
					"mean":    ptr(1.35),
					"mode":    ptr(1.3),
					"sum":     ptr(2.7),
				},
			}

			for tenant, expectedAggValues := range expectedAggValuesByTenant {
				resp, err := client.GraphQL().Aggregate().
					WithClassName("Pizza").
					WithTenant(tenant).
					WithFields(graphql.Field{
						Name: "price",
						Fields: []graphql.Field{
							{Name: "count"},
							{Name: "maximum"},
							{Name: "minimum"},
							{Name: "median"},
							{Name: "mean"},
							{Name: "mode"},
							{Name: "sum"},
						},
					}).
					Do(context.Background())

				assert.Nil(t, err)
				assertAggregateNumFieldHasValues(t, resp, "Pizza", "price", expectedAggValues)
			}
		})

		t.Run("aggregate filtered data for tenant", func(t *testing.T) {
			expectedAggValuesByTenant := map[string]map[string]*float64{
				tenant1: {
					"count":   ptr(0),
					"maximum": nil,
					"minimum": nil,
					"median":  nil,
					"mean":    nil,
					"mode":    nil,
					"sum":     nil,
				},
				tenant2: {
					"count":   ptr(1),
					"maximum": ptr(1.4),
					"minimum": ptr(1.4),
					"median":  ptr(1.4),
					"mean":    ptr(1.4),
					"mode":    ptr(1.4),
					"sum":     ptr(1.4),
				},
			}
			where := filters.Where().
				WithPath([]string{"price"}).
				WithOperator(filters.GreaterThan).
				WithValueNumber(1.3)

			for tenant, expectedAggValues := range expectedAggValuesByTenant {
				resp, err := client.GraphQL().Aggregate().
					WithClassName("Pizza").
					WithTenant(tenant).
					WithWhere(where).
					WithFields(graphql.Field{
						Name: "price",
						Fields: []graphql.Field{
							{Name: "count"},
							{Name: "maximum"},
							{Name: "minimum"},
							{Name: "median"},
							{Name: "mean"},
							{Name: "mode"},
							{Name: "sum"},
						},
					}).
					Do(context.Background())

				assert.Nil(t, err)
				assertAggregateNumFieldHasValues(t, resp, "Pizza", "price", expectedAggValues)
			}
		})
	})

	t.Run("GraphQL Explore", func(t *testing.T) {
		defer cleanup()

		tenant1 := "tenantNo1"
		tenant2 := "tenantNo2"

		assertExploreContainsErrors := func(t *testing.T, response *models.GraphQLResponse,
			expectedErrorMessage string,
		) {
			require.NotNil(t, response)
			require.NotNil(t, response.Errors)
			require.Nil(t, response.Data["Explore"])
			require.NotNil(t, response.Data)
			require.Len(t, response.Errors, 1)
			assert.NotEmpty(t, response.Errors[0].Message)
			assert.Equal(t, expectedErrorMessage, response.Errors[0].Message)
		}

		t.Run("add data", func(t *testing.T) {
			fixtures.CreateSchemaPizzaForTenants(t, client)
			fixtures.CreateTenantsPizza(t, client, tenant1, tenant2)
			fixtures.CreateDataPizzaQuattroFormaggiForTenants(t, client, tenant1)
			fixtures.CreateDataPizzaFruttiDiMareForTenants(t, client, tenant1)
			fixtures.CreateDataPizzaHawaiiForTenants(t, client, tenant2)
			fixtures.CreateDataPizzaDoenerForTenants(t, client, tenant2)
		})

		t.Run("explore with nearText", func(t *testing.T) {
			nearText := client.GraphQL().NearTextArgBuilder().
				WithConcepts([]string{"Italian"})

			resp, err := client.GraphQL().Explore().
				WithNearText(nearText).
				WithFields(graphql.Beacon, graphql.Certainty, graphql.ClassName).
				Do(context.Background())

			require.Nil(t, err)
			assertExploreContainsErrors(t, resp,
				"vector search: search index pizza: class Pizza has multi-tenancy enabled, but request was without tenant",
			)
		})
	})
}
