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

package aggregate

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/tailor-platform/graphql/gqlerrors"

	testhelper "github.com/weaviate/weaviate/adapters/handlers/graphql/test/helper"
	"github.com/weaviate/weaviate/entities/aggregation"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	"github.com/weaviate/weaviate/usecases/config"
)

// carSchemaWithMTAlias is CarSchema (CarAlias -> Car) plus a multi-tenant
// collection MTCar behind MTCarAlias.
func carSchemaWithMTAlias() *schema.SchemaWithAliases {
	classes := append([]*models.Class{}, testhelper.CarSchema.Objects.Classes...)
	classes = append(classes, &models.Class{
		Class:              "MTCar",
		Properties:         []*models.Property{{Name: "horsepower", DataType: schema.DataTypeInt.PropString()}},
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
	})
	aliases := map[string]string{"MTCarAlias": "MTCar"}
	for alias, target := range testhelper.CarSchema.Aliases {
		aliases[alias] = target
	}
	return &schema.SchemaWithAliases{
		Schema:  schema.Schema{Objects: &models.Schema{Classes: classes}},
		Aliases: aliases,
	}
}

func newAliasResolver(t *testing.T, authorizer *mocks.FakeAuthorizer) *mockResolver {
	field, err := Build(carSchemaWithMTAlias(), config.Config{}, nil, authorizer)
	require.NoError(t, err)

	resolver := &mockResolver{}
	resolver.RootFieldName = "Aggregate"
	resolver.RootField = field
	resolver.RootObject = map[string]interface{}{"Resolver": Resolver(resolver)}
	return resolver
}

func TestAggregateThroughAliasAuthorizesTarget(t *testing.T) {
	t.Parallel()

	carData := authorization.ShardsData("Car", "")[0]
	tests := []struct {
		name   string
		query  string
		alias  string
		target string
		tenant string
		// wantResources must all have been authorized
		wantResources []string
	}{
		{
			name:          "meta count",
			query:         `{ Aggregate { CarAlias { meta { count } } } }`,
			alias:         "CarAlias",
			target:        "Car",
			wantResources: []string{carData},
		},
		{
			name:          "groupBy",
			query:         `{ Aggregate { CarAlias(groupBy: ["modelName"]) { groupedBy { value } meta { count } } } }`,
			alias:         "CarAlias",
			target:        "Car",
			wantResources: []string{carData},
		},
		{
			name: "nested where operands",
			query: `{ Aggregate { CarAlias(where: {operator: Or, operands: [
				{path: ["horsepower"], operator: GreaterThan, valueInt: 1},
				{operator: And, operands: [
					{path: ["modelName"], operator: Equal, valueText: "a"},
					{path: ["horsepower"], operator: LessThan, valueInt: 9}
				]}
			]}) { meta { count } } } }`,
			alias:         "CarAlias",
			target:        "Car",
			wantResources: []string{carData},
		},
		{
			name:          "cross-reference where",
			query:         `{ Aggregate { CarAlias(where: {path: ["madeBy", "Manufacturer", "name"], operator: Equal, valueText: "a"}) { meta { count } } } }`,
			alias:         "CarAlias",
			target:        "Car",
			wantResources: []string{carData, authorization.CollectionsData("Manufacturer")[0]},
		},
		{
			name:          "nearObject",
			query:         `{ Aggregate { CarAlias(nearObject: {id: "45000000-0000-0000-0000-000000000001"}, objectLimit: 1) { meta { count } } } }`,
			alias:         "CarAlias",
			target:        "Car",
			wantResources: []string{carData},
		},
		{
			name:          "nearVector with objectLimit",
			query:         `{ Aggregate { CarAlias(nearVector: {vector: [1, 0, 0]}, objectLimit: 1) { meta { count } } } }`,
			alias:         "CarAlias",
			target:        "Car",
			wantResources: []string{carData},
		},
		{
			name:          "hybrid",
			query:         `{ Aggregate { CarAlias(hybrid: {query: "a"}, objectLimit: 1) { meta { count } } } }`,
			alias:         "CarAlias",
			target:        "Car",
			wantResources: []string{carData},
		},
		{
			name:          "tenant",
			query:         `{ Aggregate { MTCarAlias(tenant: "t1") { meta { count } } } }`,
			alias:         "MTCarAlias",
			target:        "MTCar",
			tenant:        "t1",
			wantResources: []string{authorization.ShardsData("MTCar", "t1")[0]},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorizer := mocks.NewMockAuthorizer()
			resolver := newAliasResolver(t, authorizer)
			resolver.On("Aggregate", mock.MatchedBy(func(p *aggregation.Params) bool {
				return p.ClassName.String() == tt.target && p.Tenant == tt.tenant
			})).Return(&aggregation.Result{}, nil).Once()

			res := resolver.Resolve(tt.query)
			require.Empty(t, res.Errors)
			resolver.AssertExpectations(t)

			var resources []string
			for _, call := range authorizer.Calls() {
				resources = append(resources, call.Resources...)
			}
			for _, want := range tt.wantResources {
				assert.Contains(t, resources, want)
			}
			for _, r := range resources {
				assert.False(t, strings.Contains(r, "/collections/"+tt.alias+"/"),
					"authorized alias name instead of its target: %s", r)
			}
		})
	}
}

func TestAggregateThroughAliasDeniedOnTarget(t *testing.T) {
	t.Parallel()

	authorizer := mocks.NewMockAuthorizer()
	authorizer.Deny(authorization.ShardsData("Car", "")[0])
	resolver := newAliasResolver(t, authorizer)

	res := resolver.Resolve(`{ Aggregate { CarAlias { meta { count } } } }`)

	require.Len(t, res.Errors, 1)
	assert.Contains(t, res.Errors[0].Message, "forbidden")
	resolver.AssertNotCalled(t, "Aggregate", mock.Anything)

	var gqlErr *gqlerrors.Error
	require.ErrorAs(t, res.Errors[0].OriginalError(), &gqlErr)
	var userErr enterrors.ErrGraphQLUser
	require.ErrorAs(t, gqlErr.OriginalError, &userErr)
	assert.Equal(t, "Car", userErr.ClassName(), "the user-error metric must count the collection, not the alias")
}
