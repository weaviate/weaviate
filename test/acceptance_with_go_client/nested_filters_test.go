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

package acceptance_with_go_client

import (
	"context"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
	client "github.com/weaviate/weaviate-go-client/v5/weaviate"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/filters"
	"github.com/weaviate/weaviate-go-client/v5/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"

	"acceptance_tests_with_client/internal/wvhost"
)

// TestNestedFiltersViaGraphQL exercises the smallest set of nested-property
// filters that prove GraphQL ingress accepts the dotted single-path syntax
// (e.g. "cars.make", "cars[0].make") used by the nested filter pipeline.
//
// Filter-correctness corner cases (same-element AND, IsNull, NOT, every
// scalar datatype, etc.) live in the Python e2e suites
// (test_nested_props_*.py) which exercise the gRPC path. The three subtests
// here are the GraphQL parity smoke tests.
func TestNestedFiltersViaGraphQL(t *testing.T) {
	c := client.New(client.Config{Scheme: "http", Host: wvhost.REST()})
	ctx := context.Background()

	const className = "NestedFiltersGraphQLSmoke"
	class := &models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:     "cars",
				DataType: schema.DataTypeObjectArray.PropString(),
				NestedProperties: []*models.NestedProperty{
					{
						Name:            "make",
						DataType:        schema.DataTypeText.PropString(),
						Tokenization:    models.NestedPropertyTokenizationField,
						IndexFilterable: &vTrue,
					},
					{
						Name:            "color",
						DataType:        schema.DataTypeText.PropString(),
						Tokenization:    models.NestedPropertyTokenizationField,
						IndexFilterable: &vTrue,
					},
				},
			},
		},
		Vectorizer: "none",
	}
	c.Schema().ClassDeleter().WithClassName(className).Do(ctx)
	require.NoError(t, c.Schema().ClassCreator().WithClass(class).Do(ctx))
	defer c.Schema().ClassDeleter().WithClassName(className).Do(ctx)

	car := func(make, color string) map[string]any {
		return map[string]any{"make": make, "color": color}
	}

	// Four docs covering the partitions needed by all three subtests.
	docs := []struct {
		id   string
		cars []any
	}{
		{
			id:   "00000000-0000-0000-0000-000000000001",
			cars: []any{car("Toyota", "red"), car("Honda", "blue")},
		},
		{
			id:   "00000000-0000-0000-0000-000000000002",
			cars: []any{car("Honda", "blue"), car("Toyota", "red")},
		},
		{
			id:   "00000000-0000-0000-0000-000000000003",
			cars: []any{car("Toyota", "blue"), car("Honda", "red")},
		},
		{
			id:   "00000000-0000-0000-0000-000000000004",
			cars: []any{car("Ford", "green")},
		},
	}
	objs := make([]*models.Object, len(docs))
	for i, d := range docs {
		objs[i] = &models.Object{
			Class:      className,
			ID:         strfmt.UUID(d.id),
			Properties: map[string]any{"cars": d.cars},
		}
	}
	resp, err := c.Batch().ObjectsBatcher().WithObjects(objs...).Do(ctx)
	require.NoError(t, err)
	for i := range resp {
		require.Nil(t, resp[i].Result.Errors)
	}

	runQuery := func(t *testing.T, filter *filters.WhereBuilder) []string {
		t.Helper()
		result, err := c.GraphQL().Get().
			WithClassName(className).
			WithWhere(filter).
			WithLimit(len(docs)).
			WithFields(graphql.Field{Name: "_additional", Fields: []graphql.Field{{Name: "id"}}}).
			Do(ctx)
		require.NoError(t, err)
		require.Nil(t, result.Errors, "graphql errors: %+v", result.Errors)
		return GetIds(t, result, className)
	}

	t.Run("Equal_on_nested_leaf", func(t *testing.T) {
		// Existential: any doc with any car owning make=Toyota matches.
		// Exercises the dotted path "cars.make" + nested filterable value
		// bucket.
		ids := runQuery(t, filters.Where().
			WithOperator(filters.Equal).
			WithValueText("Toyota").
			WithPath([]string{"cars.make"}))
		require.ElementsMatch(t, []string{
			"00000000-0000-0000-0000-000000000001",
			"00000000-0000-0000-0000-000000000002",
			"00000000-0000-0000-0000-000000000003",
		}, ids)
	})

	t.Run("ArrayIndexPin", func(t *testing.T) {
		// cars[0].make=Toyota — pins to position 0 only. Exercises the
		// indexed path "cars[0].make" + the meta bucket entry _idx.cars.0.
		ids := runQuery(t, filters.Where().
			WithOperator(filters.Equal).
			WithValueText("Toyota").
			WithPath([]string{"cars[0].make"}))
		require.ElementsMatch(t, []string{
			"00000000-0000-0000-0000-000000000001",
			"00000000-0000-0000-0000-000000000003",
		}, ids)
	})

	t.Run("SameElementAnd", func(t *testing.T) {
		// cars.make=Toyota AND cars.color=red — both leaves must be
		// satisfied by the SAME car. Exercises same-element correlation
		// dispatch through the GraphQL ingress.
		andFilter := filters.Where().
			WithOperator(filters.And).
			WithOperands([]*filters.WhereBuilder{
				filters.Where().
					WithOperator(filters.Equal).
					WithValueText("Toyota").
					WithPath([]string{"cars.make"}),
				filters.Where().
					WithOperator(filters.Equal).
					WithValueText("red").
					WithPath([]string{"cars.color"}),
			})
		ids := runQuery(t, andFilter)
		require.ElementsMatch(t, []string{
			"00000000-0000-0000-0000-000000000001", // cars[0] = Toyota+red
			"00000000-0000-0000-0000-000000000002", // cars[1] = Toyota+red
		}, ids)
	})
}
