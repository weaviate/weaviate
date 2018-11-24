// These tests verify that the parameters to the resolver are properly extracted from a GraphQL query.
package local_get

import (
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/common_resolver"
	test_helper "github.com/creativesoftwarefdn/weaviate/graphqlapi/test/helper"
	"testing"
)

func TestSimpleFieldParamsOK(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver()

	expectedParams := &LocalGetClassParams{
		Kind:       kind.ACTION_KIND,
		ClassName:  "SomeAction",
		Properties: []SelectProperty{{Name: "intField", IsPrimitive: true}},
	}

	resolver.On("LocalGetClass", expectedParams).
		Return(test_helper.EmptyListThunk(), nil).Once()

	resolver.AssertResolve(t, "{ Get { Actions { SomeAction { intField } } } }")
}

func TestExtractIntField(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver()

	expectedParams := &LocalGetClassParams{
		Kind:       kind.ACTION_KIND,
		ClassName:  "SomeAction",
		Properties: []SelectProperty{{Name: "intField", IsPrimitive: true}},
	}

	resolver.On("LocalGetClass", expectedParams).
		Return(test_helper.EmptyListThunk(), nil).Once()

	query := "{ Get { Actions { SomeAction { intField } } } }"
	resolver.AssertResolve(t, query)
}

func TestExtractPagination(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver()

	expectedParams := &LocalGetClassParams{
		Kind:       kind.ACTION_KIND,
		ClassName:  "SomeAction",
		Properties: []SelectProperty{{Name: "intField", IsPrimitive: true}},
		Pagination: &common_resolver.Pagination{
			First: 10,
			After: 20,
		},
	}

	resolver.On("LocalGetClass", expectedParams).
		Return(test_helper.EmptyListThunk(), nil).Once()

	query := "{ Get { Actions { SomeAction(first:10, after: 20) { intField } } } }"
	resolver.AssertResolve(t, query)
}

func TestGetRelation(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver()

	expectedParams := &LocalGetClassParams{
		Kind:      kind.ACTION_KIND,
		ClassName: "SomeAction",
		Properties: []SelectProperty{
			{
				Name:        "HasAction",
				IsPrimitive: false,
				Refs: []SelectClass{
					{
						ClassName: "SomeAction",
						RefProperties: []SelectProperty{
							{
								Name:        "intField",
								IsPrimitive: true,
							},
							{
								Name:        "HasAction",
								IsPrimitive: false,
								Refs: []SelectClass{
									{
										ClassName: "SomeAction",
										RefProperties: []SelectProperty{
											{
												Name:        "intField",
												IsPrimitive: true,
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

	resolver.On("LocalGetClass", expectedParams).
		Return(test_helper.EmptyListThunk(), nil).Once()

	query := "{ Get { Actions { SomeAction { HasAction { ... on SomeAction { intField, HasAction { ... on SomeAction { intField } } } } } } } }"
	resolver.AssertResolve(t, query)
}
