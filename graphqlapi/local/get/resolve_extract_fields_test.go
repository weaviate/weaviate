// These tests verify that the extraction of fields from a result is correct.
package local_get

import (
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/graphqlapi/common_resolver"
	common_local "github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_resolver"
	test_helper "github.com/creativesoftwarefdn/weaviate/graphqlapi/test/helper"
	"testing"
)

func TestExtractIntField(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver()

	expectedParams := &LocalGetClassParams{
		Kind:       kind.ACTION_KIND,
		ClassName:  "SomeAction",
		Filters:    &common_local.LocalFilters{},
		Properties: []SelectProperty{{Name: "intField"}},
	}

	resolver.On("LocalGetClass", expectedParams).
		Return(test_helper.EmptyListThunk(), nil).Once()

	query := "{ Root { Actions { SomeAction { intField } } } }"
	resolver.AssertResolve(t, query)
}

func TestExtractFilters(t *testing.T) {
	t.Parallel()

	resolver := newMockResolver()

	expectedParams := &LocalGetClassParams{
		Kind:       kind.ACTION_KIND,
		ClassName:  "SomeAction",
		Filters:    &common_local.LocalFilters{},
		Properties: []SelectProperty{{Name: "intField"}},
		Pagination: &common_resolver.Pagination{
			First: 10,
			After: 20,
		},
	}

	resolver.On("LocalGetClass", expectedParams).
		Return(test_helper.EmptyListThunk(), nil).Once()

	query := "{ Root { Actions { SomeAction(first:10, after: 20) { intField } } } }"
	resolver.AssertResolve(t, query)
}
