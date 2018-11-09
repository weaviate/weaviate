// These tests verify that the parameters to the resolver are properly extracted from a GraphQL query.
package local_get

import (
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	common_local "github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_resolver"
	test_helper "github.com/creativesoftwarefdn/weaviate/graphqlapi/test/helper"
	"testing"
)

func TestSimpleFieldParamsOK(t *testing.T) {
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

	resolver.AssertResolve(t, "{ Root { Actions { SomeAction { intField } } } }")
}
