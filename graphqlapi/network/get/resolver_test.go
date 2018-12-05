package network_get

import (
	"testing"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/graphql-go/graphql/language/source"
)

func TestRegularNetworkGetQuery(t *testing.T) {
	t.Parallel()
	resolver := &fakeNetworkResolver{}

	query := []byte(`{
  Network {
    Get {
      Things {
        City {
          name
        }
      }
    }
  }
}
`)

	expectedSubQuery :=
		`Get {
      Things {
        City {
          name
        }
      }
    }`

	// in a real life scenario graphql will set the start and end
	// correctly. We just need to manually specify them in the test
	params := paramsFromQueryWithStartAndEnd(query, 18, 92, resolver)
	NetworkGetResolve(params)

	if resolver.CalledWith == nil {
		t.Error("expected resolver.ProxyNetworkGet to have been called, but it was never called")
	}

	if actual := resolver.CalledWith.SubQuery; string(actual) != expectedSubQuery {
		t.Errorf("expected subquery to be %s, but got %s", expectedSubQuery, actual)
	}
}

func paramsFromQueryWithStartAndEnd(query []byte, start int, end int, resolver Resolver) graphql.ResolveParams {
	return graphql.ResolveParams{
		Source: map[string]interface{}{
			"Resolver": resolver,
		},
		Info: graphql.ResolveInfo{
			FieldASTs: []*ast.Field{
				&ast.Field{
					Loc: &ast.Location{
						Start: start,
						Source: &source.Source{
							Body: []byte(query)},
						End: end,
					},
				},
			},
		},
	}
}

type fakeNetworkResolver struct {
	CalledWith *NetworkGetParams
}

func (r *fakeNetworkResolver) ProxyNetworkGet(info *NetworkGetParams) (func() interface{}, error) {
	r.CalledWith = info
	return (func() interface{} { return nil }), nil
}
