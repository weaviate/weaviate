package network_get

import (
	"testing"

	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/graphql-go/graphql/language/source"
)

func TestRegularNetworkGetInstanceQuery(t *testing.T) {
	t.Parallel()
	resolver := &fakeNetworkResolver{}

	query := []byte(
		`{ Network { Get { weaviateA { Things { City { name } } } } } } `,
	)

	expectedSubQuery := `Get { Things { City { name } } }`
	expectedTarget := "weaviateA"
	expectedResultString := "placeholder for result from Local.Get"

	// in a real life scenario graphql will set the start and end
	// correctly. We just need to manually specify them in the test
	params := paramsFromQueryWithStartAndEnd(query, 18, 56, "weaviateA", resolver)
	result, err := NetworkGetInstanceResolve(params)

	if err != nil {
		t.Errorf("Expected no error, but got: %s", err)
	}

	if resolver.Called != true {
		t.Error("expected resolver.ProxyNetworkGetInstance to have been called, but it was never called")
	}

	if actual := resolver.CalledWith.SubQuery; string(actual) != expectedSubQuery {
		t.Errorf("expected subquery to be %s, but got %s", expectedSubQuery, actual)
	}

	if actual := resolver.CalledWith.TargetInstance; string(actual) != expectedTarget {
		t.Errorf("expected targetInstance to be %#v, but got %#v", expectedTarget, actual)
	}

	if _, ok := result.(string); !ok {
		t.Errorf("expected result to be a string, but was %t", result)
	}

	if resultString, ok := result.(string); !ok || resultString != expectedResultString {
		t.Errorf("expected result to be %s, but was %s", expectedResultString, resultString)
	}
}

func paramsFromQueryWithStartAndEnd(query []byte, start int, end int,
	instanceName string, resolver Resolver) graphql.ResolveParams {
	return graphql.ResolveParams{
		Source: map[string]interface{}{
			"NetworkResolver": resolver,
		},
		Info: graphql.ResolveInfo{
			FieldName: instanceName,
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
	Called     bool
	CalledWith ProxyGetInstanceParams
}

func (r *fakeNetworkResolver) ProxyGetInstance(info ProxyGetInstanceParams) (*models.GraphQLResponse, error) {
	r.Called = true
	r.CalledWith = info
	return &models.GraphQLResponse{
		Data: map[string]models.JSONObject{
			"Local": map[string]interface{}{
				"Get": "placeholder for result from Local.Get"},
		},
	}, nil
}
