//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
// 
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package getmeta

// This file contains unit tests for the resolver itself. If you're looking for
// a test of the overall Network-Get-Meta GraphQL functionality, please take a
// look at the component_test.go file.

import (
	"testing"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/graphql-go/graphql/language/source"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/network/common"
	"github.com/semi-technologies/weaviate/entities/models"
)

type mockRequestsLog struct{}

func (m *mockRequestsLog) Register(first string, second string) {

}

func TestNetworkGetInstanceQueryWithoutFilters(t *testing.T) {
	t.Parallel()
	resolver := &fakeNetworkResolver{}

	query := []byte(
		`{ Network { GetMeta { weaviateA { Things { City { meta { count } } } } } } } `,
	)

	expectedSubQuery := `{ Local { GetMeta { Things { City { meta { count } } } } } }`
	expectedTarget := "weaviateA"
	expectedResultString := "placeholder for result from Local.GetMeta"

	// in a real life scenario graphql will set the start and end
	// correctly. We just need to manually specify them in the test
	params := paramsFromQueryWithStartAndEnd(query, 22, 70, "weaviateA", resolver, nil)
	result, err := Resolve(params)

	if err != nil {
		t.Errorf("Expected no error, but got: %s", err)
	}

	if resolver.Called != true {
		t.Error("expected resolver.ProxyNetworkGetMetaInstance to have been called, but it was never called")
	}

	if actual := resolver.CalledWith.SubQuery; string(actual) != expectedSubQuery {
		t.Errorf("expected subquery to be \n%#+v\n, but got \n%#+v", expectedSubQuery, actual)
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
	instanceName string, resolver Resolver, principal interface{}) graphql.ResolveParams {
	paramSource := make(map[string]interface{})
	paramSource["NetworkResolver"] = resolver
	paramSource["RequestsLog"] = &mockRequestsLog{}
	return graphql.ResolveParams{
		Source: paramSource,
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
		// Context: context.WithValue(context.Background(), "principal", principal),
	}
}

type fakeNetworkResolver struct {
	Called     bool
	CalledWith common.Params
}

func (r *fakeNetworkResolver) ProxyGetMetaInstance(info common.Params) (*models.GraphQLResponse, error) {
	r.Called = true
	r.CalledWith = info
	return &models.GraphQLResponse{
		Data: map[string]models.JSONObject{
			"Local": map[string]interface{}{
				"GetMeta": "placeholder for result from Local.GetMeta"},
		},
	}, nil
}
