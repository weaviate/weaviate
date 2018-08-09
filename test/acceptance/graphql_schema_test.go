package test

// Acceptance tests for GraphQL Schema

// There is a helper struct called GraphQLResult that helps to navigate through the output,
// and a few helper see thelper functions to access the GraphQL endpoint.
// See the end of this file for more details on how those work.

import (
	"testing"

	"github.com/go-openapi/runtime"

	"github.com/creativesoftwarefdn/weaviate/models"

	graphql_client "github.com/creativesoftwarefdn/weaviate/client/graphql"
	"github.com/creativesoftwarefdn/weaviate/test/acceptance/helper"
)

func TestGettingTypeNames(t *testing.T) {
	query := `{ 
    __schema { 
      types {
        name
      }
    }
  }` // use backticks for multiline queries.

	expectedTypes := []string{"Key", "Thing", "Action", "BadType"}

	response := queryGraphqlAssertOK(t, helper.RootAuth, query)

	for _, expected := range expectedTypes {
		found := false
		for _, type_ := range response.AssertKey(t, "__schema").AssertKey(t, "types").AssertSlice(t) {
			foundType := type_.AssertKey(t, "name").AssertString(t)
			if expected == foundType {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Could not find expected type '%s'", expected)
		}
	}
}

///////////////////////////////////////////////////////////////////////////////////////////////////
// Helper functions

type GraphQLResult struct {
	Result interface{}
}

// Asserts thtat the result is a map, and index in this key
func (g *GraphQLResult) AssertKey(t *testing.T, key string) *GraphQLResult {
	m, ok := g.Result.(map[string]interface{})
	if !ok {
		t.Fatalf("Can't index into key %s, because this is not a map", key)
	}

	x, ok := m[key]
	if !ok {
		t.Fatalf("Can't index into key %s, because no such key exists", key)
	}

	return &GraphQLResult{Result: x}
}

// Assert that this is a slice.
// Wraps a GraphQLResult over all children too.
func (g *GraphQLResult) AssertSlice(t *testing.T) []*GraphQLResult {
	m, ok := g.Result.([]interface{})
	if !ok {
		t.Fatalf("This is not a slice!")
	}

	var result []*GraphQLResult

	for _, s := range m {
		result = append(result, &GraphQLResult{Result: s})
	}

	return result
}

// Assert that this is a string
func (g *GraphQLResult) AssertString(t *testing.T) string {
	str, ok := g.Result.(string)
	if !ok {
		t.Fatalf("This is not a string!")
	}
	return str
}

// Perform a GraphQL query
func queryGraphql(t *testing.T, auth runtime.ClientAuthInfoWriterFunc, operation string, query string, variables map[string]interface{}) (*models.GraphQLResponse, error) {
	var vars interface{} = variables
	params := graphql_client.NewWeaviateGraphqlPostParams().WithBody(&models.GraphQLQuery{OperationName: operation, Query: query, Variables: vars})
	response, err := helper.Client(t).Graphql.WeaviateGraphqlPost(params, auth)

	if err != nil {
		return nil, err
	}

	return response.Payload, nil
}

// Perform a query and assert that it is successful
func queryGraphqlAssertOK(t *testing.T, auth runtime.ClientAuthInfoWriterFunc, query string) *GraphQLResult {
	response, err := queryGraphql(t, auth, "", query, nil)
	if err != nil {
		t.Fatalf("Expected the query to succeed, but failed due to: %#v", err)
	}

	data := make(map[string]interface{})

	// get rid of models.JSONData
	for key, value := range response.Data {
		data[key] = value
	}

	return &GraphQLResult{Result: data}
}

// Perform a query and assert that it is incorrect
func queryGraphqlAssertFail(t *testing.T, auth runtime.ClientAuthInfoWriterFunc, query string) ([]*models.GraphQLError, error) {
	response, err := queryGraphql(t, auth, "", query, nil)
	if err == nil {
		t.Fatalf("Expected the query to fail, but it succeeded %#v", response)
	}

	return response.Errors, err
}
