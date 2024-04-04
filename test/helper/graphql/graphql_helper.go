//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package graphqlhelper

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/runtime"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/graphql"
	graphql_client "github.com/weaviate/weaviate/client/graphql"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

type GraphQLResult struct {
	Result interface{}
}

// Perform a GraphQL request
func QueryGraphQL(t *testing.T, auth runtime.ClientAuthInfoWriterFunc, operation string, query string, variables map[string]interface{}) (*models.GraphQLResponse, error) {
	var vars interface{} = variables
	params := graphql_client.NewGraphqlPostParams().WithBody(&models.GraphQLQuery{OperationName: operation, Query: query, Variables: vars})
	response, err := helper.Client(t).Graphql.GraphqlPost(params, nil)
	if err != nil {
		return nil, err
	}

	return response.Payload, nil
}

// Perform a GraphQL request with timeout
func QueryGraphQLWithTimeout(t *testing.T, auth runtime.ClientAuthInfoWriterFunc, operation string, query string, variables map[string]interface{}, timeout time.Duration) (*models.GraphQLResponse, error) {
	var vars interface{} = variables
	params := graphql_client.NewGraphqlPostParamsWithTimeout(timeout).WithBody(&models.GraphQLQuery{OperationName: operation, Query: query, Variables: vars})
	response, err := helper.Client(t).Graphql.GraphqlPost(params, nil)
	if err != nil {
		return nil, err
	}

	return response.Payload, nil
}

// Perform a GraphQL request and call fatal on failure
func QueryGraphQLOrFatal(t *testing.T, auth runtime.ClientAuthInfoWriterFunc, operation string, query string, variables map[string]interface{}) *models.GraphQLResponse {
	response, err := QueryGraphQL(t, auth, operation, query, variables)
	return getGraphQLResponseOrFatal(t, response, err)
}

// Perform a GraphQL request and call fatal on failure with timeout
func QueryGraphQLOrFatalWithTimeout(t *testing.T, auth runtime.ClientAuthInfoWriterFunc, operation string, query string, variables map[string]interface{}, timeout time.Duration) *models.GraphQLResponse {
	response, err := QueryGraphQLWithTimeout(t, auth, operation, query, variables, timeout)
	return getGraphQLResponseOrFatal(t, response, err)
}

func getGraphQLResponseOrFatal(t *testing.T, response *models.GraphQLResponse, err error) *models.GraphQLResponse {
	if err != nil {
		parsedErr, ok := err.(*graphql.GraphqlPostUnprocessableEntity)
		if !ok {
			t.Fatalf("Expected the query to succeed, but failed due to: %#v", err)
		}
		t.Fatalf("Expected the query to succeed, but failed with unprocessable entity: %v", parsedErr.Payload.Error[0])
	}
	return response
}

// Perform a query and assert that it is successful
func AssertGraphQL(t *testing.T, auth runtime.ClientAuthInfoWriterFunc, query string) *GraphQLResult {
	response := QueryGraphQLOrFatal(t, auth, "", query, nil)
	return getGraphQLResult(t, response)
}

// Perform a query and assert that it is successful with timeout
func AssertGraphQLWithTimeout(t *testing.T, auth runtime.ClientAuthInfoWriterFunc, timeout time.Duration, query string) *GraphQLResult {
	response := QueryGraphQLOrFatalWithTimeout(t, auth, "", query, nil, timeout)
	return getGraphQLResult(t, response)
}

func getGraphQLResult(t *testing.T, response *models.GraphQLResponse) *GraphQLResult {
	if len(response.Errors) != 0 {
		j, _ := json.Marshal(response.Errors)
		t.Fatal("GraphQL resolved to an error:", string(j))
	}

	data := make(map[string]interface{})

	// get rid of models.JSONData
	for key, value := range response.Data {
		data[key] = value
	}

	return &GraphQLResult{Result: data}
}

// Perform a query and assert that it has errors
func ErrorGraphQL(t *testing.T, auth runtime.ClientAuthInfoWriterFunc, query string) []*models.GraphQLError {
	response := QueryGraphQLOrFatal(t, auth, "", query, nil)

	if len(response.Errors) == 0 {
		j, _ := json.Marshal(response.Errors)
		t.Fatal("GraphQL resolved to data:", string(j))
	}

	return response.Errors
}

// Drill down in the result
func (g GraphQLResult) Get(paths ...string) *GraphQLResult {
	current := g.Result
	for _, path := range paths {
		var ok bool
		currentAsMap := (current.(map[string]interface{}))
		current, ok = currentAsMap[path]
		if !ok {
			panic(fmt.Sprintf("Cannot get element %s in %#v; result: %#v", path, paths, g.Result))
		}
	}

	return &GraphQLResult{
		Result: current,
	}
}

// Cast the result to a slice
func (g *GraphQLResult) AsSlice() []interface{} {
	return g.Result.([]interface{})
}

func Vec2String(v []float32) (s string) {
	for _, n := range v {
		s = fmt.Sprintf("%s, %f", s, n)
	}
	s = strings.TrimLeft(s, ", ")
	return fmt.Sprintf("[%s]", s)
}

func ParseVec(t *testing.T, iVec []interface{}) []float32 {
	vec := make([]float32, len(iVec))
	for i, val := range iVec {
		parsed, err := val.(json.Number).Float64()
		require.Nil(t, err)
		vec[i] = float32(parsed)
	}
	return vec
}
