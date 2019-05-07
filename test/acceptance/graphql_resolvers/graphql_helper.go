/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */
package test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/semi-technologies/weaviate/client/graphql"
	graphql_client "github.com/semi-technologies/weaviate/client/graphql"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/test/acceptance/helper"
	"github.com/go-openapi/runtime"
)

type GraphQLResult struct {
	Result interface{}
}

// Perform a GraphQL request
func QueryGraphQL(t *testing.T, auth runtime.ClientAuthInfoWriterFunc, operation string, query string, variables map[string]interface{}) (*models.GraphQLResponse, error) {
	var vars interface{} = variables
	params := graphql_client.NewWeaviateGraphqlPostParams().WithBody(&models.GraphQLQuery{OperationName: operation, Query: query, Variables: vars})
	response, err := helper.Client(t).Graphql.WeaviateGraphqlPost(params, nil)

	if err != nil {
		return nil, err
	}

	return response.Payload, nil
}

// Perform a query and assert that it is successful
func AssertGraphQL(t *testing.T, auth runtime.ClientAuthInfoWriterFunc, query string) *GraphQLResult {
	response, err := QueryGraphQL(t, auth, "", query, nil)
	if err != nil {
		parsedErr, ok := err.(*graphql.WeaviateGraphqlPostUnprocessableEntity)
		if !ok {
			t.Fatalf("Expected the query to succeed, but failed due to: %#v", err)
		}
		t.Fatalf("Expected the query to succeed, but failed with unprocessable entity: %v", parsedErr.Payload.Error)
	}

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
