//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/go-openapi/runtime"
	"github.com/semi-technologies/weaviate/client/batch"
	"github.com/semi-technologies/weaviate/client/graphql"
	graphql_client "github.com/semi-technologies/weaviate/client/graphql"
	"github.com/semi-technologies/weaviate/client/objects"
	"github.com/semi-technologies/weaviate/client/schema"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/test/acceptance/helper"
	"github.com/stretchr/testify/assert"
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

// Perform a GraphQL request and call fatal on failure
func QueryGraphQLOrFatal(t *testing.T, auth runtime.ClientAuthInfoWriterFunc, operation string, query string, variables map[string]interface{}) *models.GraphQLResponse {
	response, err := QueryGraphQL(t, auth, operation, query, variables)
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

func createObjectClass(t *testing.T, class *models.Class) {
	params := schema.NewSchemaObjectsCreateParams().WithObjectClass(class)
	resp, err := helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)
}

func createObject(t *testing.T, object *models.Object) {
	params := objects.NewObjectsCreateParams().WithBody(object)
	resp, err := helper.Client(t).Objects.ObjectsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)
}

func createObjectsBatch(t *testing.T, objects []*models.Object) {
	params := batch.NewBatchObjectsCreateParams().
		WithBody(batch.BatchObjectsCreateBody{
			Objects: objects,
		})
	resp, err := helper.Client(t).Batch.BatchObjectsCreate(params, nil)
	helper.AssertRequestOk(t, resp, err, nil)
	for _, elem := range resp.Payload {
		assert.Nil(t, elem.Result.Errors)
	}
}

func deleteObjectClass(t *testing.T, class string) {
	delParams := schema.NewSchemaObjectsDeleteParams().WithClassName(class)
	delRes, err := helper.Client(t).Schema.SchemaObjectsDelete(delParams, nil)
	helper.AssertRequestOk(t, delRes, err, nil)
}
