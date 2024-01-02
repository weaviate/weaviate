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

package helper

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/tailor-inc/graphql"
	"github.com/tailor-inc/graphql/gqlerrors"
	"github.com/weaviate/weaviate/entities/schema"
)

type MockResolver struct {
	mock.Mock
	Schema        *schema.Schema
	RootField     *graphql.Field
	RootFieldName string
	RootObject    map[string]interface{}
}

var schemaBuildLock sync.Mutex

func (mr *MockResolver) Resolve(query string) *graphql.Result {
	fields := graphql.Fields{}
	fields[mr.RootFieldName] = mr.RootField
	schemaObject := graphql.ObjectConfig{
		Name:        "RootObj",
		Description: "Location of the root query",
		Fields:      fields,
	}

	schemaBuildLock.Lock() // serialize creation of GraphQL schema.
	schema, err := graphql.NewSchema(graphql.SchemaConfig{
		Query: graphql.NewObject(schemaObject),
	})
	schemaBuildLock.Unlock()

	if err != nil {
		panic(err)
	}

	result := graphql.Do(graphql.Params{
		Schema:        schema,
		RequestString: query,
		RootObject:    mr.RootObject,
		Context:       context.Background(),
	})

	return result
}

func (mr *MockResolver) AssertResolve(t *testing.T, query string) *GraphQLResult {
	result := mr.Resolve(query)
	if len(result.Errors) > 0 {
		t.Fatalf("Failed to resolve; %s", spew.Sdump(result.Errors))
	}

	mr.AssertExpectations(t)
	return &GraphQLResult{Result: result.Data}
}

func (mr *MockResolver) AssertFailToResolve(t *testing.T, query string, errors ...string) {
	result := mr.Resolve(query)
	if len(result.Errors) == 0 {
		t.Fatalf("Expected to not resolve; %#v", result.Errors)
	} else {
		t.Log("Resolve failed, as expected, with error", result.Errors)
	}
	if len(errors) > 0 {
		require.Equal(t, errors[0], result.Errors[0].Error())
	}
}

func (mr *MockResolver) AssertErrors(t *testing.T, query string, errors []gqlerrors.FormattedError) {
	result := mr.Resolve(query)
	for i, actual := range result.Errors {
		assert.Equal(t, errors[i].Error(), actual.Error(), "should have failed in a specific way, but didnt")
	}
}

func (mr *MockResolver) AssertJSONResponse(t *testing.T, query string, expectedResponseString string) {
	var expectedResponse map[string]interface{}
	err := json.Unmarshal([]byte(expectedResponseString), &expectedResponse)
	if err != nil {
		t.Fatalf("Could not parse '%s' as json: %v", expectedResponseString, err)
	}

	response := mr.AssertResolve(t, query)

	assert.Equal(t, expectedResponse, response)
}

type GraphQLResult struct {
	Result interface{}
}

// Drill down in the result
func (g GraphQLResult) Get(paths ...string) *GraphQLResult {
	current := g.Result
	for _, path := range paths {
		var ok bool
		currentAsMap, ok := (current.(map[string]interface{}))
		if !ok {
			panic(fmt.Sprintf("Cannot get element %s in %#v; result: %#v", path, paths, g.Result))
		}

		current, ok = currentAsMap[path]
		if !ok {
			panic(fmt.Sprintf("Cannot get element %s in %#v; result: %#v", path, paths, g.Result))
		}
	}

	return &GraphQLResult{
		Result: current,
	}
}
