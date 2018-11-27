package helper

import (
	"encoding/json"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/graphql-go/graphql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"sync"
	"testing"
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
	})

	return result
}

func (mr *MockResolver) AssertResolve(t *testing.T, query string) interface{} {
	result := mr.Resolve(query)
	if len(result.Errors) > 0 {
		t.Fatalf("Failed to resolve; %#v", result.Errors)
	}

	mr.AssertExpectations(t)
	return result.Data
}

func (mr *MockResolver) AssertFailToResolve(t *testing.T, query string) {
	result := mr.Resolve(query)
	if len(result.Errors) == 0 {
		t.Fatalf("Expected to not resulve resolve; %#v", result.Errors)
	} else {
		t.Log("Resolve failed, as expected, with error", result.Errors)
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
