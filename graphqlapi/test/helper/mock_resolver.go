package helper

import (
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/graphql-go/graphql"
	"github.com/stretchr/testify/mock"
	"testing"
)

type MockResolver struct {
	mock.Mock
	Schema     *schema.Schema
	RootField  *graphql.Field
	RootObject map[string]interface{}
}

func (mr *MockResolver) Resolve(query string) *graphql.Result {
	schemaObject := graphql.ObjectConfig{
		Name:        "RootObj",
		Description: "Location of the root query",
		Fields: graphql.Fields{
			"Root": mr.RootField,
		},
	}

	schema, err := graphql.NewSchema(graphql.SchemaConfig{
		Query: graphql.NewObject(schemaObject),
	})

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
