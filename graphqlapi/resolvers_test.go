package graphqlapi

import (
	"fmt"
	"github.com/creativesoftwarefdn/weaviate/database/schema"
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	"github.com/creativesoftwarefdn/weaviate/models"
	"github.com/graphql-go/graphql"
	"testing"
	//  "github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func buildSchema() GraphQL {
	fakeDbSchema := schema.Schema{
		Things: &models.SemanticSchema{
			Classes: []*models.SemanticSchemaClass{
				&models.SemanticSchemaClass{
					Class: "SomeThing",
				},
			},
		},
		Actions: &models.SemanticSchema{
			Classes: []*models.SemanticSchemaClass{
				&models.SemanticSchemaClass{
					Class: "SomeAction",
					Properties: []*models.SemanticSchemaClassProperty{
						&models.SemanticSchemaClassProperty{
							Name:       "intField",
							AtDataType: []string{"int"},
						},
					},
				},
			},
		},
	}
	oldFakeDbSchema := schema.HackFromDatabaseSchema(fakeDbSchema)
	graphqlSchema, err := CreateSchema(nil, nil, &oldFakeDbSchema, nil)

	if err != nil {
		panic(fmt.Sprintf("failed to create schema: %#v", err))
	}
	return graphqlSchema
}

type mockResolver struct {
	mock.Mock
}

func newMockResolver() *mockResolver {
	return &mockResolver{}
}

func (tr *mockResolver) resolve(query string) *graphql.Result {
	schema := buildSchema()
	schema.setResolver(tr)

	result := graphql.Do(graphql.Params{
		Schema:        *schema.Schema(),
		RequestString: query,
		// has principal
		//Context:        graphql_context,
	})

	return result
}

func (tr *mockResolver) assertResolve(t *testing.T, query string) interface{} {
	result := tr.resolve(query)
	if len(result.Errors) > 0 {
		t.Fatalf("Failed to resolve; %#v", result.Errors)
	}
	return result.Data
}

//func nilThunk() interface{} {
//	return nil
//}
//
//func simpleThunk(x interface{}) func() interface{} {
//	return func() interface{} {
//		return x
//	}
//}

func emptyListThunk() resolvePromise {
	return func() (interface{}, error) {
		list := []interface{}{}
		return interface{}(list), nil
	}
}

func singletonThunk(x interface{}) resolvePromise {
	return func() (interface{}, error) {
		//list := []interface{}{x}
    return "blurp", nil
		//return interface{}(list), nil
	}
}

func (m *mockResolver) ResolveGetClass(params *getClassParams) (resolvePromise, error) {
	args := m.Called(params)
	return args.Get(0).(resolvePromise), args.Error(1)
}

func TestSimpleField(t *testing.T) {
	return
	r := newMockResolver()

	expectedParams := &getClassParams{
		kind:       kind.ACTION_KIND,
		className:  "SomeAction",
		filters:    &localGetFilters{},
		properties: []property{{name: "intField"}},
	}

	r.On("ResolveGetClass", expectedParams).
		Return(emptyListThunk(), nil).Once()

	r.resolve("{ Local { Get { Actions { SomeAction { intField } } } } }")
	r.AssertExpectations(t)
}

func TestSimplePagination(t *testing.T) {
	return
	r := newMockResolver()

	expectedParams := &getClassParams{
		kind:       kind.ACTION_KIND,
		className:  "SomeAction",
		filters:    &localGetFilters{},
		properties: []property{{name: "intField"}},
		pagination: &pagination{
			first: 10,
			after: 20,
		},
	}

	r.On("ResolveGetClass", expectedParams).
		Return(emptyListThunk(), nil).Once()

	r.assertResolve(t, "{ Local { Get { Actions { SomeAction(first: 10, after:20) { intField } } } } }")
	r.AssertExpectations(t)
}

func TestSimpleOneResult(t *testing.T) {
	r := newMockResolver()

	expectedParams := &getClassParams{
		kind:       kind.ACTION_KIND,
		className:  "SomeAction",
		filters:    &localGetFilters{},
		properties: []property{{name: "uuid"}},
		pagination: &pagination{
			first: 10,
			after: 20,
		},
	}

	result := resolvedClass{
		kind:      kind.ACTION_KIND,
		className: "SomeAction",
		uuid:      "some-uuid",
		properties: map[string]resolvedProperty{
			"intField": interface{}(42),
		},
	}

  _ = result
	r.On("ResolveGetClass", expectedParams).
		Return(nil, nil).Once()

	value := r.assertResolve(t, "{ Local { Get { Actions { SomeAction(first: 10, after:20) { uuid } } } } }")
	r.AssertExpectations(t)
	t.Logf("VALUE: %#v", value)
}
