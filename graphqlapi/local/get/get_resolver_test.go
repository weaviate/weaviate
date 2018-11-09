package local_get

import (
	"github.com/creativesoftwarefdn/weaviate/database/schema/kind"
	common_local "github.com/creativesoftwarefdn/weaviate/graphqlapi/local/common_resolver"
	test_helper "github.com/creativesoftwarefdn/weaviate/graphqlapi/test/helper"
	"testing"
)

type mockResolver struct {
	test_helper.MockResolver
}

func newMockResolver() *mockResolver {
	field, err := Build(&test_helper.SimpleSchema)
	if err != nil {
		panic(err)
	}
	mocker := &mockResolver{}
	mocker.RootField = field
	mocker.RootObject = map[string]interface{}{"Resolver": Resolver(mocker)}
	return mocker
}
func (m *mockResolver) LocalGetClass(params *LocalGetClassParams) (func() interface{}, error) {
	args := m.Called(params)
	return args.Get(0).(func() interface{}), args.Error(1)
}

func TestSimpleField(t *testing.T) {
	resolver := newMockResolver()

	expectedParams := &LocalGetClassParams{
		Kind:       kind.ACTION_KIND,
		ClassName:  "SomeAction",
		Filters:    &common_local.LocalFilters{},
		Properties: []SelectProperty{{Name: "intField"}},
	}

	resolver.On("LocalGetClass", expectedParams).
		Return(test_helper.EmptyListThunk(), nil).Once()

	resolver.AssertResolve(t, "{ Root { Actions { SomeAction { intField } } } }")
}

/*
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
*/
