package local_get

import (
	test_helper "github.com/creativesoftwarefdn/weaviate/graphqlapi/test/helper"
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
	mocker.RootFieldName = "Get"
	mocker.RootField = field
	mocker.RootObject = map[string]interface{}{"Resolver": Resolver(mocker)}
	return mocker
}
func (m *mockResolver) LocalGetClass(params *LocalGetClassParams) (func() interface{}, error) {
	args := m.Called(params)
	return args.Get(0).(func() interface{}), args.Error(1)
}
