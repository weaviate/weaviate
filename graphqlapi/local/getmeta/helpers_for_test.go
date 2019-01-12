package getmeta

import (
	"fmt"

	testhelper "github.com/creativesoftwarefdn/weaviate/graphqlapi/test/helper"
)

type mockResolver struct {
	testhelper.MockResolver
}

func newMockResolver() *mockResolver {
	field, err := Build(&testhelper.CarSchema)
	if err != nil {
		panic(fmt.Sprintf("could not build graphql test schema: %s", err))
	}
	mocker := &mockResolver{}
	mocker.RootFieldName = "GetMeta"
	mocker.RootField = field
	mocker.RootObject = map[string]interface{}{"Resolver": Resolver(mocker)}
	return mocker
}

func (m *mockResolver) LocalGetMeta(params *Params) (interface{}, error) {
	args := m.Called(params)
	return args.Get(0), args.Error(1)
}
