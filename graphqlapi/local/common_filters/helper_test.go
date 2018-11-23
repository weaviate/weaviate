package common_filters

import (
	test_helper "github.com/creativesoftwarefdn/weaviate/graphqlapi/test/helper"
	"github.com/graphql-go/graphql"
)

type mockResolver struct {
	test_helper.MockResolver
}

func newMockResolver() *mockResolver {
	// Build a FakeGet.
	fakeGet := &graphql.Field{
		Name:        "FakeGet",
		Description: "FakeGet",
		Args: graphql.FieldConfigArgument{
			"where": &graphql.ArgumentConfig{
				Description: "Filter options for the Get search, to convert the data to the filter input",
				Type: graphql.NewInputObject(
					graphql.InputObjectConfig{
						Name:        "WeaviateLocalGetWhereInpObj",
						Fields:      CommonFilters,
						Description: "",
					},
				),
			},
		},
		Type: graphql.Int,
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			resolver := p.Source.(map[string]interface{})["Resolver"].(*mockResolver)
			filters, err := ExtractFilters(p.Args)
			if err != nil {
				return nil, err
			}

			result, err := resolver.ReportFilters(filters)
			return result, err
		},
	}

	mocker := &mockResolver{}
	mocker.RootFieldName = "FakeGet"
	mocker.RootField = fakeGet
	mocker.RootObject = map[string]interface{}{"Resolver": mocker}
	return mocker
}

func (m *mockResolver) ReportFilters(filter *LocalFilter) (func() interface{}, error) {
	args := m.Called(filter)
	return args.Get(0).(func() interface{}), args.Error(1)
}
