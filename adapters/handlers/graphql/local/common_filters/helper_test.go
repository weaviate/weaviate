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

package common_filters

import (
	"testing"

	"github.com/tailor-inc/graphql"
	test_helper "github.com/weaviate/weaviate/adapters/handlers/graphql/test/helper"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/searchparams"
)

type mockResolver struct {
	test_helper.MockResolver
}

type mockParams struct {
	reportFilter     bool
	reportNearVector bool
	reportNearObject bool
}

func newMockResolver(t *testing.T, params mockParams) *mockResolver {
	if params.reportNearVector && params.reportNearObject {
		t.Fatal("cannot provide both nearVector and nearObject")
	}

	// Build a FakeGet.
	fakeGet := &graphql.Field{
		Name:        "SomeAction",
		Description: "Fake Some Action",
		Args: graphql.FieldConfigArgument{
			"where": &graphql.ArgumentConfig{
				Description: "Filter options for the Get search, to convert the data to the filter input",
				Type: graphql.NewInputObject(
					graphql.InputObjectConfig{
						Name:        "GetWhereInpObj",
						Fields:      BuildNew("Get"),
						Description: "",
					},
				),
			},
			"nearVector": NearVectorArgument("Get", "SomeAction"),
			"nearObject": NearObjectArgument("Get", "SomeAction"),
		},
		Type: graphql.Int,
		Resolve: func(p graphql.ResolveParams) (interface{}, error) {
			resolver := p.Source.(map[string]interface{})["Resolver"].(*mockResolver)
			return resolver.ReportArgs(params, p.Args, p.Info.FieldName)
		},
	}

	mocker := &mockResolver{}
	mocker.RootFieldName = "SomeAction"
	mocker.RootField = fakeGet
	mocker.RootObject = map[string]interface{}{"Resolver": mocker}
	return mocker
}

func (m *mockResolver) ReportArgs(params mockParams, args map[string]interface{},
	fieldName string,
) (result interface{}, err error) {
	if params.reportFilter {
		filters, err := ExtractFilters(args, fieldName)
		if err != nil {
			return nil, err
		}
		result, err = m.ReportFilters(filters)
		if err != nil {
			return nil, err
		}
	}

	if params.reportNearVector {
		nearVec, err := ExtractNearVector(args["nearVector"].(map[string]interface{}))
		if err != nil {
			return nil, err
		}
		result, err = m.ReportNearVector(nearVec)
		if err != nil {
			return nil, err
		}
	}

	if params.reportNearObject {
		nearObj, err := ExtractNearObject(args["nearObject"].(map[string]interface{}))
		if err != nil {
			return nil, err
		}
		result, err = m.ReportNearObject(nearObj)
		if err != nil {
			return nil, err
		}
	}

	return
}

func (m *mockResolver) ReportFilters(filter *filters.LocalFilter) (interface{}, error) {
	args := m.Called(filter)
	return args.Get(0), args.Error(1)
}

func (m *mockResolver) ReportNearVector(params searchparams.NearVector) (interface{}, error) {
	args := m.Called(params)
	return args.Get(0), args.Error(1)
}

func (m *mockResolver) ReportNearObject(params searchparams.NearObject) (interface{}, error) {
	args := m.Called(params)
	return args.Get(0), args.Error(1)
}
