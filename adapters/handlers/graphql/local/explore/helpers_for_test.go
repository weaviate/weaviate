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

package explore

import (
	"context"
	"fmt"
	"net/http"

	"github.com/tailor-inc/graphql"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/descriptions"
	testhelper "github.com/weaviate/weaviate/adapters/handlers/graphql/test/helper"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/usecases/traverser"
)

type mockRequestsLog struct{}

func (m *mockRequestsLog) Register(first string, second string) {
}

type mockResolver struct {
	testhelper.MockResolver
}

type fakeModulesProvider struct{}

func (p *fakeModulesProvider) VectorFromInput(ctx context.Context, className string, input string) ([]float32, error) {
	panic("not implemented")
}

func (p *fakeModulesProvider) ExploreArguments(schema *models.Schema) map[string]*graphql.ArgumentConfig {
	args := map[string]*graphql.ArgumentConfig{}
	txt2vec := &nearCustomTextModule{}
	for _, c := range schema.Classes {
		if c.Vectorizer == txt2vec.Name() {
			for name, argument := range txt2vec.Arguments() {
				args[name] = argument.ExploreArgumentsFunction()
			}
		}
	}
	return args
}

func (p *fakeModulesProvider) CrossClassExtractSearchParams(arguments map[string]interface{}) map[string]interface{} {
	exractedParams := map[string]interface{}{}
	if param, ok := arguments["nearCustomText"]; ok {
		exractedParams["nearCustomText"] = extractNearCustomTextParam(param.(map[string]interface{}))
	}
	return exractedParams
}

func extractNearCustomTextParam(param map[string]interface{}) interface{} {
	nearCustomText := &nearCustomTextModule{}
	argument := nearCustomText.Arguments()["nearCustomText"]
	return argument.ExtractFunction(param)
}

func getFakeModulesProvider() ModulesProvider {
	return &fakeModulesProvider{}
}

func newMockResolver() *mockResolver {
	field := Build(testhelper.SimpleSchema.Objects, getFakeModulesProvider())
	mocker := &mockResolver{}
	mockLog := &mockRequestsLog{}
	mocker.RootFieldName = "Explore"
	mocker.RootField = field
	mocker.RootObject = map[string]interface{}{
		"Resolver":    Resolver(mocker),
		"RequestsLog": mockLog,
	}
	return mocker
}

func newMockResolverNoModules() *mockResolver {
	field := Build(testhelper.SimpleSchema.Objects, nil)
	mocker := &mockResolver{}
	mockLog := &mockRequestsLog{}
	mocker.RootFieldName = "Explore"
	mocker.RootField = field
	mocker.RootObject = map[string]interface{}{
		"Resolver":    Resolver(mocker),
		"RequestsLog": mockLog,
	}
	return mocker
}

func newMockResolverEmptySchema() *mockResolver {
	field := Build(&models.Schema{}, getFakeModulesProvider())
	mocker := &mockResolver{}
	mockLog := &mockRequestsLog{}
	mocker.RootFieldName = "Explore"
	mocker.RootField = field
	mocker.RootObject = map[string]interface{}{
		"Resolver":    Resolver(mocker),
		"RequestsLog": mockLog,
	}
	return mocker
}

func (m *mockResolver) Explore(ctx context.Context,
	principal *models.Principal, params traverser.ExploreParams,
) ([]search.Result, error) {
	args := m.Called(params)
	return args.Get(0).([]search.Result), args.Error(1)
}

type nearCustomTextParams struct {
	Values       []string
	MoveTo       nearExploreMove
	MoveAwayFrom nearExploreMove
	Certainty    float64
	Distance     float64
	WithDistance bool
}

type nearExploreMove struct {
	Values  []string
	Force   float32
	Objects []nearObjectMove
}

type nearObjectMove struct {
	ID     string
	Beacon string
}

type nearCustomTextModule struct{}

func (m *nearCustomTextModule) Name() string {
	return "text2vec-contextionary"
}

func (m *nearCustomTextModule) Init(params moduletools.ModuleInitParams) error {
	return nil
}

func (m *nearCustomTextModule) RootHandler() http.Handler {
	return nil
}

func (m *nearCustomTextModule) Arguments() map[string]modulecapabilities.GraphQLArgument {
	arguments := map[string]modulecapabilities.GraphQLArgument{}
	// define nearCustomText argument
	arguments["nearCustomText"] = modulecapabilities.GraphQLArgument{
		GetArgumentsFunction: func(classname string) *graphql.ArgumentConfig {
			return m.getNearCustomTextArgument(classname)
		},
		ExploreArgumentsFunction: func() *graphql.ArgumentConfig {
			return m.getNearCustomTextArgument("")
		},
		ExtractFunction: func(source map[string]interface{}) interface{} {
			return m.extractNearCustomTextArgument(source)
		},
		ValidateFunction: func(param interface{}) error {
			// all is valid
			return nil
		},
	}
	return arguments
}

func (m *nearCustomTextModule) getNearCustomTextArgument(classname string) *graphql.ArgumentConfig {
	prefix := classname
	return &graphql.ArgumentConfig{
		Type: graphql.NewInputObject(
			graphql.InputObjectConfig{
				Name: fmt.Sprintf("%sNearCustomTextInpObj", prefix),
				Fields: graphql.InputObjectConfigFieldMap{
					"concepts": &graphql.InputObjectFieldConfig{
						Type: graphql.NewNonNull(graphql.NewList(graphql.String)),
					},
					"moveTo": &graphql.InputObjectFieldConfig{
						Description: descriptions.VectorMovement,
						Type: graphql.NewInputObject(
							graphql.InputObjectConfig{
								Name: fmt.Sprintf("%sMoveTo", prefix),
								Fields: graphql.InputObjectConfigFieldMap{
									"concepts": &graphql.InputObjectFieldConfig{
										Description: descriptions.Keywords,
										Type:        graphql.NewList(graphql.String),
									},
									"objects": &graphql.InputObjectFieldConfig{
										Description: "objects",
										Type: graphql.NewList(graphql.NewInputObject(
											graphql.InputObjectConfig{
												Name: fmt.Sprintf("%sMovementObjectsToInpObj", prefix),
												Fields: graphql.InputObjectConfigFieldMap{
													"id": &graphql.InputObjectFieldConfig{
														Type:        graphql.String,
														Description: "id of an object",
													},
													"beacon": &graphql.InputObjectFieldConfig{
														Type:        graphql.String,
														Description: descriptions.Beacon,
													},
												},
												Description: "Movement Object",
											},
										)),
									},
									"force": &graphql.InputObjectFieldConfig{
										Description: descriptions.Force,
										Type:        graphql.NewNonNull(graphql.Float),
									},
								},
							}),
					},
					"moveAwayFrom": &graphql.InputObjectFieldConfig{
						Description: descriptions.VectorMovement,
						Type: graphql.NewInputObject(
							graphql.InputObjectConfig{
								Name: fmt.Sprintf("%sMoveAway", prefix),
								Fields: graphql.InputObjectConfigFieldMap{
									"concepts": &graphql.InputObjectFieldConfig{
										Description: descriptions.Keywords,
										Type:        graphql.NewList(graphql.String),
									},
									"objects": &graphql.InputObjectFieldConfig{
										Description: "objects",
										Type: graphql.NewList(graphql.NewInputObject(
											graphql.InputObjectConfig{
												Name: fmt.Sprintf("%sMovementObjectsAwayInpObj", prefix),
												Fields: graphql.InputObjectConfigFieldMap{
													"id": &graphql.InputObjectFieldConfig{
														Type:        graphql.String,
														Description: "id of an object",
													},
													"beacon": &graphql.InputObjectFieldConfig{
														Type:        graphql.String,
														Description: descriptions.Beacon,
													},
												},
												Description: "Movement Object",
											},
										)),
									},
									"force": &graphql.InputObjectFieldConfig{
										Description: descriptions.Force,
										Type:        graphql.NewNonNull(graphql.Float),
									},
								},
							}),
					},
					"certainty": &graphql.InputObjectFieldConfig{
						Description: descriptions.Certainty,
						Type:        graphql.Float,
					},
					"distance": &graphql.InputObjectFieldConfig{
						Description: descriptions.Distance,
						Type:        graphql.Float,
					},
				},
				Description: descriptions.GetWhereInpObj,
			},
		),
	}
}

func (m *nearCustomTextModule) extractNearCustomTextArgument(source map[string]interface{}) *nearCustomTextParams {
	var args nearCustomTextParams

	concepts := source["concepts"].([]interface{})
	args.Values = make([]string, len(concepts))
	for i, value := range concepts {
		args.Values[i] = value.(string)
	}

	certainty, ok := source["certainty"]
	if ok {
		args.Certainty = certainty.(float64)
	}

	distance, ok := source["distance"]
	if ok {
		args.Distance = distance.(float64)
		args.WithDistance = true
	}

	// moveTo is an optional arg, so it could be nil
	moveTo, ok := source["moveTo"]
	if ok {
		moveToMap := moveTo.(map[string]interface{})
		args.MoveTo = m.parseMoveParam(moveToMap)
	}

	moveAwayFrom, ok := source["moveAwayFrom"]
	if ok {
		moveAwayFromMap := moveAwayFrom.(map[string]interface{})
		args.MoveAwayFrom = m.parseMoveParam(moveAwayFromMap)
	}

	return &args
}

func (m *nearCustomTextModule) parseMoveParam(source map[string]interface{}) nearExploreMove {
	res := nearExploreMove{}
	res.Force = float32(source["force"].(float64))

	concepts, ok := source["concepts"].([]interface{})
	if ok {
		res.Values = make([]string, len(concepts))
		for i, value := range concepts {
			res.Values[i] = value.(string)
		}
	}

	objects, ok := source["objects"].([]interface{})
	if ok {
		res.Objects = make([]nearObjectMove, len(objects))
		for i, value := range objects {
			v, ok := value.(map[string]interface{})
			if ok {
				if v["id"] != nil {
					res.Objects[i].ID = v["id"].(string)
				}
				if v["beacon"] != nil {
					res.Objects[i].Beacon = v["beacon"].(string)
				}
			}
		}
	}

	return res
}
