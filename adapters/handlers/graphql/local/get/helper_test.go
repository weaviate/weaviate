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

package get

import (
	"context"
	"fmt"
	"net/http"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/tailor-inc/graphql"
	"github.com/tailor-inc/graphql/language/ast"
	"github.com/weaviate/weaviate/adapters/handlers/graphql/descriptions"
	test_helper "github.com/weaviate/weaviate/adapters/handlers/graphql/test/helper"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/usecases/config"
)

type mockRequestsLog struct{}

func (m *mockRequestsLog) Register(first string, second string) {
}

type mockResolver struct {
	test_helper.MockResolver
}

type fakeInterpretation struct {
	returnArgs []search.Result
}

func (f *fakeInterpretation) AdditionalPropertyFn(ctx context.Context,
	in []search.Result, params interface{}, limit *int,
	argumentModuleParams map[string]interface{},
) ([]search.Result, error) {
	return f.returnArgs, nil
}

func (f *fakeInterpretation) ExtractAdditionalFn(param []*ast.Argument) interface{} {
	return true
}

func (f *fakeInterpretation) AdditonalPropertyDefaultValue() interface{} {
	return true
}

type fakeExtender struct {
	returnArgs []search.Result
}

func (f *fakeExtender) AdditionalPropertyFn(ctx context.Context,
	in []search.Result, params interface{}, limit *int,
	argumentModuleParams map[string]interface{},
) ([]search.Result, error) {
	return f.returnArgs, nil
}

func (f *fakeExtender) ExtractAdditionalFn(param []*ast.Argument) interface{} {
	return true
}

func (f *fakeExtender) AdditonalPropertyDefaultValue() interface{} {
	return true
}

type fakeProjectorParams struct {
	Enabled          bool
	Algorithm        string
	Dimensions       int
	Perplexity       int
	Iterations       int
	LearningRate     int
	IncludeNeighbors bool
}

type fakeProjector struct {
	returnArgs []search.Result
}

func (f *fakeProjector) AdditionalPropertyFn(ctx context.Context,
	in []search.Result, params interface{}, limit *int,
	argumentModuleParams map[string]interface{},
) ([]search.Result, error) {
	return f.returnArgs, nil
}

func (f *fakeProjector) ExtractAdditionalFn(param []*ast.Argument) interface{} {
	if len(param) > 0 {
		return &fakeProjectorParams{
			Enabled:      true,
			Algorithm:    "tsne",
			Dimensions:   3,
			Iterations:   100,
			LearningRate: 15,
			Perplexity:   10,
		}
	}
	return &fakeProjectorParams{
		Enabled: true,
	}
}

func (f *fakeProjector) AdditonalPropertyDefaultValue() interface{} {
	return &fakeProjectorParams{}
}

type pathBuilderParams struct{}

type fakePathBuilder struct {
	returnArgs []search.Result
}

func (f *fakePathBuilder) AdditionalPropertyFn(ctx context.Context,
	in []search.Result, params interface{}, limit *int,
) ([]search.Result, error) {
	return f.returnArgs, nil
}

func (f *fakePathBuilder) ExtractAdditionalFn(param []*ast.Argument) interface{} {
	return &pathBuilderParams{}
}

func (f *fakePathBuilder) AdditonalPropertyDefaultValue() interface{} {
	return &pathBuilderParams{}
}

type nearCustomTextParams struct {
	Values       []string
	MoveTo       nearExploreMove
	MoveAwayFrom nearExploreMove
	Certainty    float64
	Distance     float64
	WithDistance bool
}

// implements the modulecapabilities.NearParam interface
func (n *nearCustomTextParams) GetCertainty() float64 {
	return n.Certainty
}

func (n nearCustomTextParams) GetDistance() float64 {
	return n.Distance
}

func (n nearCustomTextParams) SimilarityMetricProvided() bool {
	return n.Certainty != 0 || n.WithDistance
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

type nearCustomTextModule struct {
	fakePathBuilder    *fakePathBuilder
	fakeProjector      *fakeProjector
	fakeExtender       *fakeExtender
	fakeInterpretation *fakeInterpretation
}

func newNearCustomTextModule() *nearCustomTextModule {
	return &nearCustomTextModule{
		fakePathBuilder:    &fakePathBuilder{},
		fakeProjector:      &fakeProjector{},
		fakeExtender:       &fakeExtender{},
		fakeInterpretation: &fakeInterpretation{},
	}
}

func (m *nearCustomTextModule) Name() string {
	return "mock-custom-near-text-module"
}

func (m *nearCustomTextModule) Init(params moduletools.ModuleInitParams) error {
	return nil
}

func (m *nearCustomTextModule) RootHandler() http.Handler {
	return nil
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

func (m *nearCustomTextModule) Arguments() map[string]modulecapabilities.GraphQLArgument {
	arguments := map[string]modulecapabilities.GraphQLArgument{}
	// define nearCustomText argument
	arguments["nearCustomText"] = modulecapabilities.GraphQLArgument{
		GetArgumentsFunction: func(classname string) *graphql.ArgumentConfig {
			return m.getNearCustomTextArgument(classname)
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

// additional properties
func (m *nearCustomTextModule) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	additionalProperties := map[string]modulecapabilities.AdditionalProperty{}
	additionalProperties["featureProjection"] = m.getFeatureProjection()
	additionalProperties["nearestNeighbors"] = m.getNearestNeighbors()
	additionalProperties["semanticPath"] = m.getSemanticPath()
	additionalProperties["interpretation"] = m.getInterpretation()
	return additionalProperties
}

func (m *nearCustomTextModule) getFeatureProjection() modulecapabilities.AdditionalProperty {
	return modulecapabilities.AdditionalProperty{
		DefaultValue: m.fakeProjector.AdditonalPropertyDefaultValue(),
		GraphQLNames: []string{"featureProjection"},
		GraphQLFieldFunction: func(classname string) *graphql.Field {
			return &graphql.Field{
				Args: graphql.FieldConfigArgument{
					"algorithm": &graphql.ArgumentConfig{
						Type:         graphql.String,
						DefaultValue: nil,
					},
					"dimensions": &graphql.ArgumentConfig{
						Type:         graphql.Int,
						DefaultValue: nil,
					},
					"learningRate": &graphql.ArgumentConfig{
						Type:         graphql.Int,
						DefaultValue: nil,
					},
					"iterations": &graphql.ArgumentConfig{
						Type:         graphql.Int,
						DefaultValue: nil,
					},
					"perplexity": &graphql.ArgumentConfig{
						Type:         graphql.Int,
						DefaultValue: nil,
					},
				},
				Type: graphql.NewObject(graphql.ObjectConfig{
					Name: fmt.Sprintf("%sAdditionalFeatureProjection", classname),
					Fields: graphql.Fields{
						"vector": &graphql.Field{Type: graphql.NewList(graphql.Float)},
					},
				}),
			}
		},
		GraphQLExtractFunction: m.fakeProjector.ExtractAdditionalFn,
	}
}

func (m *nearCustomTextModule) getNearestNeighbors() modulecapabilities.AdditionalProperty {
	return modulecapabilities.AdditionalProperty{
		DefaultValue: m.fakeExtender.AdditonalPropertyDefaultValue(),
		GraphQLNames: []string{"nearestNeighbors"},
		GraphQLFieldFunction: func(classname string) *graphql.Field {
			return &graphql.Field{
				Type: graphql.NewObject(graphql.ObjectConfig{
					Name: fmt.Sprintf("%sAdditionalNearestNeighbors", classname),
					Fields: graphql.Fields{
						"neighbors": &graphql.Field{Type: graphql.NewList(graphql.NewObject(graphql.ObjectConfig{
							Name: fmt.Sprintf("%sAdditionalNearestNeighborsNeighbors", classname),
							Fields: graphql.Fields{
								"concept":  &graphql.Field{Type: graphql.String},
								"distance": &graphql.Field{Type: graphql.Float},
							},
						}))},
					},
				}),
			}
		},
		GraphQLExtractFunction: m.fakeExtender.ExtractAdditionalFn,
	}
}

func (m *nearCustomTextModule) getSemanticPath() modulecapabilities.AdditionalProperty {
	return modulecapabilities.AdditionalProperty{
		DefaultValue: m.fakePathBuilder.AdditonalPropertyDefaultValue(),
		GraphQLNames: []string{"semanticPath"},
		GraphQLFieldFunction: func(classname string) *graphql.Field {
			return &graphql.Field{
				Type: graphql.NewObject(graphql.ObjectConfig{
					Name: fmt.Sprintf("%sAdditionalSemanticPath", classname),
					Fields: graphql.Fields{
						"path": &graphql.Field{Type: graphql.NewList(graphql.NewObject(graphql.ObjectConfig{
							Name: fmt.Sprintf("%sAdditionalSemanticPathElement", classname),
							Fields: graphql.Fields{
								"concept":            &graphql.Field{Type: graphql.String},
								"distanceToQuery":    &graphql.Field{Type: graphql.Float},
								"distanceToResult":   &graphql.Field{Type: graphql.Float},
								"distanceToNext":     &graphql.Field{Type: graphql.Float},
								"distanceToPrevious": &graphql.Field{Type: graphql.Float},
							},
						}))},
					},
				}),
			}
		},
		GraphQLExtractFunction: m.fakePathBuilder.ExtractAdditionalFn,
	}
}

func (m *nearCustomTextModule) getInterpretation() modulecapabilities.AdditionalProperty {
	return modulecapabilities.AdditionalProperty{
		DefaultValue: m.fakeInterpretation.AdditonalPropertyDefaultValue(),
		GraphQLNames: []string{"interpretation"},
		GraphQLFieldFunction: func(classname string) *graphql.Field {
			return &graphql.Field{
				Type: graphql.NewObject(graphql.ObjectConfig{
					Name: fmt.Sprintf("%sAdditionalInterpretation", classname),
					Fields: graphql.Fields{
						"source": &graphql.Field{Type: graphql.NewList(graphql.NewObject(graphql.ObjectConfig{
							Name: fmt.Sprintf("%sAdditionalInterpretationSource", classname),
							Fields: graphql.Fields{
								"concept":    &graphql.Field{Type: graphql.String},
								"weight":     &graphql.Field{Type: graphql.Float},
								"occurrence": &graphql.Field{Type: graphql.Int},
							},
						}))},
					},
				}),
			}
		},
		GraphQLExtractFunction: m.fakeInterpretation.ExtractAdditionalFn,
	}
}

type fakeModulesProvider struct {
	nearCustomTextModule *nearCustomTextModule
}

func newFakeModulesProvider() *fakeModulesProvider {
	return &fakeModulesProvider{newNearCustomTextModule()}
}

func (fmp *fakeModulesProvider) GetAll() []modulecapabilities.Module {
	panic("implement me")
}

func (fmp *fakeModulesProvider) VectorFromInput(ctx context.Context, className string, input string) ([]float32, error) {
	panic("not implemented")
}

func (fmp *fakeModulesProvider) GetArguments(class *models.Class) map[string]*graphql.ArgumentConfig {
	args := map[string]*graphql.ArgumentConfig{}
	if class.Vectorizer == fmp.nearCustomTextModule.Name() {
		for name, argument := range fmp.nearCustomTextModule.Arguments() {
			args[name] = argument.GetArgumentsFunction(class.Class)
		}
	}
	return args
}

func (fmp *fakeModulesProvider) ExtractSearchParams(arguments map[string]interface{}, className string) map[string]interface{} {
	exractedParams := map[string]interface{}{}
	if param, ok := arguments["nearCustomText"]; ok {
		exractedParams["nearCustomText"] = extractNearTextParam(param.(map[string]interface{}))
	}
	return exractedParams
}

func (fmp *fakeModulesProvider) GetAdditionalFields(class *models.Class) map[string]*graphql.Field {
	additionalProperties := map[string]*graphql.Field{}
	for name, additionalProperty := range fmp.nearCustomTextModule.AdditionalProperties() {
		if additionalProperty.GraphQLFieldFunction != nil {
			additionalProperties[name] = additionalProperty.GraphQLFieldFunction(class.Class)
		}
	}
	return additionalProperties
}

func (fmp *fakeModulesProvider) ExtractAdditionalField(className, name string, params []*ast.Argument) interface{} {
	if additionalProperties := fmp.nearCustomTextModule.AdditionalProperties(); len(additionalProperties) > 0 {
		if additionalProperty, ok := additionalProperties[name]; ok {
			if additionalProperty.GraphQLExtractFunction != nil {
				return additionalProperty.GraphQLExtractFunction(params)
			}
		}
	}
	return nil
}

func (fmp *fakeModulesProvider) GraphQLAdditionalFieldNames() []string {
	additionalPropertiesNames := []string{}
	for _, additionalProperty := range fmp.nearCustomTextModule.AdditionalProperties() {
		if additionalProperty.GraphQLNames != nil {
			additionalPropertiesNames = append(additionalPropertiesNames, additionalProperty.GraphQLNames...)
		}
	}
	return additionalPropertiesNames
}

func extractNearTextParam(param map[string]interface{}) interface{} {
	nearCustomTextModule := newNearCustomTextModule()
	argument := nearCustomTextModule.Arguments()["nearCustomText"]
	return argument.ExtractFunction(param)
}

func createArg(name string, value string) *ast.Argument {
	n := ast.Name{
		Value: name,
	}
	val := ast.StringValue{
		Kind:  "Kind",
		Value: value,
	}
	arg := ast.Argument{
		Name:  ast.NewName(&n),
		Kind:  "Kind",
		Value: ast.NewStringValue(&val),
	}
	a := ast.NewArgument(&arg)
	return a
}

func extractAdditionalParam(name string, args []*ast.Argument) interface{} {
	nearCustomTextModule := newNearCustomTextModule()
	additionalProperties := nearCustomTextModule.AdditionalProperties()
	switch name {
	case "semanticPath", "featureProjection":
		if ap, ok := additionalProperties[name]; ok {
			return ap.GraphQLExtractFunction(args)
		}
		return nil
	default:
		return nil
	}
}

func getFakeModulesProvider() ModulesProvider {
	return newFakeModulesProvider()
}

func newMockResolver() *mockResolver {
	return newMockResolverWithVectorizer(config.VectorizerModuleText2VecContextionary)
}

func newMockResolverWithVectorizer(vectorizer string) *mockResolver {
	logger, _ := test.NewNullLogger()
	simpleSchema := test_helper.CreateSimpleSchema(vectorizer)
	field, err := Build(&simpleSchema, logger, getFakeModulesProvider())
	if err != nil {
		panic(fmt.Sprintf("could not build graphql test schema: %s", err))
	}
	mocker := &mockResolver{}
	mockLog := &mockRequestsLog{}
	mocker.RootFieldName = "Get"
	mocker.RootField = field
	mocker.RootObject = map[string]interface{}{"Resolver": Resolver(mocker), "RequestsLog": RequestsLog(mockLog)}
	return mocker
}

func newMockResolverWithNoModules() *mockResolver {
	logger, _ := test.NewNullLogger()
	field, err := Build(&test_helper.SimpleSchema, logger, nil)
	if err != nil {
		panic(fmt.Sprintf("could not build graphql test schema: %s", err))
	}
	mocker := &mockResolver{}
	mockLog := &mockRequestsLog{}
	mocker.RootFieldName = "Get"
	mocker.RootField = field
	mocker.RootObject = map[string]interface{}{"Resolver": Resolver(mocker), "RequestsLog": RequestsLog(mockLog)}
	return mocker
}

func (m *mockResolver) GetClass(ctx context.Context, principal *models.Principal,
	params dto.GetParams,
) ([]interface{}, error) {
	args := m.Called(params)
	return args.Get(0).([]interface{}), args.Error(1)
}
