//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package traverser

import (
	"context"
	"fmt"
	"net/http"

	"github.com/go-openapi/strfmt"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/semi-technologies/weaviate/entities/additional"
	"github.com/semi-technologies/weaviate/entities/aggregation"
	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/moduletools"
	"github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/search"
	"github.com/semi-technologies/weaviate/usecases/sharding"
	"github.com/stretchr/testify/mock"
)

type fakeLocks struct{}

func (f *fakeLocks) LockConnector() (func() error, error) {
	return func() error { return nil }, nil
}

func (f *fakeLocks) LockSchema() (func() error, error) {
	return func() error { return nil }, nil
}

type ClassIndexCheck interface {
	PropertyIndexed(property string) bool
	VectorizeClassName() bool
	VectorizePropertyName(propertyName string) bool
}

type fakeTxt2VecVectorizer struct{}

func (f *fakeTxt2VecVectorizer) Object(ctx context.Context, object *models.Object, icheck ClassIndexCheck) error {
	panic("not implemented")
}

func (f *fakeTxt2VecVectorizer) Corpi(ctx context.Context, corpi []string) ([]float32, error) {
	return []float32{1, 2, 3}, nil
}

func (f *fakeTxt2VecVectorizer) MoveTo(source []float32, target []float32, weight float32) ([]float32, error) {
	res := make([]float32, len(source))
	for i, v := range source {
		res[i] = v + 1
	}
	return res, nil
}

func (f *fakeTxt2VecVectorizer) MoveAwayFrom(source []float32, target []float32, weight float32) ([]float32, error) {
	res := make([]float32, len(source))
	for i, v := range source {
		res[i] = v - 0.5
	}
	return res, nil
}

type fakeVectorSearcher struct {
	mock.Mock
	calledWithVector []float32
	calledWithLimit  int
	results          []search.Result
}

func (f *fakeVectorSearcher) VectorSearch(ctx context.Context,
	vector []float32, limit int, filters *filters.LocalFilter) ([]search.Result, error) {
	f.calledWithVector = vector
	f.calledWithLimit = limit
	return f.results, nil
}

func (f *fakeVectorSearcher) Aggregate(ctx context.Context,
	params aggregation.Params) (*aggregation.Result, error) {
	args := f.Called(params)
	return args.Get(0).(*aggregation.Result), args.Error(1)
}

func (f *fakeVectorSearcher) VectorClassSearch(ctx context.Context,
	params GetParams) ([]search.Result, error) {
	args := f.Called(params)
	return args.Get(0).([]search.Result), args.Error(1)
}

func (f *fakeVectorSearcher) ClassSearch(ctx context.Context,
	params GetParams) ([]search.Result, error) {
	args := f.Called(params)
	return args.Get(0).([]search.Result), args.Error(1)
}

func (f *fakeVectorSearcher) ObjectByID(ctx context.Context, id strfmt.UUID,
	props search.SelectProperties, additional additional.Properties) (*search.Result, error) {
	args := f.Called(id)
	return args.Get(0).(*search.Result), args.Error(1)
}

type fakeAuthorizer struct{}

func (f *fakeAuthorizer) Authorize(principal *models.Principal, verb, resource string) error {
	return nil
}

type fakeVectorRepo struct {
	mock.Mock
}

func (f *fakeVectorRepo) PutObject(ctx context.Context, index string,
	concept *models.Object, vector []float32) error {
	return nil
}

func (f *fakeVectorRepo) VectorSearch(ctx context.Context,
	vector []float32, limit int, filters *filters.LocalFilter) ([]search.Result, error) {
	return nil, nil
}

func (f *fakeVectorRepo) Aggregate(ctx context.Context,
	params aggregation.Params) (*aggregation.Result, error) {
	args := f.Called(params)
	return args.Get(0).(*aggregation.Result), args.Error(1)
}

func (f *fakeVectorRepo) GetObject(ctx context.Context, uuid strfmt.UUID,
	res *models.Object) error {
	args := f.Called(uuid)
	*res = args.Get(0).(models.Object)
	return args.Error(1)
}

type fakeExplorer struct{}

func (f *fakeExplorer) GetClass(ctx context.Context, p GetParams) ([]interface{}, error) {
	return nil, nil
}

func (f *fakeExplorer) Concepts(ctx context.Context, p ExploreParams) ([]search.Result, error) {
	return nil, nil
}

type fakeSchemaGetter struct {
	schema schema.Schema
}

func (f *fakeSchemaGetter) GetSchemaSkipAuth() schema.Schema {
	return f.schema
}

func (f *fakeSchemaGetter) ShardingState(class string) *sharding.State {
	panic("not implemented")
}

type fakeInterpretation struct{}

func (f *fakeInterpretation) AdditionalPropertyFn(ctx context.Context,
	in []search.Result, params interface{}, limit *int,
	argumentModuleParams map[string]interface{}) ([]search.Result, error) {
	return in, nil
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
	argumentModuleParams map[string]interface{}) ([]search.Result, error) {
	return f.returnArgs, nil
}

func (f *fakeExtender) ExtractAdditionalFn(param []*ast.Argument) interface{} {
	return nil
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
	argumentModuleParams map[string]interface{}) ([]search.Result, error) {
	return f.returnArgs, nil
}

func (f *fakeProjector) ExtractAdditionalFn(param []*ast.Argument) interface{} {
	return nil
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
	argumentModuleParams map[string]interface{}) ([]search.Result, error) {
	return f.returnArgs, nil
}

func (f *fakePathBuilder) ExtractAdditionalFn(param []*ast.Argument) interface{} {
	return nil
}

func (f *fakePathBuilder) AdditonalPropertyDefaultValue() interface{} {
	return &pathBuilderParams{}
}

type fakeText2vecContextionaryModule struct {
	customExtender       *fakeExtender
	customProjector      *fakeProjector
	customPathBuilder    *fakePathBuilder
	customInterpretation *fakeInterpretation
}

func newFakeText2vecContextionaryModuleWithCustomExtender(
	customExtender *fakeExtender,
	customProjector *fakeProjector,
	customPathBuilder *fakePathBuilder,
) *fakeText2vecContextionaryModule {
	return &fakeText2vecContextionaryModule{customExtender, customProjector, customPathBuilder, &fakeInterpretation{}}
}

func (m *fakeText2vecContextionaryModule) Name() string {
	return "text2vec-contextionary"
}

func (m *fakeText2vecContextionaryModule) Init(params moduletools.ModuleInitParams) error {
	return nil
}

func (m *fakeText2vecContextionaryModule) RootHandler() http.Handler {
	return nil
}

func (m *fakeText2vecContextionaryModule) Arguments() map[string]modulecapabilities.GraphQLArgument {
	return newNearCustomTextModule(m.getExtender(), m.getProjector(), m.getPathBuilder(), m.getInterpretation()).Arguments()
}

func (m *fakeText2vecContextionaryModule) VectorSearches() map[string]modulecapabilities.VectorForParams {
	searcher := &fakeSearcher{&fakeTxt2VecVectorizer{}}
	return searcher.VectorSearches()
}

func (m *fakeText2vecContextionaryModule) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	return newNearCustomTextModule(m.getExtender(), m.getProjector(), m.getPathBuilder(), m.getInterpretation()).AdditionalProperties()
}

func (m *fakeText2vecContextionaryModule) getExtender() *fakeExtender {
	if m.customExtender != nil {
		return m.customExtender
	}
	return &fakeExtender{}
}

func (m *fakeText2vecContextionaryModule) getProjector() *fakeProjector {
	if m.customProjector != nil {
		return m.customProjector
	}
	return &fakeProjector{}
}

func (m *fakeText2vecContextionaryModule) getPathBuilder() *fakePathBuilder {
	if m.customPathBuilder != nil {
		return m.customPathBuilder
	}
	return &fakePathBuilder{}
}

func (m *fakeText2vecContextionaryModule) getInterpretation() *fakeInterpretation {
	if m.customInterpretation != nil {
		return m.customInterpretation
	}
	return &fakeInterpretation{}
}

type nearCustomTextParams struct {
	Values       []string
	MoveTo       nearExploreMove
	MoveAwayFrom nearExploreMove
	Certainty    float64
}

func (p nearCustomTextParams) GetCertainty() float64 {
	return p.Certainty
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
	fakeExtender       *fakeExtender
	fakeProjector      *fakeProjector
	fakePathBuilder    *fakePathBuilder
	fakeInterpretation *fakeInterpretation
}

func newNearCustomTextModule(
	fakeExtender *fakeExtender,
	fakeProjector *fakeProjector,
	fakePathBuilder *fakePathBuilder,
	fakeInterpretation *fakeInterpretation,
) *nearCustomTextModule {
	return &nearCustomTextModule{fakeExtender, fakeProjector, fakePathBuilder, fakeInterpretation}
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
		ExploreArgumentsFunction: func() *graphql.ArgumentConfig {
			return m.getNearCustomTextArgument("")
		},
		ExtractFunction: func(source map[string]interface{}) interface{} {
			return m.extractNearCustomTextArgument(source)
		},
		ValidateFunction: func(param interface{}) error {
			nearText, ok := param.(*nearCustomTextParams)
			if !ok {
				return errors.New("'nearCustomText' invalid parameter")
			}

			if nearText.MoveTo.Force > 0 &&
				nearText.MoveTo.Values == nil && nearText.MoveTo.Objects == nil {
				return errors.Errorf("'nearCustomText.moveTo' parameter " +
					"needs to have defined either 'concepts' or 'objects' fields")
			}

			if nearText.MoveAwayFrom.Force > 0 &&
				nearText.MoveAwayFrom.Values == nil && nearText.MoveAwayFrom.Objects == nil {
				return errors.Errorf("'nearCustomText.moveAwayFrom' parameter " +
					"needs to have defined either 'concepts' or 'objects' fields")
			}
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
		SearchFunctions: modulecapabilities.AdditionalSearch{
			ObjectList:  m.fakeProjector.AdditionalPropertyFn,
			ExploreGet:  m.fakeProjector.AdditionalPropertyFn,
			ExploreList: m.fakeProjector.AdditionalPropertyFn,
		},
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
		SearchFunctions: modulecapabilities.AdditionalSearch{
			ObjectGet:   m.fakeExtender.AdditionalPropertyFn,
			ObjectList:  m.fakeExtender.AdditionalPropertyFn,
			ExploreGet:  m.fakeExtender.AdditionalPropertyFn,
			ExploreList: m.fakeExtender.AdditionalPropertyFn,
		},
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
		SearchFunctions: modulecapabilities.AdditionalSearch{
			ExploreGet: m.fakePathBuilder.AdditionalPropertyFn,
		},
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
		SearchFunctions: modulecapabilities.AdditionalSearch{
			ObjectGet:   m.fakeInterpretation.AdditionalPropertyFn,
			ObjectList:  m.fakeInterpretation.AdditionalPropertyFn,
			ExploreGet:  m.fakeInterpretation.AdditionalPropertyFn,
			ExploreList: m.fakeInterpretation.AdditionalPropertyFn,
		},
	}
}

func (m *nearCustomTextModule) VectorSearches() map[string]modulecapabilities.VectorForParams {
	vectorSearches := map[string]modulecapabilities.VectorForParams{}
	return vectorSearches
}

type fakeSearcher struct {
	vectorizer *fakeTxt2VecVectorizer
}

func (s *fakeSearcher) VectorSearches() map[string]modulecapabilities.VectorForParams {
	vectorSearches := map[string]modulecapabilities.VectorForParams{}
	vectorSearches["nearCustomText"] = s.vectorForNearTextParam
	return vectorSearches
}

func (s *fakeSearcher) vectorForNearTextParam(ctx context.Context, params interface{},
	findVectorFn modulecapabilities.FindVectorFn, cfg moduletools.ClassConfig) ([]float32, error) {
	vector, err := s.vectorizer.Corpi(ctx, nil)
	if err != nil {
		return nil, err
	}

	p, ok := params.(*nearCustomTextParams)
	if ok && p.MoveTo.Force > 0 {
		afterMoveTo, err := s.vectorizer.MoveTo(vector, nil, 0)
		if err != nil {
			return nil, err
		}
		vector = afterMoveTo
	}
	if ok && p.MoveAwayFrom.Force > 0 {
		afterMoveAway, err := s.vectorizer.MoveAwayFrom(vector, nil, 0)
		if err != nil {
			return nil, err
		}
		vector = afterMoveAway
	}
	return vector, nil
}
