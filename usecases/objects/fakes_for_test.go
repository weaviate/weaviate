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

package objects

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/tailor-inc/graphql"
	"github.com/tailor-inc/graphql/language/ast"

	"github.com/weaviate/weaviate/adapters/handlers/graphql/descriptions"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/entities/versioned"
)

const FindObjectFn = "func(context.Context, string, strfmt.UUID, " +
	"search.SelectProperties, additional.Properties, string) (*search.Result, error)"

type fakeSchemaManager struct {
	CalledWith struct {
		fromClass string
		property  string
		toClass   string
	}
	GetSchemaResponse schema.Schema
	GetschemaErr      error
	tenantsEnabled    bool
}

func (f *fakeSchemaManager) UpdatePropertyAddDataType(ctx context.Context, principal *models.Principal,
	fromClass, property, toClass string,
) error {
	f.CalledWith = struct {
		fromClass string
		property  string
		toClass   string
	}{
		fromClass: fromClass,
		property:  property,
		toClass:   toClass,
	}
	return nil
}

func (f *fakeSchemaManager) GetSchema(principal *models.Principal) (schema.Schema, error) {
	return f.GetSchemaResponse, f.GetschemaErr
}

func (f *fakeSchemaManager) GetConsistentSchema(ctx context.Context, principal *models.Principal, consistency bool) (schema.Schema, error) {
	return f.GetSchema(principal)
}

func (f *fakeSchemaManager) ShardOwner(class, shard string) (string, error) { return "", nil }

func (f *fakeSchemaManager) ShardFromUUID(class string, uuid []byte) string { return "" }

func (f *fakeSchemaManager) GetClass(ctx context.Context, principal *models.Principal,
	name string,
) (*models.Class, error) {
	if f.GetSchemaResponse.Objects == nil {
		return nil, f.GetschemaErr
	}
	for _, class := range f.GetSchemaResponse.Objects.Classes {
		if class.Class == name {
			return class, f.GetschemaErr
		}
	}
	return nil, f.GetschemaErr
}

func (f *fakeSchemaManager) GetConsistentClass(ctx context.Context, principal *models.Principal,
	name string, consistency bool,
) (*models.Class, uint64, error) {
	cls, err := f.GetClass(ctx, principal, name)
	return cls, 0, err
}

func (f *fakeSchemaManager) GetCachedClass(ctx context.Context,
	principal *models.Principal, names ...string,
) (map[string]versioned.Class, error) {
	res := map[string]versioned.Class{}
	for _, name := range names {
		cls, err := f.GetClass(ctx, principal, name)
		if err != nil {
			return res, err
		}
		res[name] = versioned.Class{Class: cls}
	}
	return res, nil
}

func (f *fakeSchemaManager) GetCachedClassNoAuth(ctx context.Context, names ...string,
) (map[string]versioned.Class, error) {
	res := map[string]versioned.Class{}
	for _, name := range names {
		cls, err := f.GetClass(ctx, nil, name)
		if err != nil {
			return res, err
		}
		res[name] = versioned.Class{Class: cls}
	}
	return f.GetCachedClass(ctx, nil, names...)
}

func (f *fakeSchemaManager) ReadOnlyClass(name string) *models.Class {
	c, err := f.GetClass(context.TODO(), nil, name)
	if err != nil {
		return nil
	}
	return c
}

func (f *fakeSchemaManager) AddClass(ctx context.Context, principal *models.Principal,
	class *models.Class,
) (*models.Class, uint64, error) {
	if f.GetSchemaResponse.Objects == nil {
		f.GetSchemaResponse.Objects = schema.Empty().Objects
	}
	class.VectorIndexConfig = hnsw.UserConfig{}
	class.VectorIndexType = "hnsw"
	class.Vectorizer = "none"
	classes := f.GetSchemaResponse.Objects.Classes
	if classes != nil {
		classes = append(classes, class)
	} else {
		classes = []*models.Class{class}
	}
	f.GetSchemaResponse.Objects.Classes = classes
	return class, 0, nil
}

func (f *fakeSchemaManager) AddClassProperty(ctx context.Context, principal *models.Principal,
	class *models.Class, className string, merge bool, newProps ...*models.Property,
) (*models.Class, uint64, error) {
	existing := map[string]int{}
	var existedClass *models.Class
	for _, c := range f.GetSchemaResponse.Objects.Classes {
		if c.Class == class.Class {
			existedClass = c
			for idx, p := range c.Properties {
				existing[strings.ToLower(p.Name)] = idx
			}
			break
		}
	}

	// update existed
	for _, prop := range newProps {
		if idx, exists := existing[strings.ToLower(prop.Name)]; exists {
			prop.NestedProperties, _ = schema.MergeRecursivelyNestedProperties(existedClass.Properties[idx].NestedProperties,
				prop.NestedProperties)
			existedClass.Properties[idx] = prop
		} else {
			existedClass.Properties = append(existedClass.Properties, prop)
		}
	}

	return class, 0, nil
}

func (f *fakeSchemaManager) AddTenants(ctx context.Context,
	principal *models.Principal, class string, tenants []*models.Tenant,
) (uint64, error) {
	f.tenantsEnabled = true
	return 0, nil
}

func (f *fakeSchemaManager) WaitForUpdate(ctx context.Context, schemaVersion uint64) error {
	return nil
}

func (f *fakeSchemaManager) StorageCandidates() []string {
	return []string{}
}

type fakeVectorRepo struct {
	mock.Mock
}

func (f *fakeVectorRepo) Exists(ctx context.Context, class string, id strfmt.UUID, repl *additional.ReplicationProperties, tenant string) (bool, error) {
	args := f.Called(class, id)
	return args.Bool(0), args.Error(1)
}

func (f *fakeVectorRepo) Object(ctx context.Context, cls string, id strfmt.UUID,
	props search.SelectProperties, additional additional.Properties,
	repl *additional.ReplicationProperties, tenant string,
) (*search.Result, error) {
	args := f.Called(cls, id, props, additional, tenant)
	if args.Get(0) != nil {
		return args.Get(0).(*search.Result), args.Error(1)
	}
	return nil, args.Error(1)
}

func (f *fakeVectorRepo) ObjectByID(ctx context.Context, id strfmt.UUID,
	props search.SelectProperties, additional additional.Properties,
	tenant string,
) (*search.Result, error) {
	args := f.Called(id, props, additional)
	if args.Get(0) != nil {
		return args.Get(0).(*search.Result), args.Error(1)
	}
	return nil, args.Error(1)
}

func (f *fakeVectorRepo) ObjectSearch(ctx context.Context, offset, limit int, filters *filters.LocalFilter,
	sort []filters.Sort, additional additional.Properties, tenant string,
) (search.Results, error) {
	args := f.Called(offset, limit, sort, filters, additional)
	return args.Get(0).([]search.Result), args.Error(1)
}

func (f *fakeVectorRepo) Query(ctx context.Context, q *QueryInput) (search.Results, *Error) {
	args := f.Called(q)
	var customEr *Error
	errors.As(args.Error(1), &customEr)
	return args.Get(0).([]search.Result), customEr
}

func (f *fakeVectorRepo) PutObject(ctx context.Context, concept *models.Object, vector []float32,
	vectors map[string][]float32, multiVectors map[string][][]float32, repl *additional.ReplicationProperties, schemaVersion uint64,
) error {
	args := f.Called(concept, vector)
	return args.Error(0)
}

func (f *fakeVectorRepo) BatchPutObjects(ctx context.Context, batch BatchObjects,
	repl *additional.ReplicationProperties, schemaVersion uint64,
) (BatchObjects, error) {
	args := f.Called(batch)
	return batch, args.Error(0)
}

func (f *fakeVectorRepo) AddBatchReferences(ctx context.Context, batch BatchReferences,
	repl *additional.ReplicationProperties, schemaVersion uint64,
) (BatchReferences, error) {
	args := f.Called(batch)
	return batch, args.Error(0)
}

func (f *fakeVectorRepo) BatchDeleteObjects(ctx context.Context, params BatchDeleteParams,
	deletionTime time.Time, repl *additional.ReplicationProperties, tenant string, schemaVersion uint64,
) (BatchDeleteResult, error) {
	args := f.Called(params)
	return args.Get(0).(BatchDeleteResult), args.Error(1)
}

func (f *fakeVectorRepo) Merge(ctx context.Context, merge MergeDocument, repl *additional.ReplicationProperties, tenant string, schemaVersion uint64) error {
	args := f.Called(merge)
	return args.Error(0)
}

func (f *fakeVectorRepo) DeleteObject(ctx context.Context, className string,
	id strfmt.UUID, deletionTime time.Time, repl *additional.ReplicationProperties, tenant string, schemaVersion uint64,
) error {
	args := f.Called(className, id, deletionTime)
	return args.Error(0)
}

func (f *fakeVectorRepo) AddReference(ctx context.Context, source *crossref.RefSource,
	target *crossref.Ref, repl *additional.ReplicationProperties, tenant string, schemaVersion uint64,
) error {
	args := f.Called(source, target)
	return args.Error(0)
}

func (f *fakeVectorRepo) ReferenceVectorSearch(ctx context.Context,
	obj *models.Object, refProps map[string]struct{},
) ([][]float32, error) {
	return nil, nil
}

type fakeExtender struct {
	multi []search.Result
}

func (f *fakeExtender) AdditionalPropertyFn(ctx context.Context,
	in []search.Result, params interface{}, limit *int,
	argumentModuleParams map[string]interface{}, cfg moduletools.ClassConfig,
) ([]search.Result, error) {
	return f.multi, nil
}

func (f *fakeExtender) ExtractAdditionalFn(param []*ast.Argument, class *models.Class) interface{} {
	return nil
}

func (f *fakeExtender) AdditionalPropertyDefaultValue() interface{} {
	return getDefaultParam("nearestNeighbors")
}

type fakeProjector struct {
	multi []search.Result
}

func (f *fakeProjector) AdditionalPropertyFn(ctx context.Context,
	in []search.Result, params interface{}, limit *int,
	argumentModuleParams map[string]interface{}, cfg moduletools.ClassConfig,
) ([]search.Result, error) {
	return f.multi, nil
}

func (f *fakeProjector) ExtractAdditionalFn(param []*ast.Argument, class *models.Class) interface{} {
	return nil
}

func (f *fakeProjector) AdditionalPropertyDefaultValue() interface{} {
	return getDefaultParam("featureProjection")
}

type fakePathBuilder struct {
	multi []search.Result
}

func (f *fakePathBuilder) AdditionalPropertyFn(ctx context.Context,
	in []search.Result, params interface{}, limit *int,
	argumentModuleParams map[string]interface{}, cfg moduletools.ClassConfig,
) ([]search.Result, error) {
	return f.multi, nil
}

func (f *fakePathBuilder) ExtractAdditionalFn(param []*ast.Argument, class *models.Class) interface{} {
	return nil
}

func (f *fakePathBuilder) AdditionalPropertyDefaultValue() interface{} {
	return getDefaultParam("semanticPath")
}

type fakeModulesProvider struct {
	mock.Mock
	customExtender  *fakeExtender
	customProjector *fakeProjector
}

func (p *fakeModulesProvider) GetObjectAdditionalExtend(ctx context.Context,
	in *search.Result, moduleParams map[string]interface{},
) (*search.Result, error) {
	res, err := p.additionalExtend(ctx, search.Results{*in}, moduleParams, "ObjectGet")
	if err != nil {
		return nil, err
	}
	return &res[0], nil
}

func (p *fakeModulesProvider) ListObjectsAdditionalExtend(ctx context.Context,
	in search.Results, moduleParams map[string]interface{},
) (search.Results, error) {
	return p.additionalExtend(ctx, in, moduleParams, "ObjectList")
}

func (p *fakeModulesProvider) UsingRef2Vec(moduleName string) bool {
	args := p.Called(moduleName)
	return args.Bool(0)
}

func (p *fakeModulesProvider) UpdateVector(ctx context.Context, object *models.Object, class *models.Class,
	findObjFn modulecapabilities.FindObjectFn, logger logrus.FieldLogger,
) error {
	args := p.Called(object, findObjFn)
	switch vec := args.Get(0).(type) {
	case models.C11yVector:
		object.Vector = vec
		return args.Error(1)
	case []float32:
		object.Vector = vec
		return args.Error(1)
	default:
		return args.Error(1)
	}
}

func (p *fakeModulesProvider) BatchUpdateVector(ctx context.Context, class *models.Class, objects []*models.Object,
	findObjectFn modulecapabilities.FindObjectFn,
	logger logrus.FieldLogger,
) (map[int]error, error) {
	args := p.Called()

	for _, obj := range objects {
		switch vec := args.Get(0).(type) {
		case models.C11yVector:
			obj.Vector = vec
		case []float32:
			obj.Vector = vec
		default:
		}
	}

	return nil, nil
}

func (p *fakeModulesProvider) VectorizerName(className string) (string, error) {
	args := p.Called(className)
	return args.String(0), args.Error(1)
}

func (p *fakeModulesProvider) additionalExtend(ctx context.Context,
	in search.Results, moduleParams map[string]interface{}, capability string,
) (search.Results, error) {
	txt2vec := newNearCustomTextModule(p.getExtender(), p.getProjector(), &fakePathBuilder{})
	additionalProperties := txt2vec.AdditionalProperties()
	if err := p.checkCapabilities(additionalProperties, moduleParams, capability); err != nil {
		return nil, err
	}
	for name, value := range moduleParams {
		additionalPropertyFn := p.getAdditionalPropertyFn(additionalProperties[name], capability)
		if additionalPropertyFn != nil && value != nil {
			resArray, err := additionalPropertyFn(ctx, in, nil, nil, nil, nil)
			if err != nil {
				return nil, err
			}
			in = resArray
		}
	}
	return in, nil
}

func (p *fakeModulesProvider) checkCapabilities(additionalProperties map[string]modulecapabilities.AdditionalProperty,
	moduleParams map[string]interface{}, capability string,
) error {
	for name := range moduleParams {
		additionalPropertyFn := p.getAdditionalPropertyFn(additionalProperties[name], capability)
		if additionalPropertyFn == nil {
			return errors.Errorf("unknown capability: %s", name)
		}
	}
	return nil
}

func (p *fakeModulesProvider) getAdditionalPropertyFn(additionalProperty modulecapabilities.AdditionalProperty,
	capability string,
) modulecapabilities.AdditionalPropertyFn {
	switch capability {
	case "ObjectGet":
		return additionalProperty.SearchFunctions.ObjectGet
	case "ObjectList":
		return additionalProperty.SearchFunctions.ObjectList
	case "ExploreGet":
		return additionalProperty.SearchFunctions.ExploreGet
	case "ExploreList":
		return additionalProperty.SearchFunctions.ExploreList
	default:
		return nil
	}
}

func (p *fakeModulesProvider) getExtender() *fakeExtender {
	if p.customExtender != nil {
		return p.customExtender
	}
	return &fakeExtender{}
}

func (p *fakeModulesProvider) getProjector() *fakeProjector {
	if p.customProjector != nil {
		return p.customProjector
	}
	return &fakeProjector{}
}

type nearCustomTextParams struct {
	Values    []string
	MoveTo    nearExploreMove
	Certainty float64
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
	fakeExtender    *fakeExtender
	fakeProjector   *fakeProjector
	fakePathBuilder *fakePathBuilder
}

func newNearCustomTextModule(
	fakeExtender *fakeExtender,
	fakeProjector *fakeProjector,
	fakePathBuilder *fakePathBuilder,
) *nearCustomTextModule {
	return &nearCustomTextModule{fakeExtender, fakeProjector, fakePathBuilder}
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
												Name: fmt.Sprintf("%sMovementObjectsInpObj", prefix),
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
		res := nearExploreMove{}
		res.Force = float32(moveToMap["force"].(float64))

		concepts, ok := moveToMap["concepts"].([]interface{})
		if ok {
			res.Values = make([]string, len(concepts))
			for i, value := range concepts {
				res.Values[i] = value.(string)
			}
		}

		objects, ok := moveToMap["objects"].([]interface{})
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

		args.MoveTo = res
	}

	return &args
}

func (m *nearCustomTextModule) Arguments() map[string]modulecapabilities.GraphQLArgument {
	arguments := map[string]modulecapabilities.GraphQLArgument{}
	// define nearCustomText argument
	arguments["nearCustomText"] = modulecapabilities.GraphQLArgument{
		GetArgumentsFunction: func(classname string) *graphql.ArgumentConfig {
			return m.getNearCustomTextArgument(classname)
		},
		ExtractFunction: func(source map[string]interface{}) (interface{}, *dto.TargetCombination, error) {
			params := m.extractNearCustomTextArgument(source)
			return params, nil, nil
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
	return additionalProperties
}

func (m *nearCustomTextModule) getFeatureProjection() modulecapabilities.AdditionalProperty {
	return modulecapabilities.AdditionalProperty{
		DefaultValue: m.fakeProjector.AdditionalPropertyDefaultValue(),
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
		DefaultValue: m.fakeExtender.AdditionalPropertyDefaultValue(),
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
		DefaultValue: m.fakePathBuilder.AdditionalPropertyDefaultValue(),
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

type fakeParams struct{}

func getDefaultParam(name string) interface{} {
	switch name {
	case "featureProjection", "semanticPath":
		return &fakeParams{}
	case "nearestNeighbors":
		return true
	default:
		return nil
	}
}

func getFakeModulesProvider(opts ...func(p *fakeModulesProvider)) *fakeModulesProvider {
	p := &fakeModulesProvider{}
	p.applyOptions(opts...)
	return p
}

func (p *fakeModulesProvider) applyOptions(opts ...func(provider *fakeModulesProvider)) {
	for _, opt := range opts {
		opt(p)
	}
}

func getFakeModulesProviderWithCustomExtenders(
	customExtender *fakeExtender,
	customProjector *fakeProjector,
	opts ...func(provider *fakeModulesProvider),
) *fakeModulesProvider {
	p := &fakeModulesProvider{mock.Mock{}, customExtender, customProjector}
	p.applyOptions(opts...)
	return p
}

type fakeMetrics struct {
	// Note: only those metric functions that relate to usage-related metrics are
	// covered by this mock, others are empty shells
	mock.Mock
}

func (f *fakeMetrics) BatchInc() {
}

func (f *fakeMetrics) BatchDec() {
}

func (f *fakeMetrics) BatchRefInc() {
}

func (f *fakeMetrics) BatchRefDec() {
}

func (f *fakeMetrics) BatchDeleteInc() {
}

func (f *fakeMetrics) BatchDeleteDec() {
}

func (f *fakeMetrics) AddObjectInc() {
}

func (f *fakeMetrics) AddObjectDec() {
}

func (f *fakeMetrics) UpdateObjectInc() {
}

func (f *fakeMetrics) UpdateObjectDec() {
}

func (f *fakeMetrics) MergeObjectInc() {
}

func (f *fakeMetrics) MergeObjectDec() {
}

func (f *fakeMetrics) DeleteObjectInc() {
}

func (f *fakeMetrics) DeleteObjectDec() {
}

func (f *fakeMetrics) GetObjectInc() {
}

func (f *fakeMetrics) GetObjectDec() {
}

func (f *fakeMetrics) HeadObjectInc() {
}

func (f *fakeMetrics) HeadObjectDec() {
}

func (f *fakeMetrics) AddReferenceInc() {
}

func (f *fakeMetrics) AddReferenceDec() {
}

func (f *fakeMetrics) UpdateReferenceInc() {
}

func (f *fakeMetrics) UpdateReferenceDec() {
}

func (f *fakeMetrics) DeleteReferenceInc() {
}

func (f *fakeMetrics) DeleteReferenceDec() {
}

func (f *fakeMetrics) AddUsageDimensions(className, queryType, op string, dims int) {
	f.Mock.MethodCalled("AddUsageDimensions", className, queryType, op, dims)
}
