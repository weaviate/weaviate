//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package modules

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/graphql-go/graphql"
	"github.com/semi-technologies/weaviate/adapters/repos/backups"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/moduletools"
	enitiesSchema "github.com/semi-technologies/weaviate/entities/schema"
	"github.com/semi-technologies/weaviate/entities/snapshots"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
)

func TestModulesProvider(t *testing.T) {
	t.Run("should register simple module", func(t *testing.T) {
		// given
		modulesProvider := NewProvider()
		class := &models.Class{
			Class:      "ClassOne",
			Vectorizer: "mod1",
		}
		schema := &models.Schema{
			Classes: []*models.Class{class},
		}
		schemaGetter := getFakeSchemaGetter()
		modulesProvider.SetSchemaGetter(schemaGetter)

		params := map[string]interface{}{}
		params["nearArgumentSomeParam"] = string("doesn't matter here")
		arguments := map[string]interface{}{}
		arguments["nearArgument"] = params

		// when
		modulesProvider.Register(newGraphQLModule("mod1").withArg("nearArgument"))
		logger, _ := test.NewNullLogger()
		err := modulesProvider.Init(context.Background(), nil, logger)
		registered := modulesProvider.GetAll()
		getArgs := modulesProvider.GetArguments(class)
		exploreArgs := modulesProvider.ExploreArguments(schema)
		extractedArgs := modulesProvider.ExtractSearchParams(arguments, class.Class)

		// then
		mod1 := registered[0]
		assert.Nil(t, err)
		assert.Equal(t, "mod1", mod1.Name())
		assert.NotNil(t, getArgs["nearArgument"])
		assert.NotNil(t, exploreArgs["nearArgument"])
		assert.NotNil(t, extractedArgs["nearArgument"])
	})

	t.Run("should not register modules providing the same search param", func(t *testing.T) {
		// given
		modulesProvider := NewProvider()
		schemaGetter := getFakeSchemaGetter()
		modulesProvider.SetSchemaGetter(schemaGetter)

		// when
		modulesProvider.Register(newGraphQLModule("mod1").withArg("nearArgument"))
		modulesProvider.Register(newGraphQLModule("mod2").withArg("nearArgument"))
		logger, _ := test.NewNullLogger()
		err := modulesProvider.Init(context.Background(), nil, logger)

		// then
		assert.Nil(t, err)
	})

	t.Run("should not register modules providing internal search param", func(t *testing.T) {
		// given
		modulesProvider := NewProvider()
		schemaGetter := getFakeSchemaGetter()
		modulesProvider.SetSchemaGetter(schemaGetter)

		// when
		modulesProvider.Register(newGraphQLModule("mod1").withArg("nearArgument"))
		modulesProvider.Register(newGraphQLModule("mod3").
			withExtractFn("limit").
			withExtractFn("where").
			withExtractFn("nearVector").
			withExtractFn("nearObject").
			withExtractFn("group"),
		)
		logger, _ := test.NewNullLogger()
		err := modulesProvider.Init(context.Background(), nil, logger)

		// then
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "nearObject conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "nearVector conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "where conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "group conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "limit conflicts with weaviate's internal searcher in modules: [mod3]")
	})

	t.Run("should not register modules providing faulty params", func(t *testing.T) {
		// given
		modulesProvider := NewProvider()
		schemaGetter := getFakeSchemaGetter()
		modulesProvider.SetSchemaGetter(schemaGetter)

		// when
		modulesProvider.Register(newGraphQLModule("mod1").withArg("nearArgument"))
		modulesProvider.Register(newGraphQLModule("mod2").withArg("nearArgument"))
		modulesProvider.Register(newGraphQLModule("mod3").
			withExtractFn("limit").
			withExtractFn("where").
			withExtractFn("nearVector").
			withExtractFn("nearObject").
			withExtractFn("group"),
		)
		logger, _ := test.NewNullLogger()
		err := modulesProvider.Init(context.Background(), nil, logger)

		// then
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "nearObject conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "nearVector conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "where conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "group conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "limit conflicts with weaviate's internal searcher in modules: [mod3]")
	})

	t.Run("should register simple additional property module", func(t *testing.T) {
		// given
		modulesProvider := NewProvider()
		class := &models.Class{
			Class:      "ClassOne",
			Vectorizer: "mod1",
		}
		schema := &models.Schema{
			Classes: []*models.Class{class},
		}
		schemaGetter := getFakeSchemaGetter()
		modulesProvider.SetSchemaGetter(schemaGetter)

		params := map[string]interface{}{}
		params["nearArgumentSomeParam"] = string("doesn't matter here")
		arguments := map[string]interface{}{}
		arguments["nearArgument"] = params

		// when
		modulesProvider.Register(newGraphQLAdditionalModule("mod1").
			withGraphQLArg("featureProjection", []string{"featureProjection"}).
			withGraphQLArg("interpretation", []string{"interpretation"}).
			withRestApiArg("featureProjection", []string{"featureProjection", "fp", "f-p"}).
			withRestApiArg("interpretation", []string{"interpretation"}).
			withArg("nearArgument"),
		)
		logger, _ := test.NewNullLogger()
		err := modulesProvider.Init(context.Background(), nil, logger)
		registered := modulesProvider.GetAll()
		getArgs := modulesProvider.GetArguments(class)
		exploreArgs := modulesProvider.ExploreArguments(schema)
		extractedArgs := modulesProvider.ExtractSearchParams(arguments, class.Class)
		restApiFPArgs := modulesProvider.RestApiAdditionalProperties("featureProjection", class)
		restApiInterpretationArgs := modulesProvider.RestApiAdditionalProperties("interpretation", class)
		graphQLArgs := modulesProvider.GraphQLAdditionalFieldNames()

		// then
		mod1 := registered[0]
		assert.Nil(t, err)
		assert.Equal(t, "mod1", mod1.Name())
		assert.NotNil(t, getArgs["nearArgument"])
		assert.NotNil(t, exploreArgs["nearArgument"])
		assert.NotNil(t, extractedArgs["nearArgument"])
		assert.NotNil(t, restApiFPArgs["featureProjection"])
		assert.NotNil(t, restApiInterpretationArgs["interpretation"])
		assert.Contains(t, graphQLArgs, "featureProjection")
		assert.Contains(t, graphQLArgs, "interpretation")
	})

	t.Run("should not register additional property modules providing the same params", func(t *testing.T) {
		// given
		modulesProvider := NewProvider()
		schemaGetter := getFakeSchemaGetter()
		modulesProvider.SetSchemaGetter(schemaGetter)

		// when
		modulesProvider.Register(newGraphQLAdditionalModule("mod1").
			withArg("nearArgument").
			withGraphQLArg("featureProjection", []string{"featureProjection"}).
			withRestApiArg("featureProjection", []string{"featureProjection", "fp", "f-p"}),
		)
		modulesProvider.Register(newGraphQLAdditionalModule("mod2").
			withArg("nearArgument").
			withGraphQLArg("featureProjection", []string{"featureProjection"}).
			withRestApiArg("featureProjection", []string{"featureProjection", "fp", "f-p"}),
		)
		logger, _ := test.NewNullLogger()
		err := modulesProvider.Init(context.Background(), nil, logger)

		// then
		assert.Nil(t, err)
	})

	t.Run("should not register additional property modules providing internal search param", func(t *testing.T) {
		// given
		modulesProvider := NewProvider()
		schemaGetter := getFakeSchemaGetter()
		modulesProvider.SetSchemaGetter(schemaGetter)

		// when
		modulesProvider.Register(newGraphQLAdditionalModule("mod1").withArg("nearArgument"))
		modulesProvider.Register(newGraphQLAdditionalModule("mod3").
			withExtractFn("limit").
			withExtractFn("where").
			withExtractFn("nearVector").
			withExtractFn("nearObject").
			withExtractFn("group").
			withGraphQLArg("classification", []string{"classification"}).
			withRestApiArg("classification", []string{"classification"}).
			withGraphQLArg("certainty", []string{"certainty"}).
			withRestApiArg("certainty", []string{"certainty"}).
			withGraphQLArg("distance", []string{"distance"}).
			withRestApiArg("distance", []string{"distance"}).
			withGraphQLArg("id", []string{"id"}).
			withRestApiArg("id", []string{"id"}),
		)
		logger, _ := test.NewNullLogger()
		err := modulesProvider.Init(context.Background(), nil, logger)

		// then
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "searcher: nearObject conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "searcher: nearVector conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "searcher: where conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "searcher: group conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "searcher: limit conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "rest api additional property: classification conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "rest api additional property: certainty conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "rest api additional property: distance conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "rest api additional property: id conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "graphql additional property: classification conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "graphql additional property: certainty conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "graphql additional property: distance conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "graphql additional property: id conflicts with weaviate's internal searcher in modules: [mod3]")
	})

	t.Run("should not register additional property modules providing faulty params", func(t *testing.T) {
		// given
		modulesProvider := NewProvider()
		schemaGetter := getFakeSchemaGetter()
		modulesProvider.SetSchemaGetter(schemaGetter)

		// when
		modulesProvider.Register(newGraphQLAdditionalModule("mod1").
			withArg("nearArgument").
			withGraphQLArg("semanticPath", []string{"semanticPath"}).
			withRestApiArg("featureProjection", []string{"featureProjection", "fp", "f-p"}),
		)
		modulesProvider.Register(newGraphQLAdditionalModule("mod2").
			withArg("nearArgument").
			withGraphQLArg("semanticPath", []string{"semanticPath"}).
			withRestApiArg("featureProjection", []string{"featureProjection", "fp", "f-p"}),
		)
		modulesProvider.Register(newGraphQLModule("mod3").
			withExtractFn("limit").
			withExtractFn("where").
			withExtractFn("nearVector").
			withExtractFn("nearObject").
			withExtractFn("group"),
		)
		modulesProvider.Register(newGraphQLAdditionalModule("mod4").
			withGraphQLArg("classification", []string{"classification"}).
			withRestApiArg("classification", []string{"classification"}).
			withGraphQLArg("certainty", []string{"certainty"}).
			withRestApiArg("certainty", []string{"certainty"}).
			withGraphQLArg("id", []string{"id"}).
			withRestApiArg("id", []string{"id"}),
		)
		logger, _ := test.NewNullLogger()
		err := modulesProvider.Init(context.Background(), nil, logger)

		// then
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "searcher: nearObject conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "searcher: nearVector conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "searcher: where conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "searcher: group conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "searcher: limit conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "rest api additional property: classification conflicts with weaviate's internal searcher in modules: [mod4]")
		assert.Contains(t, err.Error(), "rest api additional property: certainty conflicts with weaviate's internal searcher in modules: [mod4]")
		assert.Contains(t, err.Error(), "rest api additional property: id conflicts with weaviate's internal searcher in modules: [mod4]")
		assert.Contains(t, err.Error(), "graphql additional property: classification conflicts with weaviate's internal searcher in modules: [mod4]")
		assert.Contains(t, err.Error(), "graphql additional property: certainty conflicts with weaviate's internal searcher in modules: [mod4]")
		assert.Contains(t, err.Error(), "graphql additional property: id conflicts with weaviate's internal searcher in modules: [mod4]")
	})

	t.Run("should register module with alt names", func(t *testing.T) {
		module := &dummyStorageModuleWithAltNames{}
		modulesProvider := NewProvider()
		modulesProvider.Register(module)

		modByName := modulesProvider.GetByName("SomeStorage")
		modByAltName1 := modulesProvider.GetByName("AltStorageName")
		modByAltName2 := modulesProvider.GetByName("YetAnotherStorageName")
		modMissing := modulesProvider.GetByName("DoesNotExist")

		assert.NotNil(t, modByName)
		assert.NotNil(t, modByAltName1)
		assert.NotNil(t, modByAltName2)
		assert.Nil(t, modMissing)
	})

	t.Run("should provide backup storage", func(t *testing.T) {
		module := &dummyStorageModuleWithAltNames{}
		modulesProvider := NewProvider()
		modulesProvider.Register(module)

		provider, ok := interface{}(modulesProvider).(backups.BackupStorageProvider)
		assert.True(t, ok)

		fmt.Printf("provider: %v\n", provider)

		storageByName, err1 := provider.BackupStorage("SomeStorage")
		storageByAltName, err2 := provider.BackupStorage("YetAnotherStorageName")

		assert.NotNil(t, storageByName)
		assert.Nil(t, err1)
		assert.NotNil(t, storageByAltName)
		assert.Nil(t, err2)
	})
}

func fakeExtractFn(param map[string]interface{}) interface{} {
	extracted := map[string]interface{}{}
	extracted["nearArgumentParam"] = []string{"fake"}
	return extracted
}

func fakeValidateFn(param interface{}) error {
	return nil
}

func newGraphQLModule(name string) *dummyGraphQLModule {
	return &dummyGraphQLModule{
		dummyModuleNoCapabilities: newDummyModuleWithName(name),
		arguments:                 map[string]modulecapabilities.GraphQLArgument{},
	}
}

type dummyGraphQLModule struct {
	dummyModuleNoCapabilities
	arguments map[string]modulecapabilities.GraphQLArgument
}

func (m *dummyGraphQLModule) withArg(argName string) *dummyGraphQLModule {
	arg := modulecapabilities.GraphQLArgument{
		GetArgumentsFunction:     func(classname string) *graphql.ArgumentConfig { return &graphql.ArgumentConfig{} },
		ExploreArgumentsFunction: func() *graphql.ArgumentConfig { return &graphql.ArgumentConfig{} },
		ExtractFunction:          fakeExtractFn,
		ValidateFunction:         fakeValidateFn,
	}
	m.arguments[argName] = arg
	return m
}

func (m *dummyGraphQLModule) withExtractFn(argName string) *dummyGraphQLModule {
	arg := m.arguments[argName]
	arg.ExtractFunction = fakeExtractFn
	m.arguments[argName] = arg
	return m
}

func (m *dummyGraphQLModule) Arguments() map[string]modulecapabilities.GraphQLArgument {
	return m.arguments
}

func newGraphQLAdditionalModule(name string) *dummyAdditionalModule {
	return &dummyAdditionalModule{
		dummyGraphQLModule:   *newGraphQLModule(name),
		additionalProperties: map[string]modulecapabilities.AdditionalProperty{},
	}
}

type dummyAdditionalModule struct {
	dummyGraphQLModule
	additionalProperties map[string]modulecapabilities.AdditionalProperty
}

func (m *dummyAdditionalModule) withArg(argName string) *dummyAdditionalModule {
	m.dummyGraphQLModule.withArg(argName)
	return m
}

func (m *dummyAdditionalModule) withExtractFn(argName string) *dummyAdditionalModule {
	arg := m.dummyGraphQLModule.arguments[argName]
	arg.ExtractFunction = fakeExtractFn
	m.dummyGraphQLModule.arguments[argName] = arg
	return m
}

func (m *dummyAdditionalModule) withGraphQLArg(argName string, values []string) *dummyAdditionalModule {
	prop := m.additionalProperties[argName]
	if prop.GraphQLNames == nil {
		prop.GraphQLNames = []string{}
	}
	prop.GraphQLNames = append(prop.GraphQLNames, values...)

	m.additionalProperties[argName] = prop
	return m
}

func (m *dummyAdditionalModule) withRestApiArg(argName string, values []string) *dummyAdditionalModule {
	prop := m.additionalProperties[argName]
	if prop.RestNames == nil {
		prop.RestNames = []string{}
	}
	prop.RestNames = append(prop.RestNames, values...)
	prop.DefaultValue = 100

	m.additionalProperties[argName] = prop
	return m
}

func (m *dummyAdditionalModule) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	return m.additionalProperties
}

func getFakeSchemaGetter() schemaGetter {
	sch := enitiesSchema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{
					Class:      "ClassOne",
					Vectorizer: "mod1",
					ModuleConfig: map[string]interface{}{
						"mod": map[string]interface{}{
							"some-config": "some-config-value",
						},
					},
				},
				{
					Class:      "ClassTwo",
					Vectorizer: "mod2",
					ModuleConfig: map[string]interface{}{
						"mod": map[string]interface{}{
							"some-config": "some-config-value",
						},
					},
				},
				{
					Class:      "ClassThree",
					Vectorizer: "mod3",
					ModuleConfig: map[string]interface{}{
						"mod": map[string]interface{}{
							"some-config": "some-config-value",
						},
					},
				},
			},
		},
	}
	return &fakeSchemaGetter{schema: sch}
}

type dummyStorageModuleWithAltNames struct{}

func (m *dummyStorageModuleWithAltNames) Name() string {
	return "SomeStorage"
}

func (m *dummyStorageModuleWithAltNames) AltNames() []string {
	return []string{"AltStorageName", "YetAnotherStorageName"}
}

func (m *dummyStorageModuleWithAltNames) Init(ctx context.Context, params moduletools.ModuleInitParams) error {
	return nil
}

func (m *dummyStorageModuleWithAltNames) RootHandler() http.Handler {
	return nil
}

func (m *dummyStorageModuleWithAltNames) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Storage
}

func (m *dummyStorageModuleWithAltNames) StoreSnapshot(ctx context.Context, snapshot *snapshots.Snapshot) error {
	return nil
}

func (m *dummyStorageModuleWithAltNames) RestoreSnapshot(ctx context.Context, className, snapshotID string) (*snapshots.Snapshot, error) {
	return nil, nil
}

func (m *dummyStorageModuleWithAltNames) SetMetaStatus(ctx context.Context, className, snapshotID, status string) error {
	return nil
}

func (m *dummyStorageModuleWithAltNames) SetMetaError(ctx context.Context, className, snapshotID string, err error) error {
	return nil
}

func (m *dummyStorageModuleWithAltNames) GetMeta(ctx context.Context, className, snapshotID string) (*snapshots.Snapshot, error) {
	return nil, nil
}

func (m *dummyStorageModuleWithAltNames) DestinationPath(className, snapshotId string) string {
	return ""
}

func (m *dummyStorageModuleWithAltNames) InitSnapshot(ctx context.Context, className, snapshotID string) (*snapshots.Snapshot, error) {
	return nil, nil
}
