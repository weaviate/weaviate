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

package modules

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/tailor-inc/graphql"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	enitiesSchema "github.com/weaviate/weaviate/entities/schema"
	ubackup "github.com/weaviate/weaviate/usecases/backup"
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
			withExtractFn("groupBy").
			withExtractFn("hybrid").
			withExtractFn("bm25").
			withExtractFn("offset").
			withExtractFn("after").
			withGraphQLArg("group", []string{"group"}).
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
		assert.Contains(t, err.Error(), "searcher: groupBy conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "searcher: hybrid conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "searcher: bm25 conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "searcher: limit conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "searcher: offset conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "searcher: after conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "rest api additional property: classification conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "rest api additional property: certainty conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "rest api additional property: distance conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "rest api additional property: id conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "graphql additional property: classification conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "graphql additional property: certainty conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "graphql additional property: distance conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "graphql additional property: id conflicts with weaviate's internal searcher in modules: [mod3]")
		assert.Contains(t, err.Error(), "graphql additional property: group conflicts with weaviate's internal searcher in modules: [mod3]")
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
		module := &dummyBackupModuleWithAltNames{}
		modulesProvider := NewProvider()
		modulesProvider.Register(module)

		modByName := modulesProvider.GetByName("SomeBackend")
		modByAltName1 := modulesProvider.GetByName("AltBackendName")
		modByAltName2 := modulesProvider.GetByName("YetAnotherBackendName")
		modMissing := modulesProvider.GetByName("DoesNotExist")

		assert.NotNil(t, modByName)
		assert.NotNil(t, modByAltName1)
		assert.NotNil(t, modByAltName2)
		assert.Nil(t, modMissing)
	})

	t.Run("should provide backup backend", func(t *testing.T) {
		module := &dummyBackupModuleWithAltNames{}
		modulesProvider := NewProvider()
		modulesProvider.Register(module)

		provider, ok := interface{}(modulesProvider).(ubackup.BackupBackendProvider)
		assert.True(t, ok)

		fmt.Printf("provider: %v\n", provider)

		backendByName, err1 := provider.BackupBackend("SomeBackend")
		backendByAltName, err2 := provider.BackupBackend("YetAnotherBackendName")

		assert.NotNil(t, backendByName)
		assert.Nil(t, err1)
		assert.NotNil(t, backendByAltName)
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
		dummyText2VecModuleNoCapabilities: newDummyText2VecModule(name),
		arguments:                         map[string]modulecapabilities.GraphQLArgument{},
	}
}

type dummyGraphQLModule struct {
	dummyText2VecModuleNoCapabilities
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

type dummyBackupModuleWithAltNames struct{}

func (m *dummyBackupModuleWithAltNames) Name() string {
	return "SomeBackend"
}

func (m *dummyBackupModuleWithAltNames) AltNames() []string {
	return []string{"AltBackendName", "YetAnotherBackendName"}
}

func (m *dummyBackupModuleWithAltNames) Init(ctx context.Context, params moduletools.ModuleInitParams) error {
	return nil
}

func (m *dummyBackupModuleWithAltNames) RootHandler() http.Handler {
	return nil
}

func (m *dummyBackupModuleWithAltNames) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Backup
}

func (m *dummyBackupModuleWithAltNames) HomeDir(backupID string) string {
	return ""
}

func (m *dummyBackupModuleWithAltNames) GetObject(ctx context.Context, backupID, key string) ([]byte, error) {
	return nil, nil
}

func (m *dummyBackupModuleWithAltNames) WriteToFile(ctx context.Context, backupID, key, destPath string) error {
	return nil
}

func (m *dummyBackupModuleWithAltNames) Write(ctx context.Context, backupID, key string, r io.ReadCloser) (int64, error) {
	return 0, nil
}

func (m *dummyBackupModuleWithAltNames) Read(ctx context.Context, backupID, key string, w io.WriteCloser) (int64, error) {
	return 0, nil
}

func (m *dummyBackupModuleWithAltNames) SourceDataPath() string {
	return ""
}

func (*dummyBackupModuleWithAltNames) IsExternal() bool {
	return true
}

func (m *dummyBackupModuleWithAltNames) PutFile(ctx context.Context, backupID, key, srcPath string) error {
	return nil
}

func (m *dummyBackupModuleWithAltNames) PutObject(ctx context.Context, backupID, key string, byes []byte) error {
	return nil
}

func (m *dummyBackupModuleWithAltNames) Initialize(ctx context.Context, backupID string) error {
	return nil
}
