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

package modcontextionary

import (
	"context"
	"net/http"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/handlers/rest/state"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	text2vecadditional "github.com/weaviate/weaviate/modules/text2vec-contextionary/additional"
	text2vecinterpretation "github.com/weaviate/weaviate/modules/text2vec-contextionary/additional/interpretation"
	text2vecnn "github.com/weaviate/weaviate/modules/text2vec-contextionary/additional/nearestneighbors"
	text2vecprojector "github.com/weaviate/weaviate/modules/text2vec-contextionary/additional/projector"
	text2vecsempath "github.com/weaviate/weaviate/modules/text2vec-contextionary/additional/sempath"
	text2vecclassification "github.com/weaviate/weaviate/modules/text2vec-contextionary/classification"
	"github.com/weaviate/weaviate/modules/text2vec-contextionary/client"
	"github.com/weaviate/weaviate/modules/text2vec-contextionary/concepts"
	"github.com/weaviate/weaviate/modules/text2vec-contextionary/extensions"
	text2vecneartext "github.com/weaviate/weaviate/modules/text2vec-contextionary/neartext"
	"github.com/weaviate/weaviate/modules/text2vec-contextionary/vectorizer"
	localvectorizer "github.com/weaviate/weaviate/modules/text2vec-contextionary/vectorizer"
)

// MinimumRequiredRemoteVersion describes the minimal semver version
// (independent of the model version) of the remote model inference API
const MinimumRequiredRemoteVersion = "1.0.0"

func New() *ContextionaryModule {
	return &ContextionaryModule{}
}

// ContextionaryModule for now only handles storage and retrieval of extensions,
// but with making Weaviate more modular, this should contain anything related
// to the module
type ContextionaryModule struct {
	storageProvider              moduletools.StorageProvider
	extensions                   *extensions.RESTHandlers
	concepts                     *concepts.RESTHandlers
	vectorizer                   *localvectorizer.Vectorizer
	configValidator              configValidator
	graphqlProvider              modulecapabilities.GraphQLArguments
	additionalPropertiesProvider modulecapabilities.AdditionalProperties
	searcher                     modulecapabilities.Searcher
	remote                       remoteClient
	classifierContextual         modulecapabilities.Classifier
	logger                       logrus.FieldLogger
	nearTextTransformer          modulecapabilities.TextTransform
}

type remoteClient interface {
	localvectorizer.RemoteClient
	extensions.Proxy
	vectorizer.InspectorClient
	text2vecsempath.Remote
	modulecapabilities.MetaProvider
	modulecapabilities.VectorizerClient
	WaitForStartupAndValidateVersion(ctx context.Context, version string,
		interval time.Duration) error
}

type configValidator interface {
	Do(ctx context.Context, class *models.Class, cfg moduletools.ClassConfig,
		indexChecker localvectorizer.IndexChecker) error
}

func (m *ContextionaryModule) Name() string {
	return "text2vec-contextionary"
}

func (m *ContextionaryModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Text2Vec
}

func (m *ContextionaryModule) Init(ctx context.Context,
	params moduletools.ModuleInitParams,
) error {
	m.storageProvider = params.GetStorageProvider()
	appState, ok := params.GetAppState().(*state.State)
	if !ok {
		return errors.Errorf("appState is not a *state.State")
	}

	m.logger = appState.Logger

	url := appState.ServerConfig.Config.Contextionary.URL
	remote, err := client.NewClient(url, m.logger)
	if err != nil {
		return errors.Wrap(err, "init remote client")
	}
	m.remote = remote

	if err := m.remote.WaitForStartupAndValidateVersion(ctx,
		MinimumRequiredRemoteVersion, 1*time.Second); err != nil {
		return errors.Wrap(err, "validate remote inference api")
	}

	if err := m.initExtensions(); err != nil {
		return errors.Wrap(err, "init extensions")
	}

	if err := m.initConcepts(); err != nil {
		return errors.Wrap(err, "init concepts")
	}

	if err := m.initVectorizer(); err != nil {
		return errors.Wrap(err, "init vectorizer")
	}

	if err := m.initGraphqlAdditionalPropertiesProvider(); err != nil {
		return errors.Wrap(err, "init graphql additional properties provider")
	}

	if err := m.initClassifiers(); err != nil {
		return errors.Wrap(err, "init classifiers")
	}

	return nil
}

func (m *ContextionaryModule) InitExtension(modules []modulecapabilities.Module) error {
	for _, module := range modules {
		if module.Name() == m.Name() {
			continue
		}
		if arg, ok := module.(modulecapabilities.TextTransformers); ok {
			if arg != nil && arg.TextTransformers() != nil {
				m.nearTextTransformer = arg.TextTransformers()["nearText"]
			}
		}
	}

	if err := m.initGraphqlProvider(); err != nil {
		return errors.Wrap(err, "init graphql provider")
	}
	return nil
}

func (m *ContextionaryModule) initExtensions() error {
	storage, err := m.storageProvider.Storage("contextionary-extensions")
	if err != nil {
		return errors.Wrap(err, "initialize extensions storage")
	}

	uc := extensions.NewUseCase(storage)
	m.extensions = extensions.NewRESTHandlers(uc, m.remote)

	return nil
}

func (m *ContextionaryModule) initConcepts() error {
	uc := localvectorizer.NewInspector(m.remote)
	m.concepts = concepts.NewRESTHandlers(uc)

	return nil
}

func (m *ContextionaryModule) initVectorizer() error {
	m.vectorizer = localvectorizer.New(m.remote)
	m.configValidator = localvectorizer.NewConfigValidator(m.remote, m.logger)

	m.searcher = text2vecneartext.NewSearcher(m.vectorizer)

	return nil
}

func (m *ContextionaryModule) initGraphqlProvider() error {
	m.graphqlProvider = text2vecneartext.New(m.nearTextTransformer)
	return nil
}

func (m *ContextionaryModule) initGraphqlAdditionalPropertiesProvider() error {
	nnExtender := text2vecnn.NewExtender(m.remote)
	featureProjector := text2vecprojector.New()
	pathBuilder := text2vecsempath.New(m.remote)
	interpretation := text2vecinterpretation.New()
	m.additionalPropertiesProvider = text2vecadditional.New(nnExtender, featureProjector, pathBuilder, interpretation)
	return nil
}

func (m *ContextionaryModule) initClassifiers() error {
	m.classifierContextual = text2vecclassification.New(m.remote)
	return nil
}

func (m *ContextionaryModule) RootHandler() http.Handler {
	mux := http.NewServeMux()

	mux.Handle("/extensions-storage/", http.StripPrefix("/extensions-storage",
		m.extensions.StorageHandler()))
	mux.Handle("/extensions", http.StripPrefix("/extensions",
		m.extensions.UserFacingHandler()))
	mux.Handle("/concepts/", http.StripPrefix("/concepts", m.concepts.Handler()))

	return mux
}

func (m *ContextionaryModule) VectorizeObject(ctx context.Context,
	obj *models.Object, objDiff *moduletools.ObjectDiff, cfg moduletools.ClassConfig,
) error {
	icheck := localvectorizer.NewIndexChecker(cfg)
	return m.vectorizer.Object(ctx, obj, objDiff, icheck)
}

func (m *ContextionaryModule) VectorizeInput(ctx context.Context,
	input string, cfg moduletools.ClassConfig,
) ([]float32, error) {
	return m.vectorizer.Corpi(ctx, []string{input})
}

func (m *ContextionaryModule) Arguments() map[string]modulecapabilities.GraphQLArgument {
	return m.graphqlProvider.Arguments()
}

func (m *ContextionaryModule) VectorSearches() map[string]modulecapabilities.VectorForParams {
	return m.searcher.VectorSearches()
}

func (m *ContextionaryModule) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	return m.additionalPropertiesProvider.AdditionalProperties()
}

func (m *ContextionaryModule) Classifiers() []modulecapabilities.Classifier {
	return []modulecapabilities.Classifier{m.classifierContextual}
}

func (m *ContextionaryModule) MetaInfo() (map[string]interface{}, error) {
	return m.remote.MetaInfo()
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.Vectorizer(New())
	_ = modulecapabilities.InputVectorizer(New())
)
