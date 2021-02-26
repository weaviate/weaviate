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

package modcontextionary

import (
	"context"
	"net/http"

	"github.com/graphql-go/graphql"
	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/state"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/moduletools"
	"github.com/semi-technologies/weaviate/modules/text2vec-contextionary/concepts"
	"github.com/semi-technologies/weaviate/modules/text2vec-contextionary/extensions"
	text2vecneartext "github.com/semi-technologies/weaviate/modules/text2vec-contextionary/neartext"
	localvectorizer "github.com/semi-technologies/weaviate/modules/text2vec-contextionary/vectorizer"
)

func New() *ContextionaryModule {
	return &ContextionaryModule{}
}

// ContextionaryModule for now only handles storage and retrival of extensions,
// but with making Weaviate more modular, this should contain anything related
// to the module
type ContextionaryModule struct {
	storageProvider moduletools.StorageProvider
	extensions      *extensions.RESTHandlers
	concepts        *concepts.RESTHandlers
	appState        *state.State
	vectorizer      *localvectorizer.Vectorizer
	configValidator configValidator
	graphqlProvider modulecapabilities.GraphQLArguments
	searcher        modulecapabilities.Searcher
}

type configValidator interface {
	Do(ctx context.Context, class *models.Class, cfg moduletools.ClassConfig,
		indexChecker localvectorizer.IndexChecker) error
}

func (m *ContextionaryModule) Name() string {
	return "text2vec-contextionary"
}

func (m *ContextionaryModule) Init(ctx context.Context,
	params moduletools.ModuleInitParams) error {
	m.storageProvider = params.GetStorageProvider()
	appState, ok := params.GetAppState().(*state.State)
	if !ok {
		return errors.Errorf("appState is not a *state.State")
	}
	m.appState = appState

	if err := m.initExtensions(); err != nil {
		return errors.Wrap(err, "init extensions")
	}

	if err := m.initConcepts(); err != nil {
		return errors.Wrap(err, "init concepts")
	}

	if err := m.initVectorizer(); err != nil {
		return errors.Wrap(err, "init vectorizer")
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
	m.extensions = extensions.NewRESTHandlers(uc, m.appState.Contextionary)

	return nil
}

func (m *ContextionaryModule) initConcepts() error {
	uc := localvectorizer.NewInspector(m.appState.Contextionary)
	m.concepts = concepts.NewRESTHandlers(uc)

	return nil
}

func (m *ContextionaryModule) initVectorizer() error {
	m.vectorizer = localvectorizer.New(m.appState.Contextionary)
	rc, ok := m.appState.Contextionary.(localvectorizer.RemoteClient)
	if !ok {
		return errors.Errorf("invalid contextionary remote client")
	}

	m.configValidator = localvectorizer.NewConfigValidator(rc)

	m.searcher = text2vecneartext.NewSearcher(m.vectorizer)

	return nil
}

func (m *ContextionaryModule) initGraphqlProvider() error {
	m.graphqlProvider = text2vecneartext.New()
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
	obj *models.Object, cfg moduletools.ClassConfig) error {
	icheck := localvectorizer.NewIndexChecker(cfg)
	return m.vectorizer.Object(ctx, obj, icheck)
}

func (m *ContextionaryModule) GetArguments(classname string) map[string]*graphql.ArgumentConfig {
	return m.graphqlProvider.GetArguments(classname)
}

func (m *ContextionaryModule) ExploreArguments() map[string]*graphql.ArgumentConfig {
	return m.graphqlProvider.ExploreArguments()
}

func (m *ContextionaryModule) ExtractFunctions() map[string]modulecapabilities.ExtractFn {
	return m.graphqlProvider.ExtractFunctions()
}

func (m *ContextionaryModule) ValidateFunctions() map[string]modulecapabilities.ValidateFn {
	return m.graphqlProvider.ValidateFunctions()
}

func (m *ContextionaryModule) VectorSearches() map[string]modulecapabilities.VectorForParams {
	return m.searcher.VectorSearches()
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.Vectorizer(New())
)
