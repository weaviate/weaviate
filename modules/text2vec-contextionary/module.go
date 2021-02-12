//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package modcontextionary

import (
	"context"
	"net/http"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/state"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/modules/text2vec-contextionary/concepts"
	"github.com/semi-technologies/weaviate/modules/text2vec-contextionary/extensions"
	localvectorizer "github.com/semi-technologies/weaviate/modules/text2vec-contextionary/vectorizer"
	"github.com/semi-technologies/weaviate/usecases/modules"
	"github.com/semi-technologies/weaviate/usecases/vectorizer"
)

func New() *ContextionaryModule {
	return &ContextionaryModule{}
}

// ContextionaryModule for now only handles storage and retrival of extensions,
// but with making Weaviate more modular, this should contain anything related
// to the module
type ContextionaryModule struct {
	storageProvider modules.StorageProvider
	extensions      *extensions.RESTHandlers
	concepts        *concepts.RESTHandlers
	appState        *state.State
	vectorizer      *vectorizer.Vectorizer
	configValidator configValidator
}

type configValidator interface {
	Do(class *models.Class, cfg modulecapabilities.ClassConfig,
		indexChecker localvectorizer.IndexChecker) error
}

func (m *ContextionaryModule) Name() string {
	return "text2vec-contextionary"
}

func (m *ContextionaryModule) Init(params modules.ModuleInitParams) error {
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
	uc := vectorizer.NewInspector(m.appState.Contextionary)
	m.concepts = concepts.NewRESTHandlers(uc)

	return nil
}

func (m *ContextionaryModule) initVectorizer() error {
	m.vectorizer = vectorizer.New(m.appState.Contextionary)
	rc, ok := m.appState.Contextionary.(localvectorizer.RemoteClient)
	if !ok {
		return errors.Errorf("invalid contextionary remote client")
	}

	m.configValidator = localvectorizer.NewConfigValidator(rc)

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

func (m *ContextionaryModule) UpdateObject(ctx context.Context,
	obj *models.Object, cfg modulecapabilities.ClassConfig) error {
	icheck := localvectorizer.NewIndexChecker(cfg)
	return m.vectorizer.Object(ctx, obj, icheck)
}

// verify we implement the modules.Module interface
var (
	_ = modules.Module(New())
	_ = modulecapabilities.Vectorizer(New())
)
