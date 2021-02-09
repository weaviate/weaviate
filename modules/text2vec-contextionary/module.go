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
	"net/http"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/adapters/handlers/rest/state"
	"github.com/semi-technologies/weaviate/modules/text2vec-contextionary/concepts"
	"github.com/semi-technologies/weaviate/modules/text2vec-contextionary/extensions"
	"github.com/semi-technologies/weaviate/usecases/modules"
	"github.com/semi-technologies/weaviate/usecases/vectorizer"
)

func New(sp modules.StorageProvider, state *state.State) *ContextionaryModule {
	return &ContextionaryModule{
		storageProvider: sp,
		appState:        state,
	}
}

// ContextionaryModule for now only handles storage and retrival of extensions,
// but with making Weaviate more modular, this should contain anything related
// to the module
type ContextionaryModule struct {
	storageProvider modules.StorageProvider
	extensions      *extensions.RESTHandlers
	concepts        *concepts.RESTHandlers
	appState        *state.State
}

func (m *ContextionaryModule) Name() string {
	return "text2vec-contextionary"
}

func (m *ContextionaryModule) Init(params modules.ModuleInitParams) error {
	if m.storageProvider == nil && params.GetStorageProvider() != nil {
		m.storageProvider = params.GetStorageProvider()
	}
	if m.appState == nil && params.GetAppState() != nil {
		appState, ok := params.GetAppState().(*state.State)
		if ok {
			m.appState = appState
		}
	}

	if err := m.initExtensions(); err != nil {
		return errors.Wrap(err, "init extensions")
	}

	if err := m.initConcepts(); err != nil {
		return errors.Wrap(err, "init concepts")
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

func (m *ContextionaryModule) RootHandler() http.Handler {
	mux := http.NewServeMux()

	mux.Handle("/extensions-storage/", http.StripPrefix("/extensions-storage",
		m.extensions.StorageHandler()))
	mux.Handle("/extensions", http.StripPrefix("/extensions",
		m.extensions.UserFacingHandler()))
	mux.Handle("/concepts/", http.StripPrefix("/concepts", m.concepts.Handler()))

	return mux
}

// verify we implement the modules.Module interface
var _ = modules.Module(New(nil, nil))
