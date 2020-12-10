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
	"github.com/semi-technologies/weaviate/modules/contextionary/extensions"
	"github.com/semi-technologies/weaviate/usecases/modules"
)

func New(sp modules.StorageProvider) *ContextionaryModule {
	return &ContextionaryModule{
		storageProvider: sp,
	}
}

// ContextionaryModule for now only handles storage and retrival of extensions,
// but with making Weaviate more modular, this should contain anything related
// to the module
type ContextionaryModule struct {
	storageProvider modules.StorageProvider
	extensions      *extensions.RESTHandlers
}

func (m *ContextionaryModule) Name() string {
	return "contextionary"
}

func (m *ContextionaryModule) Init() error {
	if err := m.initExtensions(); err != nil {
		return errors.Wrap(err, "init extensions")
	}

	return nil
}

func (m *ContextionaryModule) initExtensions() error {
	storage, err := m.storageProvider.Storage("contextionary-extensions")
	if err != nil {
		return errors.Wrap(err, "initialize extensions storage")
	}

	uc := extensions.NewUseCase(storage)
	m.extensions = extensions.NewRESTHandlers(uc)

	return nil
}

func (m *ContextionaryModule) RootHandler() http.Handler {
	mux := http.NewServeMux()

	mux.Handle("/extensions-storage/", http.StripPrefix("/extensions-storage", m.extensions.Handler()))

	return mux
}

// verify we implement the modules.Module interface
var _ = modules.Module(New(nil))
