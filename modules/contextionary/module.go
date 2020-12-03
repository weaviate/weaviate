package modcontextionary

import (
	"net/http"

	"github.com/semi-technologies/weaviate/modules/contextionary/extensions"
	"github.com/semi-technologies/weaviate/usecases/modules"
)

func New() *ContextionaryModule {
	return &ContextionaryModule{}
}

// ContextionaryModule for now only handles storage and retrival of extensions,
// but with making Weaviate more modular, this should contain anything related
// to the module
type ContextionaryModule struct {
}

func (m *ContextionaryModule) Name() string {
	return "contextionary"
}

func (m *ContextionaryModule) Init() error {
	return nil
}

func (m *ContextionaryModule) RootHandler() http.Handler {
	mux := http.NewServeMux()
	extensionsHandlers := extensions.NewRESTHandlers()
	mux.Handle("/extensions", http.StripPrefix("/extensions", extensionsHandlers.Handler()))

	return mux
}

// verify we implement the modules.Module interface
var _ = modules.Module(New())
