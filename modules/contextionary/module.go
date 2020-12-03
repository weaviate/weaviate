package modcontextionary

import (
	"net/http"

	"github.com/semi-technologies/weaviate/usecases/modules"
)

func New() *ContextionaryModule {
	return &ContextionaryModule{}
}

type ContextionaryModule struct {
}

func (m *ContextionaryModule) Name() string {
	return "contextionary"
}

func (m *ContextionaryModule) Init() error {
	return nil
}

func (m *ContextionaryModule) RegisterHandlers(h *modules.ModuleHTTPObject) {
	h.HandleFunc("/v1/modules/contextionary/hello", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`hello world from my module`))
	})
}

// verify we implement the modules.Module interface

var _ = modules.Module(New())
