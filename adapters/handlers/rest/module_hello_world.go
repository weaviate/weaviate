package rest

import "net/http"

type helloWorldModule struct {
}

func (m *helloWorldModule) Init() error {
	return nil
}

func (m *helloWorldModule) RegisterHandlers(h *ModuleHTTPObject) {
	h.HandleFunc("/v1/modules/hello-world/hello", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`hello world from my module`))
		return
	})
}
