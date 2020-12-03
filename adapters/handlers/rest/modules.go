package rest

import "net/http"

type Module interface {
	Init() error
	RegisterHandlers(http *ModuleHTTPObject)
}

type ModuleHTTPObject struct {
	handlers map[string]http.Handler
}

func (m *ModuleHTTPObject) HandleFunc(pattern string,
	handler func(http.ResponseWriter, *http.Request)) {
	m.handlers[pattern] = http.HandlerFunc(handler)
}

func newModuleHTTPObject() *ModuleHTTPObject {
	return &ModuleHTTPObject{
		handlers: map[string]http.Handler{},
	}
}

// faking the module registration process as a global var
var registeredModules = map[string]Module{
	"hello-world": &helloWorldModule{},
}
