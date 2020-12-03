package modules

import "net/http"

type Module interface {
	Name() string
	Init() error
	RegisterHandlers(http *ModuleHTTPObject)
}

type ModuleHTTPObject struct {
	Handlers map[string]http.Handler
}

func (m *ModuleHTTPObject) HandleFunc(pattern string,
	handler func(http.ResponseWriter, *http.Request)) {
	m.Handlers[pattern] = http.HandlerFunc(handler)
}

func NewModuleHTTPObject() *ModuleHTTPObject {
	return &ModuleHTTPObject{
		Handlers: map[string]http.Handler{},
	}
}

var registeredModules = map[string]Module{}

func Register(mod Module) {
	registeredModules[mod.Name()] = mod
}

func GetByName(name string) Module {
	return registeredModules[name]
}

func GetAll() []Module {
	out := make([]Module, len(registeredModules))
	i := 0
	for _, mod := range registeredModules {
		out[i] = mod
		i++
	}

	return out
}
