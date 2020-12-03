package modules

import "net/http"

type Module interface {
	Name() string
	Init() error
	RootHandler() http.Handler
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
