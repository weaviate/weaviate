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

package modules

import (
	"net/http"

	"github.com/pkg/errors"
)

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

func Init() error {
	for i, mod := range GetAll() {
		if err := mod.Init(); err != nil {
			return errors.Wrapf(err, "init module %d (%q)", i, mod.Name())
		}
	}

	return nil
}
