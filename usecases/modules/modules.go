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
	"github.com/semi-technologies/weaviate/entities/schema"
)

type Module interface {
	Name() string
	Init(params ModuleInitParams) error
	RootHandler() http.Handler // TODO: remove from overall module, this is a capability
}

type Provider struct {
	registered   map[string]Module
	schemaGetter schemaGetter
}

type schemaGetter interface {
	GetSchemaSkipAuth() schema.Schema
}

func NewProvider() *Provider {
	return &Provider{
		registered: map[string]Module{},
	}
}

func (m *Provider) Register(mod Module) {
	m.registered[mod.Name()] = mod
}

func (m *Provider) GetByName(name string) Module {
	return m.registered[name]
}

func (m *Provider) GetAll() []Module {
	out := make([]Module, len(m.registered))
	i := 0
	for _, mod := range m.registered {
		out[i] = mod
		i++
	}

	return out
}

func (m *Provider) SetSchemaGetter(sg schemaGetter) {
	m.schemaGetter = sg
}

func (m *Provider) Init(params ModuleInitParams) error {
	for i, mod := range m.GetAll() {
		if err := mod.Init(params); err != nil {
			return errors.Wrapf(err, "init module %d (%q)", i, mod.Name())
		}
	}

	return nil
}
