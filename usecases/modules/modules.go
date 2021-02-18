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
	"fmt"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/schema"
)

var internalSearchers = []string{"nearObject", "nearVector", "where", "group", "limit"}

type Provider struct {
	registered   map[string]modulecapabilities.Module
	schemaGetter schemaGetter
}

type schemaGetter interface {
	GetSchemaSkipAuth() schema.Schema
}

func NewProvider() *Provider {
	return &Provider{
		registered: map[string]modulecapabilities.Module{},
	}
}

func (m *Provider) Register(mod modulecapabilities.Module) {
	m.registered[mod.Name()] = mod
}

func (m *Provider) GetByName(name string) modulecapabilities.Module {
	return m.registered[name]
}

func (m *Provider) GetAll() []modulecapabilities.Module {
	out := make([]modulecapabilities.Module, len(m.registered))
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

func (m *Provider) Init(params modulecapabilities.ModuleInitParams) error {
	for i, mod := range m.GetAll() {
		if err := mod.Init(params); err != nil {
			return errors.Wrapf(err, "init module %d (%q)", i, mod.Name())
		}
	}
	if err := m.validate(); err != nil {
		return errors.Wrap(err, "validate modules")
	}

	return nil
}

func (m *Provider) validate() error {
	searchers := map[string][]string{}
	for _, mod := range m.GetAll() {
		if module, ok := mod.(modulecapabilities.GraphQLArguments); ok {
			for argument := range module.ExtractFunctions() {
				if searchers[argument] == nil {
					searchers[argument] = []string{}
				}
				modules := searchers[argument]
				modules = append(modules, mod.Name())
				searchers[argument] = modules
			}
		}
	}

	var errorMessages []string
	for searcher, modules := range searchers {
		for i := range internalSearchers {
			if internalSearchers[i] == searcher {
				errorMessages = append(errorMessages,
					fmt.Sprintf("searcher: %s conflicts with weaviate's internal searcher in modules: %v",
						searcher, modules))
			}
		}
		if len(modules) > 1 {
			errorMessages = append(errorMessages,
				fmt.Sprintf("searcher: %s defined in more than one module: %v", searcher, modules))
		}
	}

	if len(errorMessages) > 0 {
		return errors.Errorf("%v", errorMessages)
	}

	return nil
}
