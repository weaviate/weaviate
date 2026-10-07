//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package modules

import (
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
)

// typedModule is a module of any type that serves the given additional
// properties.
type typedModule struct {
	dummyNonVectorizerModule
	moduleType modulecapabilities.ModuleType
	properties []string
}

func (m typedModule) Type() modulecapabilities.ModuleType {
	return m.moduleType
}

func (m typedModule) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	out := map[string]modulecapabilities.AdditionalProperty{}
	for _, name := range m.properties {
		out[name] = modulecapabilities.AdditionalProperty{}
	}
	return out
}

// The schema allows one module that serves the rerank property per class,
// whichever type the module has.
func TestIsRerankerByCapability(t *testing.T) {
	tests := []struct {
		name       string
		module     modulecapabilities.Module
		want       bool
		unregister bool
	}{
		{
			name:   "reranker module without additional properties",
			module: typedModule{dummyNonVectorizerModule{name: "reranker-x"}, modulecapabilities.Text2TextReranker, nil},
			want:   true,
		},
		{
			name: "reranker module that serves rerank",
			module: typedModule{
				dummyNonVectorizerModule{name: "reranker-y"},
				modulecapabilities.Text2TextReranker,
				[]string{modulecomponents.AdditionalPropertyRerank},
			},
			want: true,
		},
		{
			name: "module of another type that serves rerank",
			module: typedModule{
				dummyNonVectorizerModule{name: "other-x"},
				modulecapabilities.Text2TextQnA,
				[]string{modulecomponents.AdditionalPropertyRerank},
			},
			want: true,
		},
		{
			name: "decisions module that serves rerank",
			module: typedModule{
				dummyNonVectorizerModule{name: "decisions-x"},
				modulecapabilities.Decisions,
				[]string{modulecomponents.AdditionalPropertyRerank, "decide"},
			},
			want: true,
		},
		{
			name:   "decisions module that does not serve rerank",
			module: typedModule{dummyNonVectorizerModule{name: "decisions-y"}, modulecapabilities.Decisions, []string{"decide"}},
			want:   false,
		},
		{
			name:   "generative module",
			module: typedModule{dummyNonVectorizerModule{name: "generative-x"}, modulecapabilities.Text2TextGenerative, nil},
			want:   false,
		},
		{
			name:   "module of another type with another property",
			module: typedModule{dummyNonVectorizerModule{name: "qna-x"}, modulecapabilities.Text2TextQnA, []string{"answer"}},
			want:   false,
		},
		{
			name:       "module that is not registered",
			module:     typedModule{dummyNonVectorizerModule{name: "reranker-missing"}, modulecapabilities.Text2TextReranker, nil},
			unregister: true,
			want:       false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			p := NewProvider(logger, config.Config{})
			if !tt.unregister {
				p.Register(tt.module)
			}

			assert.Equal(t, tt.want, p.IsReranker(tt.module.Name()))
		})
	}
}

// A class whose moduleConfig names no module that serves rerank gets one
// implicitly only when exactly one registered module serves rerank,
// whatever the types of the modules.
func TestImplicitRerankProviderCountsModulesServingRerank(t *testing.T) {
	reranker := typedModule{
		dummyNonVectorizerModule{name: "reranker-x"},
		modulecapabilities.Text2TextReranker,
		[]string{modulecomponents.AdditionalPropertyRerank},
	}
	decisions := typedModule{
		dummyNonVectorizerModule{name: "decisions-x"},
		modulecapabilities.Decisions,
		[]string{modulecomponents.AdditionalPropertyRerank, "decide"},
	}
	class := &models.Class{Class: "Plain"}
	tests := []struct {
		name       string
		registered []modulecapabilities.Module
		want       map[string]bool
	}{
		{
			name:       "one reranker",
			registered: []modulecapabilities.Module{reranker},
			want:       map[string]bool{"reranker-x": true},
		},
		{
			name:       "one decisions module",
			registered: []modulecapabilities.Module{decisions},
			want:       map[string]bool{"decisions-x": true},
		},
		{
			name:       "a reranker and a decisions module",
			registered: []modulecapabilities.Module{reranker, decisions},
			want:       map[string]bool{"reranker-x": false, "decisions-x": false},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			p := NewProvider(logger, config.Config{})
			for _, module := range tt.registered {
				p.Register(module)
			}
			for name, want := range tt.want {
				module := p.GetByName(name)
				got := p.shouldIncludeClassArgument(class, name, module.Type(), p.getModuleAltNames(module))
				assert.Equal(t, want, got, name)
			}
		})
	}
}
