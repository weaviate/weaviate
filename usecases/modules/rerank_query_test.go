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
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/usecases/config"
)

// queryReranker is a reranker module that states whether it requires
// rerank.query.
type queryReranker struct {
	plainReranker
	required bool
}

func (m queryReranker) RerankQueryRequired() bool {
	return m.required
}

var _ modulecapabilities.RerankQueryRequired = queryReranker{}

func TestValidateRerankQuery(t *testing.T) {
	plain := func(name string) plainReranker {
		return plainReranker{dummyNonVectorizerModule: newDummyNonVectorizerModule(name)}
	}
	requiring := queryReranker{plainReranker: plain("reranker-query"), required: true}
	optional := queryReranker{plainReranker: plain("reranker-query")}
	configOf := func(modules ...string) map[string]any {
		moduleConfig := map[string]any{}
		for _, module := range modules {
			moduleConfig[module] = map[string]any{}
		}
		return moduleConfig
	}
	const wantErr = "rerank.query is required by reranker-query"

	tests := []struct {
		name         string
		modules      []modulecapabilities.Module
		moduleConfig any
		query        string
		wantErr      string
	}{
		{
			name:         "module without the interface, no query",
			modules:      []modulecapabilities.Module{plain("reranker-plain")},
			moduleConfig: configOf("reranker-plain"),
		},
		{
			name:         "module that does not require a query, no query",
			modules:      []modulecapabilities.Module{optional},
			moduleConfig: configOf("reranker-query"),
		},
		{
			name:         "module that requires a query, no query",
			modules:      []modulecapabilities.Module{requiring},
			moduleConfig: configOf("reranker-query"),
			wantErr:      wantErr,
		},
		{
			name:         "module that requires a query, with a query",
			modules:      []modulecapabilities.Module{requiring},
			moduleConfig: configOf("reranker-query"),
			query:        "billing",
		},
		{
			name:         "module that requires a query, whitespace query is a query",
			modules:      []modulecapabilities.Module{requiring},
			moduleConfig: configOf("reranker-query"),
			query:        " ",
		},
		{
			// The only reranker module serves every class, configured or not.
			name:         "only reranker module, not in the class's moduleConfig",
			modules:      []modulecapabilities.Module{requiring},
			moduleConfig: configOf(),
			wantErr:      wantErr,
		},
		{
			name:         "class names another reranker",
			modules:      []modulecapabilities.Module{requiring, plain("reranker-plain")},
			moduleConfig: configOf("reranker-plain"),
		},
		{
			name:         "class names two rerankers, the requiring one last by name",
			modules:      []modulecapabilities.Module{requiring, plain("reranker-plain")},
			moduleConfig: configOf("reranker-query", "reranker-plain"),
			wantErr:      wantErr,
		},
		{
			name:         "class names two rerankers, the plain one last by name",
			modules:      []modulecapabilities.Module{requiring, plain("reranker-untyped")},
			moduleConfig: configOf("reranker-query", "reranker-untyped"),
		},
		{name: "class without a reranker module, no query"},
		{name: "class without a reranker module, with a query", query: "billing"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			p := NewProvider(logger, config.Config{})
			for _, module := range tt.modules {
				p.Register(module)
			}
			class := &models.Class{Class: "Tickets", ModuleConfig: tt.moduleConfig}

			// The registered modules are a map: the answer must not depend on
			// its iteration order.
			for range additionalExtendRuns {
				err := p.ValidateRerankQuery(class, tt.query)
				if tt.wantErr == "" {
					require.NoError(t, err)
				} else {
					require.EqualError(t, err, tt.wantErr)
				}
			}
		})
	}
}
