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
	"errors"
	"fmt"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/usecases/config"
)

// queryReranker is a reranker module that rejects an empty rerank.query and
// one longer than its limit.
type queryReranker struct {
	plainReranker
	maxQueryBytes int
}

func (m queryReranker) ValidateRerankQuery(query string) error {
	if query == "" {
		return errors.New("rerank.query is required by reranker-query")
	}
	if len(query) > m.maxQueryBytes {
		return fmt.Errorf("rerank.query is %d bytes; reranker-query takes %d", len(query), m.maxQueryBytes)
	}
	return nil
}

var _ modulecapabilities.RerankQueryValidator = queryReranker{}

func TestValidateRerankQuery(t *testing.T) {
	plain := func(name string) plainReranker {
		return plainReranker{dummyNonVectorizerModule: newDummyNonVectorizerModule(name)}
	}
	requiring := queryReranker{plainReranker: plain("reranker-query"), maxQueryBytes: 8}
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
			name:         "the query reaches the module as sent",
			modules:      []modulecapabilities.Module{requiring},
			moduleConfig: configOf("reranker-query"),
			query:        " ",
		},
		{
			name:         "query at the module's limit",
			modules:      []modulecapabilities.Module{requiring},
			moduleConfig: configOf("reranker-query"),
			query:        "12345678",
		},
		{
			name:         "query above the module's limit",
			modules:      []modulecapabilities.Module{requiring},
			moduleConfig: configOf("reranker-query"),
			query:        "123456789",
			wantErr:      "rerank.query is 9 bytes; reranker-query takes 8",
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
			name:         "class names two rerankers, the validating one last by name",
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
