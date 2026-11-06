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
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/config"
)

// typedReranker is a reranker module that reads only the property data types
// in types.
type typedReranker struct {
	plainReranker
	types []schema.DataType
}

func (m typedReranker) RerankPropertyDataTypes() []schema.DataType {
	return m.types
}

var _ modulecapabilities.RerankPropertyDataTypes = typedReranker{}

func TestValidateRerankProperty(t *testing.T) {
	reranker := func(name string) plainReranker {
		return plainReranker{dummyNonVectorizerModule: newDummyNonVectorizerModule(name)}
	}
	typed := typedReranker{
		plainReranker: reranker("reranker-typed"),
		types:         []schema.DataType{schema.DataTypeText, schema.DataTypeBlob},
	}
	configOf := func(modules ...string) map[string]any {
		moduleConfig := map[string]any{}
		for _, module := range modules {
			moduleConfig[module] = map[string]any{}
		}
		return moduleConfig
	}
	onlyTyped := []modulecapabilities.Module{typed}
	typedConfig := configOf("reranker-typed")

	tests := []struct {
		name         string
		modules      []modulecapabilities.Module
		moduleConfig any
		property     string
		wantErr      string
	}{
		{name: "text property", modules: onlyTyped, moduleConfig: typedConfig, property: "text"},
		{name: "blob property", modules: onlyTyped, moduleConfig: typedConfig, property: "photo"},
		{
			name:         "text array",
			modules:      onlyTyped,
			moduleConfig: typedConfig,
			property:     "tags",
			wantErr:      `rerank: property "tags" of collection "Tickets" is of type text[]; reranker-typed reranks text, blob properties`,
		},
		{
			name:         "int",
			modules:      onlyTyped,
			moduleConfig: typedConfig,
			property:     "count",
			wantErr:      `rerank: property "count" of collection "Tickets" is of type int; reranker-typed reranks text, blob properties`,
		},
		{
			name:         "deprecated string property reads as text",
			modules:      onlyTyped,
			moduleConfig: typedConfig,
			property:     "legacy",
		},
		{
			name:         "reference property",
			modules:      onlyTyped,
			moduleConfig: typedConfig,
			property:     "author",
			wantErr:      `rerank: property "author" of collection "Tickets" is of type cref; reranker-typed reranks text, blob properties`,
		},
		{
			name:         "property without a data type",
			modules:      onlyTyped,
			moduleConfig: typedConfig,
			property:     "broken",
			wantErr:      `rerank: property "broken" of collection "Tickets" has no data type`,
		},
		{
			name:         "unknown property",
			modules:      onlyTyped,
			moduleConfig: typedConfig,
			property:     "nope",
			wantErr:      `rerank: property "nope" does not exist in collection "Tickets"`,
		},
		{
			name:         "empty property name",
			modules:      onlyTyped,
			moduleConfig: typedConfig,
			property:     "",
			wantErr:      `rerank: property "" does not exist in collection "Tickets"`,
		},
		{
			name:         "reranker that declares nothing",
			modules:      []modulecapabilities.Module{reranker("reranker-plain")},
			moduleConfig: configOf("reranker-plain"),
			property:     "tags",
		},
		{
			name:         "reranker that declares an empty list",
			modules:      []modulecapabilities.Module{typedReranker{plainReranker: reranker("reranker-typed")}},
			moduleConfig: configOf("reranker-typed"),
			property:     "tags",
		},
		{
			name:         "module registered but not in the class's moduleConfig",
			modules:      []modulecapabilities.Module{typed, reranker("reranker-plain")},
			moduleConfig: configOf(),
			property:     "tags",
		},
		{
			// The only reranker module serves every class, configured or not.
			name:         "only reranker module, not in the class's moduleConfig",
			modules:      onlyTyped,
			moduleConfig: configOf(),
			property:     "tags",
			wantErr:      `rerank: property "tags" of collection "Tickets" is of type text[]; reranker-typed reranks text, blob properties`,
		},
		{
			// The rerank of a class that names two rerankers runs with the
			// last by name.
			name:         "class names two rerankers, the typed one last by name",
			modules:      []modulecapabilities.Module{typed, reranker("reranker-plain")},
			moduleConfig: configOf("reranker-typed", "reranker-plain"),
			property:     "tags",
			wantErr:      `rerank: property "tags" of collection "Tickets" is of type text[]; reranker-typed reranks text, blob properties`,
		},
		{
			name:         "class names two rerankers, the plain one last by name",
			modules:      []modulecapabilities.Module{typed, reranker("reranker-untyped")},
			moduleConfig: configOf("reranker-typed", "reranker-untyped"),
			property:     "tags",
		},
		{name: "class without a reranker", property: "tags"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			p := NewProvider(logger, config.Config{})
			for _, module := range tt.modules {
				p.Register(module)
			}
			class := &models.Class{
				Class: "Tickets",
				Properties: []*models.Property{
					{Name: "text", DataType: schema.DataTypeText.PropString()},
					{Name: "photo", DataType: schema.DataTypeBlob.PropString()},
					{Name: "tags", DataType: schema.DataTypeTextArray.PropString()},
					{Name: "count", DataType: schema.DataTypeInt.PropString()},
					{Name: "legacy", DataType: schema.DataTypeString.PropString()},
					{Name: "author", DataType: []string{"Author"}},
					{Name: "broken", DataType: []string{}},
				},
				ModuleConfig: tt.moduleConfig,
			}

			// The registered modules are a map: the answer must not depend on
			// its iteration order.
			for range additionalExtendRuns {
				err := p.ValidateRerankProperty(class, tt.property)
				if tt.wantErr == "" {
					require.NoError(t, err)
				} else {
					require.EqualError(t, err, tt.wantErr)
				}
			}
		})
	}
}
