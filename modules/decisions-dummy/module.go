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

// Package moddecisionsdummy is a decisions module without a provider, for
// tests of the rerank and decide properties. Its answers are a function of
// the document text, see clients.
package moddecisionsdummy

import (
	"context"
	"maps"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/modules/decisions-dummy/clients"
	rerankeradditional "github.com/weaviate/weaviate/usecases/modulecomponents/additional"
)

const Name = "decisions-dummy"

func New() *DecisionsDummyModule {
	return &DecisionsDummyModule{}
}

type DecisionsDummyModule struct {
	client               *clients.Client
	additionalProperties map[string]modulecapabilities.AdditionalProperty
}

func (m *DecisionsDummyModule) Name() string {
	return Name
}

func (m *DecisionsDummyModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Decisions
}

func (m *DecisionsDummyModule) Init(ctx context.Context, params moduletools.ModuleInitParams) error {
	m.client = clients.New()
	m.additionalProperties = rerankeradditional.NewDecisionsRankerProvider(m.client, clients.DecisionDefaults).AdditionalProperties()
	maps.Copy(m.additionalProperties, rerankeradditional.NewDecideProvider(m.client).AdditionalProperties())
	return nil
}

func (m *DecisionsDummyModule) MetaInfo() (map[string]any, error) {
	return map[string]any{"name": "Decisions - Dummy"}, nil
}

func (m *DecisionsDummyModule) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	return m.additionalProperties
}

// DecisionLimits: the Weaviate ceilings, text and blob, and the maxDocuments
// class setting (default 100) so that tests can exercise the page bound.
func (m *DecisionsDummyModule) DecisionLimits(cfg moduletools.ClassConfig) (modulecapabilities.DecisionLimits, error) {
	// A blob is judged as the text of its base64, so clients can test the
	// image path without a provider.
	return modulecapabilities.DecisionLimits{
		MaxDocuments: clients.MaxDocuments(cfg),
		DataTypes:    []schema.DataType{schema.DataTypeText, schema.DataTypeBlob},
	}, nil
}

func (m *DecisionsDummyModule) ClassConfigDefaults() map[string]any {
	return map[string]any{}
}

func (m *DecisionsDummyModule) PropertyConfigDefaults(dt *schema.DataType) map[string]any {
	return map[string]any{}
}

func (m *DecisionsDummyModule) ValidateClass(ctx context.Context, class *models.Class, cfg moduletools.ClassConfig) error {
	return nil
}

var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.AdditionalProperties(New())
	_ = modulecapabilities.MetaProvider(New())
	_ = modulecapabilities.ClassConfigurator(New())
	_ = modulecapabilities.DecisionLimitsProvider(New())
)
