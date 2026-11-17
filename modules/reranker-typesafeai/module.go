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

package modrerankertypesafeai

import (
	"context"
	"os"

	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/modules/reranker-typesafeai/clients"
	"github.com/weaviate/weaviate/modules/reranker-typesafeai/config"
	rerankeradditional "github.com/weaviate/weaviate/usecases/modulecomponents/additional"
	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
)

const Name = config.ModuleName

func New() *ReRankerTypeSafeAIModule {
	return &ReRankerTypeSafeAIModule{}
}

type ReRankerTypeSafeAIModule struct {
	reranker                     ReRankerTypeSafeAIClient
	additionalPropertiesProvider modulecapabilities.AdditionalProperties
}

type ReRankerTypeSafeAIClient interface {
	RankProperty(ctx context.Context, query, property string, documents []ent.Document, cfg moduletools.ClassConfig) (*ent.RankResult, error)
	MetaInfo() (map[string]any, error)
}

func (m *ReRankerTypeSafeAIModule) Name() string {
	return Name
}

func (m *ReRankerTypeSafeAIModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Text2TextReranker
}

func (m *ReRankerTypeSafeAIModule) RerankPropertyDataTypes() []schema.DataType {
	return []schema.DataType{schema.DataTypeText, schema.DataTypeBlob}
}

// ValidateRerankQuery applies the rule of the rank call, which the GraphQL
// search reaches without this check.
func (m *ReRankerTypeSafeAIModule) ValidateRerankQuery(query string) error {
	return clients.ValidateQuery(query)
}

func (m *ReRankerTypeSafeAIModule) Init(ctx context.Context,
	params moduletools.ModuleInitParams,
) error {
	apiKey := os.Getenv("TYPESAFEAI_APIKEY")
	client := clients.New(apiKey, params.GetConfig().ModuleHttpClientTimeout, clients.MaxConcurrentRequests, params.GetLogger())
	m.reranker = client
	m.additionalPropertiesProvider = rerankeradditional.NewPropertyRankerProvider(client)
	return nil
}

func (m *ReRankerTypeSafeAIModule) MetaInfo() (map[string]any, error) {
	return m.reranker.MetaInfo()
}

func (m *ReRankerTypeSafeAIModule) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	return m.additionalPropertiesProvider.AdditionalProperties()
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.AdditionalProperties(New())
	_ = modulecapabilities.MetaProvider(New())
	_ = modulecapabilities.RerankPropertyDataTypes(New())
	_ = modulecapabilities.RerankQueryValidator(New())
)
