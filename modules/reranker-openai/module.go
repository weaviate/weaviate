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

package modrerankeropenai

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/modules/reranker-openai/clients"
	"github.com/weaviate/weaviate/modules/reranker-openai/config"
	rerankeradditional "github.com/weaviate/weaviate/usecases/modulecomponents/additional"
	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
)

const (
	Name = config.ModuleName

	maxConcurrentRequestsEnv = "RERANKER_OPENAI_MAX_CONCURRENT_REQUESTS"
	maxDocumentsPerQueryEnv  = clients.MaxDocumentsPerQueryEnv
)

func New() *ReRankerOpenAIModule {
	return &ReRankerOpenAIModule{}
}

type ReRankerOpenAIModule struct {
	reranker                     ReRankerOpenAIClient
	additionalPropertiesProvider modulecapabilities.AdditionalProperties
}

type ReRankerOpenAIClient interface {
	RankProperty(ctx context.Context, query, property string, documents []ent.Document, cfg moduletools.ClassConfig) (*ent.RankResult, error)
	MetaInfo() (map[string]any, error)
}

func (m *ReRankerOpenAIModule) Name() string {
	return Name
}

func (m *ReRankerOpenAIModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Text2TextReranker
}

func (m *ReRankerOpenAIModule) RerankPropertyDataTypes() []schema.DataType {
	return []schema.DataType{schema.DataTypeText, schema.DataTypeBlob}
}

func (m *ReRankerOpenAIModule) Init(ctx context.Context,
	params moduletools.ModuleInitParams,
) error {
	maxConcurrentRequests, err := maxConcurrentRequestsFromEnv()
	if err != nil {
		return errors.Wrap(err, "init reranker")
	}
	maxDocumentsPerQuery, err := maxDocumentsPerQueryFromEnv()
	if err != nil {
		return errors.Wrap(err, "init reranker")
	}

	apiKey := os.Getenv("OPENAI_APIKEY")
	organization := os.Getenv("OPENAI_ORGANIZATION")
	client := clients.New(apiKey, organization, params.GetConfig().ModuleHttpClientTimeout, maxConcurrentRequests,
		maxDocumentsPerQuery, params.GetLogger())
	m.reranker = client
	m.additionalPropertiesProvider = rerankeradditional.NewPropertyRankerProvider(client)
	return nil
}

func maxConcurrentRequestsFromEnv() (int, error) {
	v := os.Getenv(maxConcurrentRequestsEnv)
	if v == "" {
		return clients.DefaultMaxConcurrentRequests, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 || n > clients.MaxConcurrentRequestsLimit {
		return 0, fmt.Errorf("%s must be a whole number between 1 and %d, got %q",
			maxConcurrentRequestsEnv, clients.MaxConcurrentRequestsLimit, v)
	}
	return n, nil
}

func maxDocumentsPerQueryFromEnv() (int, error) {
	v := os.Getenv(maxDocumentsPerQueryEnv)
	if v == "" {
		return clients.DefaultMaxDocumentsPerQuery, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("%s must be a whole number of at least 1, got %q", maxDocumentsPerQueryEnv, v)
	}
	return n, nil
}

func (m *ReRankerOpenAIModule) MetaInfo() (map[string]any, error) {
	return m.reranker.MetaInfo()
}

func (m *ReRankerOpenAIModule) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	return m.additionalPropertiesProvider.AdditionalProperties()
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.AdditionalProperties(New())
	_ = modulecapabilities.MetaProvider(New())
	_ = modulecapabilities.RerankPropertyDataTypes(New())
)
