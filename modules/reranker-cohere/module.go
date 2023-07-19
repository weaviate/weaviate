//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package modrerankercohere

import (
	"context"
	"net/http"
	"os"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	rerankeradditional "github.com/weaviate/weaviate/modules/reranker-cohere/additional"
	rerankeradditionalrank "github.com/weaviate/weaviate/modules/reranker-cohere/additional/rank"
	"github.com/weaviate/weaviate/modules/reranker-cohere/clients"
	"github.com/weaviate/weaviate/modules/reranker-cohere/ent"
)

const Name = "reranker-cohere"

func New() *ReRankerCohereModule {
	return &ReRankerCohereModule{}
}

type ReRankerCohereModule struct {
	reranker                     ReRankerCohereClient
	additionalPropertiesProvider modulecapabilities.AdditionalProperties
}

type ReRankerCohereClient interface {
	Rank(ctx context.Context, query string, documents []string, cfg moduletools.ClassConfig) (*ent.RankResult, error)
	MetaInfo() (map[string]interface{}, error)
}

func (m *ReRankerCohereModule) Name() string {
	return Name
}

func (m *ReRankerCohereModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Text2TextReranker
}

func (m *ReRankerCohereModule) Init(ctx context.Context,
	params moduletools.ModuleInitParams,
) error {
	if err := m.initAdditional(ctx, params.GetLogger()); err != nil {
		return errors.Wrap(err, "init cross encoder")
	}

	return nil
}

func (m *ReRankerCohereModule) initAdditional(ctx context.Context,
	logger logrus.FieldLogger,
) error {
	apiKey := os.Getenv("COHERE_APIKEY")

	client := clients.New(apiKey, logger)

	m.reranker = client

	rerankerProvider := rerankeradditionalrank.New(m.reranker)
	m.additionalPropertiesProvider = rerankeradditional.New(rerankerProvider)
	return nil
}

func (m *ReRankerCohereModule) MetaInfo() (map[string]interface{}, error) {
	return m.reranker.MetaInfo()
}

func (m *ReRankerCohereModule) RootHandler() http.Handler {
	// TODO: remove once this is a capability interface
	return nil
}

func (m *ReRankerCohereModule) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	return m.additionalPropertiesProvider.AdditionalProperties()
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.AdditionalProperties(New())
	_ = modulecapabilities.MetaProvider(New())
)
