//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package modrerankernvidia

import (
	"context"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	client "github.com/weaviate/weaviate/modules/reranker-nvidia/clients"
	additionalprovider "github.com/weaviate/weaviate/usecases/modulecomponents/additional"
	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
)

const Name = "reranker-nvidia"

func New() *RerankerModule {
	return &RerankerModule{}
}

type RerankerModule struct {
	reranker                     ReRankerClient
	additionalPropertiesProvider modulecapabilities.AdditionalProperties
}

type ReRankerClient interface {
	Rank(ctx context.Context, query string, documents []string, cfg moduletools.ClassConfig) (*ent.RankResult, error)
	MetaInfo() (map[string]interface{}, error)
}

func (m *RerankerModule) Name() string {
	return Name
}

func (m *RerankerModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Text2TextReranker
}

func (m *RerankerModule) Init(ctx context.Context,
	params moduletools.ModuleInitParams,
) error {
	if err := m.initAdditional(ctx, params.GetConfig().ModuleHttpClientTimeout, params.GetLogger()); err != nil {
		return errors.Wrap(err, "init NVIDIA reranker")
	}

	return nil
}

func (m *RerankerModule) initAdditional(ctx context.Context, timeout time.Duration,
	logger logrus.FieldLogger,
) error {
	apiKey := os.Getenv("NVIDIA_APIKEY")
	client := client.New(apiKey, timeout, logger)
	m.additionalPropertiesProvider = additionalprovider.NewRankerProvider(client)
	m.reranker = client
	return nil
}

func (m *RerankerModule) MetaInfo() (map[string]interface{}, error) {
	return m.reranker.MetaInfo()
}

func (m *RerankerModule) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	return m.additionalPropertiesProvider.AdditionalProperties()
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.AdditionalProperties(New())
	_ = modulecapabilities.MetaProvider(New())
)
