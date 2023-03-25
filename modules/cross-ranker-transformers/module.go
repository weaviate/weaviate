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

package modcrossrankertransformers

import (
	"context"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	crossrankeradditional "github.com/weaviate/weaviate/modules/cross-ranker-transformers/additional"
	crossrankeradditionalrank "github.com/weaviate/weaviate/modules/cross-ranker-transformers/additional/rank"
	"github.com/weaviate/weaviate/modules/cross-ranker-transformers/clients"
	"github.com/weaviate/weaviate/modules/cross-ranker-transformers/ent"
	"net/http"
	"os"
	"time"
)

const Name = "cross-ranker-transformers"

func New() *CrossRankerModule {
	return &CrossRankerModule{}
}

type CrossRankerModule struct {
	crossranker                  CrossRankerClient
	additionalPropertiesProvider modulecapabilities.AdditionalProperties
}

type CrossRankerClient interface {
	Rank(ctx context.Context, property string, query string) (*ent.RankResult, error)
	MetaInfo() (map[string]interface{}, error)
}

func (m *CrossRankerModule) Name() string {
	return Name
}

func (m *CrossRankerModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Text2Text
}

func (m *CrossRankerModule) Init(ctx context.Context,
	params moduletools.ModuleInitParams,
) error {
	if err := m.initAdditional(ctx, params.GetLogger()); err != nil {
		return errors.Wrap(err, "init cross encoder")
	}

	return nil
}

func (m *CrossRankerModule) initAdditional(ctx context.Context,
	logger logrus.FieldLogger,
) error {
	uri := os.Getenv("CROSSRANKER_INFERENCE_API")
	if uri == "" {
		return nil
	}

	client := client.New(uri, logger)

	m.crossranker = client
	if err := client.WaitForStartup(ctx, 1*time.Second); err != nil {
		return errors.Wrap(err, "init remote sum module")
	}

	crossrankerProvider := crossrankeradditionalrank.New(m.crossranker)
	m.additionalPropertiesProvider = crossrankeradditional.New(crossrankerProvider)
	return nil
}

func (m *CrossRankerModule) MetaInfo() (map[string]interface{}, error) {
	return m.crossranker.MetaInfo()
}

func (m *CrossRankerModule) RootHandler() http.Handler {
	// TODO: remove once this is a capability interface
	return nil
}

func (m *CrossRankerModule) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	return m.additionalPropertiesProvider.AdditionalProperties()
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.AdditionalProperties(New())
	_ = modulecapabilities.MetaProvider(New())
)
