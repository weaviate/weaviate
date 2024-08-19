//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package modrerankerdummy

import (
	"context"
	"net/http"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/reranker-dummy/clients"
	rerankeradditional "github.com/weaviate/weaviate/usecases/modulecomponents/additional"
	"github.com/weaviate/weaviate/usecases/modulecomponents/ent"
)

const Name = "reranker-dummy"

func New() *ReRankerDummyModule {
	return &ReRankerDummyModule{}
}

type ReRankerDummyModule struct {
	reranker                     ReRankerDummyClient
	additionalPropertiesProvider modulecapabilities.AdditionalProperties
}

type ReRankerDummyClient interface {
	Rank(ctx context.Context, query string, documents []string, cfg moduletools.ClassConfig) (*ent.RankResult, error)
	MetaInfo() (map[string]interface{}, error)
}

func (m *ReRankerDummyModule) Name() string {
	return Name
}

func (m *ReRankerDummyModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Text2TextReranker
}

func (m *ReRankerDummyModule) Init(ctx context.Context,
	params moduletools.ModuleInitParams,
) error {
	if err := m.initAdditional(ctx, params.GetConfig().ModuleHttpClientTimeout, params.GetLogger()); err != nil {
		return errors.Wrap(err, "init cross encoder")
	}

	return nil
}

func (m *ReRankerDummyModule) initAdditional(ctx context.Context, timeout time.Duration,
	logger logrus.FieldLogger,
) error {
	client := clients.New(logger)
	m.reranker = client
	m.additionalPropertiesProvider = rerankeradditional.NewRankerProvider(m.reranker)
	return nil
}

func (m *ReRankerDummyModule) MetaInfo() (map[string]interface{}, error) {
	return m.reranker.MetaInfo()
}

func (m *ReRankerDummyModule) RootHandler() http.Handler {
	// TODO: remove once this is a capability interface
	return nil
}

func (m *ReRankerDummyModule) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	return m.additionalPropertiesProvider.AdditionalProperties()
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.AdditionalProperties(New())
	_ = modulecapabilities.MetaProvider(New())
)
