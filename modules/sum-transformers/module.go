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

package modsum

import (
	"context"
	"net/http"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	sumadditional "github.com/weaviate/weaviate/modules/sum-transformers/additional"
	sumadditionalsummary "github.com/weaviate/weaviate/modules/sum-transformers/additional/summary"
	"github.com/weaviate/weaviate/modules/sum-transformers/client"
	"github.com/weaviate/weaviate/modules/sum-transformers/ent"
)

func New() *SUMModule {
	return &SUMModule{}
}

type SUMModule struct {
	sum                          sumClient
	additionalPropertiesProvider modulecapabilities.AdditionalProperties
}

type sumClient interface {
	GetSummary(ctx context.Context, property, text string) ([]ent.SummaryResult, error)
	MetaInfo() (map[string]interface{}, error)
}

func (m *SUMModule) Name() string {
	return "sum-transformers"
}

func (m *SUMModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Text2TextSummarize
}

func (m *SUMModule) Init(ctx context.Context,
	params moduletools.ModuleInitParams,
) error {
	if err := m.initAdditional(ctx, params.GetConfig().ModuleHttpClientTimeout, params.GetLogger()); err != nil {
		return errors.Wrap(err, "init additional")
	}
	return nil
}

func (m *SUMModule) initAdditional(ctx context.Context, timeout time.Duration,
	logger logrus.FieldLogger,
) error {
	uri := os.Getenv("SUM_INFERENCE_API")
	if uri == "" {
		return errors.Errorf("required variable SUM_INFERENCE_API is not set")
	}

	client := client.New(uri, timeout, logger)
	if err := client.WaitForStartup(ctx, 1*time.Second); err != nil {
		return errors.Wrap(err, "init remote sum module")
	}

	m.sum = client

	tokenProvider := sumadditionalsummary.New(m.sum)
	m.additionalPropertiesProvider = sumadditional.New(tokenProvider)

	return nil
}

func (m *SUMModule) RootHandler() http.Handler {
	// TODO: remove once this is a capability interface
	return nil
}

func (m *SUMModule) MetaInfo() (map[string]interface{}, error) {
	return m.sum.MetaInfo()
}

func (m *SUMModule) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	return m.additionalPropertiesProvider.AdditionalProperties()
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.AdditionalProperties(New())
	_ = modulecapabilities.MetaProvider(New())
)
