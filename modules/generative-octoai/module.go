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

package modgenerativeoctoai

import (
	"context"
	"net/http"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/generative-octoai/clients"
	"github.com/weaviate/weaviate/modules/generative-octoai/parameters"
)

const Name = "generative-octoai"

func New() *GenerativeOctoAIModule {
	return &GenerativeOctoAIModule{}
}

type GenerativeOctoAIModule struct {
	generative                   generativeClient
	additionalPropertiesProvider map[string]modulecapabilities.GenerativeProperty
}

type generativeClient interface {
	modulecapabilities.GenerativeClient
	MetaInfo() (map[string]interface{}, error)
}

func (m *GenerativeOctoAIModule) Name() string {
	return Name
}

func (m *GenerativeOctoAIModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Text2TextGenerative
}

func (m *GenerativeOctoAIModule) Init(ctx context.Context,
	params moduletools.ModuleInitParams,
) error {
	if err := m.initAdditional(ctx, params.GetConfig().ModuleHttpClientTimeout, params.GetLogger()); err != nil {
		return errors.Wrapf(err, "init %s", Name)
	}

	return nil
}

func (m *GenerativeOctoAIModule) initAdditional(ctx context.Context, timeout time.Duration,
	logger logrus.FieldLogger,
) error {
	apiKey := os.Getenv("OCTOAI_APIKEY")

	client := clients.New(apiKey, timeout, logger)
	m.generative = client
	m.additionalPropertiesProvider = parameters.AdditionalGenerativeParameters(m.generative)

	return nil
}

func (m *GenerativeOctoAIModule) RootHandler() http.Handler {
	// TODO: remove once this is a capability interface
	return nil
}

func (m *GenerativeOctoAIModule) MetaInfo() (map[string]interface{}, error) {
	return m.generative.MetaInfo()
}

func (m *GenerativeOctoAIModule) AdditionalGenerativeProperties() map[string]modulecapabilities.GenerativeProperty {
	return m.additionalPropertiesProvider
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.MetaProvider(New())
	_ = modulecapabilities.AdditionalGenerativeProperties(New())
)
