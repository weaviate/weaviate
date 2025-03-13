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

package modgenerativexai

import (
	"context"
	"net/http"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/generative-xai/clients"
	"github.com/weaviate/weaviate/modules/generative-xai/parameters"
)

const Name = "generative-xai"

func New() *GenerativeXaiModule {
	return &GenerativeXaiModule{}
}

type GenerativeXaiModule struct {
	generative                   generativeClient
	additionalPropertiesProvider map[string]modulecapabilities.GenerativeProperty
}

type generativeClient interface {
	modulecapabilities.GenerativeClient
	MetaInfo() (map[string]interface{}, error)
}

func (m *GenerativeXaiModule) Name() string {
	return Name
}

func (m *GenerativeXaiModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Text2TextGenerative
}

func (m *GenerativeXaiModule) Init(ctx context.Context,
	params moduletools.ModuleInitParams,
) error {
	if err := m.initAdditional(ctx, params.GetConfig().ModuleHttpClientTimeout, params.GetLogger()); err != nil {
		return errors.Wrapf(err, "init %s", Name)
	}

	return nil
}

func (m *GenerativeXaiModule) initAdditional(ctx context.Context, timeout time.Duration,
	logger logrus.FieldLogger,
) error {
	apiKey := os.Getenv("XAI_APIKEY")

	client := clients.New(apiKey, timeout, logger)
	m.generative = client
	m.additionalPropertiesProvider = parameters.AdditionalGenerativeParameters(m.generative)

	return nil
}

func (m *GenerativeXaiModule) RootHandler() http.Handler {
	// TODO: remove once this is a capability interface
	return nil
}

func (m *GenerativeXaiModule) MetaInfo() (map[string]interface{}, error) {
	return m.generative.MetaInfo()
}

func (m *GenerativeXaiModule) AdditionalGenerativeProperties() map[string]modulecapabilities.GenerativeProperty {
	return m.additionalPropertiesProvider
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.MetaProvider(New())
	_ = modulecapabilities.AdditionalGenerativeProperties(New())
)
