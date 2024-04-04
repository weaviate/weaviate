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

package modgenerativeollama

import (
	"context"
	"net/http"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	ollama "github.com/weaviate/weaviate/modules/generative-ollama/clients"
	additionalprovider "github.com/weaviate/weaviate/usecases/modulecomponents/additional"
	generativemodels "github.com/weaviate/weaviate/usecases/modulecomponents/additional/models"
)

const Name = "generative-ollama"

func New() *GenerativeOllamaModule {
	return &GenerativeOllamaModule{}
}

type GenerativeOllamaModule struct {
	generative                   generativeClient
	additionalPropertiesProvider modulecapabilities.AdditionalProperties
}

type generativeClient interface {
	GenerateSingleResult(ctx context.Context, textProperties map[string]string, prompt string, cfg moduletools.ClassConfig) (*generativemodels.GenerateResponse, error)
	GenerateAllResults(ctx context.Context, textProperties []map[string]string, task string, cfg moduletools.ClassConfig) (*generativemodels.GenerateResponse, error)
	Generate(ctx context.Context, cfg moduletools.ClassConfig, prompt string) (*generativemodels.GenerateResponse, error)
	MetaInfo() (map[string]interface{}, error)
}

func (m *GenerativeOllamaModule) Name() string {
	return Name
}

func (m *GenerativeOllamaModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Text2TextGenerative
}

func (m *GenerativeOllamaModule) Init(ctx context.Context,
	params moduletools.ModuleInitParams,
) error {
	if err := m.initAdditional(ctx, params.GetConfig().ModuleHttpClientTimeout, params.GetLogger()); err != nil {
		return errors.Wrap(err, "init q/a")
	}

	return nil
}

func (m *GenerativeOllamaModule) initAdditional(ctx context.Context, timeout time.Duration,
	logger logrus.FieldLogger,
) error {
	client := ollama.New(timeout, logger)

	m.generative = client

	m.additionalPropertiesProvider = additionalprovider.NewGenerativeProvider(client, logger)
	return nil
}

func (m *GenerativeOllamaModule) MetaInfo() (map[string]interface{}, error) {
	return m.generative.MetaInfo()
}

func (m *GenerativeOllamaModule) RootHandler() http.Handler {
	// TODO: remove once this is a capability interface
	return nil
}

func (m *GenerativeOllamaModule) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	return m.additionalPropertiesProvider.AdditionalProperties()
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.AdditionalProperties(New())
	_ = modulecapabilities.MetaProvider(New())
)
