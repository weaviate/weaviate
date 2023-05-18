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

package modgenerativepalm

import (
	"context"
	"net/http"
	"os"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	generativeadditional "github.com/weaviate/weaviate/modules/generative-palm/additional"
	generativeadditionalgenerate "github.com/weaviate/weaviate/modules/generative-palm/additional/generate"
	"github.com/weaviate/weaviate/modules/generative-palm/clients"
	"github.com/weaviate/weaviate/modules/generative-palm/ent"
)

const Name = "generative-palm"

func New() *GenerativePaLMModule {
	return &GenerativePaLMModule{}
}

type GenerativePaLMModule struct {
	generative                   generativeClient
	additionalPropertiesProvider modulecapabilities.AdditionalProperties
}

type generativeClient interface {
	GenerateSingleResult(ctx context.Context, textProperties map[string]string, prompt string, cfg moduletools.ClassConfig) (*ent.GenerateResult, error)
	GenerateAllResults(ctx context.Context, textProperties []map[string]string, task string, cfg moduletools.ClassConfig) (*ent.GenerateResult, error)
	Generate(ctx context.Context, cfg moduletools.ClassConfig, prompt string) (*ent.GenerateResult, error)
	MetaInfo() (map[string]interface{}, error)
}

func (m *GenerativePaLMModule) Name() string {
	return Name
}

func (m *GenerativePaLMModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Text2Text
}

func (m *GenerativePaLMModule) Init(ctx context.Context,
	params moduletools.ModuleInitParams,
) error {
	if err := m.initAdditional(ctx, params.GetLogger()); err != nil {
		return errors.Wrap(err, "init q/a")
	}

	return nil
}

func (m *GenerativePaLMModule) initAdditional(ctx context.Context,
	logger logrus.FieldLogger,
) error {
	apiKey := os.Getenv("PALM_APIKEY")

	client := clients.New(apiKey, logger)

	m.generative = client

	generateProvider := generativeadditionalgenerate.New(m.generative)
	m.additionalPropertiesProvider = generativeadditional.New(generateProvider)

	return nil
}

func (m *GenerativePaLMModule) MetaInfo() (map[string]interface{}, error) {
	return m.generative.MetaInfo()
}

func (m *GenerativePaLMModule) RootHandler() http.Handler {
	// TODO: remove once this is a capability interface
	return nil
}

func (m *GenerativePaLMModule) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	return m.additionalPropertiesProvider.AdditionalProperties()
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.AdditionalProperties(New())
)
