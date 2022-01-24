//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package modner

import (
	"context"
	"net/http"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/moduletools"
	neradditional "github.com/semi-technologies/weaviate/modules/ner-transformers/additional"
	neradditionaltoken "github.com/semi-technologies/weaviate/modules/ner-transformers/additional/tokens"
	"github.com/semi-technologies/weaviate/modules/ner-transformers/clients"
	"github.com/semi-technologies/weaviate/modules/ner-transformers/ent"
	"github.com/sirupsen/logrus"
)

func New() *NERModule {
	return &NERModule{}
}

type NERModule struct {
	ner                          nerClient
	additionalPropertiesProvider modulecapabilities.AdditionalProperties
}

type nerClient interface {
	GetTokens(ctx context.Context, property, text string) ([]ent.TokenResult, error)
	MetaInfo() (map[string]interface{}, error)
}

func (m *NERModule) Name() string {
	return "ner-transformers"
}

func (m *NERModule) Init(ctx context.Context,
	params moduletools.ModuleInitParams) error {
	if err := m.initAdditional(ctx, params.GetLogger()); err != nil {
		return errors.Wrap(err, "init additional")
	}
	return nil
}

func (m *NERModule) initAdditional(ctx context.Context,
	logger logrus.FieldLogger) error {
	uri := os.Getenv("NER_INFERENCE_API")
	if uri == "" {
		return errors.Errorf("required variable NER_INFERENCE_API is not set")
	}

	client := clients.New(uri, logger)
	if err := client.WaitForStartup(ctx, 1*time.Second); err != nil {
		return errors.Wrap(err, "init remote ner module")
	}

	m.ner = client

	tokenProvider := neradditionaltoken.New(m.ner)
	m.additionalPropertiesProvider = neradditional.New(tokenProvider)

	return nil
}

func (m *NERModule) RootHandler() http.Handler {
	// TODO: remove once this is a capability interface
	return nil
}

func (m *NERModule) MetaInfo() (map[string]interface{}, error) {
	return m.ner.MetaInfo()
}

func (m *NERModule) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	return m.additionalPropertiesProvider.AdditionalProperties()
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.AdditionalProperties(New())
	_ = modulecapabilities.MetaProvider(New())
)
