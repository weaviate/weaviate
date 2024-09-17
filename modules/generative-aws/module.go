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

package modgenerativeaws

import (
	"context"
	"net/http"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/generative-aws/clients"
	"github.com/weaviate/weaviate/modules/generative-aws/parameters"
)

const Name = "generative-aws"

func New() *GenerativeAWSModule {
	return &GenerativeAWSModule{}
}

type GenerativeAWSModule struct {
	generative                   generativeClient
	additionalPropertiesProvider map[string]modulecapabilities.GenerativeProperty
}

type generativeClient interface {
	modulecapabilities.GenerativeClient
	MetaInfo() (map[string]interface{}, error)
}

func (m *GenerativeAWSModule) Name() string {
	return Name
}

func (m *GenerativeAWSModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Text2TextGenerative
}

func (m *GenerativeAWSModule) Init(ctx context.Context,
	params moduletools.ModuleInitParams,
) error {
	if err := m.initAdditional(ctx, params.GetConfig().ModuleHttpClientTimeout, params.GetLogger()); err != nil {
		return errors.Wrapf(err, "init %s", Name)
	}

	return nil
}

func (m *GenerativeAWSModule) initAdditional(ctx context.Context, timeout time.Duration,
	logger logrus.FieldLogger,
) error {
	awsAccessKey := m.getAWSAccessKey()
	awsSecret := m.getAWSSecretAccessKey()
	awsSessionToken := os.Getenv("AWS_SESSION_TOKEN")
	client := clients.New(awsAccessKey, awsSecret, awsSessionToken, timeout, logger)

	m.generative = client
	m.additionalPropertiesProvider = parameters.AdditionalGenerativeParameters(m.generative)

	return nil
}

func (m *GenerativeAWSModule) getAWSAccessKey() string {
	if os.Getenv("AWS_ACCESS_KEY_ID") != "" {
		return os.Getenv("AWS_ACCESS_KEY_ID")
	}
	return os.Getenv("AWS_ACCESS_KEY")
}

func (m *GenerativeAWSModule) getAWSSecretAccessKey() string {
	if os.Getenv("AWS_SECRET_ACCESS_KEY") != "" {
		return os.Getenv("AWS_SECRET_ACCESS_KEY")
	}
	return os.Getenv("AWS_SECRET_KEY")
}

func (m *GenerativeAWSModule) RootHandler() http.Handler {
	// TODO: remove once this is a capability interface
	return nil
}

func (m *GenerativeAWSModule) MetaInfo() (map[string]interface{}, error) {
	return m.generative.MetaInfo()
}

func (m *GenerativeAWSModule) AdditionalGenerativeProperties() map[string]modulecapabilities.GenerativeProperty {
	return m.additionalPropertiesProvider
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.MetaProvider(New())
	_ = modulecapabilities.AdditionalGenerativeProperties(New())
)
