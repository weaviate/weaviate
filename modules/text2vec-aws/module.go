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

package modaws

import (
	"context"
	"net/http"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/text2vec-aws/clients"
	"github.com/weaviate/weaviate/modules/text2vec-aws/vectorizer"
	"github.com/weaviate/weaviate/usecases/modulecomponents/additional"
)

const Name = "text2vec-aws"

func New() *AwsModule {
	return &AwsModule{}
}

type AwsModule struct {
	vectorizer                   textVectorizer
	metaProvider                 metaProvider
	graphqlProvider              modulecapabilities.GraphQLArguments
	searcher                     modulecapabilities.Searcher
	nearTextTransformer          modulecapabilities.TextTransform
	logger                       logrus.FieldLogger
	additionalPropertiesProvider modulecapabilities.AdditionalProperties
}

type textVectorizer interface {
	Object(ctx context.Context, obj *models.Object, objDiff *moduletools.ObjectDiff,
		cfg moduletools.ClassConfig) error
	Texts(ctx context.Context, input []string,
		cfg moduletools.ClassConfig) ([]float32, error)
}

type metaProvider interface {
	MetaInfo() (map[string]interface{}, error)
}

func (m *AwsModule) Name() string {
	return "text2vec-aws"
}

func (m *AwsModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Text2Vec
}

func (m *AwsModule) Init(ctx context.Context,
	params moduletools.ModuleInitParams,
) error {
	m.logger = params.GetLogger()

	if err := m.initVectorizer(ctx, params.GetConfig().ModuleHttpClientTimeout, m.logger); err != nil {
		return errors.Wrap(err, "init vectorizer")
	}

	if err := m.initAdditionalPropertiesProvider(); err != nil {
		return errors.Wrap(err, "init additional properties provider")
	}

	return nil
}

func (m *AwsModule) InitExtension(modules []modulecapabilities.Module) error {
	for _, module := range modules {
		if module.Name() == m.Name() {
			continue
		}
		if arg, ok := module.(modulecapabilities.TextTransformers); ok {
			if arg != nil && arg.TextTransformers() != nil {
				m.nearTextTransformer = arg.TextTransformers()["nearText"]
			}
		}
	}

	if err := m.initNearText(); err != nil {
		return errors.Wrap(err, "init graphql provider")
	}
	return nil
}

func (m *AwsModule) initVectorizer(ctx context.Context, timeout time.Duration,
	logger logrus.FieldLogger,
) error {
	awsAccessKey := m.getAWSAccessKey()
	awsSecret := m.getAWSSecretAccessKey()
	client := clients.New(awsAccessKey, awsSecret, timeout, logger)

	m.vectorizer = vectorizer.New(client)
	m.metaProvider = client

	return nil
}

func (m *AwsModule) getAWSAccessKey() string {
	if os.Getenv("AWS_ACCESS_KEY_ID") != "" {
		return os.Getenv("AWS_ACCESS_KEY_ID")
	}
	return os.Getenv("AWS_ACCESS_KEY")
}

func (m *AwsModule) getAWSSecretAccessKey() string {
	if os.Getenv("AWS_SECRET_ACCESS_KEY") != "" {
		return os.Getenv("AWS_SECRET_ACCESS_KEY")
	}
	return os.Getenv("AWS_SECRET_KEY")
}

func (m *AwsModule) initAdditionalPropertiesProvider() error {
	m.additionalPropertiesProvider = additional.NewText2VecProvider()
	return nil
}

func (m *AwsModule) RootHandler() http.Handler {
	// TODO: remove once this is a capability interface
	return nil
}

func (m *AwsModule) VectorizeObject(ctx context.Context,
	obj *models.Object, objDiff *moduletools.ObjectDiff, cfg moduletools.ClassConfig,
) error {
	return m.vectorizer.Object(ctx, obj, objDiff, cfg)
}

func (m *AwsModule) MetaInfo() (map[string]interface{}, error) {
	return m.metaProvider.MetaInfo()
}

func (m *AwsModule) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	return m.additionalPropertiesProvider.AdditionalProperties()
}

func (m *AwsModule) VectorizeInput(ctx context.Context,
	input string, cfg moduletools.ClassConfig,
) ([]float32, error) {
	return m.vectorizer.Texts(ctx, []string{input}, cfg)
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.Vectorizer(New())
	_ = modulecapabilities.MetaProvider(New())
	_ = modulecapabilities.Searcher(New())
	_ = modulecapabilities.GraphQLArguments(New())
)
