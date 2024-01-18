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

package modtransformers

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
	"github.com/weaviate/weaviate/modules/text2vec-transformers/clients"
	"github.com/weaviate/weaviate/modules/text2vec-transformers/vectorizer"
	"github.com/weaviate/weaviate/usecases/modulecomponents/additional"
)

func New() *TransformersModule {
	return &TransformersModule{}
}

type TransformersModule struct {
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

func (m *TransformersModule) Name() string {
	return "text2vec-transformers"
}

func (m *TransformersModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Text2Vec
}

func (m *TransformersModule) Init(ctx context.Context,
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

func (m *TransformersModule) InitExtension(modules []modulecapabilities.Module) error {
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

func (m *TransformersModule) initVectorizer(ctx context.Context, timeout time.Duration,
	logger logrus.FieldLogger,
) error {
	// TODO: gh-1486 proper config management
	uriPassage := os.Getenv("TRANSFORMERS_PASSAGE_INFERENCE_API")
	uriQuery := os.Getenv("TRANSFORMERS_QUERY_INFERENCE_API")
	uriCommon := os.Getenv("TRANSFORMERS_INFERENCE_API")

	if uriCommon == "" {
		if uriPassage == "" && uriQuery == "" {
			return errors.Errorf("required variable TRANSFORMERS_INFERENCE_API or both variables TRANSFORMERS_PASSAGE_INFERENCE_API and TRANSFORMERS_QUERY_INFERENCE_API are not set")
		}
		if uriPassage != "" && uriQuery == "" {
			return errors.Errorf("required variable TRANSFORMERS_QUERY_INFERENCE_API is not set")
		}
		if uriPassage == "" && uriQuery != "" {
			return errors.Errorf("required variable TRANSFORMERS_PASSAGE_INFERENCE_API is not set")
		}
	} else {
		if uriPassage != "" || uriQuery != "" {
			return errors.Errorf("either variable TRANSFORMERS_INFERENCE_API or both variables TRANSFORMERS_PASSAGE_INFERENCE_API and TRANSFORMERS_QUERY_INFERENCE_API should be set")
		}
		uriPassage = uriCommon
		uriQuery = uriCommon
	}

	client := clients.New(uriPassage, uriQuery, timeout, logger)
	if err := client.WaitForStartup(ctx, 1*time.Second); err != nil {
		return errors.Wrap(err, "init remote vectorizer")
	}

	m.vectorizer = vectorizer.New(client)
	m.metaProvider = client

	return nil
}

func (m *TransformersModule) initAdditionalPropertiesProvider() error {
	m.additionalPropertiesProvider = additional.NewText2VecProvider()
	return nil
}

func (m *TransformersModule) RootHandler() http.Handler {
	// TODO: remove once this is a capability interface
	return nil
}

func (m *TransformersModule) VectorizeObject(ctx context.Context,
	obj *models.Object, objDiff *moduletools.ObjectDiff, cfg moduletools.ClassConfig,
) error {
	return m.vectorizer.Object(ctx, obj, objDiff, cfg)
}

func (m *TransformersModule) MetaInfo() (map[string]interface{}, error) {
	return m.metaProvider.MetaInfo()
}

func (m *TransformersModule) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	return m.additionalPropertiesProvider.AdditionalProperties()
}

func (m *TransformersModule) VectorizeInput(ctx context.Context,
	input string, cfg moduletools.ClassConfig,
) ([]float32, error) {
	return m.vectorizer.Texts(ctx, []string{input}, cfg)
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.Vectorizer(New())
	_ = modulecapabilities.MetaProvider(New())
)
