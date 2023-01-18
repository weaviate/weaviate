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

package modopenai

import (
	"context"
	"net/http"
	"os"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/text2vec-openai/additional"
	"github.com/weaviate/weaviate/modules/text2vec-openai/additional/projector"
	"github.com/weaviate/weaviate/modules/text2vec-openai/clients"
	"github.com/weaviate/weaviate/modules/text2vec-openai/vectorizer"
)

func New() *OpenAIModule {
	return &OpenAIModule{}
}

type OpenAIModule struct {
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
		settings vectorizer.ClassSettings) error
	Texts(ctx context.Context, input []string,
		settings vectorizer.ClassSettings) ([]float32, error)
	// TODO all of these should be moved out of here, gh-1470

	MoveTo(source, target []float32, weight float32) ([]float32, error)
	MoveAwayFrom(source, target []float32, weight float32) ([]float32, error)
	CombineVectors([][]float32) []float32
}

type metaProvider interface {
	MetaInfo() (map[string]interface{}, error)
}

func (m *OpenAIModule) Name() string {
	return "text2vec-openai"
}

func (m *OpenAIModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Text2MultiVec
}

func (m *OpenAIModule) Init(ctx context.Context,
	params moduletools.ModuleInitParams,
) error {
	m.logger = params.GetLogger()

	if err := m.initVectorizer(ctx, m.logger); err != nil {
		return errors.Wrap(err, "init vectorizer")
	}

	if err := m.initAdditionalPropertiesProvider(); err != nil {
		return errors.Wrap(err, "init additional properties provider")
	}

	return nil
}

func (m *OpenAIModule) InitExtension(modules []modulecapabilities.Module) error {
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

func (m *OpenAIModule) initVectorizer(ctx context.Context,
	logger logrus.FieldLogger,
) error {
	apiKey := os.Getenv("OPENAI_APIKEY")
	client := clients.New(apiKey, logger)

	m.vectorizer = vectorizer.New(client)
	m.metaProvider = client

	return nil
}

func (m *OpenAIModule) initAdditionalPropertiesProvider() error {
	projector := projector.New()
	m.additionalPropertiesProvider = additional.New(projector)
	return nil
}

func (m *OpenAIModule) RootHandler() http.Handler {
	// TODO: remove once this is a capability interface
	return nil
}

func (m *OpenAIModule) VectorizeObject(ctx context.Context,
	obj *models.Object, objDiff *moduletools.ObjectDiff, cfg moduletools.ClassConfig,
) error {
	icheck := vectorizer.NewClassSettings(cfg)
	return m.vectorizer.Object(ctx, obj, objDiff, icheck)
}

func (m *OpenAIModule) MetaInfo() (map[string]interface{}, error) {
	return m.metaProvider.MetaInfo()
}

func (m *OpenAIModule) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	return m.additionalPropertiesProvider.AdditionalProperties()
}

func (m *OpenAIModule) VectorizeInput(ctx context.Context,
	input string, cfg moduletools.ClassConfig,
) ([]float32, error) {
	icheck := vectorizer.NewClassSettings(cfg)
	return m.vectorizer.Texts(ctx, []string{input}, icheck)
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.Vectorizer(New())
	_ = modulecapabilities.MetaProvider(New())
	_ = modulecapabilities.Searcher(New())
	_ = modulecapabilities.GraphQLArguments(New())
)
