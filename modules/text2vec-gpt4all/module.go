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
	"github.com/weaviate/weaviate/modules/text2vec-gpt4all/additional"
	"github.com/weaviate/weaviate/modules/text2vec-gpt4all/additional/projector"
	"github.com/weaviate/weaviate/modules/text2vec-gpt4all/clients"
	"github.com/weaviate/weaviate/modules/text2vec-gpt4all/vectorizer"
)

const Name = "text2vec-gpt4all"

func New() *GPT4AllModule {
	return &GPT4AllModule{}
}

type GPT4AllModule struct {
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

func (m *GPT4AllModule) Name() string {
	return Name
}

func (m *GPT4AllModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Text2Vec
}

func (m *GPT4AllModule) Init(ctx context.Context,
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

func (m *GPT4AllModule) InitExtension(modules []modulecapabilities.Module) error {
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

func (m *GPT4AllModule) initVectorizer(ctx context.Context,
	logger logrus.FieldLogger,
) error {
	uri := os.Getenv("GPT4ALL_INFERENCE_API")
	if uri == "" {
		return errors.New("required variable GPT4ALL_INFERENCE_API is not set")
	}

	client := clients.New(uri, logger)
	if err := client.WaitForStartup(ctx, 1*time.Second); err != nil {
		return errors.Wrap(err, "init remote vectorizer")
	}

	m.vectorizer = vectorizer.New(client)
	m.metaProvider = client

	return nil
}

func (m *GPT4AllModule) initAdditionalPropertiesProvider() error {
	projector := projector.New()
	m.additionalPropertiesProvider = additional.New(projector)
	return nil
}

func (m *GPT4AllModule) RootHandler() http.Handler {
	// TODO: remove once this is a capability interface
	return nil
}

func (m *GPT4AllModule) VectorizeObject(ctx context.Context,
	obj *models.Object, objDiff *moduletools.ObjectDiff, cfg moduletools.ClassConfig,
) error {
	icheck := vectorizer.NewClassSettings(cfg)
	return m.vectorizer.Object(ctx, obj, objDiff, icheck)
}

func (m *GPT4AllModule) MetaInfo() (map[string]interface{}, error) {
	return m.metaProvider.MetaInfo()
}

func (m *GPT4AllModule) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	return m.additionalPropertiesProvider.AdditionalProperties()
}

func (m *GPT4AllModule) VectorizeInput(ctx context.Context,
	input string, cfg moduletools.ClassConfig,
) ([]float32, error) {
	return m.vectorizer.Texts(ctx, []string{input}, vectorizer.NewClassSettings(cfg))
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.Vectorizer(New())
	_ = modulecapabilities.MetaProvider(New())
)
