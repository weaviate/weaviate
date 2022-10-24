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

package modcohere

import (
	"context"
	"net/http"
	"os"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/moduletools"
	"github.com/semi-technologies/weaviate/modules/text2vec-cohere/additional"
	"github.com/semi-technologies/weaviate/modules/text2vec-cohere/additional/projector"
	"github.com/semi-technologies/weaviate/modules/text2vec-cohere/clients"
	"github.com/semi-technologies/weaviate/modules/text2vec-cohere/vectorizer"
	"github.com/sirupsen/logrus"
)

const Name = "text2vec-cohere"

func New() *CohereModule {
	return &CohereModule{}
}

type CohereModule struct {
	vectorizer                   textVectorizer
	metaProvider                 metaProvider
	graphqlProvider              modulecapabilities.GraphQLArguments
	searcher                     modulecapabilities.Searcher
	nearTextTransformer          modulecapabilities.TextTransform
	logger                       logrus.FieldLogger
	additionalPropertiesProvider modulecapabilities.AdditionalProperties
}

type textVectorizer interface {
	Object(ctx context.Context, obj *models.Object,
		settings vectorizer.ClassSettings) error
	Texts(ctx context.Context, input []string,
		settings vectorizer.ClassSettings) ([]float32, error)

	MoveTo(source, target []float32, weight float32) ([]float32, error)
	MoveAwayFrom(source, target []float32, weight float32) ([]float32, error)
	CombineVectors([][]float32) []float32
}

type metaProvider interface {
	MetaInfo() (map[string]interface{}, error)
}

func (m *CohereModule) Name() string {
	return Name
}

func (m *CohereModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Text2MultiVec
}

func (m *CohereModule) Init(ctx context.Context,
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

func (m *CohereModule) InitExtension(modules []modulecapabilities.Module) error {
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

func (m *CohereModule) initVectorizer(ctx context.Context,
	logger logrus.FieldLogger,
) error {
	apiKey := os.Getenv("COHERE_APIKEY")
	client := clients.New(apiKey, logger)

	m.vectorizer = vectorizer.New(client)
	m.metaProvider = client

	return nil
}

func (m *CohereModule) initAdditionalPropertiesProvider() error {
	projector := projector.New()
	m.additionalPropertiesProvider = additional.New(projector)
	return nil
}

func (m *CohereModule) RootHandler() http.Handler {
	// TODO: remove once this is a capability interface
	return nil
}

func (m *CohereModule) VectorizeObject(ctx context.Context,
	obj *models.Object, cfg moduletools.ClassConfig,
) error {
	icheck := vectorizer.NewClassSettings(cfg)
	return m.vectorizer.Object(ctx, obj, icheck)
}

func (m *CohereModule) MetaInfo() (map[string]interface{}, error) {
	return m.metaProvider.MetaInfo()
}

func (m *CohereModule) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	return m.additionalPropertiesProvider.AdditionalProperties()
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.Vectorizer(New())
	_ = modulecapabilities.MetaProvider(New())
	_ = modulecapabilities.Searcher(New())
	_ = modulecapabilities.GraphQLArguments(New())
)
