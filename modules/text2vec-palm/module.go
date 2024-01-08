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

package modpalm

import (
	"context"
	"net/http"
	"os"
	"time"

	"github.com/weaviate/weaviate/modules/text2vec-palm/config"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/text2vec-palm/additional"
	"github.com/weaviate/weaviate/modules/text2vec-palm/additional/projector"
	"github.com/weaviate/weaviate/modules/text2vec-palm/clients"
	"github.com/weaviate/weaviate/modules/text2vec-palm/vectorizer"
)

const Name = "text2vec-palm"

func New() *PalmModule {
	return &PalmModule{}
}

type PalmModule struct {
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

	MoveTo(source, target []float32, weight float32) ([]float32, error)
	MoveAwayFrom(source, target []float32, weight float32) ([]float32, error)
	CombineVectors([][]float32) []float32
}

type metaProvider interface {
	MetaInfo() (map[string]interface{}, error)
}

func (m *PalmModule) Name() string {
	return "text2vec-palm"
}

func (m *PalmModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Text2Vec
}

func (m *PalmModule) Init(ctx context.Context,
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

func (m *PalmModule) InitExtension(modules []modulecapabilities.Module) error {
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

func (m *PalmModule) initVectorizer(ctx context.Context, timeout time.Duration,
	logger logrus.FieldLogger,
) error {
	apiKey := os.Getenv("PALM_APIKEY")
	client := clients.New(apiKey, timeout, logger)

	m.vectorizer = vectorizer.New(client)
	m.metaProvider = client

	return nil
}

func (m *PalmModule) initAdditionalPropertiesProvider() error {
	projector := projector.New()
	m.additionalPropertiesProvider = additional.New(projector)
	return nil
}

func (m *PalmModule) RootHandler() http.Handler {
	// TODO: remove once this is a capability interface
	return nil
}

func (m *PalmModule) VectorizeObject(ctx context.Context,
	obj *models.Object, objDiff *moduletools.ObjectDiff, cfg moduletools.ClassConfig,
) error {
	icheck := config.NewClassSettings(cfg)
	return m.vectorizer.Object(ctx, obj, objDiff, icheck)
}

func (m *PalmModule) MetaInfo() (map[string]interface{}, error) {
	return m.metaProvider.MetaInfo()
}

func (m *PalmModule) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	return m.additionalPropertiesProvider.AdditionalProperties()
}

func (m *PalmModule) VectorizeInput(ctx context.Context,
	input string, cfg moduletools.ClassConfig,
) ([]float32, error) {
	return m.vectorizer.Texts(ctx, []string{input}, config.NewClassSettings(cfg))
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.Vectorizer(New())
	_ = modulecapabilities.MetaProvider(New())
	_ = modulecapabilities.Searcher(New())
	_ = modulecapabilities.GraphQLArguments(New())
)
