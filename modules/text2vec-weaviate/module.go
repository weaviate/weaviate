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

package modwcsembed

import (
	"context"
	"net/http"
	"os"
	"time"

	"github.com/weaviate/weaviate/modules/text2vec-weaviate/ent"

	"github.com/weaviate/weaviate/usecases/modulecomponents/text2vecbase"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/text2vec-weaviate/clients"
	"github.com/weaviate/weaviate/modules/text2vec-weaviate/vectorizer"
	"github.com/weaviate/weaviate/usecases/modulecomponents/additional"
)

const Name = "text2vec-weaviate"

type WCSEmbedModule struct {
	vectorizer                   text2vecbase.TextVectorizerBatch
	metaProvider                 text2vecbase.MetaProvider
	graphqlProvider              modulecapabilities.GraphQLArguments
	searcher                     modulecapabilities.Searcher
	nearTextTransformer          modulecapabilities.TextTransform
	logger                       logrus.FieldLogger
	additionalPropertiesProvider modulecapabilities.AdditionalProperties
}

func New() *WCSEmbedModule {
	return &WCSEmbedModule{}
}

func (m *WCSEmbedModule) Name() string {
	return Name
}

func (m *WCSEmbedModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Text2MultiVec
}

func (m *WCSEmbedModule) Init(ctx context.Context,
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

func (m *WCSEmbedModule) InitExtension(modules []modulecapabilities.Module) error {
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

func (m *WCSEmbedModule) initVectorizer(ctx context.Context, timeout time.Duration,
	logger logrus.FieldLogger,
) error {
	apiKey := os.Getenv("WEAVIATE_INFERENCE_KEY")
	client := clients.New(apiKey, timeout, logger)

	m.vectorizer = vectorizer.New(client, m.logger)
	m.metaProvider = client

	return nil
}

func (m *WCSEmbedModule) initAdditionalPropertiesProvider() error {
	m.additionalPropertiesProvider = additional.NewText2VecProvider()
	return nil
}

func (m *WCSEmbedModule) RootHandler() http.Handler {
	// TODO: remove once this is a capability interface
	return nil
}

func (m *WCSEmbedModule) VectorizeObject(ctx context.Context,
	obj *models.Object, cfg moduletools.ClassConfig,
) ([]float32, models.AdditionalProperties, error) {
	return m.vectorizer.Object(ctx, obj, cfg, ent.NewClassSettings(cfg))
}

func (m *WCSEmbedModule) VectorizeBatch(ctx context.Context, objs []*models.Object, skipObject []bool, cfg moduletools.ClassConfig) ([][]float32, []models.AdditionalProperties, map[int]error) {
	vecs, errs := m.vectorizer.ObjectBatch(ctx, objs, skipObject, cfg)

	return vecs, nil, errs
}

func (m *WCSEmbedModule) MetaInfo() (map[string]interface{}, error) {
	return m.metaProvider.MetaInfo()
}

func (m *WCSEmbedModule) VectorizableProperties(cfg moduletools.ClassConfig) (bool, []string, error) {
	return true, nil, nil
}

func (m *WCSEmbedModule) VectorizeInput(ctx context.Context,
	input string, cfg moduletools.ClassConfig,
) ([]float32, error) {
	return m.vectorizer.Texts(ctx, []string{input}, cfg)
}

func (m *WCSEmbedModule) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	return m.additionalPropertiesProvider.AdditionalProperties()
}

var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.Vectorizer(New())
	_ = modulecapabilities.MetaProvider(New())
	_ = modulecapabilities.Searcher(New())
	_ = modulecapabilities.GraphQLArguments(New())
	_ = modulecapabilities.InputVectorizer(New())
)
