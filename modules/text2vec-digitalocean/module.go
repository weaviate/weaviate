//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package moddigitalocean

import (
	"context"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/text2vec-digitalocean/clients"
	"github.com/weaviate/weaviate/modules/text2vec-digitalocean/ent"
	"github.com/weaviate/weaviate/usecases/modulecomponents/additional"
	"github.com/weaviate/weaviate/usecases/modulecomponents/batch"
	"github.com/weaviate/weaviate/usecases/modulecomponents/text2vecbase"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

const Name = "text2vec-digitalocean"

var batchSettings = batch.Settings{
	TokenMultiplier:    1,
	MaxTimePerBatch:    float64(10),
	MaxObjectsPerBatch: 96,
	MaxTokensPerBatch:  func(cfg moduletools.ClassConfig) int { return 300000 },
	HasTokenLimit:      false,
	ReturnsRateLimit:   true,
}

func New() *DigitalOceanModule {
	return &DigitalOceanModule{}
}

type DigitalOceanModule struct {
	vectorizer                   text2vecbase.TextVectorizerBatch[[]float32]
	metaProvider                 text2vecbase.MetaProvider
	graphqlProvider              modulecapabilities.GraphQLArguments
	searcher                     modulecapabilities.Searcher[[]float32]
	nearTextTransformer          modulecapabilities.TextTransform
	logger                       logrus.FieldLogger
	additionalPropertiesProvider modulecapabilities.AdditionalProperties
}

func (m *DigitalOceanModule) Name() string {
	return Name
}

func (m *DigitalOceanModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Text2ManyVec
}

func (m *DigitalOceanModule) Init(ctx context.Context,
	params moduletools.ModuleInitParams,
) error {
	m.logger = params.GetLogger()

	if err := m.initVectorizer(params.GetConfig().ModuleHttpClientTimeout, m.logger); err != nil {
		return errors.Wrap(err, "init vectorizer")
	}

	if err := m.initAdditionalPropertiesProvider(); err != nil {
		return errors.Wrap(err, "init additional properties provider")
	}

	return nil
}

func (m *DigitalOceanModule) InitExtension(modules []modulecapabilities.Module) error {
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

func (m *DigitalOceanModule) initVectorizer(timeout time.Duration,
	logger logrus.FieldLogger,
) error {
	apiKey := os.Getenv("DIGITALOCEAN_APIKEY")

	client := clients.New(apiKey, timeout, logger)

	m.vectorizer = text2vecbase.New(
		client,
		batch.NewBatchVectorizer(client, 50*time.Second, batchSettings, logger, m.Name()),
		batch.ReturnBatchTokenizer(batchSettings.TokenMultiplier, m.Name(), ent.LowerCaseInput),
	)

	m.metaProvider = client

	return nil
}

func (m *DigitalOceanModule) initAdditionalPropertiesProvider() error {
	m.additionalPropertiesProvider = additional.NewText2VecProvider()
	return nil
}

func (m *DigitalOceanModule) VectorizeObject(ctx context.Context,
	obj *models.Object, cfg moduletools.ClassConfig,
) ([]float32, models.AdditionalProperties, error) {
	monitoring.GetMetrics().ModuleExternalRequestSingleCount.WithLabelValues(m.Name(), "vectorizeObject").Inc()
	icheck := ent.NewClassSettings(cfg)
	return m.vectorizer.Object(ctx, obj, cfg, icheck)
}

func (m *DigitalOceanModule) VectorizeBatch(ctx context.Context, objs []*models.Object, skipObject []bool, cfg moduletools.ClassConfig) ([][]float32, []models.AdditionalProperties, map[int]error) {
	monitoring.GetMetrics().ModuleExternalBatchLength.WithLabelValues("vectorizeBatch", m.Name()).Observe(float64(len(objs)))
	monitoring.GetMetrics().ModuleExternalRequestBatchCount.WithLabelValues(m.Name(), "vectorizeBatch").Inc()
	vecs, errs := m.vectorizer.ObjectBatch(ctx, objs, skipObject, cfg)
	return vecs, nil, errs
}

func (m *DigitalOceanModule) MetaInfo() (map[string]any, error) {
	return m.metaProvider.MetaInfo()
}

func (m *DigitalOceanModule) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	return m.additionalPropertiesProvider.AdditionalProperties()
}

func (m *DigitalOceanModule) VectorizeInput(ctx context.Context,
	input string, cfg moduletools.ClassConfig,
) ([]float32, error) {
	monitoring.GetMetrics().ModuleExternalRequestSingleCount.WithLabelValues(m.Name(), "vectorizeTexts").Inc()
	monitoring.GetMetrics().ModuleExternalRequestSize.WithLabelValues(m.Name(), "vectorizeTexts").Observe(float64(len(input)))
	return m.vectorizer.Texts(ctx, []string{input}, cfg)
}

func (m *DigitalOceanModule) VectorizableProperties(cfg moduletools.ClassConfig) (bool, []string, error) {
	return true, nil, nil
}

var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.Vectorizer[[]float32](New())
	_ = modulecapabilities.MetaProvider(New())
	_ = modulecapabilities.Searcher[[]float32](New())
	_ = modulecapabilities.GraphQLArguments(New())
)
