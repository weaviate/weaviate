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

package modopenai

import (
	"context"
	"net/http"
	"os"
	"time"

	"github.com/weaviate/weaviate/usecases/modulecomponents/batch"
	"github.com/weaviate/weaviate/usecases/monitoring"

	"github.com/weaviate/weaviate/modules/text2vec-openai/ent"

	"github.com/weaviate/weaviate/usecases/modulecomponents/text2vecbase"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/text2vec-openai/clients"
	"github.com/weaviate/weaviate/usecases/modulecomponents/additional"
)

const (
	Name = "text2vec-openai"
)

var batchSettings = batch.Settings{
	TokenMultiplier:    1,
	MaxTimePerBatch:    float64(10),
	MaxObjectsPerBatch: 2000, // https://platform.openai.com/docs/api-reference/embeddings/create
	// cant find any info about this on the website besides this forum thread:https://community.openai.com/t/max-total-embeddings-tokens-per-request/1254699
	// we had customers run into this error, too
	MaxTokensPerBatch: func(cfg moduletools.ClassConfig) int { return 300000 },
	HasTokenLimit:     true,
	ReturnsRateLimit:  true,
}

func New() *OpenAIModule {
	return &OpenAIModule{}
}

type OpenAIModule struct {
	vectorizer                   text2vecbase.TextVectorizerBatch[[]float32]
	metaProvider                 text2vecbase.MetaProvider
	graphqlProvider              modulecapabilities.GraphQLArguments
	searcher                     modulecapabilities.Searcher[[]float32]
	nearTextTransformer          modulecapabilities.TextTransform
	logger                       logrus.FieldLogger
	additionalPropertiesProvider modulecapabilities.AdditionalProperties
}

func (m *OpenAIModule) Name() string {
	return Name
}

func (m *OpenAIModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Text2ManyVec
}

func (m *OpenAIModule) Init(ctx context.Context,
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

func (m *OpenAIModule) initVectorizer(ctx context.Context, timeout time.Duration,
	logger logrus.FieldLogger,
) error {
	openAIApiKey := os.Getenv("OPENAI_APIKEY")
	openAIOrganization := os.Getenv("OPENAI_ORGANIZATION")
	azureApiKey := os.Getenv("AZURE_APIKEY")

	client := clients.New(openAIApiKey, openAIOrganization, azureApiKey, timeout, logger)

	m.vectorizer = text2vecbase.New(client,
		batch.NewBatchVectorizer(client, 50*time.Second, batchSettings, logger, m.Name()),
		batch.ReturnBatchTokenizer(batchSettings.TokenMultiplier, m.Name(), ent.LowerCaseInput),
	)

	m.metaProvider = client

	return nil
}

func (m *OpenAIModule) initAdditionalPropertiesProvider() error {
	m.additionalPropertiesProvider = additional.NewText2VecProvider()
	return nil
}

func (m *OpenAIModule) RootHandler() http.Handler {
	// TODO: remove once this is a capability interface
	return nil
}

func (m *OpenAIModule) VectorizeObject(ctx context.Context,
	obj *models.Object, cfg moduletools.ClassConfig,
) ([]float32, models.AdditionalProperties, error) {
	monitoring.GetMetrics().ModuleExternalRequestSingleCount.WithLabelValues(m.Name(), "vectorizeObject").Inc()
	icheck := ent.NewClassSettings(cfg)
	return m.vectorizer.Object(ctx, obj, cfg, icheck)
}

func (m *OpenAIModule) VectorizeBatch(ctx context.Context, objs []*models.Object, skipObject []bool, cfg moduletools.ClassConfig) ([][]float32, []models.AdditionalProperties, map[int]error) {
	monitoring.GetMetrics().ModuleExternalBatchLength.WithLabelValues("vectorizeBatch", m.Name()).Observe(float64(len(objs)))
	monitoring.GetMetrics().ModuleExternalRequestBatchCount.WithLabelValues(m.Name(), "vectorizeBatch").Inc()
	vecs, errs := m.vectorizer.ObjectBatch(ctx, objs, skipObject, cfg)
	return vecs, nil, errs
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
	monitoring.GetMetrics().ModuleExternalRequestSingleCount.WithLabelValues(m.Name(), "vectorizeTexts").Inc()
	monitoring.GetMetrics().ModuleExternalRequestSize.WithLabelValues(m.Name(), "vectorizeTexts").Observe(float64(len(input)))
	return m.vectorizer.Texts(ctx, []string{input}, cfg)
}

func (m *OpenAIModule) VectorizableProperties(cfg moduletools.ClassConfig) (bool, []string, error) {
	return true, nil, nil
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.Vectorizer[[]float32](New())
	_ = modulecapabilities.MetaProvider(New())
	_ = modulecapabilities.Searcher[[]float32](New())
	_ = modulecapabilities.GraphQLArguments(New())
)
