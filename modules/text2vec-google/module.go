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

package modgoogle

import (
	"context"
	"os"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	entcfg "github.com/weaviate/weaviate/entities/config"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/text2vec-google/clients"
	"github.com/weaviate/weaviate/modules/text2vec-google/vectorizer"
	"github.com/weaviate/weaviate/usecases/modulecomponents/additional"
	"github.com/weaviate/weaviate/usecases/modulecomponents/batch"
	"github.com/weaviate/weaviate/usecases/modulecomponents/text2vecbase"
	"github.com/weaviate/weaviate/usecases/modulecomponents/vectorizer/batchtext"
)

const (
	Name                      = "text2vec-google"
	LegacyName                = "text2vec-palm"
	defaultBatchSize          = 50
	defaultMaxObjectsPerBatch = 100
	defaultRPM                = 10000
)

func newBatchSettings(maxObjectsPerBatch int) batch.Settings {
	return batch.Settings{
		TokenMultiplier:    1.3,
		MaxObjectsPerBatch: maxObjectsPerBatch,
		MaxTimePerBatch:    float64(10),
		MaxTokensPerBatch:  func(cfg moduletools.ClassConfig) int { return 20000 },
		HasTokenLimit:      true,
		ReturnsRateLimit:   false,
	}
}

func New() *GoogleModule {
	return &GoogleModule{}
}

type GoogleModule struct {
	vectorizer                   text2vecbase.TextVectorizerBatch[[]float32]
	vectorizerBatchSimple        batchtext.Vectorizer[[]float32]
	useBatchSimpleVectorizer     bool
	sendObjectsInBatch           bool
	batchSize                    int
	batchSimple                  *batch.BatchSimple[[]float32]
	vectorizerWithTitleProperty  text2vecbase.TextVectorizer[[]float32]
	metaProvider                 text2vecbase.MetaProvider
	graphqlProvider              modulecapabilities.GraphQLArguments
	searcher                     modulecapabilities.Searcher[[]float32]
	nearTextTransformer          modulecapabilities.TextTransform
	logger                       logrus.FieldLogger
	additionalPropertiesProvider modulecapabilities.AdditionalProperties
}

func (m *GoogleModule) Name() string {
	return Name
}

func (m *GoogleModule) AltNames() []string {
	return []string{LegacyName}
}

func (m *GoogleModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Text2Vec
}

func (m *GoogleModule) Init(ctx context.Context,
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

func (m *GoogleModule) InitExtension(modules []modulecapabilities.Module) error {
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

func (m *GoogleModule) initVectorizer(ctx context.Context, timeout time.Duration,
	logger logrus.FieldLogger,
) error {
	apiKey := os.Getenv("GOOGLE_APIKEY")
	if apiKey == "" {
		apiKey = os.Getenv("PALM_APIKEY")
	}

	useGoogleAuth := entcfg.Enabled(os.Getenv("USE_GOOGLE_AUTH"))
	m.useBatchSimpleVectorizer = entcfg.Enabled(os.Getenv("USE_T2V_GOOGLE_BATCH_SIMPLE_LOGIC"))
	m.sendObjectsInBatch = entcfg.Enabled(os.Getenv("USE_T2V_GOOGLE_BATCH_SEND_OBJECTS_IN_BATCH"))
	m.batchSize = m.envPositiveInt("T2V_GOOGLE_BATCH_SIMPLE_BATCH_SIZE", defaultBatchSize, logger)
	maxObjectsPerBatch := m.envPositiveInt("T2V_GOOGLE_BATCH_MAX_OBJECTS_PER_BATCH", defaultMaxObjectsPerBatch, logger)
	rpm := m.envPositiveInt("T2V_GOOGLE_BATCH_RPM", defaultRPM, logger)

	client := clients.New(apiKey, useGoogleAuth, rpm, timeout, logger)
	m.vectorizerBatchSimple = batchtext.NewWithAltNames(Name, m.AltNames(), vectorizer.LowerCaseInput, client)
	m.vectorizerWithTitleProperty = vectorizer.New(client)
	m.batchSimple = batch.NewBatchSimple[[]float32](logger, rpm)

	batchSettings := newBatchSettings(maxObjectsPerBatch)
	m.vectorizer = text2vecbase.New(client,
		batch.NewBatchVectorizer(client, 50*time.Second, batchSettings, logger, m.Name()),
		batch.ReturnBatchTokenizerWithAltNames(batchSettings.TokenMultiplier, m.Name(), m.AltNames(), vectorizer.LowerCaseInput),
	)

	m.metaProvider = client

	return nil
}

func (m *GoogleModule) initAdditionalPropertiesProvider() error {
	m.additionalPropertiesProvider = additional.NewText2VecProvider()
	return nil
}

func (m *GoogleModule) VectorizeObject(ctx context.Context,
	obj *models.Object, cfg moduletools.ClassConfig,
) ([]float32, models.AdditionalProperties, error) {
	if m.useBatchSimpleVectorizer {
		// use batch simple logic
		return m.vectorizerBatchSimple.Object(ctx, obj, cfg)
	}
	// use default batch logic
	icheck := vectorizer.NewClassSettings(cfg)
	return m.vectorizer.Object(ctx, obj, cfg, icheck)
}

func (m *GoogleModule) VectorizeBatch(ctx context.Context, objs []*models.Object, skipObject []bool, cfg moduletools.ClassConfig) ([][]float32, []models.AdditionalProperties, map[int]error) {
	if m.useBatchSimpleVectorizer {
		// use batch simple logic
		return m.batchSimple.VectorizeBatchObjects(ctx, objs, skipObject, cfg, m.vectorizerBatchSimple.Objects, m.batchSize)
	}
	// use default batch logic
	icheck := vectorizer.NewClassSettings(cfg)
	if m.sendObjectsInBatch && icheck.TitleProperty() == "" {
		// this logic sends a batch of objects in 1 request
		vecs, errs := m.vectorizer.ObjectBatch(ctx, objs, skipObject, cfg)
		return vecs, nil, errs
	}
	// this logic always sends multiple requests, each containing 1 object
	return m.batchSimple.VectorizeBatch(ctx, objs, skipObject, cfg, m.vectorizerWithTitleProperty.Object)
}

func (m *GoogleModule) MetaInfo() (map[string]interface{}, error) {
	return m.metaProvider.MetaInfo()
}

func (m *GoogleModule) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	return m.additionalPropertiesProvider.AdditionalProperties()
}

func (m *GoogleModule) VectorizeInput(ctx context.Context,
	input string, cfg moduletools.ClassConfig,
) ([]float32, error) {
	if m.useBatchSimpleVectorizer {
		// use batch simple logic
		return m.vectorizerBatchSimple.Texts(ctx, []string{input}, cfg)
	}
	// use default batch logic
	return m.vectorizer.Texts(ctx, []string{input}, cfg)
}

func (m *GoogleModule) VectorizableProperties(cfg moduletools.ClassConfig) (bool, []string, error) {
	return true, nil, nil
}

func (m *GoogleModule) envPositiveInt(name string, defaultValue int, logger logrus.FieldLogger) int {
	value := os.Getenv(name)
	if value == "" {
		return defaultValue
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		logger.Warnf("invalid (must be a positive number > 0) %s value: %s, using default: %v", name, value, defaultValue)
		return defaultValue
	}
	return parsed
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.Vectorizer[[]float32](New())
	_ = modulecapabilities.MetaProvider(New())
	_ = modulecapabilities.Searcher[[]float32](New())
	_ = modulecapabilities.GraphQLArguments(New())
	_ = modulecapabilities.ModuleHasAltNames(New())
)
