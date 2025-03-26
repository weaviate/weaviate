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

	"github.com/weaviate/weaviate/usecases/modulecomponents/text2vecbase"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	entcfg "github.com/weaviate/weaviate/entities/config"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/text2vec-transformers/clients"
	"github.com/weaviate/weaviate/modules/text2vec-transformers/vectorizer"
	"github.com/weaviate/weaviate/usecases/modulecomponents/additional"
)

const Name = "text2vec-transformers"

func New() *TransformersModule {
	return &TransformersModule{}
}

type TransformersModule struct {
	vectorizer                   text2vecbase.TextVectorizer[[]float32]
	metaProvider                 text2vecbase.MetaProvider
	graphqlProvider              modulecapabilities.GraphQLArguments
	searcher                     modulecapabilities.Searcher[[]float32]
	nearTextTransformer          modulecapabilities.TextTransform
	logger                       logrus.FieldLogger
	additionalPropertiesProvider modulecapabilities.AdditionalProperties
}

func (m *TransformersModule) Name() string {
	return Name
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

	waitForStartup := true
	if envWaitForStartup := os.Getenv("TRANSFORMERS_WAIT_FOR_STARTUP"); envWaitForStartup != "" {
		waitForStartup = entcfg.Enabled(envWaitForStartup)
	}

	client := clients.New(uriPassage, uriQuery, timeout, logger)
	if waitForStartup {
		if err := client.WaitForStartup(ctx, 1*time.Second); err != nil {
			return errors.Wrap(err, "init remote vectorizer")
		}
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
	obj *models.Object, cfg moduletools.ClassConfig,
) ([]float32, models.AdditionalProperties, error) {
	return m.vectorizer.Object(ctx, obj, cfg)
}

// VectorizeBatch is _slower_ if many requests are done in parallel. So do all objects sequentially
func (m *TransformersModule) VectorizeBatch(ctx context.Context, objs []*models.Object, skipObject []bool, cfg moduletools.ClassConfig) ([][]float32, []models.AdditionalProperties, map[int]error) {
	vecs := make([][]float32, len(objs))
	addProps := make([]models.AdditionalProperties, len(objs))
	// error should be the exception so dont preallocate
	errs := make(map[int]error, 0)
	for i, obj := range objs {
		if skipObject[i] {
			continue
		}
		vec, addProp, err := m.vectorizer.Object(ctx, obj, cfg)
		if err != nil {
			errs[i] = err
			continue
		}
		addProps[i] = addProp
		vecs[i] = vec
	}

	return vecs, addProps, errs
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

func (m *TransformersModule) VectorizableProperties(cfg moduletools.ClassConfig) (bool, []string, error) {
	return true, nil, nil
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.Vectorizer[[]float32](New())
	_ = modulecapabilities.MetaProvider(New())
)
