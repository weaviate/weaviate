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
	"github.com/weaviate/weaviate/modules/text2vec-model2vec/clients"
	"github.com/weaviate/weaviate/modules/text2vec-model2vec/vectorizer"
	"github.com/weaviate/weaviate/usecases/modulecomponents/additional"
)

const Name = "text2vec-model2vec"

func New() *Model2VecModule {
	return &Model2VecModule{}
}

type Model2VecModule struct {
	vectorizer                   text2vecbase.TextVectorizer[[]float32]
	metaProvider                 text2vecbase.MetaProvider
	graphqlProvider              modulecapabilities.GraphQLArguments
	searcher                     modulecapabilities.Searcher[[]float32]
	nearTextTransformer          modulecapabilities.TextTransform
	logger                       logrus.FieldLogger
	additionalPropertiesProvider modulecapabilities.AdditionalProperties
}

func (m *Model2VecModule) Name() string {
	return Name
}

func (m *Model2VecModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Text2Vec
}

func (m *Model2VecModule) Init(ctx context.Context,
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

func (m *Model2VecModule) InitExtension(modules []modulecapabilities.Module) error {
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

func (m *Model2VecModule) initVectorizer(ctx context.Context, timeout time.Duration,
	logger logrus.FieldLogger,
) error {
	waitForStartup := true
	if envWaitForStartup := os.Getenv("MODEL2VEC_WAIT_FOR_STARTUP"); envWaitForStartup != "" {
		waitForStartup = entcfg.Enabled(envWaitForStartup)
	}

	url := os.Getenv("MODEL2VEC_INFERENCE_API")

	client := clients.New(url, timeout, logger)
	if waitForStartup {
		if err := client.WaitForStartup(ctx, 1*time.Second); err != nil {
			return errors.Wrap(err, "init remote vectorizer")
		}
	}

	m.vectorizer = vectorizer.New(client)
	m.metaProvider = client

	return nil
}

func (m *Model2VecModule) initAdditionalPropertiesProvider() error {
	m.additionalPropertiesProvider = additional.NewText2VecProvider()
	return nil
}

func (m *Model2VecModule) RootHandler() http.Handler {
	// TODO: remove once this is a capability interface
	return nil
}

func (m *Model2VecModule) VectorizeObject(ctx context.Context,
	obj *models.Object, cfg moduletools.ClassConfig,
) ([]float32, models.AdditionalProperties, error) {
	return m.vectorizer.Object(ctx, obj, cfg)
}

// VectorizeBatch is _slower_ if many requests are done in parallel. So do all objects sequentially
func (m *Model2VecModule) VectorizeBatch(ctx context.Context, objs []*models.Object, skipObject []bool, cfg moduletools.ClassConfig) ([][]float32, []models.AdditionalProperties, map[int]error) {
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

func (m *Model2VecModule) MetaInfo() (map[string]interface{}, error) {
	return m.metaProvider.MetaInfo()
}

func (m *Model2VecModule) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	return m.additionalPropertiesProvider.AdditionalProperties()
}

func (m *Model2VecModule) VectorizeInput(ctx context.Context,
	input string, cfg moduletools.ClassConfig,
) ([]float32, error) {
	return m.vectorizer.Texts(ctx, []string{input}, cfg)
}

func (m *Model2VecModule) VectorizableProperties(cfg moduletools.ClassConfig) (bool, []string, error) {
	return true, nil, nil
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.Vectorizer[[]float32](New())
	_ = modulecapabilities.MetaProvider(New())
)
