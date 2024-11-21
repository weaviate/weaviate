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

package modclip

import (
	"context"
	"net/http"
	"os"
	"time"

	"github.com/weaviate/weaviate/usecases/modulecomponents/batch"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/multi2vec-jinaai/clients"
	"github.com/weaviate/weaviate/modules/multi2vec-jinaai/ent"
	"github.com/weaviate/weaviate/modules/multi2vec-jinaai/vectorizer"
)

const Name = "multi2vec-jinaai"

func New() *Module {
	return &Module{}
}

type Module struct {
	vectorizer               *vectorizer.Vectorizer
	nearImageGraphqlProvider modulecapabilities.GraphQLArguments
	nearImageSearcher        modulecapabilities.Searcher
	nearTextGraphqlProvider  modulecapabilities.GraphQLArguments
	nearTextSearcher         modulecapabilities.Searcher
	nearTextTransformer      modulecapabilities.TextTransform
	metaClient               metaClient
	logger                   logrus.FieldLogger
}

type metaClient interface {
	MetaInfo() (map[string]interface{}, error)
}

func (m *Module) Name() string {
	return Name
}

func (m *Module) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Multi2Vec
}

func (m *Module) Init(ctx context.Context,
	params moduletools.ModuleInitParams,
) error {
	m.logger = params.GetLogger()
	if err := m.initVectorizer(ctx, params.GetConfig().ModuleHttpClientTimeout, params.GetLogger()); err != nil {
		return errors.Wrap(err, "init vectorizer")
	}

	if err := m.initNearImage(); err != nil {
		return errors.Wrap(err, "init near image")
	}

	return nil
}

func (m *Module) InitExtension(modules []modulecapabilities.Module) error {
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
		return errors.Wrap(err, "init near text")
	}

	return nil
}

func (m *Module) initVectorizer(ctx context.Context, timeout time.Duration,
	logger logrus.FieldLogger,
) error {
	apiKey := os.Getenv("JINAAI_APIKEY")
	client := clients.New(apiKey, timeout, logger)

	m.vectorizer = vectorizer.New(client)
	m.metaClient = client

	return nil
}

func (m *Module) RootHandler() http.Handler {
	// TODO: remove once this is a capability interface
	return nil
}

func (m *Module) VectorizeObject(ctx context.Context,
	obj *models.Object, cfg moduletools.ClassConfig,
) ([]float32, models.AdditionalProperties, error) {
	return m.vectorizer.Object(ctx, obj, cfg)
}

func (m *Module) VectorizeBatch(ctx context.Context, objs []*models.Object, skipObject []bool, cfg moduletools.ClassConfig) ([][]float32, []models.AdditionalProperties, map[int]error) {
	return batch.VectorizeBatch(ctx, objs, skipObject, cfg, m.logger, m.vectorizer.Object)
}

func (m *Module) VectorizableProperties(cfg moduletools.ClassConfig) (bool, []string, error) {
	ichek := ent.NewClassSettings(cfg)
	mediaProps, err := ichek.Properties()
	return false, mediaProps, err
}

func (m *Module) MetaInfo() (map[string]interface{}, error) {
	return m.metaClient.MetaInfo()
}

func (m *Module) VectorizeInput(ctx context.Context,
	input string, cfg moduletools.ClassConfig,
) ([]float32, error) {
	return m.vectorizer.Texts(ctx, []string{input}, cfg)
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.Vectorizer(New())
	_ = modulecapabilities.InputVectorizer(New())
)
