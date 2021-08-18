//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2021 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package modtransformers

import (
	"context"
	"net/http"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/moduletools"
	"github.com/semi-technologies/weaviate/modules/text2vec-transformers/clients"
	"github.com/semi-technologies/weaviate/modules/text2vec-transformers/vectorizer"
	"github.com/sirupsen/logrus"
)

func New() *TransformersModule {
	return &TransformersModule{}
}

type TransformersModule struct {
	vectorizer          textVectorizer
	metaProvider        metaProvider
	graphqlProvider     modulecapabilities.GraphQLArguments
	searcher            modulecapabilities.Searcher
	nearTextTransformer modulecapabilities.TextTransform
	logger              logrus.FieldLogger
}

type textVectorizer interface {
	Object(ctx context.Context, obj *models.Object,
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

func (m *TransformersModule) Name() string {
	return "text2vec-transformers"
}

func (m *TransformersModule) Init(ctx context.Context,
	params moduletools.ModuleInitParams) error {
	m.logger = params.GetLogger()

	if err := m.initVectorizer(ctx, m.logger); err != nil {
		return errors.Wrap(err, "init vectorizer")
	}

	return nil
}

func (m *TransformersModule) InitDependency(modules []modulecapabilities.Module) error {
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

func (m *TransformersModule) initVectorizer(ctx context.Context,
	logger logrus.FieldLogger) error {
	// TODO: gh-1486 proper config management
	uri := os.Getenv("TRANSFORMERS_INFERENCE_API")
	if uri == "" {
		return errors.Errorf("required variable TRANSFORMERS_INFERENCE_API is not set")
	}

	client := clients.New(uri, logger)
	if err := client.WaitForStartup(ctx, 1*time.Second); err != nil {
		return errors.Wrap(err, "init remote vectorizer")
	}

	m.vectorizer = vectorizer.New(client)
	m.metaProvider = client

	return nil
}

func (m *TransformersModule) RootHandler() http.Handler {
	// TODO: remove once this is a capability interface
	return nil
}

func (m *TransformersModule) VectorizeObject(ctx context.Context,
	obj *models.Object, cfg moduletools.ClassConfig) error {
	icheck := vectorizer.NewClassSettings(cfg)
	return m.vectorizer.Object(ctx, obj, icheck)
}

func (m *TransformersModule) MetaInfo() (map[string]interface{}, error) {
	return m.metaProvider.MetaInfo()
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.Vectorizer(New())
	_ = modulecapabilities.MetaProvider(New())
)
