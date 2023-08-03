//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package modkserve

import (
	"context"
	"net/http"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/text2vec-kserve/clients"
	"github.com/weaviate/weaviate/modules/text2vec-kserve/vectorizer"
)

const Name = "text2vec-kserve"

func New() *KServeModule {
	return &KServeModule{}
}

type KServeModule struct {
	vectorizer          textVectorizer
	validatorFactory    clients.ValidatorFactory
	graphqlProvider     modulecapabilities.GraphQLArguments
	searcher            modulecapabilities.Searcher
	nearTextTransformer modulecapabilities.TextTransform
	// additionalPropertiesProvider modulecapabilities.AdditionalProperties
	logger logrus.FieldLogger
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

func (m *KServeModule) Name() string {
	return Name
}

func (m *KServeModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Text2Vec
}

// Init implements modulecapabilities.Module
func (m *KServeModule) Init(ctx context.Context, params moduletools.ModuleInitParams) error {
	m.logger = params.GetLogger()

	if err := m.initModule(ctx, m.logger); err != nil {
		return errors.Wrap(err, "init module")
	}

	if err := m.initNearText(); err != nil {
		return errors.Wrap(err, "init neartext")
	}

	return nil
}

func (m *KServeModule) initModule(ctx context.Context,
	logger logrus.FieldLogger,
) error {
	client := clients.NewClientFacade(logger)

	m.validatorFactory = clients.ValidatorFactory(client)
	m.vectorizer = vectorizer.New(client)

	return nil
}

// RootHandler implements modulecapabilities.Module
func (m *KServeModule) RootHandler() http.Handler {
	return nil
}

// VectorizeObject implements modulecapabilities.Vectorizer
func (m *KServeModule) VectorizeObject(ctx context.Context, obj *models.Object, objDiff *moduletools.ObjectDiff, cfg moduletools.ClassConfig) error {
	settings := vectorizer.NewClassSettings(cfg)
	return m.vectorizer.Object(ctx, obj, objDiff, settings)
}

// VectorizeInput implements modulecapabilities.InputVectorizer
func (m *KServeModule) VectorizeInput(ctx context.Context, input string, cfg moduletools.ClassConfig) ([]float32, error) {
	settings := vectorizer.NewClassSettings(cfg)
	vector, err := m.vectorizer.Texts(ctx, []string{input}, settings)
	if err != nil {
		return nil, err
	}
	return vector, nil
}

// MetaInfo implements modulecapabilities.MetaProvider
func (m *KServeModule) MetaInfo() (map[string]interface{}, error) {
	return map[string]interface{}{
		"name":              "KServe Module",
		"documentationHref": "https://kserve.github.io/website",
	}, nil
}

var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.Vectorizer(New())
	_ = modulecapabilities.MetaProvider(New())
	_ = modulecapabilities.InputVectorizer(New())
)
