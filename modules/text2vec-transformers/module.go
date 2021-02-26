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
	vectorizer      textVectorizer
	graphqlProvider modulecapabilities.GraphQLArguments
	searcher        modulecapabilities.Searcher
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

func (m *TransformersModule) Name() string {
	return "text2vec-transformers"
}

func (m *TransformersModule) Init(ctx context.Context,
	params moduletools.ModuleInitParams) error {
	if err := m.initVectorizer(ctx, params.GetLogger()); err != nil {
		return errors.Wrap(err, "init vectorizer")
	}

	if err := m.initNearText(); err != nil {
		return errors.Wrap(err, "init near text")
	}

	return nil
}

func (m *TransformersModule) initVectorizer(ctx context.Context,
	logger logrus.FieldLogger) error {
	client := clients.New("http://localhost:8000", logger)
	if err := client.WaitForStartup(ctx, 1*time.Second); err != nil {
		return errors.Wrap(err, "init remote vectorizer")
	}

	m.vectorizer = vectorizer.New(client)

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

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.Vectorizer(New())
)
