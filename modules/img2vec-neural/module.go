//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package modimage

import (
	"context"
	"net/http"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
	"github.com/semi-technologies/weaviate/entities/moduletools"
	"github.com/semi-technologies/weaviate/modules/img2vec-neural/clients"
	"github.com/semi-technologies/weaviate/modules/img2vec-neural/vectorizer"
	"github.com/sirupsen/logrus"
)

func New() *ImageModule {
	return &ImageModule{}
}

type ImageModule struct {
	vectorizer      imageVectorizer
	graphqlProvider modulecapabilities.GraphQLArguments
	searcher        modulecapabilities.Searcher
}

type imageVectorizer interface {
	Object(ctx context.Context, object *models.Object,
		settings vectorizer.ClassSettings) error
	VectorizeImage(ctx context.Context,
		id, image string) ([]float32, error)
}

func (m *ImageModule) Name() string {
	return "img2vec-neural"
}

func (m *ImageModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Img2Vec
}

func (m *ImageModule) Init(ctx context.Context,
	params moduletools.ModuleInitParams) error {
	if err := m.initVectorizer(ctx, params.GetLogger()); err != nil {
		return errors.Wrap(err, "init vectorizer")
	}

	if err := m.initNearImage(); err != nil {
		return errors.Wrap(err, "init near text")
	}

	return nil
}

func (m *ImageModule) initVectorizer(ctx context.Context,
	logger logrus.FieldLogger) error {
	// TODO: proper config management
	uri := os.Getenv("IMAGE_INFERENCE_API")
	if uri == "" {
		return errors.Errorf("required variable IMAGE_INFERENCE_API is not set")
	}

	client := clients.New(uri, logger)
	if err := client.WaitForStartup(ctx, 1*time.Second); err != nil {
		return errors.Wrap(err, "init remote vectorizer")
	}

	m.vectorizer = vectorizer.New(client)

	return nil
}

func (m *ImageModule) RootHandler() http.Handler {
	// TODO: remove once this is a capability interface
	return nil
}

func (m *ImageModule) VectorizeObject(ctx context.Context,
	obj *models.Object, cfg moduletools.ClassConfig) error {
	icheck := vectorizer.NewClassSettings(cfg)
	return m.vectorizer.Object(ctx, obj, icheck)
}

func (m *ImageModule) MetaInfo() (map[string]interface{}, error) {
	return map[string]interface{}{}, nil
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.Vectorizer(New())
)
