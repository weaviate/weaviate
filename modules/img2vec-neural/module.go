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

package modimage

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
	"github.com/weaviate/weaviate/modules/img2vec-neural/clients"
	"github.com/weaviate/weaviate/modules/img2vec-neural/vectorizer"
)

const Name = "img2vec-neural"

func New() *ImageModule {
	return &ImageModule{}
}

type ImageModule struct {
	vectorizer      imageVectorizer
	graphqlProvider modulecapabilities.GraphQLArguments
	searcher        modulecapabilities.Searcher[[]float32]
	logger          logrus.FieldLogger
}

type imageVectorizer interface {
	Object(ctx context.Context, obj *models.Object, cfg moduletools.ClassConfig) ([]float32, models.AdditionalProperties, error)
	VectorizeImage(ctx context.Context,
		id, image string, cfg moduletools.ClassConfig) ([]float32, error)
}

func (m *ImageModule) Name() string {
	return Name
}

func (m *ImageModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Img2Vec
}

func (m *ImageModule) Init(ctx context.Context,
	params moduletools.ModuleInitParams,
) error {
	m.logger = params.GetLogger()
	if err := m.initVectorizer(ctx, params.GetConfig().ModuleHttpClientTimeout, params.GetLogger()); err != nil {
		return errors.Wrap(err, "init vectorizer")
	}

	if err := m.initNearImage(); err != nil {
		return errors.Wrap(err, "init near text")
	}

	return nil
}

func (m *ImageModule) initVectorizer(ctx context.Context, timeout time.Duration,
	logger logrus.FieldLogger,
) error {
	// TODO: proper config management
	uri := os.Getenv("IMAGE_INFERENCE_API")
	if uri == "" {
		return errors.Errorf("required variable IMAGE_INFERENCE_API is not set")
	}

	client := clients.New(uri, timeout, logger)
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
	obj *models.Object, cfg moduletools.ClassConfig,
) ([]float32, models.AdditionalProperties, error) {
	return m.vectorizer.Object(ctx, obj, cfg)
}

func (m *ImageModule) VectorizableProperties(cfg moduletools.ClassConfig) (bool, []string, error) {
	ichek := vectorizer.NewClassSettings(cfg)
	mediaProps, err := ichek.Properties()
	return false, mediaProps, err
}

func (m *ImageModule) VectorizeBatch(ctx context.Context, objs []*models.Object, skipObject []bool, cfg moduletools.ClassConfig) ([][]float32, []models.AdditionalProperties, map[int]error) {
	return batch.VectorizeBatch(ctx, objs, skipObject, cfg, m.logger, m.vectorizer.Object)
}

func (m *ImageModule) MetaInfo() (map[string]interface{}, error) {
	return map[string]interface{}{}, nil
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.Vectorizer[[]float32](New())
)
