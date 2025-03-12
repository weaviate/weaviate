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

package modbind

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
	"github.com/weaviate/weaviate/modules/multi2vec-bind/clients"
	"github.com/weaviate/weaviate/modules/multi2vec-bind/vectorizer"
)

const Name = "multi2vec-bind"

func New() *BindModule {
	return &BindModule{}
}

type BindModule struct {
	bindVectorizer             bindVectorizer
	nearImageGraphqlProvider   modulecapabilities.GraphQLArguments
	nearImageSearcher          modulecapabilities.Searcher[[]float32]
	nearAudioGraphqlProvider   modulecapabilities.GraphQLArguments
	nearAudioSearcher          modulecapabilities.Searcher[[]float32]
	nearVideoGraphqlProvider   modulecapabilities.GraphQLArguments
	nearVideoSearcher          modulecapabilities.Searcher[[]float32]
	nearIMUGraphqlProvider     modulecapabilities.GraphQLArguments
	nearIMUSearcher            modulecapabilities.Searcher[[]float32]
	nearThermalGraphqlProvider modulecapabilities.GraphQLArguments
	nearThermalSearcher        modulecapabilities.Searcher[[]float32]
	nearDepthGraphqlProvider   modulecapabilities.GraphQLArguments
	nearDepthSearcher          modulecapabilities.Searcher[[]float32]
	textVectorizer             textVectorizer
	nearTextGraphqlProvider    modulecapabilities.GraphQLArguments
	nearTextSearcher           modulecapabilities.Searcher[[]float32]
	nearTextTransformer        modulecapabilities.TextTransform
	metaClient                 metaClient
	logger                     logrus.FieldLogger
}

type metaClient interface {
	MetaInfo() (map[string]interface{}, error)
}

type bindVectorizer interface {
	Object(ctx context.Context, object *models.Object, cfg moduletools.ClassConfig) ([]float32, models.AdditionalProperties, error)
	VectorizeImage(ctx context.Context, id, image string, cfg moduletools.ClassConfig) ([]float32, error)
	VectorizeAudio(ctx context.Context, audio string, cfg moduletools.ClassConfig) ([]float32, error)
	VectorizeVideo(ctx context.Context, video string, cfg moduletools.ClassConfig) ([]float32, error)
	VectorizeIMU(ctx context.Context, imu string, cfg moduletools.ClassConfig) ([]float32, error)
	VectorizeThermal(ctx context.Context, thermal string, cfg moduletools.ClassConfig) ([]float32, error)
	VectorizeDepth(ctx context.Context, depth string, cfg moduletools.ClassConfig) ([]float32, error)
}

type textVectorizer interface {
	Texts(ctx context.Context, input []string,
		cfg moduletools.ClassConfig) ([]float32, error)
}

func (m *BindModule) Name() string {
	return Name
}

func (m *BindModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Multi2Vec
}

func (m *BindModule) Init(ctx context.Context,
	params moduletools.ModuleInitParams,
) error {
	m.logger = params.GetLogger()
	if err := m.initVectorizer(ctx, params.GetConfig().ModuleHttpClientTimeout, params.GetLogger()); err != nil {
		return errors.Wrap(err, "init vectorizer")
	}

	if err := m.initNearImage(); err != nil {
		return errors.Wrap(err, "init near image")
	}

	if err := m.initNearAudio(); err != nil {
		return errors.Wrap(err, "init near audio")
	}

	if err := m.initNearVideo(); err != nil {
		return errors.Wrap(err, "init near video")
	}

	if err := m.initNearIMU(); err != nil {
		return errors.Wrap(err, "init near imu")
	}

	if err := m.initNearThermal(); err != nil {
		return errors.Wrap(err, "init near thermal")
	}

	if err := m.initNearDepth(); err != nil {
		return errors.Wrap(err, "init near depth")
	}

	return nil
}

func (m *BindModule) InitExtension(modules []modulecapabilities.Module) error {
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

func (m *BindModule) initVectorizer(ctx context.Context, timeout time.Duration,
	logger logrus.FieldLogger,
) error {
	// TODO: proper config management
	uri := os.Getenv("BIND_INFERENCE_API")
	if uri == "" {
		return errors.Errorf("required variable BIND_INFERENCE_API is not set")
	}

	client := clients.New(uri, timeout, logger)
	if err := client.WaitForStartup(ctx, 1*time.Second); err != nil {
		return errors.Wrap(err, "init remote vectorizer")
	}

	m.bindVectorizer = vectorizer.New(client)
	m.textVectorizer = vectorizer.New(client)
	m.metaClient = client

	return nil
}

func (m *BindModule) RootHandler() http.Handler {
	// TODO: remove once this is a capability interface
	return nil
}

func (m *BindModule) VectorizeObject(ctx context.Context,
	obj *models.Object, cfg moduletools.ClassConfig,
) ([]float32, models.AdditionalProperties, error) {
	return m.bindVectorizer.Object(ctx, obj, cfg)
}

func (m *BindModule) VectorizableProperties(cfg moduletools.ClassConfig) (bool, []string, error) {
	ichek := vectorizer.NewClassSettings(cfg)
	mediaProps, err := ichek.Properties()
	return true, mediaProps, err
}

func (m *BindModule) MetaInfo() (map[string]interface{}, error) {
	return m.metaClient.MetaInfo()
}

func (m *BindModule) VectorizeBatch(ctx context.Context, objs []*models.Object, skipObject []bool, cfg moduletools.ClassConfig) ([][]float32, []models.AdditionalProperties, map[int]error) {
	return batch.VectorizeBatch(ctx, objs, skipObject, cfg, m.logger, m.bindVectorizer.Object)
}

func (m *BindModule) VectorizeInput(ctx context.Context,
	input string, cfg moduletools.ClassConfig,
) ([]float32, error) {
	return m.textVectorizer.Texts(ctx, []string{input}, cfg)
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.Vectorizer[[]float32](New())
	_ = modulecapabilities.InputVectorizer[[]float32](New())
)
