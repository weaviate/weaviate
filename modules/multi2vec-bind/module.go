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
	nearImageSearcher          modulecapabilities.Searcher
	nearAudioGraphqlProvider   modulecapabilities.GraphQLArguments
	nearAudioSearcher          modulecapabilities.Searcher
	nearVideoGraphqlProvider   modulecapabilities.GraphQLArguments
	nearVideoSearcher          modulecapabilities.Searcher
	nearIMUGraphqlProvider     modulecapabilities.GraphQLArguments
	nearIMUSearcher            modulecapabilities.Searcher
	nearThermalGraphqlProvider modulecapabilities.GraphQLArguments
	nearThermalSearcher        modulecapabilities.Searcher
	nearDepthGraphqlProvider   modulecapabilities.GraphQLArguments
	nearDepthSearcher          modulecapabilities.Searcher
	textVectorizer             textVectorizer
	nearTextGraphqlProvider    modulecapabilities.GraphQLArguments
	nearTextSearcher           modulecapabilities.Searcher
	nearTextTransformer        modulecapabilities.TextTransform
	metaClient                 metaClient
}

type metaClient interface {
	MetaInfo() (map[string]interface{}, error)
}

type bindVectorizer interface {
	Object(ctx context.Context, object *models.Object, objDiff *moduletools.ObjectDiff,
		settings vectorizer.ClassSettings) error
	VectorizeImage(ctx context.Context, image string) ([]float32, error)
	VectorizeAudio(ctx context.Context, audio string) ([]float32, error)
	VectorizeVideo(ctx context.Context, video string) ([]float32, error)
	VectorizeIMU(ctx context.Context, imu string) ([]float32, error)
	VectorizeThermal(ctx context.Context, thermal string) ([]float32, error)
	VectorizeDepth(ctx context.Context, depth string) ([]float32, error)
}

type textVectorizer interface {
	Texts(ctx context.Context, input []string,
		settings vectorizer.ClassSettings) ([]float32, error)
	MoveTo(source, target []float32, weight float32) ([]float32, error)
	MoveAwayFrom(source, target []float32, weight float32) ([]float32, error)
	CombineVectors(vectors [][]float32) []float32
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
	obj *models.Object, objDiff *moduletools.ObjectDiff, cfg moduletools.ClassConfig,
) error {
	icheck := vectorizer.NewClassSettings(cfg)
	return m.bindVectorizer.Object(ctx, obj, objDiff, icheck)
}

func (m *BindModule) MetaInfo() (map[string]interface{}, error) {
	return m.metaClient.MetaInfo()
}

func (m *BindModule) VectorizeInput(ctx context.Context,
	input string, cfg moduletools.ClassConfig,
) ([]float32, error) {
	return m.textVectorizer.Texts(ctx, []string{input}, vectorizer.NewClassSettings(cfg))
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.Vectorizer(New())
	_ = modulecapabilities.InputVectorizer(New())
)
