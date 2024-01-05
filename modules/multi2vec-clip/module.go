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

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/multi2vec-clip/clients"
	"github.com/weaviate/weaviate/modules/multi2vec-clip/vectorizer"
)

func New() *ClipModule {
	return &ClipModule{}
}

type ClipModule struct {
	imageVectorizer          imageVectorizer
	nearImageGraphqlProvider modulecapabilities.GraphQLArguments
	nearImageSearcher        modulecapabilities.Searcher
	textVectorizer           textVectorizer
	nearTextGraphqlProvider  modulecapabilities.GraphQLArguments
	nearTextSearcher         modulecapabilities.Searcher
	nearTextTransformer      modulecapabilities.TextTransform
	metaClient               metaClient
}

type metaClient interface {
	MetaInfo() (map[string]interface{}, error)
}

type imageVectorizer interface {
	Object(ctx context.Context, object *models.Object, objDiff *moduletools.ObjectDiff,
		settings vectorizer.ClassSettings) error
	VectorizeImage(ctx context.Context, image string) ([]float32, error)
}

type textVectorizer interface {
	Texts(ctx context.Context, input []string,
		settings vectorizer.ClassSettings) ([]float32, error)
	MoveTo(source, target []float32, weight float32) ([]float32, error)
	MoveAwayFrom(source, target []float32, weight float32) ([]float32, error)
	CombineVectors(vectors [][]float32) []float32
}

func (m *ClipModule) Name() string {
	return "multi2vec-clip"
}

func (m *ClipModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Multi2Vec
}

func (m *ClipModule) Init(ctx context.Context,
	params moduletools.ModuleInitParams,
) error {
	if err := m.initVectorizer(ctx, params.GetConfig().ModuleHttpClientTimeout, params.GetLogger()); err != nil {
		return errors.Wrap(err, "init vectorizer")
	}

	if err := m.initNearImage(); err != nil {
		return errors.Wrap(err, "init near text")
	}

	return nil
}

func (m *ClipModule) InitExtension(modules []modulecapabilities.Module) error {
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

func (m *ClipModule) initVectorizer(ctx context.Context, timeout time.Duration,
	logger logrus.FieldLogger,
) error {
	// TODO: proper config management
	uri := os.Getenv("CLIP_INFERENCE_API")
	if uri == "" {
		return errors.Errorf("required variable CLIP_INFERENCE_API is not set")
	}

	client := clients.New(uri, timeout, logger)
	if err := client.WaitForStartup(ctx, 1*time.Second); err != nil {
		return errors.Wrap(err, "init remote vectorizer")
	}

	m.imageVectorizer = vectorizer.New(client)
	m.textVectorizer = vectorizer.New(client)
	m.metaClient = client

	return nil
}

func (m *ClipModule) RootHandler() http.Handler {
	// TODO: remove once this is a capability interface
	return nil
}

func (m *ClipModule) VectorizeObject(ctx context.Context,
	obj *models.Object, objDiff *moduletools.ObjectDiff, cfg moduletools.ClassConfig,
) error {
	icheck := vectorizer.NewClassSettings(cfg)
	return m.imageVectorizer.Object(ctx, obj, objDiff, icheck)
}

func (m *ClipModule) MetaInfo() (map[string]interface{}, error) {
	return m.metaClient.MetaInfo()
}

func (m *ClipModule) VectorizeInput(ctx context.Context,
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
