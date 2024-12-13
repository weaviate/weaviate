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
	entcfg "github.com/weaviate/weaviate/entities/config"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/multi2vec-clip/clients"
	"github.com/weaviate/weaviate/modules/multi2vec-clip/vectorizer"
)

const Name = "multi2vec-clip"

func New() *ClipModule {
	return &ClipModule{}
}

type ClipModule struct {
	imageVectorizer          imageVectorizer
	nearImageGraphqlProvider modulecapabilities.GraphQLArguments
	nearImageSearcher        modulecapabilities.Searcher[[]float32]
	textVectorizer           textVectorizer
	nearTextGraphqlProvider  modulecapabilities.GraphQLArguments
	nearTextSearcher         modulecapabilities.Searcher[[]float32]
	nearTextTransformer      modulecapabilities.TextTransform
	metaClient               metaClient
	logger                   logrus.FieldLogger
}

type metaClient interface {
	MetaInfo() (map[string]interface{}, error)
}

type imageVectorizer interface {
	Object(ctx context.Context, obj *models.Object, cfg moduletools.ClassConfig) ([]float32, models.AdditionalProperties, error)
	VectorizeImage(ctx context.Context, id, image string, cfg moduletools.ClassConfig) ([]float32, error)
}

type textVectorizer interface {
	Texts(ctx context.Context, input []string,
		cfg moduletools.ClassConfig) ([]float32, error)
}

func (m *ClipModule) Name() string {
	return Name
}

func (m *ClipModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Multi2Vec
}

func (m *ClipModule) Init(ctx context.Context,
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
	uri := os.Getenv("CLIP_INFERENCE_API")
	if uri == "" {
		return errors.Errorf("required variable CLIP_INFERENCE_API is not set")
	}

	waitForStartup := true
	if envWaitForStartup := os.Getenv("CLIP_WAIT_FOR_STARTUP"); envWaitForStartup != "" {
		waitForStartup = entcfg.Enabled(envWaitForStartup)
	}

	client := clients.New(uri, timeout, logger)
	if waitForStartup {
		if err := client.WaitForStartup(ctx, 1*time.Second); err != nil {
			return errors.Wrap(err, "init remote vectorizer")
		}
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
	obj *models.Object, cfg moduletools.ClassConfig,
) ([]float32, models.AdditionalProperties, error) {
	return m.imageVectorizer.Object(ctx, obj, cfg)
}

func (m *ClipModule) VectorizeBatch(ctx context.Context, objs []*models.Object, skipObject []bool, cfg moduletools.ClassConfig) ([][]float32, []models.AdditionalProperties, map[int]error) {
	return batch.VectorizeBatch(ctx, objs, skipObject, cfg, m.logger, m.imageVectorizer.Object)
}

func (m *ClipModule) MetaInfo() (map[string]interface{}, error) {
	return m.metaClient.MetaInfo()
}

func (m *ClipModule) VectorizeInput(ctx context.Context,
	input string, cfg moduletools.ClassConfig,
) ([]float32, error) {
	return m.textVectorizer.Texts(ctx, []string{input}, cfg)
}

func (m *ClipModule) VectorizableProperties(cfg moduletools.ClassConfig) (bool, []string, error) {
	ichek := vectorizer.NewClassSettings(cfg)
	mediaProps, err := ichek.Properties()
	return false, mediaProps, err
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.Vectorizer[[]float32](New())
	_ = modulecapabilities.InputVectorizer[[]float32](New())
)
