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

package modvoyageai

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
	"github.com/weaviate/weaviate/modules/multi2vec-voyageai/clients"
	"github.com/weaviate/weaviate/modules/multi2vec-voyageai/vectorizer"
)

const Name = "multi2vec-voyageai"

func New() *VoyageAIModule {
	return &VoyageAIModule{}
}

type VoyageAIModule struct {
	vectorizer   vectorizer.Vectorizer
	metaProvider metaProvider
}
type Vectorizer interface {
	Object(ctx context.Context, obj *models.Object, comp moduletools.VectorizablePropsComparator,
		cfg moduletools.ClassConfig) ([]float32, models.AdditionalProperties, error)
}

type metaProvider interface {
	MetaInfo() (map[string]interface{}, error)
}

func (m *VoyageAIModule) Name() string {
	return Name
}

func (m *VoyageAIModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Multi2Vec
}

func (m *VoyageAIModule) Init(ctx context.Context,
	params moduletools.ModuleInitParams,
) error {
	if err := m.initVectorizer(ctx, params.GetConfig().ModuleHttpClientTimeout, params.GetLogger()); err != nil {
		return errors.Wrap(err, "init vectorizer")
	}
	return nil
}

func (m *VoyageAIModule) initVectorizer(ctx context.Context, timeout time.Duration,
	logger logrus.FieldLogger,
) error {
	apiKey := os.Getenv("VOYAGEAI_APIKEY")
	client := clients.New(apiKey, timeout, logger)

	m.vectorizer = *vectorizer.New(client)
	m.metaProvider = client

	return nil
}

func (m *VoyageAIModule) RootHandler() http.Handler {
	// TODO: remove once this is a capability interface
	return nil
}

func (m *VoyageAIModule) VectorizeObject(ctx context.Context,
	obj *models.Object, comp moduletools.VectorizablePropsComparator, cfg moduletools.ClassConfig,
) ([]float32, models.AdditionalProperties, error) {
	return m.vectorizer.Object(ctx, obj, comp, cfg)
}

func (m *VoyageAIModule) MetaInfo() (map[string]interface{}, error) {
	return m.metaProvider.MetaInfo()
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.Vectorizer(New())
	_ = modulecapabilities.MetaProvider(New())
)
