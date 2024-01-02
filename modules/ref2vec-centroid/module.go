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

package modcentroid

import (
	"context"
	"net/http"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/modules/ref2vec-centroid/vectorizer"
)

const (
	Name = "ref2vec-centroid"
)

func New() *CentroidModule {
	return &CentroidModule{}
}

type CentroidModule struct {
	logger logrus.FieldLogger
}

func (m *CentroidModule) Name() string {
	return Name
}

func (m *CentroidModule) Init(ctx context.Context, params moduletools.ModuleInitParams) error {
	m.logger = params.GetLogger()
	return nil
}

func (m *CentroidModule) RootHandler() http.Handler {
	// TODO: remove from overall module, this is a capability
	return nil
}

func (m *CentroidModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Ref2Vec
}

func (m *CentroidModule) MetaInfo() (map[string]interface{}, error) {
	return map[string]interface{}{}, nil
}

func (m *CentroidModule) VectorizeObject(ctx context.Context,
	obj *models.Object, cfg moduletools.ClassConfig,
	findRefVecsFn modulecapabilities.FindObjectFn,
) error {
	vzr := vectorizer.New(cfg, findRefVecsFn)
	return vzr.Object(ctx, obj)
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.ReferenceVectorizer(New())
	_ = modulecapabilities.MetaProvider(New())
)
