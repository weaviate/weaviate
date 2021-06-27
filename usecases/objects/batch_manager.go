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

package objects

import (
	"context"

	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/sirupsen/logrus"
)

// BatchManager manages kind changes in batch at a use-case level , i.e.
// agnostic of underlying databases or storage providers
type BatchManager struct {
	config             *config.WeaviateConfig
	locks              locks
	schemaManager      schemaManager
	logger             logrus.FieldLogger
	authorizer         authorizer
	vectorRepo         BatchVectorRepo
	vectorizerProvider VectorizerProvider
	autoSchemaManager  *autoSchemaManager
}

type BatchVectorRepo interface {
	VectorRepo
	batchRepoNew
}

type batchRepoNew interface {
	BatchPutObjects(ctx context.Context, objects BatchObjects) (BatchObjects, error)
	AddBatchReferences(ctx context.Context, references BatchReferences) (BatchReferences, error)
}

// NewBatchManager creates a new manager
func NewBatchManager(vectorRepo BatchVectorRepo, vectorizer VectorizerProvider,
	locks locks, schemaManager schemaManager, config *config.WeaviateConfig,
	logger logrus.FieldLogger, authorizer authorizer) *BatchManager {
	return &BatchManager{
		config:             config,
		locks:              locks,
		schemaManager:      schemaManager,
		logger:             logger,
		vectorRepo:         vectorRepo,
		vectorizerProvider: vectorizer,
		authorizer:         authorizer,
		autoSchemaManager:  newAutoSchemaManager(schemaManager, vectorRepo, config),
	}
}
