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

package objects

import (
	"context"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// BatchManager manages kind changes in batch at a use-case level , i.e.
// agnostic of underlying databases or storage providers
type BatchManager struct {
	config            *config.WeaviateConfig
	locks             locks
	schemaManager     schemaManager
	logger            logrus.FieldLogger
	authorizer        authorizer
	vectorRepo        BatchVectorRepo
	modulesProvider   ModulesProvider
	autoSchemaManager *autoSchemaManager
	metrics           *Metrics
}

type BatchVectorRepo interface {
	VectorRepo
	batchRepoNew
}

type batchRepoNew interface {
	BatchPutObjects(ctx context.Context, objects BatchObjects,
		repl *additional.ReplicationProperties) (BatchObjects, error)
	BatchDeleteObjects(ctx context.Context, params BatchDeleteParams,
		repl *additional.ReplicationProperties, tenant string) (BatchDeleteResult, error)
	AddBatchReferences(ctx context.Context, references BatchReferences,
		repl *additional.ReplicationProperties) (BatchReferences, error)
}

// NewBatchManager creates a new manager
func NewBatchManager(vectorRepo BatchVectorRepo, modulesProvider ModulesProvider,
	locks locks, schemaManager schemaManager, config *config.WeaviateConfig,
	logger logrus.FieldLogger, authorizer authorizer,
	prom *monitoring.PrometheusMetrics,
) *BatchManager {
	return &BatchManager{
		config:            config,
		locks:             locks,
		schemaManager:     schemaManager,
		logger:            logger,
		vectorRepo:        vectorRepo,
		modulesProvider:   modulesProvider,
		authorizer:        authorizer,
		autoSchemaManager: newAutoSchemaManager(schemaManager, vectorRepo, config, logger),
		metrics:           NewMetrics(prom),
	}
}
