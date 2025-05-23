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
	"time"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// BatchManager manages kind changes in batch at a use-case level , i.e.
// agnostic of underlying databases or storage providers
type BatchManager struct {
	config            *config.WeaviateConfig
	schemaManager     schemaManager
	logger            logrus.FieldLogger
	authorizer        authorization.Authorizer
	vectorRepo        BatchVectorRepo
	timeSource        timeSource
	modulesProvider   ModulesProvider
	autoSchemaManager *AutoSchemaManager
	metrics           *Metrics
}

type BatchVectorRepo interface {
	VectorRepo
	batchRepoNew
}

type batchRepoNew interface {
	BatchPutObjects(ctx context.Context, objects BatchObjects,
		repl *additional.ReplicationProperties, schemaVersion uint64) (BatchObjects, error)
	BatchDeleteObjects(ctx context.Context, params BatchDeleteParams, deletionTime time.Time,
		repl *additional.ReplicationProperties, tenant string, schemaVersion uint64) (BatchDeleteResult, error)
	AddBatchReferences(ctx context.Context, references BatchReferences,
		repl *additional.ReplicationProperties, schemaVersion uint64) (BatchReferences, error)
}

// NewBatchManager creates a new manager
func NewBatchManager(vectorRepo BatchVectorRepo, modulesProvider ModulesProvider,
	schemaManager schemaManager, config *config.WeaviateConfig,
	logger logrus.FieldLogger, authorizer authorization.Authorizer,
	prom *monitoring.PrometheusMetrics, autoSchemaManager *AutoSchemaManager,
) *BatchManager {
	return &BatchManager{
		config:            config,
		schemaManager:     schemaManager,
		logger:            logger,
		vectorRepo:        vectorRepo,
		timeSource:        defaultTimeSource{},
		modulesProvider:   modulesProvider,
		authorizer:        authorizer,
		autoSchemaManager: autoSchemaManager,
		metrics:           NewMetrics(prom),
	}
}
