//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package objects

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/objects/alias"
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

// Alias support
func (m *BatchManager) resolveAlias(class string) (className, aliasName string) {
	return alias.ResolveAlias(m.schemaManager, class)
}

func (m *BatchManager) batchDeleteWithAlias(batchDeleteResponse *BatchDeleteResponse, aliasName string) *BatchDeleteResponse {
	if batchDeleteResponse != nil {
		if batchDeleteResponse.Match != nil {
			batchDeleteResponse.Match.Class = aliasName
		}
		batchDeleteResponse.Params.ClassName = schema.ClassName(aliasName)
	}
	return batchDeleteResponse
}

func (m *BatchManager) batchInsertWithAliases(batchObjects BatchObjects, classAlias map[string]string) BatchObjects {
	if len(classAlias) > 0 {
		for i := range batchObjects {
			batchObjects[i].Object = alias.ClassNameToAlias(batchObjects[i].Object, classAlias[batchObjects[i].Object.Class])
		}
	}
	return batchObjects
}
