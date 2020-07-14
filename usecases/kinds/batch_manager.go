//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package kinds

import (
	"context"

	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/sirupsen/logrus"
)

// BatchManager manages kind changes in batch at a use-case level , i.e.
// agnostic of underlying databases or storage providers
type BatchManager struct {
	network       network
	config        *config.WeaviateConfig
	repo          batchAndGetRepo
	locks         locks
	schemaManager schemaManager
	logger        logrus.FieldLogger
	authorizer    authorizer
	vectorRepo    BatchVectorRepo
	vectorizer    Vectorizer
}

type BatchVectorRepo interface {
	VectorRepo
	batchRepoNew
}

type batchRepoNew interface {
	BatchPutThings(ctx context.Context, things BatchThings) (BatchThings, error)
	BatchPutActions(ctx context.Context, actions BatchActions) (BatchActions, error)
	AddBatchReferences(ctx context.Context, references BatchReferences) (BatchReferences, error)
}

type batchAndGetRepo interface {
	ClassExists(ctx context.Context, id strfmt.UUID) (bool, error)
}

// NewBatchManager creates a new manager
func NewBatchManager(vectorRepo BatchVectorRepo, vectorizer Vectorizer, locks locks, schemaManager schemaManager, network network,
	config *config.WeaviateConfig, logger logrus.FieldLogger, authorizer authorizer) *BatchManager {
	return &BatchManager{
		network:       network,
		config:        config,
		locks:         locks,
		schemaManager: schemaManager,
		logger:        logger,
		vectorRepo:    vectorRepo,
		vectorizer:    vectorizer,
		authorizer:    authorizer,
	}
}
