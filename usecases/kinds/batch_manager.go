//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
// 
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package kinds

import (
	"context"

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
}

// Repo describes the requirements the kinds UC has to the connected database
type batchRepo interface {
	AddThingsBatch(ctx context.Context, things BatchThings) error
	AddActionsBatch(ctx context.Context, actions BatchActions) error
	AddBatchReferences(ctx context.Context, references BatchReferences) error
}

type batchAndGetRepo interface {
	batchRepo
	getRepo
}

// NewBatchManager creates a new manager
func NewBatchManager(repo Repo, locks locks, schemaManager schemaManager, network network,
	config *config.WeaviateConfig, logger logrus.FieldLogger, authorizer authorizer) *BatchManager {
	return &BatchManager{
		network:       network,
		config:        config,
		repo:          repo,
		locks:         locks,
		schemaManager: schemaManager,
		logger:        logger,
		authorizer:    authorizer,
	}
}
