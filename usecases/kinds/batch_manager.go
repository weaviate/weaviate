/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/creativesoftwarefdn/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@creativesoftwarefdn.org
 */package kinds

import (
	"context"

	"github.com/creativesoftwarefdn/weaviate/usecases/config"
	"github.com/creativesoftwarefdn/weaviate/usecases/network"
)

// BatchManager manages kind changes in batch at a use-case level , i.e.
// agnostic of underlying databases or storage providers
type BatchManager struct {
	network       network.Network
	config        *config.WeaviateConfig
	repo          batchAndGetRepo
	locks         locks
	schemaManager schemaManager
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
func NewBatchManager(repo Repo, locks locks, schemaManager schemaManager, network network.Network, config *config.WeaviateConfig) *BatchManager {
	return &BatchManager{
		network:       network,
		config:        config,
		repo:          repo,
		locks:         locks,
		schemaManager: schemaManager,
	}
}
