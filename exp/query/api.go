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

package query

import (
	"context"

	"github.com/sirupsen/logrus"
	v1 "github.com/weaviate/weaviate/adapters/handlers/grpc/v1"
	"github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/usecases/auth/authentication/composer"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/traverser"
)

// API is the core query API that is transport agnostic (http, grpc, etc).
type API struct {
	log    logrus.FieldLogger
	config *config.Config

	// svc provides the underlying search API via v1.WeaviateServer
	// TODO(kavi): Split `v1.WeaviateServer` into composable `v1.Searcher` and everything else.
	svc    protocol.WeaviateServer
	schema *schema.Manager
	batch  *objects.BatchManager
}

func NewAPI(
	traverser *traverser.Traverser,
	authComposer composer.TokenFunc,
	allowAnonymousAccess bool,
	schemaManager *schema.Manager,
	batchManager *objects.BatchManager,
	config *config.Config,
	log logrus.FieldLogger,
) *API {
	return &API{
		log:    log,
		config: config,
		svc:    v1.NewService(traverser, authComposer, true, schemaManager, batchManager, config, log),
		schema: schemaManager,
		batch:  batchManager,
	}
}

// Search serves vector search over the offloaded tenant on object storage.
func (a *API) Search(ctx context.Context, req SearchRequest) {
}
