package query

import (
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
	svc protocol.WeaviateServer
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
		svc:    v1.NewService(traverser, authComposer, allowAnonymousAccess, schemaManager, batchManager, config, log),
	}
}
