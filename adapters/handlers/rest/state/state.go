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

package state

import (
	"context"
	"net/http"
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/handlers/graphql"
	"github.com/weaviate/weaviate/adapters/handlers/rest/tenantactivity"
	"github.com/weaviate/weaviate/adapters/repos/classifications"
	"github.com/weaviate/weaviate/adapters/repos/db"
	rCluster "github.com/weaviate/weaviate/cluster"
	"github.com/weaviate/weaviate/internal/auth/authentication/anonymous"
	"github.com/weaviate/weaviate/internal/auth/authentication/apikey"
	"github.com/weaviate/weaviate/internal/auth/authentication/oidc"
	"github.com/weaviate/weaviate/internal/auth/authorization"
	"github.com/weaviate/weaviate/internal/backup"
	"github.com/weaviate/weaviate/internal/cluster"
	"github.com/weaviate/weaviate/internal/config"
	"github.com/weaviate/weaviate/internal/locks"
	"github.com/weaviate/weaviate/internal/memwatch"
	"github.com/weaviate/weaviate/internal/modules"
	"github.com/weaviate/weaviate/internal/monitoring"
	"github.com/weaviate/weaviate/internal/objects"
	"github.com/weaviate/weaviate/internal/replica"
	"github.com/weaviate/weaviate/internal/scaler"
	"github.com/weaviate/weaviate/internal/schema"
	"github.com/weaviate/weaviate/internal/sharding"
	"github.com/weaviate/weaviate/internal/traverser"
)

// State is the only source of application-wide state
// NOTE: This is not true yet, see gh-723
// TODO: remove dependencies to anything that's not an ent or uc
type State struct {
	OIDC                  *oidc.Client
	AnonymousAccess       *anonymous.Client
	APIKey                *apikey.Client
	Authorizer            authorization.Authorizer
	ServerConfig          *config.WeaviateConfig
	Locks                 locks.ConnectorSchemaLock
	Logger                *logrus.Logger
	gqlMutex              sync.Mutex
	GraphQL               graphql.GraphQL
	Modules               *modules.Provider
	SchemaManager         *schema.Manager
	Scaler                *scaler.Scaler
	Cluster               *cluster.State
	RemoteIndexIncoming   *sharding.RemoteIndexIncoming
	RemoteNodeIncoming    *sharding.RemoteNodeIncoming
	RemoteReplicaIncoming *replica.RemoteReplicaIncoming
	Traverser             *traverser.Traverser

	ClassificationRepo *classifications.DistributedRepo
	Metrics            *monitoring.PrometheusMetrics
	BackupManager      *backup.Handler
	DB                 *db.DB
	BatchManager       *objects.BatchManager
	ClusterHttpClient  *http.Client
	ReindexCtxCancel   context.CancelFunc
	MemWatch           *memwatch.Monitor

	ClusterService *rCluster.Service
	TenantActivity *tenantactivity.Handler
}

// GetGraphQL is the safe way to retrieve GraphQL from the state as it can be
// replaced at runtime. Instead of passing appState.GraphQL to your adapters,
// pass appState itself which you can abstract with a local interface such as:
//
// type gqlProvider interface { GetGraphQL graphql.GraphQL }
func (s *State) GetGraphQL() graphql.GraphQL {
	s.gqlMutex.Lock()
	gql := s.GraphQL
	s.gqlMutex.Unlock()
	return gql
}

func (s *State) SetGraphQL(gql graphql.GraphQL) {
	s.gqlMutex.Lock()
	s.GraphQL = gql
	s.gqlMutex.Unlock()
}
