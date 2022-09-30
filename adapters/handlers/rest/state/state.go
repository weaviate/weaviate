//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package state

import (
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql"
	"github.com/semi-technologies/weaviate/adapters/repos/classifications"
	"github.com/semi-technologies/weaviate/usecases/auth/authentication/anonymous"
	"github.com/semi-technologies/weaviate/usecases/auth/authentication/oidc"
	"github.com/semi-technologies/weaviate/usecases/auth/authorization"
	"github.com/semi-technologies/weaviate/usecases/backup"
	"github.com/semi-technologies/weaviate/usecases/cluster"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/locks"
	"github.com/semi-technologies/weaviate/usecases/modules"
	"github.com/semi-technologies/weaviate/usecases/monitoring"
	"github.com/semi-technologies/weaviate/usecases/schema"
	"github.com/semi-technologies/weaviate/usecases/sharding"
	"github.com/sirupsen/logrus"
)

// State is the only source of application-wide state
// NOTE: This is not true yet, see gh-723
// TODO: remove dependencies to anything that's not an ent or uc
type State struct {
	OIDC               *oidc.Client
	AnonymousAccess    *anonymous.Client
	Authorizer         authorization.Authorizer
	ServerConfig       *config.WeaviateConfig
	Locks              locks.ConnectorSchemaLock
	Logger             *logrus.Logger
	GraphQL            graphql.GraphQL
	Modules            *modules.Provider
	SchemaManager      *schema.Manager
	Cluster            *cluster.State
	RemoteIncoming     *sharding.RemoteIndexIncoming
	ClassificationRepo *classifications.DistributedRepo
	Metrics            *monitoring.PrometheusMetrics
	BackupManager      *backup.Manager
}

// GetGraphQL is the safe way to retrieve GraphQL from the state as it can be
// replaced at runtime. Instead of passing appState.GraphQL to your adapters,
// pass appState itself which you can abstract with a local interface such as:
//
// type gqlProvider interface { GetGraphQL graphql.GraphQL }
func (s *State) GetGraphQL() graphql.GraphQL {
	return s.GraphQL
}
