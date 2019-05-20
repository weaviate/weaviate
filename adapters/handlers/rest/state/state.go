/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */

package state

import (
	"github.com/semi-technologies/weaviate/adapters/connectors"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql"
	"github.com/semi-technologies/weaviate/contextionary"
	schema_contextionary "github.com/semi-technologies/weaviate/contextionary/schema"
	"github.com/semi-technologies/weaviate/usecases/auth/authentication/anonymous"
	"github.com/semi-technologies/weaviate/usecases/auth/authentication/oidc"
	"github.com/semi-technologies/weaviate/usecases/auth/authorization"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/locks"
	"github.com/semi-technologies/weaviate/usecases/network"
	"github.com/semi-technologies/weaviate/usecases/telemetry"
	"github.com/sirupsen/logrus"
)

// State is the only source of appliaction-wide state
// NOTE: This is not true yet, se gh-723
// TODO: remove dependencies to anything that's not an ent or uc
type State struct {
	Network          network.Network
	OIDC             *oidc.Client
	AnonymousAccess  *anonymous.Client
	Authorizer       authorization.Authorizer
	ServerConfig     *config.WeaviateConfig
	Connector        connectors.DatabaseConnector
	Locks            locks.ConnectorSchemaLock
	Logger           *logrus.Logger
	GraphQL          graphql.GraphQL
	Contextionary    contextionary.Contextionary
	RawContextionary contextionary.Contextionary
	TelemetryLogger  *telemetry.RequestsLog
}

// GetGraphQL is the safe way to retrieve GraphQL from the state as it can be
// replaced at runtime. Instead of passing appState.GraphQL to your adapters,
// pass appState itself which you can abstract with a local interface such as:
//
// type gqlProvider interface { GetGraphQL graphql.GraphQL }
func (s *State) GetGraphQL() graphql.GraphQL {
	return s.GraphQL
}

// GetContextionary is the safe way to retrieve Contextionary from the state as it can be
// replaced at runtime. Instead of passing appState.Contextionary to your adapters,
// pass appState itself which you can abstract with a local interface such as:
//
// type gqlProvider interface { GetContextionary contextionary.Contextionary }
func (s *State) GetContextionary() contextionary.Contextionary {
	return s.Contextionary
}

// GetSchemaContextionary is the safe way to retrieve SchemaContextionary from
// the state as it can be replaced at runtime. Instead of passing
// appState.SchemaContextionary to your adapters, pass appState itself which
// you can abstract with a local interface such as:
//
// type gqlProvider interface { GetSchemaContextionary
// schema_contextionary.Contextionary }
func (s *State) GetSchemaContextionary() *schema_contextionary.Contextionary {
	return schema_contextionary.New(s.Contextionary)
}
