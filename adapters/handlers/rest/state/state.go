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

package state

import (
	"context"

	"github.com/semi-technologies/weaviate/adapters/connectors"
	"github.com/semi-technologies/weaviate/adapters/handlers/graphql"
	"github.com/semi-technologies/weaviate/usecases/auth/authentication/anonymous"
	"github.com/semi-technologies/weaviate/usecases/auth/authentication/oidc"
	"github.com/semi-technologies/weaviate/usecases/auth/authorization"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/locks"
	"github.com/semi-technologies/weaviate/usecases/network"
	"github.com/semi-technologies/weaviate/usecases/telemetry"
	"github.com/semi-technologies/weaviate/usecases/traverser"
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
	Contextionary    contextionary
	TelemetryLogger  *telemetry.RequestsLog
	StopwordDetector stopwordDetector
}

// GetGraphQL is the safe way to retrieve GraphQL from the state as it can be
// replaced at runtime. Instead of passing appState.GraphQL to your adapters,
// pass appState itself which you can abstract with a local interface such as:
//
// type gqlProvider interface { GetGraphQL graphql.GraphQL }
func (s *State) GetGraphQL() graphql.GraphQL {
	return s.GraphQL
}

type stopwordDetector interface {
	IsStopWord(ctx context.Context, word string) (bool, error)
}

type contextionary interface {
	IsWordPresent(ctx context.Context, word string) (bool, error)
	SchemaSearch(ctx context.Context, params traverser.SearchParams) (traverser.SearchResults, error)
	SafeGetSimilarWordsWithCertainty(ctx context.Context, word string, certainty float32) ([]string, error)
	VectorForWord(ctx context.Context, word string) ([]float32, error)
	NearestWordsByVector(ctx context.Context, vector []float32, n int, k int) ([]string, []float32, error)
	VectorForCorpi(ctx context.Context, corpi []string) ([]float32, error)
}
