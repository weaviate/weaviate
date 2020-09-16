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

package state

import (
	"context"

	"github.com/semi-technologies/weaviate/adapters/handlers/graphql"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/usecases/auth/authentication/anonymous"
	"github.com/semi-technologies/weaviate/usecases/auth/authentication/oidc"
	"github.com/semi-technologies/weaviate/usecases/auth/authorization"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/semi-technologies/weaviate/usecases/locks"
	"github.com/semi-technologies/weaviate/usecases/network"
	"github.com/semi-technologies/weaviate/usecases/traverser"
	"github.com/semi-technologies/weaviate/usecases/vectorizer"
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
	Locks            locks.ConnectorSchemaLock
	Logger           *logrus.Logger
	GraphQL          graphql.GraphQL
	Contextionary    contextionary
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
	MultiVectorForWord(ctx context.Context, words []string) ([][]float32, error)
	NearestWordsByVector(ctx context.Context, vector []float32, n int, k int) ([]string, []float32, error)
	MultiNearestWordsByVector(ctx context.Context, vectors [][]float32, n int, k int) ([]*models.NearestNeighbors, error)
	VectorForCorpi(ctx context.Context, corpi []string, overrides map[string]string) ([]float32, []vectorizer.InputElement, error)
	Version(ctx context.Context) (string, error)
	WordCount(ctx context.Context) (int64, error)
	AddExtension(ctx context.Context, extension *models.C11yExtension) error
}
