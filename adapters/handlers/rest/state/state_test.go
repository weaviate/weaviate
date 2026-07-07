//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package state

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tailor-platform/graphql"

	libgraphql "github.com/weaviate/weaviate/adapters/handlers/graphql"
)

type fakeGraphQL struct{ id int }

func (f *fakeGraphQL) Resolve(context.Context, string, string, map[string]interface{}) *graphql.Result {
	return nil
}

// TestSetGraphQLIfCurrent pins the generation guard that lets the DisableGraphQL
// enable hook rebuild off the RAFT apply goroutine without a stale build (from an
// older schema snapshot) clobbering a newer apply-built graph.
func TestSetGraphQLIfCurrent(t *testing.T) {
	t.Run("stale generation is dropped, current generation wins", func(t *testing.T) {
		s := &State{}
		applyGraph := &fakeGraphQL{id: 1}
		hookGraph := &fakeGraphQL{id: 2}

		// Hook captures the generation, then (concurrently) an apply lands a newer
		// graph. The hook's late store must be dropped.
		gen := s.GraphQLGeneration()
		s.SetGraphQL(applyGraph) // authoritative apply-path store
		assert.False(t, s.SetGraphQLIfCurrent(hookGraph, gen), "stale store should be skipped")
		assert.Equal(t, libgraphql.GraphQL(applyGraph), s.GetGraphQL())
	})

	t.Run("store succeeds when no apply intervened", func(t *testing.T) {
		s := &State{}
		hookGraph := &fakeGraphQL{id: 2}

		gen := s.GraphQLGeneration()
		assert.True(t, s.SetGraphQLIfCurrent(hookGraph, gen), "current store should apply")
		assert.Equal(t, libgraphql.GraphQL(hookGraph), s.GetGraphQL())
	})

	t.Run("every authoritative store advances the generation", func(t *testing.T) {
		s := &State{}
		g0 := s.GraphQLGeneration()
		s.SetGraphQL(&fakeGraphQL{id: 1})
		g1 := s.GraphQLGeneration()
		s.SetGraphQL(nil) // disable-hook drop is also authoritative
		g2 := s.GraphQLGeneration()
		assert.Greater(t, g1, g0)
		assert.Greater(t, g2, g1)
	})
}
