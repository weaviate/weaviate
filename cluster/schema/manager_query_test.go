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

package schema

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
)

func TestQueryCollectionsCount(t *testing.T) {
	newManager := func(t *testing.T) *SchemaManager {
		sm := &SchemaManager{
			schema: NewSchema(t.Name(), nil, prometheus.NewPedanticRegistry()),
		}
		ss := &sharding.State{Physical: make(map[string]sharding.Physical)}
		require.NoError(t, sm.schema.addClass(&models.Class{Class: "customer1:Movies"}, ss, 1))
		require.NoError(t, sm.schema.addClass(&models.Class{Class: "customer1:Films"}, ss, 2))
		require.NoError(t, sm.schema.addClass(&models.Class{Class: "customer2:Movies"}, ss, 3))
		return sm
	}

	t.Run("empty subcommand returns global count", func(t *testing.T) {
		sm := newManager(t)
		payload, err := sm.QueryCollectionsCount(&cmd.QueryRequest{})
		require.NoError(t, err)

		var resp cmd.QueryCollectionsCountResponse
		require.NoError(t, json.Unmarshal(payload, &resp))
		assert.Equal(t, 3, resp.Count)
	})

	t.Run("namespace selector filters", func(t *testing.T) {
		sm := newManager(t)
		sub, err := json.Marshal(cmd.QueryCollectionsCountRequest{Namespace: "customer1"})
		require.NoError(t, err)

		payload, err := sm.QueryCollectionsCount(&cmd.QueryRequest{SubCommand: sub})
		require.NoError(t, err)

		var resp cmd.QueryCollectionsCountResponse
		require.NoError(t, json.Unmarshal(payload, &resp))
		assert.Equal(t, 2, resp.Count)
	})

	t.Run("invalid subcommand JSON is a bad request", func(t *testing.T) {
		sm := newManager(t)
		_, err := sm.QueryCollectionsCount(&cmd.QueryRequest{SubCommand: []byte("not-json")})
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrBadRequest))
	})
}
