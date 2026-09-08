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
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/fakes"
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

	tests := []struct {
		name      string
		subCmd    []byte
		wantCount int
	}{
		{
			name:      "empty subcommand returns global count",
			subCmd:    nil,
			wantCount: 3,
		},
		{
			name:      "explicit empty namespace returns global count",
			subCmd:    mustMarshal(t, cmd.QueryCollectionsCountRequest{Namespace: ""}),
			wantCount: 3,
		},
		{
			name:      "namespace selector filters",
			subCmd:    mustMarshal(t, cmd.QueryCollectionsCountRequest{Namespace: "customer1"}),
			wantCount: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sm := newManager(t)
			payload, err := sm.QueryCollectionsCount(&cmd.QueryRequest{SubCommand: tt.subCmd})
			require.NoError(t, err)

			var resp cmd.QueryCollectionsCountResponse
			require.NoError(t, json.Unmarshal(payload, &resp))
			assert.Equal(t, tt.wantCount, resp.Count)
		})
	}

	t.Run("invalid subcommand JSON is a bad request", func(t *testing.T) {
		sm := newManager(t)
		_, err := sm.QueryCollectionsCount(&cmd.QueryRequest{SubCommand: []byte("not-json")})
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrBadRequest))
	})
}

// TestCollectionsCount_ThroughSchemaManager drives the count through the
// apply path with schemaOnly=true, the mode every node uses replaying the
// RAFT log at startup. apply skips updateStore in that mode, so a change
// moving the counter maintenance out of updateSchema would rebuild a wrong
// count on every restarting node while the direct-call tests stayed green.
func TestCollectionsCount_ThroughSchemaManager(t *testing.T) {
	addClass := func(t *testing.T, sm *SchemaManager, name string, version uint64) error {
		t.Helper()
		return sm.AddClass(&cmd.ApplyRequest{
			Type:       cmd.ApplyRequest_TYPE_ADD_CLASS,
			Class:      name,
			Version:    version,
			SubCommand: mustMarshal(t, cmd.AddClassRequest{Class: &models.Class{Class: name}, State: emptyShardingState()}),
		}, "test-node", true, false)
	}
	deleteClass := func(sm *SchemaManager, name string) error {
		return sm.DeleteClass(&cmd.ApplyRequest{
			Type:  cmd.ApplyRequest_TYPE_DELETE_CLASS,
			Class: name,
		}, true, false)
	}

	tests := []struct {
		name   string
		replay func(t *testing.T, sm *SchemaManager)
		want   map[string]int
	}{
		{
			name:   "no commands",
			replay: func(t *testing.T, sm *SchemaManager) {},
			want:   map[string]int{"": 0, "customer1": 0},
		},
		{
			name: "adds across namespaces",
			replay: func(t *testing.T, sm *SchemaManager) {
				require.NoError(t, addClass(t, sm, "customer1:Movies", 1))
				require.NoError(t, addClass(t, sm, "customer1:Films", 2))
				require.NoError(t, addClass(t, sm, "customer2:Movies", 3))
				require.NoError(t, addClass(t, sm, "Unqualified", 4))
			},
			want: map[string]int{"": 4, "customer1": 2, "customer2": 1, "customer3": 0},
		},
		{
			name: "a delete frees the namespace's budget",
			replay: func(t *testing.T, sm *SchemaManager) {
				require.NoError(t, addClass(t, sm, "customer1:Movies", 1))
				require.NoError(t, addClass(t, sm, "customer1:Films", 2))
				require.NoError(t, deleteClass(sm, "customer1:Films"))
			},
			want: map[string]int{"": 1, "customer1": 1},
		},
		{
			name: "deleting the last class leaves the namespace empty",
			replay: func(t *testing.T, sm *SchemaManager) {
				require.NoError(t, addClass(t, sm, "customer1:Movies", 1))
				require.NoError(t, deleteClass(sm, "customer1:Movies"))
			},
			want: map[string]int{"": 0, "customer1": 0},
		},
		{
			name: "a re-add after a delete counts once",
			replay: func(t *testing.T, sm *SchemaManager) {
				require.NoError(t, addClass(t, sm, "customer1:Movies", 1))
				require.NoError(t, deleteClass(sm, "customer1:Movies"))
				require.NoError(t, addClass(t, sm, "customer1:Movies", 2))
			},
			want: map[string]int{"": 1, "customer1": 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := fakes.NewMockParser()
			parser.On("ParseClass", mock.Anything).Return(nil)
			sm := NewSchemaManager("test-node", nil, parser, prometheus.NewPedanticRegistry(), logrus.New())

			tt.replay(t, sm)

			for namespace, want := range tt.want {
				payload, err := sm.QueryCollectionsCount(&cmd.QueryRequest{
					SubCommand: mustMarshal(t, cmd.QueryCollectionsCountRequest{Namespace: namespace}),
				})
				require.NoError(t, err)

				var resp cmd.QueryCollectionsCountResponse
				require.NoError(t, json.Unmarshal(payload, &resp))
				assert.Equal(t, want, resp.Count, "namespace %q", namespace)
			}
		})
	}
}

func emptyShardingState() *sharding.State {
	return &sharding.State{Physical: make(map[string]sharding.Physical)}
}

func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}
