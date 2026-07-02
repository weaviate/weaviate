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

package telemetry

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/verbosity"
)

func ptrTo[T any](v T) *T { return &v }

// T-FIELDS-1: curated extraction - mixed RF, MT, named-vector, hnsw/flat/dynamic.
func TestCuratedFields_Extraction(t *testing.T) {
	sg := &fakeNodesStatusGetter{}
	sm := &fakeSchemaManager{}
	logger, _ := test.NewNullLogger()
	tel := New(sg, sm, logger, Config{NodeID: "node-abc", AsyncIndexingEnabled: true}, nil)

	// 3 nodes
	sm.On("Nodes").Return([]string{"n1", "n2", "n3"})
	sg.On("LocalNodeStatus", context.Background(), "", "", verbosity.OutputVerbose).Return(
		&models.NodeStatus{Stats: &models.NodeStats{ObjectCount: 0}})

	classes := []*models.Class{
		{
			// single-vector, hnsw, RF=3, not MT
			Class:              "A",
			VectorIndexType:    "hnsw",
			ReplicationConfig:  &models.ReplicationConfig{Factor: 3},
			MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false},
		},
		{
			// single-vector, flat, RF=1, MT enabled
			Class:              "B",
			VectorIndexType:    "flat",
			ReplicationConfig:  &models.ReplicationConfig{Factor: 1},
			MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
		},
		{
			// named-vector (dynamic + flat), no RF, MT enabled
			Class:              "C",
			MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
			VectorConfig: map[string]models.VectorConfig{
				"v1": {VectorIndexType: "dynamic"},
				"v2": {VectorIndexType: "flat"},
			},
		},
	}

	sm.On("GetSchemaSkipAuth").Return(schema.Schema{
		Objects: &models.Schema{Classes: classes},
	})

	payload, err := tel.buildPayload(context.Background(), PayloadType.Init)
	require.NoError(t, err)

	require.NotNil(t, payload.NodeCount)
	assert.Equal(t, 3, *payload.NodeCount)
	require.NotNil(t, payload.MaxReplicationFactor)
	assert.Equal(t, 3, *payload.MaxReplicationFactor)
	require.NotNil(t, payload.ReplicationEnabled)
	assert.True(t, *payload.ReplicationEnabled, "RF>1 → replicationEnabled=true")
	require.NotNil(t, payload.MTCollectionCount)
	assert.Equal(t, 2, *payload.MTCollectionCount)
	require.NotNil(t, payload.NamedVectorCollectionCount)
	assert.Equal(t, 1, *payload.NamedVectorCollectionCount, "only class C has VectorConfig")
	require.NotNil(t, payload.AsyncIndexingEnabled)
	assert.True(t, *payload.AsyncIndexingEnabled)
	require.NotNil(t, payload.VectorIndexTypeCounts)
	// class A → hnsw, class B → flat, class C → dynamic(1)+flat(1)
	assert.Equal(t, 1, payload.VectorIndexTypeCounts["hnsw"])
	assert.Equal(t, 2, payload.VectorIndexTypeCounts["flat"])
	assert.Equal(t, 1, payload.VectorIndexTypeCounts["dynamic"])
}

// T-FIELDS-2: usedModules fallback - nil ModuleConfig but Vectorizer set.
func TestUsedModules_NilModuleConfigFallback(t *testing.T) {
	sg := &fakeNodesStatusGetter{}
	sm := &fakeSchemaManager{}
	logger, _ := test.NewNullLogger()
	tel := New(sg, sm, logger, Config{}, nil)

	classes := []*models.Class{
		{
			// nil ModuleConfig with a non-"none" Vectorizer → fallback adds the module
			Class:        "WithVectorizer",
			ModuleConfig: nil,
			Vectorizer:   "text2vec-openai",
		},
		{
			// "none" vectorizer should NOT appear
			Class:        "BYOV",
			ModuleConfig: nil,
			Vectorizer:   "none",
		},
		{
			// empty Vectorizer should NOT appear
			Class:        "EmptyVectorizer",
			ModuleConfig: nil,
			Vectorizer:   "",
		},
		{
			// properly configured class (non-nil ModuleConfig) - unchanged behavior
			Class: "ProperlyConfigured",
			ModuleConfig: map[string]interface{}{
				"text2vec-cohere": map[string]interface{}{},
			},
			Vectorizer: "text2vec-cohere",
		},
	}

	sm.On("GetSchemaSkipAuth").Return(schema.Schema{
		Objects: &models.Schema{Classes: classes},
	})

	modules, err := tel.getUsedModules()
	require.NoError(t, err)
	assert.Contains(t, modules, "text2vec-openai", "fallback must add vectorizer when ModuleConfig is nil")
	assert.Contains(t, modules, "text2vec-cohere", "properly-configured class unchanged")
	assert.NotContains(t, modules, "none", "BYOV vectorizer 'none' must not appear")
	assert.NotContains(t, modules, "", "empty vectorizer must not appear")
}

// T-FIELDS-3: node count comes from schemaManager.Nodes().
func TestCuratedFields_NodeCount(t *testing.T) {
	sg := &fakeNodesStatusGetter{}
	sm := &fakeSchemaManager{}
	logger, _ := test.NewNullLogger()
	tel := New(sg, sm, logger, Config{}, nil)

	sm.On("Nodes").Return([]string{"a", "b"})
	sm.On("GetSchemaSkipAuth").Return(schema.Schema{})
	sg.On("LocalNodeStatus", context.Background(), "", "", verbosity.OutputVerbose).Return(
		&models.NodeStatus{Stats: &models.NodeStats{}})

	payload, err := tel.buildPayload(context.Background(), PayloadType.Init)
	require.NoError(t, err)
	require.NotNil(t, payload.NodeCount)
	assert.Equal(t, 2, *payload.NodeCount)
}

// T-FIELDS-5: pointer fields serialize a measured zero/false rather than being
// dropped by omitempty, and a nil field is omitted. This is the guard Marcin
// asked for: with the old value-type fields, nodeCount:0 / replicationEnabled:false
// were silently dropped and unknown was indistinguishable from known-zero.
func TestPayload_PointerSemantics_JSON(t *testing.T) {
	t.Run("measured zero/false serialize; unknown clusterCreatedAt omitted", func(t *testing.T) {
		p := Payload{
			NodeCount:                  ptrTo(0),
			MaxReplicationFactor:       ptrTo(0),
			ReplicationEnabled:         ptrTo(false),
			MTCollectionCount:          ptrTo(0),
			NamedVectorCollectionCount: ptrTo(0),
			AsyncIndexingEnabled:       ptrTo(false),
			// ClusterCreatedAt nil: cluster identity not committed.
		}
		b, err := json.Marshal(p)
		require.NoError(t, err)
		s := string(b)

		assert.Contains(t, s, `"nodeCount":0`)
		assert.Contains(t, s, `"maxReplicationFactor":0`)
		assert.Contains(t, s, `"replicationEnabled":false`)
		assert.Contains(t, s, `"mtCollectionCount":0`)
		assert.Contains(t, s, `"namedVectorCollectionCount":0`)
		assert.Contains(t, s, `"asyncIndexingEnabled":false`)
		assert.NotContains(t, s, "clusterCreatedAt", "nil clusterCreatedAt must be omitted")
	})

	t.Run("nil curated fields omitted; known clusterCreatedAt present", func(t *testing.T) {
		p := Payload{
			ClusterID:        "00000000-0000-7000-0000-000000000001",
			ClusterCreatedAt: ptrTo(int64(1717171717000)),
			// all curated pointers nil: not measured.
		}
		b, err := json.Marshal(p)
		require.NoError(t, err)
		s := string(b)

		assert.Contains(t, s, `"clusterCreatedAt":1717171717000`)
		assert.NotContains(t, s, "nodeCount")
		assert.NotContains(t, s, "replicationEnabled")
		assert.NotContains(t, s, "asyncIndexingEnabled")
		assert.NotContains(t, s, "mtCollectionCount")
	})
}

// T-FIELDS-4: VectorIndexType defaults to "hnsw" when empty string.
func TestCuratedFields_DefaultVectorIndexType(t *testing.T) {
	sg := &fakeNodesStatusGetter{}
	sm := &fakeSchemaManager{}
	logger, _ := test.NewNullLogger()
	tel := New(sg, sm, logger, Config{}, nil)

	sm.On("GetSchemaSkipAuth").Return(schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{Class: "X", VectorIndexType: ""},
			},
		},
	})
	sg.On("LocalNodeStatus", context.Background(), "", "", verbosity.OutputVerbose).Return(
		&models.NodeStatus{Stats: &models.NodeStats{}})

	payload, err := tel.buildPayload(context.Background(), PayloadType.Init)
	require.NoError(t, err)
	require.NotNil(t, payload.VectorIndexTypeCounts)
	assert.Equal(t, 1, payload.VectorIndexTypeCounts["hnsw"], "empty VectorIndexType defaults to hnsw")
	assert.Equal(t, 0, payload.VectorIndexTypeCounts[""])
}
