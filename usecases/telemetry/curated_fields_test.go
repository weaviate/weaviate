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
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

func ptrTo[T any](v T) *T { return &v }

func TestCuratedFields_Extraction(t *testing.T) {
	sg := &fakeNodesStatusGetter{}
	sm := schemaUC.NewMockSchemaGetter(t)
	logger, _ := test.NewNullLogger()
	tel := New(sg, sm, logger, "", 0, false, Config{NodeID: "node-abc", AsyncIndexingEnabled: true, ClusterID: func() string { return "00000000-0000-7000-0000-0000000000aa" }})

	sm.EXPECT().Nodes().Return([]string{"n1", "n2", "n3"}).Maybe()
	sg.On("LocalNodeStatus", context.Background(), "", "", verbosity.OutputVerbose).Return(
		&models.NodeStatus{Stats: &models.NodeStats{ObjectCount: 0}},
	)

	classes := []*models.Class{
		{
			Class:              "A",
			VectorIndexType:    "hnsw",
			ReplicationConfig:  &models.ReplicationConfig{Factor: 3},
			MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false},
		},
		{
			Class:              "B",
			VectorIndexType:    "flat",
			ReplicationConfig:  &models.ReplicationConfig{Factor: 1},
			MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
		},
		{
			Class:              "C",
			MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
			VectorConfig: map[string]models.VectorConfig{
				"v1": {VectorIndexType: "dynamic"},
				"v2": {VectorIndexType: "flat"},
			},
		},
	}

	sm.EXPECT().GetSchemaSkipAuth().Return(schema.Schema{
		Objects: &models.Schema{Classes: classes},
	}).Maybe()

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

func TestUsedModules_NilModuleConfigFallback(t *testing.T) {
	sg := &fakeNodesStatusGetter{}
	sm := schemaUC.NewMockSchemaGetter(t)
	logger, _ := test.NewNullLogger()
	tel := New(sg, sm, logger, "", 0, false, Config{ClusterID: func() string { return "" }})

	classes := []*models.Class{
		{
			Class:        "WithVectorizer",
			ModuleConfig: nil,
			Vectorizer:   "text2vec-openai",
		},
		{
			Class:        "BYOV",
			ModuleConfig: nil,
			Vectorizer:   "none",
		},
		{
			Class:        "EmptyVectorizer",
			ModuleConfig: nil,
			Vectorizer:   "",
		},
		{
			Class: "ProperlyConfigured",
			ModuleConfig: map[string]interface{}{
				"text2vec-cohere": map[string]interface{}{},
			},
			Vectorizer: "text2vec-cohere",
		},
	}

	sm.EXPECT().GetSchemaSkipAuth().Return(schema.Schema{
		Objects: &models.Schema{Classes: classes},
	}).Maybe()

	modules, err := tel.getUsedModules()
	require.NoError(t, err)
	assert.Contains(t, modules, "text2vec-openai", "fallback must add vectorizer when ModuleConfig is nil")
	assert.Contains(t, modules, "text2vec-cohere", "properly-configured class unchanged")
	assert.NotContains(t, modules, "none", "BYOV vectorizer 'none' must not appear")
	assert.NotContains(t, modules, "", "empty vectorizer must not appear")
}

func TestCuratedFields_NodeCount(t *testing.T) {
	sg := &fakeNodesStatusGetter{}
	sm := schemaUC.NewMockSchemaGetter(t)
	logger, _ := test.NewNullLogger()
	tel := New(sg, sm, logger, "", 0, false, Config{ClusterID: func() string { return "" }})

	sm.EXPECT().Nodes().Return([]string{"a", "b"}).Maybe()
	sm.EXPECT().GetSchemaSkipAuth().Return(schema.Schema{}).Maybe()
	sg.On("LocalNodeStatus", context.Background(), "", "", verbosity.OutputVerbose).Return(
		&models.NodeStatus{Stats: &models.NodeStats{}},
	)

	payload, err := tel.buildPayload(context.Background(), PayloadType.Init)
	require.NoError(t, err)
	require.NotNil(t, payload.NodeCount)
	assert.Equal(t, 2, *payload.NodeCount)
}

// Pointer fields serialize a measured zero/false instead of being dropped by
// omitempty; nil means unmeasured.
func TestPayload_PointerSemantics_JSON(t *testing.T) {
	t.Run("measured zero/false serialize", func(t *testing.T) {
		p := Payload{
			NodeCount:                  ptrTo(0),
			MaxReplicationFactor:       ptrTo(0),
			ReplicationEnabled:         ptrTo(false),
			MTCollectionCount:          ptrTo(0),
			NamedVectorCollectionCount: ptrTo(0),
			AsyncIndexingEnabled:       ptrTo(false),
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
	})

	t.Run("nil curated fields omitted; measured field present", func(t *testing.T) {
		p := Payload{
			ClusterID: "00000000-0000-7000-0000-000000000001",
			NodeCount: ptrTo(3),
		}
		b, err := json.Marshal(p)
		require.NoError(t, err)
		s := string(b)

		assert.Contains(t, s, `"clusterId":"00000000-0000-7000-0000-000000000001"`)
		assert.Contains(t, s, `"nodeCount":3`)
		assert.NotContains(t, s, "replicationEnabled")
		assert.NotContains(t, s, "asyncIndexingEnabled")
		assert.NotContains(t, s, "mtCollectionCount")
	})
}

func TestCuratedFields_DefaultVectorIndexType(t *testing.T) {
	sg := &fakeNodesStatusGetter{}
	sm := schemaUC.NewMockSchemaGetter(t)
	logger, _ := test.NewNullLogger()
	tel := New(sg, sm, logger, "", 0, false, Config{ClusterID: func() string { return "" }})

	sm.EXPECT().GetSchemaSkipAuth().Return(schema.Schema{
		Objects: &models.Schema{
			Classes: []*models.Class{
				{Class: "X", VectorIndexType: ""},
			},
		},
	}).Maybe()
	sm.EXPECT().Nodes().Return([]string{"node1"}).Maybe()
	sg.On("LocalNodeStatus", context.Background(), "", "", verbosity.OutputVerbose).Return(
		&models.NodeStatus{Stats: &models.NodeStats{}},
	)

	payload, err := tel.buildPayload(context.Background(), PayloadType.Init)
	require.NoError(t, err)
	require.NotNil(t, payload.VectorIndexTypeCounts)
	assert.Equal(t, 1, payload.VectorIndexTypeCounts["hnsw"], "empty VectorIndexType defaults to hnsw")
	assert.Equal(t, 0, payload.VectorIndexTypeCounts[""])
}
