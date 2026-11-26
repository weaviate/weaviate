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

package test

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"math/rand"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

const (
	endpointTestDimensions = 1536
	endpointTestVector     = "gemini"
	vertexEndpointHost     = "us-central1-aiplatform.googleapis.com"
)

var vertexSwitch = map[string]any{
	"apiEndpoint": vertexEndpointHost,
	"projectId":   "my-project",
	"location":    "us-central1",
}

type endpointCase struct {
	name           string
	className      string
	property       string
	tenant         string
	initial        map[string]any
	modelKey       string
	immutableError string
	class          func(className string, settings map[string]any) *models.Class
	settings       func(c *models.Class) map[string]any
	setVector      func(obj *models.Object, vector []float32)
	getVector      func(obj *models.Object) []float32
}

type endpointRun struct {
	compose  *docker.DockerCompose
	tc       endpointCase
	supplied map[strfmt.UUID][]float32
	switched map[string]any
}

func TestText2VecGoogle_SwitchAIStudioToVertex_ThreeNodes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithText2VecGoogle("not-used-vectors-are-supplied").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()
	defer helper.ResetClient()

	for _, tc := range endpointCases() {
		t.Run(tc.name, func(t *testing.T) {
			run := &endpointRun{compose: compose, tc: tc}
			run.setup(t)
			run.testMissingProjectID(t)
			run.testSwitch(t)
			run.testRestart(ctx, t)
			run.testModelChangeRejected(t)
		})
	}
}

func endpointCases() []endpointCase {
	return []endpointCase{
		{
			name:      "multi-tenant named vector in the legacy text2vec-palm shape",
			className: "GoogleNamedVector",
			property:  "content",
			tenant:    "tenant1",
			initial: map[string]any{
				"apiEndpoint":        "generativelanguage.googleapis.com",
				"dimensions":         endpointTestDimensions,
				"modelId":            "gemini-embedding-001",
				"properties":         []any{"content"},
				"vectorizeClassName": true,
			},
			modelKey:       "modelId",
			immutableError: "vectorizer config of vector \"gemini\" is immutable",
			class: func(className string, settings map[string]any) *models.Class {
				return &models.Class{
					Class:      className,
					Properties: []*models.Property{{Name: "content", DataType: []string{"text"}}},
					VectorConfig: map[string]models.VectorConfig{
						endpointTestVector: {
							VectorIndexType:   "hnsw",
							VectorIndexConfig: map[string]any{"distance": "cosine", "rq": map[string]any{"enabled": true, "bits": 1}},
							Vectorizer:        map[string]any{"text2vec-palm": settings},
						},
					},
					MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
					ReplicationConfig:  &models.ReplicationConfig{Factor: 3},
				}
			},
			settings: func(c *models.Class) map[string]any {
				return c.VectorConfig[endpointTestVector].Vectorizer.(map[string]any)["text2vec-palm"].(map[string]any)
			},
			setVector: func(obj *models.Object, vector []float32) {
				obj.Vectors = models.Vectors{endpointTestVector: vector}
			},
			getVector: func(obj *models.Object) []float32 {
				vector, _ := obj.Vectors[endpointTestVector].([]float32)
				return vector
			},
		},
		{
			name:      "legacy vectorizer with text2vec-google",
			className: "GoogleLegacyVector",
			property:  "text",
			initial: map[string]any{
				"apiEndpoint":        "generativelanguage.googleapis.com",
				"model":              "gemini-embedding-001",
				"dimensions":         endpointTestDimensions,
				"vectorizeClassName": false,
			},
			modelKey:       "model",
			immutableError: "can only update generative and reranker module configs",
			class: func(className string, settings map[string]any) *models.Class {
				return &models.Class{
					Class:             className,
					Properties:        []*models.Property{{Name: "text", DataType: []string{"text"}}},
					Vectorizer:        "text2vec-google",
					VectorIndexType:   "hnsw",
					ModuleConfig:      map[string]any{"text2vec-google": settings},
					ReplicationConfig: &models.ReplicationConfig{Factor: 3},
				}
			},
			settings: func(c *models.Class) map[string]any {
				return c.ModuleConfig.(map[string]any)["text2vec-google"].(map[string]any)
			},
			setVector: func(obj *models.Object, vector []float32) {
				obj.Vector = vector
			},
			getVector: func(obj *models.Object) []float32 {
				return obj.Vector
			},
		},
	}
}

func (r *endpointRun) setup(t *testing.T) {
	helper.SetupClient(r.compose.GetWeaviate().URI())
	helper.CreateClass(t, r.tc.class(r.tc.className, r.tc.initial))
	if r.tc.tenant != "" {
		helper.CreateTenants(t, r.tc.className, []*models.Tenant{{Name: r.tc.tenant}})
	}
	r.supplied = r.insertObjects(t)
	r.switched = map[string]any{}
	maps.Copy(r.switched, r.tc.initial)
	maps.Copy(r.switched, vertexSwitch)
}

func (r *endpointRun) insertObjects(t *testing.T) map[strfmt.UUID][]float32 {
	rnd := rand.New(rand.NewSource(1))
	supplied := map[strfmt.UUID][]float32{}
	var objects []*models.Object
	for i := 0; i < 3; i++ {
		id := strfmt.UUID(uuid.NewString())
		vector := make([]float32, endpointTestDimensions)
		for j := range vector {
			vector[j] = rnd.Float32()
		}
		obj := &models.Object{Class: r.tc.className, ID: id, Tenant: r.tc.tenant, Properties: map[string]any{r.tc.property: "some text"}}
		r.tc.setVector(obj, vector)
		supplied[id] = vector
		objects = append(objects, obj)
	}
	helper.CreateObjectsBatch(t, objects)
	return supplied
}

func (r *endpointRun) updateSettings(t *testing.T, changes map[string]any) error {
	class := helper.GetClass(t, r.tc.className)
	maps.Copy(r.tc.settings(class), changes)
	params := schema.NewSchemaObjectsUpdateParams().WithClassName(r.tc.className).WithObjectClass(class)
	_, err := helper.Client(t).Schema.SchemaObjectsUpdate(params, nil)
	return err
}

func (r *endpointRun) localClassParams(node int) *schema.SchemaObjectsGetParams {
	helper.SetupClient(r.compose.GetWeaviateNode(node).URI())
	consistency := false
	return schema.NewSchemaObjectsGetParams().WithClassName(r.tc.className).WithConsistency(&consistency)
}

func (r *endpointRun) requireVertexOnNode(t *testing.T, node int) {
	t.Helper()
	params := r.localClassParams(node)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		res, err := helper.Client(t).Schema.SchemaObjectsGet(params, nil)
		if !assert.NoError(c, err) {
			return
		}
		settings := r.tc.settings(res.Payload)
		for k, want := range r.switched {
			assert.JSONEq(c, mustJSON(t, want), mustJSON(t, settings[k]), k)
		}
	}, 30*time.Second, 250*time.Millisecond, "node %d", node)
}

func (r *endpointRun) getSuppliedObject(t *testing.T, id strfmt.UUID) (*models.Object, error) {
	if r.tc.tenant != "" {
		return helper.TenantObjectWithInclude(t, r.tc.className, id, r.tc.tenant, "vector")
	}
	return helper.GetObject(t, r.tc.className, id, "vector")
}

func (r *endpointRun) requireSuppliedVectors(t *testing.T) {
	t.Helper()
	for id, vector := range r.supplied {
		obj, err := r.getSuppliedObject(t, id)
		require.NoError(t, err)
		require.Equal(t, vector, r.tc.getVector(obj))
	}
}

func (r *endpointRun) requireAllNodesVertex(t *testing.T) {
	t.Helper()
	for node := 1; node <= 3; node++ {
		r.requireVertexOnNode(t, node)
	}
}

func requireUnprocessable(t *testing.T, err error, contains string) {
	t.Helper()
	var unprocessable *schema.SchemaObjectsUpdateUnprocessableEntity
	require.True(t, errors.As(err, &unprocessable), "expected 422, got %v", err)
	require.Contains(t, unprocessable.Payload.Error[0].Message, contains)
}

func (r *endpointRun) testMissingProjectID(t *testing.T) {
	t.Run("Vertex endpoint without projectId is rejected", func(t *testing.T) {
		err := r.updateSettings(t, map[string]any{"apiEndpoint": vertexEndpointHost})
		requireUnprocessable(t, err, "projectId cannot be empty")
	})
}

func (r *endpointRun) testSwitch(t *testing.T) {
	t.Run("switch to Vertex", func(t *testing.T) {
		require.NoError(t, r.updateSettings(t, vertexSwitch))
		r.requireAllNodesVertex(t)
		helper.SetupClient(r.compose.GetWeaviate().URI())
		r.requireSuppliedVectors(t)
	})
}

func (r *endpointRun) testRestart(ctx context.Context, t *testing.T) {
	t.Run("config survives restarting every node", func(t *testing.T) {
		for node := 1; node <= 3; node++ {
			require.NoError(t, r.compose.StopNode(ctx, node-1, nil))
			require.NoError(t, r.compose.StartNode(ctx, node-1))
			r.requireVertexOnNode(t, node)
		}
		helper.SetupClient(r.compose.GetWeaviate().URI())
		r.requireSuppliedVectors(t)
	})
}

func (r *endpointRun) testModelChangeRejected(t *testing.T) {
	t.Run("model change is rejected", func(t *testing.T) {
		helper.SetupClient(r.compose.GetWeaviate().URI())
		err := r.updateSettings(t, map[string]any{r.tc.modelKey: "text-embedding-005"})
		requireUnprocessable(t, err, r.tc.immutableError)
	})
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}
