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
)

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

	vertex := map[string]any{
		"apiEndpoint": "us-central1-aiplatform.googleapis.com",
		"projectId":   "my-project",
		"location":    "us-central1",
	}

	tests := []struct {
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
	}{
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

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			helper.SetupClient(compose.GetWeaviate().URI())
			helper.CreateClass(t, tc.class(tc.className, tc.initial))
			if tc.tenant != "" {
				helper.CreateTenants(t, tc.className, []*models.Tenant{{Name: tc.tenant}})
			}

			rnd := rand.New(rand.NewSource(1))
			supplied := map[strfmt.UUID][]float32{}
			var objects []*models.Object
			for i := 0; i < 3; i++ {
				id := strfmt.UUID(uuid.NewString())
				vector := make([]float32, endpointTestDimensions)
				for j := range vector {
					vector[j] = rnd.Float32()
				}
				obj := &models.Object{Class: tc.className, ID: id, Tenant: tc.tenant, Properties: map[string]any{tc.property: "some text"}}
				tc.setVector(obj, vector)
				supplied[id] = vector
				objects = append(objects, obj)
			}
			helper.CreateObjectsBatch(t, objects)

			updateClass := func(changes map[string]any, addProperty bool) error {
				class := helper.GetClass(t, tc.className)
				settings := tc.settings(class)
				for k, v := range changes {
					settings[k] = v
				}
				if addProperty {
					class.Properties = append(class.Properties, &models.Property{Name: "added", DataType: []string{"text"}})
				}
				params := schema.NewSchemaObjectsUpdateParams().WithClassName(tc.className).WithObjectClass(class)
				_, err := helper.Client(t).Schema.SchemaObjectsUpdate(params, nil)
				return err
			}
			updateSettings := func(changes map[string]any) error {
				return updateClass(changes, false)
			}
			requireUnprocessable := func(t *testing.T, err error, contains string) {
				t.Helper()
				var unprocessable *schema.SchemaObjectsUpdateUnprocessableEntity
				require.True(t, errors.As(err, &unprocessable), "expected 422, got %v", err)
				require.Contains(t, unprocessable.Payload.Error[0].Message, contains)
			}
			switched := map[string]any{}
			for k, v := range tc.initial {
				switched[k] = v
			}
			for k, v := range vertex {
				switched[k] = v
			}
			localClassParams := func(node int) *schema.SchemaObjectsGetParams {
				helper.SetupClient(compose.GetWeaviateNode(node).URI())
				consistency := false
				return schema.NewSchemaObjectsGetParams().WithClassName(tc.className).WithConsistency(&consistency)
			}
			localClassJSON := func(t *testing.T, node int) string {
				t.Helper()
				params := localClassParams(node)
				var class string
				require.EventuallyWithT(t, func(c *assert.CollectT) {
					res, err := helper.Client(t).Schema.SchemaObjectsGet(params, nil)
					if assert.NoError(c, err) {
						class = mustJSON(t, res.Payload)
					}
				}, 30*time.Second, 250*time.Millisecond, "node %d", node)
				return class
			}
			requireVertexOnNode := func(t *testing.T, node int) {
				t.Helper()
				params := localClassParams(node)
				require.EventuallyWithT(t, func(c *assert.CollectT) {
					res, err := helper.Client(t).Schema.SchemaObjectsGet(params, nil)
					if !assert.NoError(c, err) {
						return
					}
					settings := tc.settings(res.Payload)
					for k, want := range switched {
						assert.JSONEq(c, mustJSON(t, want), mustJSON(t, settings[k]), k)
					}
				}, 30*time.Second, 250*time.Millisecond, "node %d", node)
			}
			requireSuppliedVectors := func(t *testing.T) {
				t.Helper()
				for id, vector := range supplied {
					var obj *models.Object
					var err error
					if tc.tenant != "" {
						obj, err = helper.TenantObjectWithInclude(t, tc.className, id, tc.tenant, "vector")
					} else {
						obj, err = helper.GetObject(t, tc.className, id, "vector")
					}
					require.NoError(t, err)
					require.Equal(t, vector, tc.getVector(obj))
				}
			}

			t.Run("Vertex endpoint without projectId is rejected", func(t *testing.T) {
				err := updateSettings(map[string]any{"apiEndpoint": "us-central1-aiplatform.googleapis.com"})
				requireUnprocessable(t, err, "projectId cannot be empty")
			})

			rejected := []struct {
				name        string
				changes     map[string]any
				addProperty bool
				wantError   string
			}{
				{
					name:      "endpoint switch with a model change",
					changes:   map[string]any{"apiEndpoint": vertex["apiEndpoint"], "projectId": vertex["projectId"], "location": vertex["location"], tc.modelKey: "text-embedding-005"},
					wantError: tc.immutableError,
				},
				{
					name:        "endpoint switch with an added property",
					changes:     vertex,
					addProperty: true,
					wantError:   "cannot be updated through updating the class",
				},
			}
			for _, r := range rejected {
				t.Run("rejected "+r.name+" leaves every node unchanged", func(t *testing.T) {
					before := map[int]string{}
					for node := 1; node <= 3; node++ {
						before[node] = localClassJSON(t, node)
					}
					helper.SetupClient(compose.GetWeaviate().URI())
					requireUnprocessable(t, updateClass(r.changes, r.addProperty), r.wantError)
					for node := 1; node <= 3; node++ {
						require.JSONEq(t, before[node], localClassJSON(t, node), "node %d", node)
					}
					helper.SetupClient(compose.GetWeaviate().URI())
				})
			}

			t.Run("switch to Vertex", func(t *testing.T) {
				require.NoError(t, updateSettings(vertex))
				for node := 1; node <= 3; node++ {
					requireVertexOnNode(t, node)
				}
				helper.SetupClient(compose.GetWeaviate().URI())
				requireSuppliedVectors(t)
			})

			t.Run("config survives restarting every node", func(t *testing.T) {
				for node := 1; node <= 3; node++ {
					require.NoError(t, compose.StopNode(ctx, node-1, nil))
					require.NoError(t, compose.StartNode(ctx, node-1))
					requireVertexOnNode(t, node)
				}
				helper.SetupClient(compose.GetWeaviate().URI())
				requireSuppliedVectors(t)
			})

			t.Run("model change is rejected", func(t *testing.T) {
				helper.SetupClient(compose.GetWeaviate().URI())
				err := updateSettings(map[string]any{tc.modelKey: "text-embedding-005"})
				requireUnprocessable(t, err, tc.immutableError)
				for node := 1; node <= 3; node++ {
					requireVertexOnNode(t, node)
				}
			})
		})
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return string(b)
}
