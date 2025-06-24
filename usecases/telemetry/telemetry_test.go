//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package telemetry

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestTelemetry_BuildPayload(t *testing.T) {
	t.Run("happy path", func(t *testing.T) {
		t.Run("on init", func(t *testing.T) {
			tel, sg, sm := newTestTelemeter()
			sg.On("LocalNodeStatus", context.Background(), "", "", verbosity.OutputVerbose).Return(
				&models.NodeStatus{
					Stats: &models.NodeStats{
						ObjectCount: 100,
					},
				})
			sm.On("GetSchemaSkipAuth").Return(
				schema.Schema{
					Objects: &models.Schema{Classes: []*models.Class{
						{
							Class: "GoogleModuleWithGoogleAIStudioEmptyConfig",
							ModuleConfig: map[string]interface{}{
								"text2vec-google": nil,
							},
						},
						{
							Class: "LegacyConfiguration",
							ModuleConfig: map[string]interface{}{
								"text2vec-google": map[string]interface{}{
									"modelId":     "text-embedding-004",
									"apiEndpoint": "generativelanguage.googleapis.com",
								},
								"generative-openai": map[string]interface{}{},
							},
						},
						{
							Class: "NamedVector",
							VectorConfig: map[string]models.VectorConfig{
								"description": {
									Vectorizer: map[string]interface{}{
										"text2vec-openai": map[string]interface{}{
											"properties":         []interface{}{"description"},
											"vectorizeClassName": false,
										},
									},
									VectorIndexType: "flat",
								},
							},
						},
						{
							Class: "NamedVectorWithNilVectorizer",
							VectorConfig: map[string]models.VectorConfig{
								"description": {
									Vectorizer:      nil,
									VectorIndexType: "flat",
								},
							},
						},
						{
							Class: "BothNamedVectorAndLegacyConfiguration",
							ModuleConfig: map[string]interface{}{
								"generative-google": map[string]interface{}{
									"apiEndpoint": "generativelanguage.googleapis.com",
								},
							},
							VectorConfig: map[string]models.VectorConfig{
								"description_google": {
									Vectorizer: map[string]interface{}{
										"text2vec-google": map[string]interface{}{
											"properties":         []interface{}{"description"},
											"vectorizeClassName": false,
										},
									},
									VectorIndexType: "flat",
								},
								"description_aws": {
									Vectorizer: map[string]interface{}{
										"text2vec-aws": map[string]interface{}{
											"properties":         []interface{}{"description"},
											"vectorizeClassName": false,
										},
									},
									VectorIndexType: "flat",
								},
								"description_openai": {
									Vectorizer: map[string]interface{}{
										"text2vec-openai": map[string]interface{}{},
									},
									VectorIndexType: "flat",
								},
							},
						},
					}},
				})
			payload, err := tel.buildPayload(context.Background(), PayloadType.Init)
			assert.Nil(t, err)
			assert.Equal(t, tel.machineID, payload.MachineID)
			assert.Equal(t, PayloadType.Init, payload.Type)
			assert.Equal(t, config.ServerVersion, payload.Version)
			assert.Equal(t, int64(0), payload.ObjectsCount)
			assert.Equal(t, 5, payload.CollectionsCount)
			assert.Equal(t, runtime.GOOS, payload.OS)
			assert.Equal(t, runtime.GOARCH, payload.Arch)
			assert.NotEmpty(t, payload.UsedModules)
			assert.Len(t, payload.UsedModules, 6)
			assert.Contains(t, payload.UsedModules, "text2vec-aws")
			assert.Contains(t, payload.UsedModules, "text2vec-openai")
			assert.Contains(t, payload.UsedModules, "text2vec-google-vertex-ai")
			assert.Contains(t, payload.UsedModules, "text2vec-google-ai-studio")
			assert.Contains(t, payload.UsedModules, "generative-google-ai-studio")
			assert.Contains(t, payload.UsedModules, "generative-openai")
		})

		t.Run("on update", func(t *testing.T) {
			tel, sg, sm := newTestTelemeter()
			sg.On("LocalNodeStatus", context.Background(), "", "", verbosity.OutputVerbose).Return(
				&models.NodeStatus{
					Stats: &models.NodeStats{
						ObjectCount: 1000,
					},
				})
			sm.On("GetSchemaSkipAuth").Return(
				schema.Schema{
					Objects: &models.Schema{Classes: []*models.Class{
						{
							Class: "Class",
							ModuleConfig: map[string]interface{}{
								"generative-openai": map[string]interface{}{},
							},
							VectorConfig: map[string]models.VectorConfig{
								"description_google": {
									Vectorizer: map[string]interface{}{
										"text2vec-google": map[string]interface{}{
											"properties":         []interface{}{"description"},
											"vectorizeClassName": false,
										},
									},
									VectorIndexType: "flat",
								},
								"description_aws": {
									Vectorizer: map[string]interface{}{
										"text2vec-aws": map[string]interface{}{
											"properties":         []interface{}{"description"},
											"vectorizeClassName": false,
										},
									},
									VectorIndexType: "flat",
								},
							},
						},
					}},
				})
			payload, err := tel.buildPayload(context.Background(), PayloadType.Update)
			assert.Nil(t, err)
			assert.Equal(t, tel.machineID, payload.MachineID)
			assert.Equal(t, PayloadType.Update, payload.Type)
			assert.Equal(t, config.ServerVersion, payload.Version)
			assert.Equal(t, int64(1000), payload.ObjectsCount)
			assert.Equal(t, runtime.GOOS, payload.OS)
			assert.Equal(t, runtime.GOARCH, payload.Arch)
			assert.NotEmpty(t, payload.UsedModules)
			assert.Len(t, payload.UsedModules, 3)
			assert.Contains(t, payload.UsedModules, "text2vec-google-vertex-ai")
			assert.Contains(t, payload.UsedModules, "text2vec-aws")
			assert.Contains(t, payload.UsedModules, "generative-openai")
		})

		t.Run("on terminate", func(t *testing.T) {
			tel, sg, _ := newTestTelemeter()
			sg.On("LocalNodeStatus", context.Background(), "", "", verbosity.OutputVerbose).Return(
				&models.NodeStatus{
					Stats: &models.NodeStats{
						ObjectCount: 300_000_000_000,
					},
				})
			payload, err := tel.buildPayload(context.Background(), PayloadType.Terminate)
			assert.Nil(t, err)
			assert.Equal(t, tel.machineID, payload.MachineID)
			assert.Equal(t, PayloadType.Terminate, payload.Type)
			assert.Equal(t, config.ServerVersion, payload.Version)
			assert.Equal(t, int64(300_000_000_000), payload.ObjectsCount)
			assert.Equal(t, runtime.GOOS, payload.OS)
			assert.Equal(t, runtime.GOARCH, payload.Arch)
			assert.Empty(t, payload.UsedModules)
		})
	})

	t.Run("failure path", func(t *testing.T) {
		t.Run("fail to get node status", func(t *testing.T) {
			tel, sg, _ := newTestTelemeter()
			sg.On("LocalNodeStatus", context.Background(), "", "", verbosity.OutputVerbose).Return(nil)
			payload, err := tel.buildPayload(context.Background(), PayloadType.Terminate)
			assert.Nil(t, payload)
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), "get object count")
		})

		t.Run("fail to get node status stats", func(t *testing.T) {
			tel, sg, _ := newTestTelemeter()
			sg.On("LocalNodeStatus", context.Background(), "", "", verbosity.OutputVerbose).Return(&models.NodeStatus{})
			payload, err := tel.buildPayload(context.Background(), PayloadType.Terminate)
			assert.Nil(t, payload)
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), "get object count")
		})
	})
}

func TestTelemetry_WithConsumer(t *testing.T) {
	config.ServerVersion = "X.X.X"
	server := httptest.NewServer(&testConsumer{t})
	defer server.Close()

	consumerURL := fmt.Sprintf("%s/weaviate-telemetry", server.URL)
	opts := []telemetryOpt{
		withConsumerURL(consumerURL),
		withPushInterval(100 * time.Millisecond),
	}
	tel, sg, sm := newTestTelemeter(opts...)

	sg.On("LocalNodeStatus", context.Background(), "", "", verbosity.OutputVerbose).Return(
		&models.NodeStatus{
			Stats: &models.NodeStats{
				ObjectCount: 100,
			},
		})

	sm.On("GetSchemaSkipAuth").Return(
		schema.Schema{
			Objects: &models.Schema{Classes: []*models.Class{
				{
					Class: "Class",
					ModuleConfig: map[string]interface{}{
						"generative-openai": map[string]interface{}{},
					},
					VectorConfig: map[string]models.VectorConfig{
						"description_google": {
							Vectorizer: map[string]interface{}{
								"text2vec-google": map[string]interface{}{
									"properties":         []interface{}{"description"},
									"vectorizeClassName": false,
									"apiEndpoint":        "generativelanguage.googleapis.com",
								},
							},
							VectorIndexType: "flat",
						},
						"description_aws": {
							Vectorizer: map[string]interface{}{
								"text2vec-aws": map[string]interface{}{
									"properties":         []interface{}{"description"},
									"vectorizeClassName": false,
								},
							},
							VectorIndexType: "flat",
						},
					},
				},
			}},
		})

	err := tel.Start(context.Background())
	require.Nil(t, err)

	ticker := time.NewTicker(100 * time.Millisecond)
	start := time.Now()
	wait := make(chan struct{})
	go func() {
		for range ticker.C {
			if time.Since(start) > time.Second {
				err = tel.Stop(context.Background())
				assert.Nil(t, err)
				wait <- struct{}{}
			}
		}
	}()
	<-wait
}

type telemetryOpt func(*Telemeter)

func withConsumerURL(url string) telemetryOpt {
	encoded := base64.StdEncoding.EncodeToString([]byte(url))
	return func(tel *Telemeter) {
		tel.consumer = encoded
	}
}

func withPushInterval(interval time.Duration) telemetryOpt {
	return func(tel *Telemeter) {
		tel.pushInterval = interval
	}
}

func newTestTelemeter(opts ...telemetryOpt,
) (*Telemeter, *fakeNodesStatusGetter, *fakeSchemaManager,
) {
	sg := &fakeNodesStatusGetter{}
	sm := &fakeSchemaManager{}
	logger, _ := test.NewNullLogger()
	tel := New(sg, sm, logger)
	for _, opt := range opts {
		opt(tel)
	}
	return tel, sg, sm
}

type testConsumer struct {
	t *testing.T
}

func (h *testConsumer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	assert.Equal(h.t, "/weaviate-telemetry", r.URL.String())
	assert.Equal(h.t, http.MethodPost, r.Method)
	b, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	require.Nil(h.t, err)

	var payload Payload
	err = json.Unmarshal(b, &payload)
	require.Nil(h.t, err)

	assert.NotEmpty(h.t, payload.MachineID)
	assert.Contains(h.t, []string{
		PayloadType.Init,
		PayloadType.Update,
		PayloadType.Terminate,
	}, payload.Type)
	assert.Equal(h.t, config.ServerVersion, payload.Version)
	if payload.Type == PayloadType.Init {
		assert.Zero(h.t, payload.ObjectsCount)
	} else {
		assert.NotZero(h.t, payload.ObjectsCount)
	}
	assert.Equal(h.t, runtime.GOOS, payload.OS)
	assert.Equal(h.t, runtime.GOARCH, payload.Arch)
	assert.NotEmpty(h.t, payload.CollectionsCount)
	assert.NotEmpty(h.t, payload.UsedModules)
	assert.Len(h.t, payload.UsedModules, 3)
	assert.Contains(h.t, payload.UsedModules, "text2vec-google-ai-studio")
	assert.Contains(h.t, payload.UsedModules, "text2vec-aws")
	assert.Contains(h.t, payload.UsedModules, "generative-openai")

	h.t.Logf("request body: %s", string(b))
	w.WriteHeader(http.StatusOK)
}
