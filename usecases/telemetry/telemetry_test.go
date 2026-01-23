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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
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
			tel, sg, sm, ci := newTestTelemeterWithCloudInfo()
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
			ci.On("getCloudInfo").Return(nil)

			// Track some clients before building INIT payload
			trackClientRequest(t, tel, "python", "weaviate-client-python/1.0.0")
			trackClientRequest(t, tel, "java", "weaviate-client-java/1.0.0")

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
			// INIT payloads should not include client usage data
			assert.Nil(t, payload.ClientUsage)
			assert.Nil(t, payload.CloudProvider)
			assert.Nil(t, payload.UniqueID)
		})

		t.Run("on update", func(t *testing.T) {
			tel, sg, sm, ci := newTestTelemeterWithCloudInfo()
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
			ci.On("getCloudInfo").Return(nil)

			// Track multiple client types
			trackClientRequest(t, tel, "python", "weaviate-client-python/1.0.0")
			trackClientRequest(t, tel, "python", "weaviate-client-python/1.0.0")
			trackClientRequest(t, tel, "java", "weaviate-client-java/1.0.0")
			trackClientRequest(t, tel, "typescript", "weaviate-client-typescript/1.0.0")
			trackClientRequest(t, tel, "go", "weaviate-client-go/1.0.0")
			trackClientRequest(t, tel, "csharp", "weaviate-client-csharp/1.0.0")

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
			// UPDATE payloads should include client usage data
			assert.NotNil(t, payload.ClientUsage)
			assert.NotNil(t, payload.ClientUsage[ClientTypePython])
			assert.Equal(t, int64(2), payload.ClientUsage[ClientTypePython]["1.0.0"])
			assert.NotNil(t, payload.ClientUsage[ClientTypeJava])
			assert.Equal(t, int64(1), payload.ClientUsage[ClientTypeJava]["1.0.0"])
			assert.NotNil(t, payload.ClientUsage[ClientTypeTypeScript])
			assert.Equal(t, int64(1), payload.ClientUsage[ClientTypeTypeScript]["1.0.0"])
			assert.NotNil(t, payload.ClientUsage[ClientTypeGo])
			assert.Equal(t, int64(1), payload.ClientUsage[ClientTypeGo]["1.0.0"])
			assert.NotNil(t, payload.ClientUsage[ClientTypeCSharp])
			assert.Equal(t, int64(1), payload.ClientUsage[ClientTypeCSharp]["1.0.0"])
			// Verify tracker was reset after GetAndReset
			currentCounts := tel.clientTracker.Get()
			assert.Empty(t, currentCounts)
			assert.Nil(t, payload.CloudProvider)
			assert.Nil(t, payload.UniqueID)
		})

		t.Run("on terminate", func(t *testing.T) {
			tel, sg, _, ci := newTestTelemeterWithCloudInfo()
			sg.On("LocalNodeStatus", context.Background(), "", "", verbosity.OutputVerbose).Return(
				&models.NodeStatus{
					Stats: &models.NodeStats{
						ObjectCount: 300_000_000_000,
					},
				})
			ci.On("getCloudInfo").Return(nil)

			// Track some clients before terminate
			trackClientRequest(t, tel, "python", "weaviate-client-python/1.0.0")
			trackClientRequest(t, tel, "java", "weaviate-client-java/1.0.0")
			trackClientRequest(t, tel, "typescript", "weaviate-client-typescript/1.0.0")

			payload, err := tel.buildPayload(context.Background(), PayloadType.Terminate)
			assert.Nil(t, err)
			assert.Equal(t, tel.machineID, payload.MachineID)
			assert.Equal(t, PayloadType.Terminate, payload.Type)
			assert.Equal(t, config.ServerVersion, payload.Version)
			assert.Equal(t, int64(300_000_000_000), payload.ObjectsCount)
			assert.Equal(t, runtime.GOOS, payload.OS)
			assert.Equal(t, runtime.GOARCH, payload.Arch)
			assert.Empty(t, payload.UsedModules)
			// TERMINATE payloads should include client usage data
			assert.NotNil(t, payload.ClientUsage)
			assert.NotNil(t, payload.ClientUsage[ClientTypePython])
			assert.Equal(t, int64(1), payload.ClientUsage[ClientTypePython]["1.0.0"])
			assert.NotNil(t, payload.ClientUsage[ClientTypeJava])
			assert.Equal(t, int64(1), payload.ClientUsage[ClientTypeJava]["1.0.0"])
			assert.NotNil(t, payload.ClientUsage[ClientTypeTypeScript])
			assert.Equal(t, int64(1), payload.ClientUsage[ClientTypeTypeScript]["1.0.0"])
			assert.Nil(t, payload.CloudProvider)
			assert.Nil(t, payload.UniqueID)
		})

		t.Run("on update with no client usage", func(t *testing.T) {
			tel, sg, sm := newTestTelemeter()
			sg.On("LocalNodeStatus", context.Background(), "", "", verbosity.OutputVerbose).Return(
				&models.NodeStatus{
					Stats: &models.NodeStats{
						ObjectCount: 1000,
					},
				})
			sm.On("GetSchemaSkipAuth").Return(
				schema.Schema{
					Objects: &models.Schema{Classes: []*models.Class{}},
				})
			// Don't track any clients
			payload, err := tel.buildPayload(context.Background(), PayloadType.Update)
			assert.Nil(t, err)
			// When no clients are tracked, ClientUsage should be nil
			assert.Nil(t, payload.ClientUsage)
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
	// Create a channel-based client counter for tracking expected client requests
	expectedCounts := newClientCounter()
	defer expectedCounts.Stop()

	// Channel to receive telemetry payloads for verification
	telemetryChan := make(chan *Payload, 10)

	config.ServerVersion = "X.X.X"
	server := httptest.NewServer(&testConsumer{
		t:              t,
		expectedCounts: expectedCounts,
		telemetryChan:  telemetryChan,
	})
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

	// Wait for INIT payload to be sent
	initPayload := <-telemetryChan
	assert.Equal(t, PayloadType.Init, initPayload.Type)
	assert.Nil(t, initPayload.ClientUsage)

	// Send a fixed number of requests for each client type (deterministic)
	clientRequests := []struct {
		clientType ClientType
		count      int
	}{
		{ClientTypePython, 5},
		{ClientTypeJava, 3},
		{ClientTypeTypeScript, 2},
	}

	for _, req := range clientRequests {
		for i := 0; i < req.count; i++ {
			trackClientRequest(t, tel, string(req.clientType), fmt.Sprintf("weaviate-client-%s/1.0.0", req.clientType))
			expectedCounts.Increment(req.clientType)
		}
	}

	// Wait for UPDATE payload to be sent (should include client usage)
	updatePayload := <-telemetryChan
	assert.Equal(t, PayloadType.Update, updatePayload.Type)

	// Verify client usage matches expected counts
	expectedCountsMap := expectedCounts.Get()
	assert.NotNil(t, updatePayload.ClientUsage, "Expected client usage data but got nil")
	for clientType, expectedCount := range expectedCountsMap {
		// Sum up all versions for this client type
		versions, exists := updatePayload.ClientUsage[clientType]
		assert.True(t, exists, "Expected client type %s to be in ClientUsage", clientType)
		var totalCount int64
		for _, count := range versions {
			totalCount += count
		}
		assert.Equal(t, expectedCount, totalCount, "Mismatch for client type %s: expected %d, got %d", clientType, expectedCount, totalCount)
	}

	// Stop telemetry and wait for TERMINATE payload
	err = tel.Stop(context.Background())
	require.Nil(t, err)

	terminatePayload := <-telemetryChan
	assert.Equal(t, PayloadType.Terminate, terminatePayload.Type)
}

func TestTelemetry_BuildPayload_WithCloudInfo(t *testing.T) {
	t.Run("on init with cloud info present", func(t *testing.T) {
		tel, sg, sm, ci := newTestTelemeterWithCloudInfo()
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
				}},
			})
		ci.On("getCloudInfo").Return(
			&cloudInfo{cloudProvider: "GCP", uniqueID: "id"},
		)
		payload, err := tel.buildPayload(context.Background(), PayloadType.Init)
		assert.Nil(t, err)
		assert.Equal(t, tel.machineID, payload.MachineID)
		assert.Equal(t, PayloadType.Init, payload.Type)
		assert.Equal(t, config.ServerVersion, payload.Version)
		assert.Equal(t, int64(0), payload.ObjectsCount)
		assert.Equal(t, 1, payload.CollectionsCount)
		assert.Equal(t, runtime.GOOS, payload.OS)
		assert.Equal(t, runtime.GOARCH, payload.Arch)
		assert.NotEmpty(t, payload.UsedModules)
		assert.Len(t, payload.UsedModules, 1)
		assert.Contains(t, payload.UsedModules, "text2vec-google-vertex-ai")
		require.NotNil(t, payload.CloudProvider)
		assert.Equal(t, "GCP", *payload.CloudProvider)
		require.NotNil(t, payload.UniqueID)
		assert.Equal(t, "id", *payload.UniqueID)
	})

	t.Run("on update with only cloud provider present", func(t *testing.T) {
		tel, sg, _, ci := newTestTelemeterWithCloudInfo()
		sg.On("LocalNodeStatus", context.Background(), "", "", verbosity.OutputVerbose).Return(
			&models.NodeStatus{
				Stats: &models.NodeStats{
					ObjectCount: 1000,
				},
			})
		ci.On("getCloudInfo").Return(
			&cloudInfo{cloudProvider: "GCP"},
		)
		payload, err := tel.buildPayload(context.Background(), PayloadType.Update)
		assert.Nil(t, err)
		assert.Equal(t, tel.machineID, payload.MachineID)
		assert.Equal(t, PayloadType.Update, payload.Type)
		assert.Equal(t, config.ServerVersion, payload.Version)
		assert.Equal(t, int64(1000), payload.ObjectsCount)
		assert.Equal(t, 0, payload.CollectionsCount)
		assert.Equal(t, runtime.GOOS, payload.OS)
		assert.Equal(t, runtime.GOARCH, payload.Arch)
		assert.Empty(t, payload.UsedModules)
		require.NotNil(t, payload.CloudProvider)
		require.Nil(t, payload.UniqueID)
	})

	t.Run("on terminate with empty cloud info", func(t *testing.T) {
		tel, sg, _, ci := newTestTelemeterWithCloudInfo()
		sg.On("LocalNodeStatus", context.Background(), "", "", verbosity.OutputVerbose).Return(
			&models.NodeStatus{
				Stats: &models.NodeStats{
					ObjectCount: 300_000_000_000,
				},
			})
		ci.On("getCloudInfo").Return(
			&cloudInfo{},
		)
		payload, err := tel.buildPayload(context.Background(), PayloadType.Terminate)
		assert.Nil(t, err)
		assert.Equal(t, tel.machineID, payload.MachineID)
		assert.Equal(t, PayloadType.Terminate, payload.Type)
		assert.Equal(t, config.ServerVersion, payload.Version)
		assert.Equal(t, int64(300_000_000_000), payload.ObjectsCount)
		assert.Equal(t, runtime.GOOS, payload.OS)
		assert.Equal(t, runtime.GOARCH, payload.Arch)
		assert.Empty(t, payload.UsedModules)
		require.Nil(t, payload.CloudProvider)
		require.Nil(t, payload.UniqueID)
	})
}

func TestTelemetry_WithCloudInfoConsumer_GCP(t *testing.T) {
	server := httptest.NewServer(&gcpTestConsumer{t})
	defer server.Close()
	tel, sg, _ := newTestTelemeterWithCustomCloudInfo(newGCPCloudInfo(server.URL))

	sg.On("LocalNodeStatus", context.Background(), "", "", verbosity.OutputVerbose).Return(
		&models.NodeStatus{
			Stats: &models.NodeStats{
				ObjectCount: 100,
			},
		})

	payload, err := tel.buildPayload(context.Background(), PayloadType.Init)
	assert.Nil(t, err)
	assert.Equal(t, tel.machineID, payload.MachineID)
	assert.Equal(t, PayloadType.Init, payload.Type)
	assert.Equal(t, config.ServerVersion, payload.Version)
	assert.Equal(t, int64(0), payload.ObjectsCount)
	assert.Equal(t, 0, payload.CollectionsCount)
	assert.Equal(t, runtime.GOOS, payload.OS)
	assert.Equal(t, runtime.GOARCH, payload.Arch)
	require.NotNil(t, payload.CloudProvider)
	assert.Equal(t, "GCP", *payload.CloudProvider)
	require.NotNil(t, payload.UniqueID)
	assert.Equal(t, "some-id", *payload.UniqueID)
}

func TestTelemetry_WithCloudInfoConsumer_AWS(t *testing.T) {
	server := httptest.NewServer(&awsTestConsumer{t})
	defer server.Close()
	tel, sg, _ := newTestTelemeterWithCustomCloudInfo(newAWSCloudInfo(server.URL))

	sg.On("LocalNodeStatus", context.Background(), "", "", verbosity.OutputVerbose).Return(
		&models.NodeStatus{
			Stats: &models.NodeStats{
				ObjectCount: 100,
			},
		})

	payload, err := tel.buildPayload(context.Background(), PayloadType.Init)
	assert.Nil(t, err)
	assert.Equal(t, tel.machineID, payload.MachineID)
	assert.Equal(t, PayloadType.Init, payload.Type)
	assert.Equal(t, config.ServerVersion, payload.Version)
	assert.Equal(t, int64(0), payload.ObjectsCount)
	assert.Equal(t, 0, payload.CollectionsCount)
	assert.Equal(t, runtime.GOOS, payload.OS)
	assert.Equal(t, runtime.GOARCH, payload.Arch)
	require.NotNil(t, payload.CloudProvider)
	assert.Equal(t, "AWS", *payload.CloudProvider)
	require.NotNil(t, payload.UniqueID)
	assert.Equal(t, "101", *payload.UniqueID)
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
	// Pass empty url/duration to use defaults
	tel := New(sg, sm, logger, "", 0)
	for _, opt := range opts {
		opt(tel)
	}
	return tel, sg, sm
}

func newTestTelemeterWithCloudInfo(opts ...telemetryOpt,
) (*Telemeter, *fakeNodesStatusGetter, *fakeSchemaManager, *fakeCloudInfoProvider,
) {
	tel, sg, sm := newTestTelemeter(opts...)
	ci := &fakeCloudInfoProvider{}
	tel.cloudInfoHelper = &cloudInfoHelper{provider: ci}
	return tel, sg, sm, ci
}

func newTestTelemeterWithCustomCloudInfo(ci cloudInfoProvider, opts ...telemetryOpt,
) (*Telemeter, *fakeNodesStatusGetter, *fakeSchemaManager,
) {
	tel, sg, sm := newTestTelemeter(opts...)
	tel.cloudInfoHelper = &cloudInfoHelper{provider: ci}
	return tel, sg, sm
}

// clientCounter manages client counts using channel-based concurrency
type clientCounter struct {
	increment chan ClientType
	get       chan chan map[ClientType]int64
	reset     chan struct{}
	stop      chan struct{}
}

// newClientCounter creates and starts a new client counter
func newClientCounter() *clientCounter {
	cc := &clientCounter{
		increment: make(chan ClientType),
		get:       make(chan chan map[ClientType]int64),
		reset:     make(chan struct{}),
		stop:      make(chan struct{}),
	}
	go cc.run()
	return cc
}

// run is the manager goroutine that processes all operations
func (cc *clientCounter) run() {
	counts := make(map[ClientType]int64)
	for {
		select {
		case clientType := <-cc.increment:
			counts[clientType]++
		case respChan := <-cc.get:
			// Create a copy of the map to send back
			copy := make(map[ClientType]int64)
			for k, v := range counts {
				copy[k] = v
			}
			respChan <- copy
		case <-cc.reset:
			counts = make(map[ClientType]int64)
		case <-cc.stop:
			return
		}
	}
}

// Increment increments the count for a client type
func (cc *clientCounter) Increment(clientType ClientType) {
	cc.increment <- clientType
}

// Get returns a copy of all current counts
func (cc *clientCounter) Get() map[ClientType]int64 {
	respChan := make(chan map[ClientType]int64, 1)
	cc.get <- respChan
	return <-respChan
}

// Reset clears all counts
func (cc *clientCounter) Reset() {
	cc.reset <- struct{}{}
}

// Stop stops the manager goroutine
func (cc *clientCounter) Stop() {
	cc.stop <- struct{}{}
}

type testConsumer struct {
	t              *testing.T
	expectedCounts *clientCounter
	telemetryChan  chan *Payload
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

	// Basic payload validation
	assert.Equal(h.t, runtime.GOOS, payload.OS)
	assert.Equal(h.t, runtime.GOARCH, payload.Arch)
	assert.NotEmpty(h.t, payload.CollectionsCount)
	assert.NotEmpty(h.t, payload.UsedModules)
	assert.Len(h.t, payload.UsedModules, 3)
	assert.Contains(h.t, payload.UsedModules, "text2vec-google-ai-studio")
	assert.Contains(h.t, payload.UsedModules, "text2vec-aws")
	assert.Contains(h.t, payload.UsedModules, "generative-openai")

	// Send payload to channel for test verification
	if h.telemetryChan != nil {
		select {
		case h.telemetryChan <- &payload:
		default:
			// Channel full, but don't block
		}
	}

	// For UPDATE and TERMINATE payloads, verify client usage matches expected counts
	if payload.Type != PayloadType.Init {
		expectedCounts := h.expectedCounts.Get()
		if len(expectedCounts) > 0 {
			assert.NotNil(h.t, payload.ClientUsage, "Expected client usage data but got nil")
			for clientType, expectedCount := range expectedCounts {
				// Sum up all versions for this client type
				versions, exists := payload.ClientUsage[clientType]
				if expectedCount > 0 {
					assert.True(h.t, exists, "Expected client type %s to be in ClientUsage", clientType)
					var totalCount int64
					for _, count := range versions {
						totalCount += count
					}
					assert.Equal(h.t, expectedCount, totalCount, "Mismatch for client type %s: expected %d, got %d", clientType, expectedCount, totalCount)
				}
			}
		}
		// Reset counts after verifying telemetry payload
		h.expectedCounts.Reset()
	}

	h.t.Logf("request body: %s", string(b))
	w.WriteHeader(http.StatusOK)
}

// trackClientRequest is a helper function to simulate a client request
func trackClientRequest(t *testing.T, tel *Telemeter, clientType, userAgent string) {
	req := httptest.NewRequest(http.MethodGet, "/v1/objects", nil)
	if userAgent != "" {
		req.Header.Set("User-Agent", userAgent)
	}
	// Set X-Weaviate-Client header for explicit identification
	switch clientType {
	case "python":
		req.Header.Set("X-Weaviate-Client", "weaviate-client-python/1.0.0")
	case "java":
		req.Header.Set("X-Weaviate-Client", "weaviate-client-java/1.0.0")
	case "typescript":
		req.Header.Set("X-Weaviate-Client", "weaviate-client-typescript/1.0.0")
	case "go":
		req.Header.Set("X-Weaviate-Client", "weaviate-client-go/1.0.0")
	case "csharp":
		req.Header.Set("X-Weaviate-Client", "weaviate-client-csharp/1.0.0")
	}
	tel.clientTracker.Track(req)
}

func TestClientTracker(t *testing.T) {
	t.Run("track and get client counts", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		tracker := NewClientTracker(logger)
		defer tracker.Stop()

		// Create requests for different client types
		pythonReq := httptest.NewRequest(http.MethodGet, "/v1/objects", nil)
		pythonReq.Header.Set("X-Weaviate-Client", "weaviate-client-python/1.0.0")

		javaReq := httptest.NewRequest(http.MethodGet, "/v1/objects", nil)
		javaReq.Header.Set("X-Weaviate-Client", "weaviate-client-java/1.0.0")

		typescriptReq := httptest.NewRequest(http.MethodGet, "/v1/objects", nil)
		typescriptReq.Header.Set("X-Weaviate-Client", "weaviate-client-typescript/1.0.0")

		// Track multiple requests
		tracker.Track(pythonReq)
		tracker.Track(pythonReq)
		tracker.Track(javaReq)
		tracker.Track(typescriptReq)

		// Get counts without resetting
		counts := tracker.Get()
		assert.Equal(t, int64(2), counts[ClientTypePython]["1.0.0"])
		assert.Equal(t, int64(1), counts[ClientTypeJava]["1.0.0"])
		assert.Equal(t, int64(1), counts[ClientTypeTypeScript]["1.0.0"])

		// Verify counts are still there
		counts2 := tracker.Get()
		assert.Equal(t, int64(2), counts2[ClientTypePython]["1.0.0"])
		assert.Equal(t, int64(1), counts2[ClientTypeJava]["1.0.0"])

		// Get and reset
		counts3 := tracker.GetAndReset()
		assert.Equal(t, int64(2), counts3[ClientTypePython]["1.0.0"])
		assert.Equal(t, int64(1), counts3[ClientTypeJava]["1.0.0"])
		assert.Equal(t, int64(1), counts3[ClientTypeTypeScript]["1.0.0"])

		// Verify tracker was reset
		counts4 := tracker.Get()
		assert.Empty(t, counts4)
	})

	t.Run("identify client from User-Agent", func(t *testing.T) {
		testCases := []struct {
			name     string
			header   string
			expected ClientType
		}{
			{
				name:     "Python from explicit header",
				header:   "weaviate-client-python/1.0.0",
				expected: ClientTypePython,
			},
			{
				name:     "Java from explicit header",
				header:   "weaviate-client-java/1.0.0",
				expected: ClientTypeJava,
			},
			{
				name:     "Go from explicit header",
				header:   "weaviate-client-go/1.0.0",
				expected: ClientTypeGo,
			},
			{
				name:     "TypeScript from explicit header",
				header:   "weaviate-client-typescript/1.0.0",
				expected: ClientTypeTypeScript,
			},
			{
				name:     "Csharp from explicit header",
				header:   "weaviate-client-csharp/1.0.0",
				expected: ClientTypeCSharp,
			},
			{
				name:     "Unset header",
				expected: ClientTypeUnknown,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				req := httptest.NewRequest(http.MethodGet, "/v1/objects", nil)
				if tc.header != "" {
					req.Header.Set("X-Weaviate-Client", tc.header)
				}

				// Test client identification directly
				clientInfo := identifyClient(req)
				assert.Equal(t, tc.expected, clientInfo.Type)
			})
		}
	})

	t.Run("thread safety", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		tracker := NewClientTracker(logger)
		defer tracker.Stop()

		// Create requests for different clients
		pythonReq := httptest.NewRequest(http.MethodGet, "/v1/objects", nil)
		pythonReq.Header.Set("X-Weaviate-Client", "weaviate-client-python/1.0.0")

		javaReq := httptest.NewRequest(http.MethodGet, "/v1/objects", nil)
		javaReq.Header.Set("X-Weaviate-Client", "weaviate-client-java/1.0.0")

		// Track concurrently
		const numGoroutines = 10
		const numRequestsPerGoroutine = 100
		done := make(chan struct{})

		for i := 0; i < numGoroutines; i++ {
			go func() {
				for j := 0; j < numRequestsPerGoroutine; j++ {
					if j%2 == 0 {
						tracker.Track(pythonReq)
					} else {
						tracker.Track(javaReq)
					}
				}
				done <- struct{}{}
			}()
		}

		// Wait for all goroutines to complete
		for i := 0; i < numGoroutines; i++ {
			<-done
		}

		// Verify counts
		counts := tracker.GetAndReset()
		expectedPython := int64(numGoroutines * numRequestsPerGoroutine / 2)
		expectedJava := int64(numGoroutines * numRequestsPerGoroutine / 2)
		assert.Equal(t, expectedPython, counts[ClientTypePython]["1.0.0"])
		assert.Equal(t, expectedJava, counts[ClientTypeJava]["1.0.0"])
	})

	t.Run("Get and GetAndReset return nil after Stop", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		tracker := NewClientTracker(logger)

		// Track some requests
		pythonReq := httptest.NewRequest(http.MethodGet, "/v1/objects", nil)
		pythonReq.Header.Set("X-Weaviate-Client", "weaviate-client-python/1.0.0")
		tracker.Track(pythonReq)

		// Verify we can get counts before stop
		counts := tracker.Get()
		assert.NotNil(t, counts)

		// Stop the tracker
		tracker.Stop()

		// Get and GetAndReset should return nil after stop (not block forever)
		assert.Nil(t, tracker.Get())
		assert.Nil(t, tracker.GetAndReset())
	})

	t.Run("double Stop does not panic", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		tracker := NewClientTracker(logger)

		// Should not panic when called multiple times
		assert.NotPanics(t, func() {
			tracker.Stop()
			tracker.Stop()
			tracker.Stop()
		})
	})
}

type gcpTestConsumer struct {
	t *testing.T
}

func (h *gcpTestConsumer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.Contains(r.URL.String(), "/instance") {
		w.WriteHeader(http.StatusOK)
		return
	}
	if strings.Contains(r.URL.String(), "/project/project-id") {
		w.Write([]byte("some-id"))
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusBadRequest)
}

type awsTestConsumer struct {
	t *testing.T
}

func (h *awsTestConsumer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.Contains(r.URL.String(), "/latest/meta-data/") {
		w.WriteHeader(http.StatusOK)
		return
	}
	if strings.Contains(r.URL.String(), "/latest/api/token") {
		w.Write([]byte("some-token"))
		w.WriteHeader(http.StatusOK)
		return
	}
	if strings.Contains(r.URL.String(), "/latest/dynamic/instance-identity/document") {
		w.Write([]byte(`{"accountId":"101"}`))
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusBadRequest)
}
