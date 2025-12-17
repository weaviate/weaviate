//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
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
	"math/rand"
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
			assert.Equal(t, int64(2), payload.ClientUsage[ClientTypePython])
			assert.Equal(t, int64(1), payload.ClientUsage[ClientTypeJava])
			assert.Equal(t, int64(1), payload.ClientUsage[ClientTypeTypeScript])
			assert.Equal(t, int64(1), payload.ClientUsage[ClientTypeGo])
			assert.Equal(t, int64(1), payload.ClientUsage[ClientTypeCSharp])
			// Verify tracker was reset after GetAndReset
			currentCounts := tel.clientTracker.Get()
			assert.Empty(t, currentCounts)
		})

		t.Run("on terminate", func(t *testing.T) {
			tel, sg, _ := newTestTelemeter()
			sg.On("LocalNodeStatus", context.Background(), "", "", verbosity.OutputVerbose).Return(
				&models.NodeStatus{
					Stats: &models.NodeStats{
						ObjectCount: 300_000_000_000,
					},
				})
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
			assert.Equal(t, int64(1), payload.ClientUsage[ClientTypePython])
			assert.Equal(t, int64(1), payload.ClientUsage[ClientTypeJava])
			assert.Equal(t, int64(1), payload.ClientUsage[ClientTypeTypeScript])
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

var expectedClientCounts = map[ClientType]int64{}

func TestTelemetry_WithConsumer(t *testing.T) {
	// Reset expected client counts for this test
	expectedClientCounts = make(map[ClientType]int64)

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

	// Create a context that will be cancelled when the test completes
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start goroutines for each client type that continuously send requests
	clientTypes := []struct {
		clientType ClientType
		frequency  time.Duration
	}{
		{ClientTypePython, chooseClientRequestFrequency()},
		{ClientTypeJava, chooseClientRequestFrequency()},
		{ClientTypeTypeScript, chooseClientRequestFrequency()},
	}

	for _, ct := range clientTypes {
		go func(clientType ClientType, freq time.Duration) {
			ticker := time.NewTicker(freq)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					trackClientRequest(t, tel, string(clientType), fmt.Sprintf("weaviate-client-%s/1.0.0", clientType))
					expectedClientCounts[clientType]++
				}
			}
		}(ct.clientType, ct.frequency)
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	start := time.Now()
	wait := make(chan struct{})
	go func() {
		for range ticker.C {
			if time.Since(start) > time.Second {
				cancel() // Signal the client request goroutine to stop
				err = tel.Stop(context.Background())
				assert.Nil(t, err)
				wait <- struct{}{}
				return
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
		// INIT payloads should not have client usage
		assert.Nil(h.t, payload.ClientUsage)
	} else {
		// UPDATE and TERMINATE payloads should have client usage if clients were tracked
		// (may be nil if no clients were tracked)
		if len(expectedClientCounts) > 0 {
			assert.NotNil(h.t, payload.ClientUsage, "Expected client usage data but got nil")
			for clientType, expectedCount := range expectedClientCounts {
				actualCount, exists := payload.ClientUsage[clientType]
				if expectedCount > 0 {
					assert.True(h.t, exists, "Expected client type %s to be in ClientUsage", clientType)
					assert.Equal(h.t, expectedCount, actualCount, "Mismatch for client type %s", clientType)
				}
			}
		}
		expectedClientCounts = make(map[ClientType]int64)
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
		tracker := NewClientTracker()

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
		assert.Equal(t, int64(2), counts[ClientTypePython])
		assert.Equal(t, int64(1), counts[ClientTypeJava])
		assert.Equal(t, int64(1), counts[ClientTypeTypeScript])

		// Verify counts are still there
		counts2 := tracker.Get()
		assert.Equal(t, int64(2), counts2[ClientTypePython])
		assert.Equal(t, int64(1), counts2[ClientTypeJava])

		// Get and reset
		counts3 := tracker.GetAndReset()
		assert.Equal(t, int64(2), counts3[ClientTypePython])
		assert.Equal(t, int64(1), counts3[ClientTypeJava])
		assert.Equal(t, int64(1), counts3[ClientTypeTypeScript])

		// Verify tracker was reset
		counts4 := tracker.Get()
		assert.Empty(t, counts4)
	})

	t.Run("identify client from User-Agent", func(t *testing.T) {
		tracker := NewClientTracker()

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

				// Track the request
				tracker.Track(req)

				// Get counts and verify
				counts := tracker.GetAndReset()
				if tc.expected != ClientTypeUnknown {
					assert.Greater(t, counts[tc.expected], int64(0), "Expected %s to be tracked", tc.expected)
				} else {
					// Unknown clients should not be tracked
					assert.Empty(t, counts)
				}
			})
		}
	})

	t.Run("thread safety", func(t *testing.T) {
		tracker := NewClientTracker()

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
		assert.Equal(t, expectedPython, counts[ClientTypePython])
		assert.Equal(t, expectedJava, counts[ClientTypeJava])
	})
}

// chooseClientRequestFrequency returns a random time.Duration between 5 and 100 milliseconds
func chooseClientRequestFrequency() time.Duration {
	return time.Duration(randIntBetween(50, 100)) * time.Millisecond
}

// randomIntBetween returns a random integer between min and max, inclusive.
func randIntBetween(min, max int) int {
	return min + rand.Intn(max-min+1)
}
