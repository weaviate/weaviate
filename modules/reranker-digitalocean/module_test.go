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

package modrerankerdigitalocean

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	usecasesconfig "github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/modules"
)

func TestModuleIsAReranker(t *testing.T) {
	m := New()

	assert.Equal(t, "reranker-digitalocean", m.Name())
	assert.Equal(t, modulecapabilities.Text2TextReranker, m.Type())
}

func TestInitProvidesTheRerankProperty(t *testing.T) {
	m := initModule(t, time.Second)

	assert.Contains(t, m.AdditionalProperties(), "rerank")
	meta, err := m.MetaInfo()
	require.NoError(t, err)
	assert.Equal(t, "Reranker - DigitalOcean Serverless Inference", meta["name"])
	assert.NotEmpty(t, meta["documentationHref"])
}

func TestInitPassesTheEnvAPIKey(t *testing.T) {
	t.Setenv("DIGITALOCEAN_APIKEY", "env-key")
	var authorization atomic.Value
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorization.Store(r.Header.Get("Authorization"))
		json.NewEncoder(w).Encode(map[string]any{
			"results": []map[string]any{{"index": 0, "relevance_score": 0.5}},
		})
	}))
	t.Cleanup(server.Close)
	m := initModule(t, time.Second)

	res, err := m.reranker.Rank(context.Background(), "q", "text", []string{"doc"}, classConfig(server.URL))

	require.NoError(t, err)
	require.Len(t, res.DocumentScores, 1)
	assert.Equal(t, 0.5, res.DocumentScores[0].Score)
	assert.Equal(t, "Bearer env-key", authorization.Load())
}

func TestInitPassesTheClientTimeout(t *testing.T) {
	t.Setenv("DIGITALOCEAN_APIKEY", "env-key")
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(release) })
	m := initModule(t, 50*time.Millisecond)

	_, err := m.reranker.Rank(context.Background(), "q", "text", []string{"doc"}, classConfig(server.URL))

	require.ErrorContains(t, err, "send POST request")
}

// ValidateClass through the config the module provider builds, without an
// API key, so the model is not checked against the endpoint.
func TestValidateClassChecksTheBaseURL(t *testing.T) {
	tests := []struct {
		name     string
		baseURL  string
		validate bool
		wantErr  string
	}{
		{name: "http", baseURL: "http://api.example.com", validate: true, wantErr: "baseURL must use HTTPS"},
		{name: "internal address", baseURL: "https://169.254.169.254", validate: true, wantErr: "baseURL cannot target internal addresses"},
		{name: "https to a public address", baseURL: "https://8.8.8.8", validate: true},
		{name: "http while validation is off", baseURL: "http://api.example.com"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("DIGITALOCEAN_APIKEY", "")
			if tt.validate {
				t.Setenv("MODULES_VALIDATE_BASE_URL", "true")
			}
			class := &models.Class{
				Class:        "Doc",
				ModuleConfig: map[string]any{Name: map[string]any{"baseURL": tt.baseURL}},
			}
			cfg := modules.NewClassBasedModuleConfig(class, Name, "", "", &usecasesconfig.Config{})

			err := New().ValidateClass(context.Background(), class, cfg)

			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.EqualError(t, err, tt.wantErr)
		})
	}
}

func initModule(t *testing.T, timeout time.Duration) *RerankerModule {
	t.Helper()
	logger, _ := test.NewNullLogger()
	m := New()
	require.NoError(t, m.Init(context.Background(), moduletools.NewInitParams("", nil,
		&usecasesconfig.Config{ModuleHttpClientTimeout: timeout}, logger, nil)))
	return m
}

func classConfig(baseURL string) moduletools.ClassConfig {
	class := &models.Class{
		Class:        "Doc",
		ModuleConfig: map[string]any{Name: map[string]any{"baseURL": baseURL}},
	}
	return modules.NewClassBasedModuleConfig(class, Name, "", "", &usecasesconfig.Config{})
}
