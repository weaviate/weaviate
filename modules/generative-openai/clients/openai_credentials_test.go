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

package clients

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/modules/generative-openai/config"
	openaiparams "github.com/weaviate/weaviate/modules/generative-openai/parameters"
	"github.com/weaviate/weaviate/usecases/modulecomponents/credentialtest"
)

func TestServerCredentialsOnlyGoToTheConfiguredHost(t *testing.T) {
	const (
		serverKey      = "server-openai-key"
		serverAzureKey = "server-azure-key"
		callerKey      = "caller-key"
	)
	props := &modulecapabilities.GenerateProperties{Text: map[string]string{"prop": "John"}}
	server := credentialtest.NewServer(t, generateResponse{
		Choices: []choice{{Message: &responseMessage{Role: "assistant", Content: "ok"}}},
	})
	// buildUrlFn turns this into https://<fake>/x?.openai.azure.com/...
	injectedResourceName := server.HostPort() + "/x?"

	tests := []struct {
		name         string
		classConfig  map[string]any
		options      *openaiparams.Params
		headers      map[string]string
		wantErr      string
		wantAuth     string
		wantAzureKey string
	}{
		{
			name:    "query baseURL without a request key",
			options: &openaiparams.Params{BaseURL: server.URL},
			wantErr: "a baseURL or endpoint set in the query needs X-Openai-Api-Key",
		},
		{
			name:     "query baseURL with a request key",
			options:  &openaiparams.Params{BaseURL: server.URL},
			headers:  map[string]string{"X-Openai-Api-Key": callerKey},
			wantAuth: "Bearer " + callerKey,
		},
		{
			name:    "baseURL header without a request key",
			headers: map[string]string{"X-Openai-Baseurl": server.URL},
			wantErr: "X-Openai-Baseurl needs X-Openai-Api-Key",
		},
		{
			name:        "class baseURL gets the server key",
			classConfig: map[string]any{"baseURL": server.URL},
			wantAuth:    "Bearer " + serverKey,
		},
		{
			name:    "Azure query baseURL without a request key",
			options: &openaiparams.Params{IsAzure: true, DeploymentID: "d", BaseURL: server.URL},
			wantErr: "a baseURL or endpoint set in the query needs X-Azure-Api-Key",
		},
		{
			name: "Azure from headers alone uses the Azure request key",
			headers: map[string]string{
				"X-Azure-Resource-Name": injectedResourceName, "X-Azure-Deployment-Id": "d", "X-Azure-Api-Key": callerKey,
			},
			wantAzureKey: callerKey,
		},
		{
			name:        "Azure headers on a non-Azure class without a request key",
			classConfig: map[string]any{"baseURL": server.URL},
			headers:     map[string]string{"X-Azure-Resource-Name": "r", "X-Azure-Deployment-Id": "d"},
			wantErr:     "a request that turns on Azure needs X-Azure-Api-Key",
		},
		{
			name:        "query isAzure on a non-Azure class without a request key",
			classConfig: map[string]any{"baseURL": server.URL},
			options:     &openaiparams.Params{IsAzure: true},
			wantErr:     "a request that turns on Azure needs X-Azure-Api-Key",
		},
		{
			name:        "Azure deployment header on a class with only a resourceName without a request key",
			classConfig: map[string]any{"baseURL": server.URL, "resourceName": "corp"},
			headers:     map[string]string{"X-Azure-Deployment-Id": "d"},
			wantErr:     "a request that turns on Azure needs X-Azure-Api-Key",
		},
		{
			name:         "Azure class with a baseURL gets the server key",
			classConfig:  map[string]any{"baseURL": server.URL, "isAzure": true, "deploymentId": "d"},
			wantAzureKey: serverAzureKey,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := len(server.Requests())
			c := New(serverKey, "", serverAzureKey, 0, nullLogger())
			c.httpClient = server.Client()

			classConfig := credentialtest.ClassConfig{"model": "test-model"}
			for k, v := range tt.classConfig {
				classConfig[k] = v
			}
			var options any
			if tt.options != nil {
				options = *tt.options
			}

			_, err := c.GenerateSingleResult(credentialtest.Context(tt.headers), props, "What is my name?", options, false, classConfig)

			requests := server.Requests()[before:]
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.True(t, strings.HasPrefix(err.Error(), tt.wantErr), "the error must start with the reason, got: %s", err.Error())
				assert.Empty(t, requests, "no request may leave Weaviate")
				return
			}
			require.NoError(t, err)
			require.Len(t, requests, 1)
			assert.Equal(t, tt.wantAuth, requests[0].Header.Get("Authorization"))
			assert.Equal(t, tt.wantAzureKey, requests[0].Header.Get("api-key"))
		})
	}
}

// The Azure host comes from the resource name only when the base URL is the
// OpenAI default, so a variant of the default must not move the request to
// api.openai.com with the server's Azure key.
func TestAzureDefaultBaseURLVariantsKeepTheResourceHost(t *testing.T) {
	classConfig := credentialtest.ClassConfig{"resourceName": "corp", "deploymentId": "d"}
	tests := []struct {
		name    string
		options openaiparams.Params
		headers map[string]string
		wantErr string
	}{
		{name: "baseURL header with a trailing slash", headers: map[string]string{"X-Openai-Baseurl": "https://api.openai.com/"}},
		{name: "baseURL header with an upper-case host", headers: map[string]string{"X-Openai-Baseurl": "https://API.openai.com"}},
		{name: "query baseURL with a trailing slash", options: openaiparams.Params{BaseURL: "https://api.openai.com/"}},
		{
			name:    "baseURL header with a path",
			headers: map[string]string{"X-Openai-Baseurl": "https://api.openai.com/v1"},
			wantErr: "X-Openai-Baseurl needs X-Azure-Api-Key",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := New("server-openai-key", "", "server-azure-key", 0, nullLogger())
			params := c.getParameters(classConfig, tt.options, nil)

			endpoint, isAzure, err := c.buildOpenAIUrl(credentialtest.Context(tt.headers), params, config.NewClassSettings(classConfig))

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.True(t, isAzure)
			assert.True(t, strings.HasPrefix(endpoint, "https://corp.openai.azure.com/"), endpoint)
		})
	}
}

// A request that turns on Azure for a non-Azure collection goes to Azure with
// the Azure key, so its body and its error must also be the Azure ones.
func TestAzureFromHeadersBuildsAnAzureRequest(t *testing.T) {
	props := &modulecapabilities.GenerateProperties{Text: map[string]string{"prop": "John"}}
	var mu sync.Mutex
	var bodies []map[string]any
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		bodies = append(bodies, body)
		mu.Unlock()
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":{"message":"rejected"}}`))
	}))
	t.Cleanup(server.Close)

	tests := []struct {
		name        string
		classConfig map[string]any
		headers     map[string]string
	}{
		{
			name:        "Azure class",
			classConfig: map[string]any{"baseURL": server.URL, "isAzure": true, "deploymentId": "d"},
		},
		{
			name:        "Azure headers with the Azure request key on a non-Azure class",
			classConfig: map[string]any{"baseURL": server.URL},
			headers:     map[string]string{"X-Azure-Resource-Name": "r", "X-Azure-Deployment-Id": "d", "X-Azure-Api-Key": "caller-key"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mu.Lock()
			before := len(bodies)
			mu.Unlock()
			c := New("server-openai-key", "", "server-azure-key", 0, nullLogger())
			c.httpClient = server.Client()
			classConfig := credentialtest.ClassConfig{"model": "test-model"}
			for k, v := range tt.classConfig {
				classConfig[k] = v
			}

			_, err := c.GenerateSingleResult(credentialtest.Context(tt.headers), props, "What is my name?", nil, false, classConfig)

			require.Error(t, err)
			assert.Contains(t, err.Error(), "connection to: Azure OpenAI API failed with status: 400")
			mu.Lock()
			defer mu.Unlock()
			require.Len(t, bodies[before:], 1)
			assert.NotContains(t, bodies[before], "model", "the Azure deployment picks the model")
		})
	}
}
