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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	openaiparams "github.com/weaviate/weaviate/modules/generative-openai/parameters"
	"github.com/weaviate/weaviate/usecases/modulecomponents/credentialtest"
)

func TestServerCredentialsOnlyGoToTheConfiguredHost(t *testing.T) {
	const (
		serverKey      = "server-openai-key"
		serverOrg      = "server-org"
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
		wantOrg      string
	}{
		{
			name:    "query baseURL without a request key",
			options: &openaiparams.Params{BaseURL: server.URL},
			wantErr: "baseURL needs X-Openai-Api-Key: the server's key is not sent to a host named by the request",
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
			wantErr: "X-Openai-Baseurl needs X-Openai-Api-Key: the server's key is not sent to a host named by the request",
		},
		{
			name:     "baseURL header with a request key",
			headers:  map[string]string{"X-Openai-Baseurl": server.URL, "X-Openai-Api-Key": callerKey},
			wantAuth: "Bearer " + callerKey,
		},
		{
			name:        "class baseURL gets the server key and organization",
			classConfig: map[string]any{"baseURL": server.URL},
			wantAuth:    "Bearer " + serverKey,
			wantOrg:     serverOrg,
		},
		{
			name:        "query baseURL equal to the class baseURL gets the server key",
			classConfig: map[string]any{"baseURL": server.URL},
			options:     &openaiparams.Params{BaseURL: server.URL},
			wantAuth:    "Bearer " + serverKey,
			wantOrg:     serverOrg,
		},
		{
			name:        "baseURL header equal to the class baseURL gets the server key",
			classConfig: map[string]any{"baseURL": server.URL},
			headers:     map[string]string{"X-Openai-Baseurl": server.URL},
			wantAuth:    "Bearer " + serverKey,
			wantOrg:     serverOrg,
		},
		{
			name:     "server organization not sent to a host the request names",
			options:  &openaiparams.Params{BaseURL: server.URL},
			headers:  map[string]string{"X-Openai-Api-Key": callerKey},
			wantAuth: "Bearer " + callerKey,
		},
		{
			name:     "request organization sent to a host the request names",
			options:  &openaiparams.Params{BaseURL: server.URL},
			headers:  map[string]string{"X-Openai-Api-Key": callerKey, "X-Openai-Organization": "caller-org"},
			wantAuth: "Bearer " + callerKey,
			wantOrg:  "caller-org",
		},
		{
			name:    "Azure query baseURL without a request key",
			options: &openaiparams.Params{IsAzure: true, DeploymentID: "d", BaseURL: server.URL},
			wantErr: "baseURL needs X-Azure-Api-Key",
		},
		{
			name:    "Azure query resourceName without a request key",
			options: &openaiparams.Params{IsAzure: true, ResourceName: injectedResourceName, DeploymentID: "d"},
			wantErr: "resourceName needs X-Azure-Api-Key: the server's key is not sent to a host named by the request",
		},
		{
			name:    "Azure resourceName header without a request key",
			options: &openaiparams.Params{IsAzure: true},
			headers: map[string]string{"X-Azure-Resource-Name": injectedResourceName, "X-Azure-Deployment-Id": "d"},
			wantErr: "X-Azure-Resource-Name needs X-Azure-Api-Key: the server's key is not sent to a host named by the request",
		},
		{
			name: "Azure from headers alone uses the Azure request key",
			headers: map[string]string{
				"X-Azure-Resource-Name": injectedResourceName, "X-Azure-Deployment-Id": "d", "X-Azure-Api-Key": callerKey,
			},
			wantAzureKey: callerKey,
		},
		{
			name:         "Azure query resourceName with a request key",
			options:      &openaiparams.Params{IsAzure: true, ResourceName: injectedResourceName, DeploymentID: "d"},
			headers:      map[string]string{"X-Azure-Api-Key": callerKey},
			wantAzureKey: callerKey,
		},
		{
			name:         "Azure class resourceName gets the server key",
			classConfig:  map[string]any{"resourceName": injectedResourceName, "deploymentId": "d"},
			wantAzureKey: serverAzureKey,
			wantOrg:      serverOrg,
		},
		{
			name:         "Azure query resourceName equal to the class one gets the server key",
			classConfig:  map[string]any{"resourceName": injectedResourceName, "deploymentId": "d"},
			options:      &openaiparams.Params{ResourceName: injectedResourceName},
			wantAzureKey: serverAzureKey,
			wantOrg:      serverOrg,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := len(server.Requests())
			c := New(serverKey, serverOrg, serverAzureKey, 0, nullLogger())
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
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.Empty(t, requests, "no request may leave Weaviate")
				return
			}
			require.NoError(t, err)
			require.Len(t, requests, 1)
			assert.Equal(t, tt.wantAuth, requests[0].Header.Get("Authorization"))
			assert.Equal(t, tt.wantAzureKey, requests[0].Header.Get("api-key"))
			assert.Equal(t, tt.wantOrg, requests[0].Header.Get("OpenAI-Organization"))
		})
	}
}
