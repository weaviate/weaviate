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

package openai

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/usecases/modulecomponents/credentialtest"
)

func TestServerCredentialsOnlyGoToTheConfiguredHost(t *testing.T) {
	const (
		serverKey      = "server-openai-key"
		serverOrg      = "server-org"
		serverAzureKey = "server-azure-key"
		callerKey      = "caller-key"
	)
	server := credentialtest.NewServer(t, embedding{Data: []embeddingData{{Embedding: []float32{0.1, 0.2}}}})
	// buildUrl turns this into https://<fake>/x?.openai.azure.com/...
	injectedResourceName := server.HostPort() + "/x?"

	tests := []struct {
		name         string
		settings     Settings
		headers      map[string]string
		wantErr      string
		wantAuth     string
		wantAzureKey string
		wantOrg      string
	}{
		{
			name:     "baseURL header without a request key",
			settings: Settings{BaseURL: "https://api.openai.com"},
			headers:  map[string]string{"X-Openai-Baseurl": server.URL},
			wantErr:  "X-Openai-Baseurl needs X-Openai-Api-Key: the server's key is not sent to a host named by the request",
		},
		{
			name:     "baseURL header with a request key",
			settings: Settings{BaseURL: "https://api.openai.com"},
			headers:  map[string]string{"X-Openai-Baseurl": server.URL, "X-Openai-Api-Key": callerKey},
			wantAuth: "Bearer " + callerKey,
		},
		{
			name:     "request organization sent to a host the request names",
			settings: Settings{BaseURL: "https://api.openai.com"},
			headers: map[string]string{
				"X-Openai-Baseurl": server.URL, "X-Openai-Api-Key": callerKey, "X-Openai-Organization": "caller-org",
			},
			wantAuth: "Bearer " + callerKey,
			wantOrg:  "caller-org",
		},
		{
			name:     "class baseURL gets the server key and organization",
			settings: Settings{BaseURL: server.URL},
			wantAuth: "Bearer " + serverKey,
			wantOrg:  serverOrg,
		},
		{
			name:     "baseURL header equal to the class baseURL gets the server key",
			settings: Settings{BaseURL: server.URL},
			headers:  map[string]string{"X-Openai-Baseurl": server.URL},
			wantAuth: "Bearer " + serverKey,
			wantOrg:  serverOrg,
		},
		{
			name:     "Azure baseURL header without a request key",
			settings: Settings{IsAzure: true, DeploymentID: "d"},
			headers:  map[string]string{"X-Openai-Baseurl": server.URL},
			wantErr:  "X-Openai-Baseurl needs X-Azure-Api-Key",
		},
		{
			name:     "Azure resourceName header without a request key",
			settings: Settings{IsAzure: true, ResourceName: "classResource", DeploymentID: "d"},
			headers:  map[string]string{"X-Azure-Resource-Name": injectedResourceName},
			wantErr:  "X-Azure-Resource-Name needs X-Azure-Api-Key: the server's key is not sent to a host named by the request",
		},
		{
			name:         "Azure resourceName header with a request key",
			settings:     Settings{IsAzure: true, ResourceName: "classResource", DeploymentID: "d"},
			headers:      map[string]string{"X-Azure-Resource-Name": injectedResourceName, "X-Azure-Api-Key": callerKey},
			wantAzureKey: callerKey,
		},
		{
			name:         "Azure class resourceName gets the server key",
			settings:     Settings{IsAzure: true, ResourceName: injectedResourceName, DeploymentID: "d"},
			wantAzureKey: serverAzureKey,
			wantOrg:      serverOrg,
		},
		{
			name:         "Azure resourceName header equal to the class one gets the server key",
			settings:     Settings{IsAzure: true, ResourceName: injectedResourceName, DeploymentID: "d"},
			headers:      map[string]string{"X-Azure-Resource-Name": injectedResourceName},
			wantAzureKey: serverAzureKey,
			wantOrg:      serverOrg,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := len(server.Requests())
			c := New(serverKey, serverOrg, serverAzureKey, 0, nullLogger())
			c.httpClient = server.Client()
			settings := tt.settings
			settings.Model, settings.Type = "ada", "text"

			_, _, _, err := c.Vectorize(credentialtest.Context(tt.headers), []string{"text"}, settings)

			got := server.Requests()[before:]
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.Empty(t, got, "no request may leave Weaviate")
				return
			}
			require.NoError(t, err)
			require.Len(t, got, 1)
			assert.Equal(t, tt.wantAuth, got[0].Header.Get("Authorization"))
			assert.Equal(t, tt.wantAzureKey, got[0].Header.Get("api-key"))
			assert.Equal(t, tt.wantOrg, got[0].Header.Get("OpenAI-Organization"))
		})
	}
}
