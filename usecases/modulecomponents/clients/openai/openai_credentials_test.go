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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/usecases/modulecomponents/credentialtest"
)

func TestServerCredentialsOnlyGoToTheConfiguredHost(t *testing.T) {
	const (
		serverKey      = "server-openai-key"
		serverAzureKey = "server-azure-key"
		callerKey      = "caller-key"
	)
	server := credentialtest.NewServer(t, embedding{Data: []embeddingData{{Embedding: []float32{0.1, 0.2}}}})
	tests := []struct {
		name         string
		settings     Settings
		headers      map[string]string
		wantErr      string
		wantAuth     string
		wantAzureKey string
		wantHost     string
	}{
		{
			name:     "baseURL header without a request key",
			settings: Settings{BaseURL: "https://api.openai.com"},
			headers:  map[string]string{"X-Openai-Baseurl": server.URL},
			wantErr:  "X-Openai-Baseurl needs X-Openai-Api-Key: the server's key is sent only to the configured URL",
		},
		{
			name:     "baseURL header with a request key",
			settings: Settings{BaseURL: "https://api.openai.com"},
			headers:  map[string]string{"X-Openai-Baseurl": server.URL, "X-Openai-Api-Key": callerKey},
			wantAuth: "Bearer " + callerKey,
			wantHost: server.HostPort(),
		},
		{
			name:     "class baseURL gets the server key",
			settings: Settings{BaseURL: server.URL},
			wantAuth: "Bearer " + serverKey,
			wantHost: server.HostPort(),
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
			headers:  map[string]string{"X-Azure-Resource-Name": "other"},
			wantErr:  "X-Azure-Resource-Name needs X-Azure-Api-Key: the server's key is sent only to the configured URL",
		},
		{
			name:         "Azure resourceName header with a request key",
			settings:     Settings{IsAzure: true, ResourceName: "classResource", DeploymentID: "d"},
			headers:      map[string]string{"X-Azure-Resource-Name": "callers-resource", "X-Azure-Api-Key": callerKey},
			wantAzureKey: callerKey,
			wantHost:     "callers-resource.openai.azure.com",
		},
		{
			name:         "Azure class resourceName gets the server key",
			settings:     Settings{IsAzure: true, ResourceName: "corp", DeploymentID: "d"},
			wantAzureKey: serverAzureKey,
			wantHost:     "corp.openai.azure.com",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := len(server.Requests())
			c := New(serverKey, "", serverAzureKey, 0, nullLogger())
			c.httpClient = server.ClientForAnyHost()
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
			assert.Equal(t, tt.wantHost, got[0].Host)
			assert.Equal(t, tt.wantAuth, got[0].Header.Get("Authorization"))
			assert.Equal(t, tt.wantAzureKey, got[0].Header.Get("api-key"))
		})
	}
}

// The Azure host comes from the resource name only when the base URL is the
// OpenAI default, so a variant of the default must not move the request to
// api.openai.com with the server's Azure key.
func TestAzureDefaultBaseURLVariantsKeepTheResourceHost(t *testing.T) {
	settings := Settings{
		BaseURL: "https://api.openai.com", ResourceName: "corp", DeploymentID: "d", ApiVersion: "v", IsAzure: true,
	}
	tests := []struct {
		name    string
		header  string
		wantErr string
	}{
		{name: "trailing slash", header: "https://api.openai.com/"},
		{name: "upper-case host", header: "https://API.openai.com"},
		{name: "path", header: "https://api.openai.com/v1", wantErr: "X-Openai-Baseurl needs X-Azure-Api-Key"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := New("server-openai-key", "", "server-azure-key", 0, nullLogger())

			endpoint, err := c.buildURL(credentialtest.Context(map[string]string{"X-Openai-Baseurl": tt.header}), settings)

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.True(t, strings.HasPrefix(endpoint, "https://corp.openai.azure.com/"), endpoint)
		})
	}
}

func TestResourceNameThatIsNotAHostLabelSendsNoRequest(t *testing.T) {
	server := credentialtest.NewServer(t, embedding{Data: []embeddingData{{Embedding: []float32{0.1, 0.2}}}})

	for _, resourceName := range []string{"attacker.example#", "attacker.example/", "attacker.example?", "10.0.0.5:8443/x#"} {
		tests := []struct {
			name     string
			settings Settings
			headers  map[string]string
		}{
			{
				name:     "class resourceName",
				settings: Settings{IsAzure: true, ResourceName: resourceName, DeploymentID: "d"},
			},
			{
				name:     "resourceName header with a request key",
				settings: Settings{IsAzure: true, ResourceName: "corp", DeploymentID: "d"},
				headers:  map[string]string{"X-Azure-Resource-Name": resourceName, "X-Azure-Api-Key": "caller-key"},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name+" "+resourceName, func(t *testing.T) {
				before := len(server.Requests())
				c := New("server-openai-key", "", "server-azure-key", 0, nullLogger())
				c.httpClient = server.ClientForAnyHost()
				settings := tt.settings
				settings.Model, settings.Type = "ada", "text"

				_, _, _, err := c.Vectorize(credentialtest.Context(tt.headers), []string{"text"}, settings)

				require.Error(t, err)
				assert.Contains(t, err.Error(), "resourceName must be 1 to 63 letters, digits or hyphens")
				assert.Empty(t, server.Requests()[before:], "no request may leave Weaviate")
			})
		}
	}
}
