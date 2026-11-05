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
	databricksparams "github.com/weaviate/weaviate/modules/generative-databricks/parameters"
	"github.com/weaviate/weaviate/usecases/modulecomponents/credentialtest"
)

func TestServerKeyOnlyGoesToTheConfiguredHost(t *testing.T) {
	const (
		serverToken   = "server-token"
		callerToken   = "caller-token"
		classEndpoint = "https://dbc-0000.cloud.databricks.com/serving-endpoints/chat/invocations"
	)
	server := credentialtest.NewServer(t, generateResponse{Choices: []choice{{Text: "ok"}}})
	props := &modulecapabilities.GenerateProperties{Text: map[string]string{"prop": "John"}}
	serverEndpoint := server.URL + "/serving-endpoints/chat/invocations"

	tests := []struct {
		name            string
		classConfig     credentialtest.ClassConfig
		options         *databricksparams.Params
		headers         map[string]string
		validateBaseURL bool
		wantErr         string
		wantToken       string
		wantPath        string
	}{
		{
			name:        "query endpoint without a request token",
			classConfig: credentialtest.ClassConfig{"endpoint": classEndpoint},
			options:     &databricksparams.Params{Endpoint: serverEndpoint},
			wantErr:     "baseURL needs X-Databricks-Token: the server's key is not sent to a host named by the request",
		},
		{
			name:        "endpoint header without a request token",
			classConfig: credentialtest.ClassConfig{"endpoint": classEndpoint},
			headers:     map[string]string{"X-Databricks-Endpoint": serverEndpoint},
			wantErr:     "X-Databricks-Endpoint needs X-Databricks-Token: the server's key is not sent to a host named by the request",
		},
		{
			name:        "query endpoint with a request token",
			classConfig: credentialtest.ClassConfig{"endpoint": classEndpoint},
			options:     &databricksparams.Params{Endpoint: serverEndpoint},
			headers:     map[string]string{"X-Databricks-Token": callerToken},
			wantToken:   callerToken,
		},
		{
			name:        "endpoint header with a request token",
			classConfig: credentialtest.ClassConfig{"endpoint": classEndpoint},
			headers:     map[string]string{"X-Databricks-Endpoint": serverEndpoint, "X-Databricks-Token": callerToken},
			wantToken:   callerToken,
		},
		{
			name:        "endpoint header wins over the query endpoint",
			classConfig: credentialtest.ClassConfig{"endpoint": classEndpoint},
			options:     &databricksparams.Params{Endpoint: classEndpoint},
			headers:     map[string]string{"X-Databricks-Endpoint": serverEndpoint, "X-Databricks-Token": callerToken},
			wantToken:   callerToken,
		},
		{
			name:            "query endpoint with a request token is checked against SSRF",
			classConfig:     credentialtest.ClassConfig{"endpoint": classEndpoint},
			options:         &databricksparams.Params{Endpoint: serverEndpoint},
			headers:         map[string]string{"X-Databricks-Token": callerToken},
			validateBaseURL: true,
			wantErr:         "baseURL cannot target internal addresses",
		},
		{
			name:            "endpoint header with a request token is checked against SSRF",
			classConfig:     credentialtest.ClassConfig{"endpoint": classEndpoint},
			headers:         map[string]string{"X-Databricks-Endpoint": serverEndpoint, "X-Databricks-Token": callerToken},
			validateBaseURL: true,
			wantErr:         "baseURL cannot target internal addresses",
		},
		{
			name:        "class endpoint gets the server token",
			classConfig: credentialtest.ClassConfig{"endpoint": serverEndpoint},
			wantToken:   serverToken,
		},
		{
			name:        "query endpoint equal to the class endpoint gets the server token",
			classConfig: credentialtest.ClassConfig{"endpoint": serverEndpoint},
			options:     &databricksparams.Params{Endpoint: serverEndpoint},
			wantToken:   serverToken,
		},
		{
			name:        "endpoint header equal to the class endpoint gets the server token",
			classConfig: credentialtest.ClassConfig{"endpoint": serverEndpoint},
			headers:     map[string]string{"X-Databricks-Endpoint": serverEndpoint},
			wantToken:   serverToken,
		},
		{
			name:        "query endpoint on the class host gets the server token",
			classConfig: credentialtest.ClassConfig{"endpoint": serverEndpoint},
			options:     &databricksparams.Params{Endpoint: server.URL + "/serving-endpoints/other/invocations"},
			wantToken:   serverToken,
			wantPath:    "/serving-endpoints/other/invocations",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.validateBaseURL {
				t.Setenv("MODULES_VALIDATE_BASE_URL", "true")
			}
			before := len(server.Requests())
			c := New(serverToken, 0, nullLogger())
			c.httpClient = server.Client()
			var options any
			if tt.options != nil {
				options = *tt.options
			}

			_, err := c.GenerateSingleResult(credentialtest.Context(tt.headers), props, "What is my name?", options, false, tt.classConfig)

			got := server.Requests()[before:]
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				if tt.validateBaseURL {
					assert.NotContains(t, err.Error(), "needs", "the SSRF check must reject it, not the token check")
				}
				assert.Empty(t, got, "no request may leave Weaviate")
				return
			}
			require.NoError(t, err)
			require.Len(t, got, 1)
			assert.Equal(t, "Bearer "+tt.wantToken, got[0].Header.Get("Authorization"))
			wantPath := tt.wantPath
			if wantPath == "" {
				wantPath = "/serving-endpoints/chat/invocations"
			}
			assert.Equal(t, wantPath, got[0].URL.Path)
		})
	}
}
