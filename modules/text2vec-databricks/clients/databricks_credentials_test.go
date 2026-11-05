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
	"github.com/weaviate/weaviate/usecases/modulecomponents/credentialtest"
)

func TestServerKeyOnlyGoesToTheConfiguredHost(t *testing.T) {
	const (
		serverToken   = "server-token"
		callerToken   = "caller-token"
		classEndpoint = "https://dbc-0000.cloud.databricks.com/serving-endpoints/embed/invocations"
	)
	server := credentialtest.NewServer(t, embedding{
		Object: "list",
		Data:   []embeddingData{{Object: "embedding", Embedding: []float32{0.1, 0.2, 0.3}}},
	})
	serverEndpoint := server.URL + "/serving-endpoints/embed/invocations"

	tests := []struct {
		name            string
		classEndpoint   string
		headers         map[string]string
		validateBaseURL bool
		wantErr         string
		wantToken       string
		wantPath        string
	}{
		{
			name:          "endpoint header without a request token",
			classEndpoint: classEndpoint,
			headers:       map[string]string{"X-Databricks-Endpoint": serverEndpoint},
			wantErr:       "X-Databricks-Endpoint needs X-Databricks-Token: the server's key is not sent to a host named by the request",
		},
		{
			name:          "endpoint header with a request token",
			classEndpoint: classEndpoint,
			headers:       map[string]string{"X-Databricks-Endpoint": serverEndpoint, "X-Databricks-Token": callerToken},
			wantToken:     callerToken,
		},
		{
			name:            "endpoint header with a request token is checked against SSRF",
			classEndpoint:   classEndpoint,
			headers:         map[string]string{"X-Databricks-Endpoint": serverEndpoint, "X-Databricks-Token": callerToken},
			validateBaseURL: true,
			wantErr:         "baseURL cannot target internal addresses",
		},
		{
			name:          "class endpoint gets the server token",
			classEndpoint: serverEndpoint,
			wantToken:     serverToken,
		},
		{
			name:          "endpoint header equal to the class endpoint gets the server token",
			classEndpoint: serverEndpoint,
			headers:       map[string]string{"X-Databricks-Endpoint": serverEndpoint},
			wantToken:     serverToken,
		},
		{
			name:          "endpoint header on the class host gets the server token",
			classEndpoint: serverEndpoint,
			headers:       map[string]string{"X-Databricks-Endpoint": server.URL + "/serving-endpoints/other/invocations"},
			wantToken:     serverToken,
			wantPath:      "/serving-endpoints/other/invocations",
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

			_, _, _, err := c.Vectorize(credentialtest.Context(tt.headers), []string{"This is my text"},
				credentialtest.ClassConfig{"endpoint": tt.classEndpoint})

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
				wantPath = "/serving-endpoints/embed/invocations"
			}
			assert.Equal(t, wantPath, got[0].URL.Path)
		})
	}
}
