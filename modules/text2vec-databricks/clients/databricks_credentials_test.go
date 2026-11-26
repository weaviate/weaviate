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
		name          string
		classEndpoint string
		headers       map[string]string
		wantErr       string
		wantToken     string
	}{
		{
			name:          "endpoint header without a request token",
			classEndpoint: classEndpoint,
			headers:       map[string]string{"X-Databricks-Endpoint": serverEndpoint},
			wantErr:       "X-Databricks-Endpoint needs X-Databricks-Token: the server's key is sent only to the configured URL",
		},
		{
			name:          "endpoint header with a request token",
			classEndpoint: classEndpoint,
			headers:       map[string]string{"X-Databricks-Endpoint": serverEndpoint, "X-Databricks-Token": callerToken},
			wantToken:     callerToken,
		},
		{
			name:          "class endpoint gets the server token",
			classEndpoint: serverEndpoint,
			wantToken:     serverToken,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := len(server.Requests())
			c := New(serverToken, 0, nullLogger())
			c.httpClient = server.Client()

			_, _, _, err := c.Vectorize(credentialtest.Context(tt.headers), []string{"This is my text"},
				credentialtest.ClassConfig{"endpoint": tt.classEndpoint})

			got := server.Requests()[before:]
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.Empty(t, got, "no request may leave Weaviate")
				return
			}
			require.NoError(t, err)
			require.Len(t, got, 1)
			assert.Equal(t, "Bearer "+tt.wantToken, got[0].Header.Get("Authorization"))
			assert.Equal(t, "/serving-endpoints/embed/invocations", got[0].URL.Path)
		})
	}
}
