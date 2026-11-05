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
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/usecases/modulecomponents/credentialtest"
)

const defaultHost = "api.contextual.ai"

// toTestServer sends every request to server; the Host header keeps the host
// the module asked for.
type toTestServer struct{ server *credentialtest.Server }

func (s toTestServer) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.URL.Host = s.server.HostPort()
	return s.server.Client().Transport.RoundTrip(r)
}

func TestServerKeyOnlyGoesToTheConfiguredHost(t *testing.T) {
	const (
		serverKey = "server-key"
		callerKey = "caller-key"
	)
	server := credentialtest.NewServer(t, generateResponse{Response: "ok"})
	props := &modulecapabilities.GenerateProperties{Text: map[string]string{"prop": "John"}}

	tests := []struct {
		name     string
		headers  map[string]string
		grpc     bool
		wantErr  string
		wantKey  string
		wantHost string
	}{
		{
			name:    "baseURL header without a request key",
			headers: map[string]string{"X-ContextualAI-Baseurl": server.URL},
			wantErr: "X-ContextualAI-Baseurl needs X-ContextualAI-Api-Key: the server's key is not sent to a host named by the request",
		},
		{
			name:    "gRPC baseURL header without a request key",
			headers: map[string]string{"X-ContextualAI-Baseurl": server.URL},
			grpc:    true,
			wantErr: "X-ContextualAI-Baseurl needs X-ContextualAI-Api-Key: the server's key is not sent to a host named by the request",
		},
		{
			name:     "baseURL header with a request key",
			headers:  map[string]string{"X-ContextualAI-Baseurl": server.URL, "X-ContextualAI-Api-Key": callerKey},
			wantKey:  callerKey,
			wantHost: server.HostPort(),
		},
		{
			name:     "gRPC baseURL header with a request key",
			headers:  map[string]string{"X-ContextualAI-Baseurl": server.URL, "X-ContextualAI-Api-Key": callerKey},
			grpc:     true,
			wantKey:  callerKey,
			wantHost: server.HostPort(),
		},
		{
			name:     "default host gets the server key",
			wantKey:  serverKey,
			wantHost: defaultHost,
		},
		{
			name:     "baseURL header equal to the default gets the server key",
			headers:  map[string]string{"X-ContextualAI-Baseurl": "https://" + defaultHost},
			wantKey:  serverKey,
			wantHost: defaultHost,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := len(server.Requests())
			c := New(serverKey, 0, nullLogger())
			c.httpClient = &http.Client{Transport: toTestServer{server: server}}
			ctx := credentialtest.Context(tt.headers)
			if tt.grpc {
				ctx = metadata.NewIncomingContext(context.Background(), metadata.New(tt.headers))
			}

			_, err := c.GenerateSingleResult(ctx, props, "What is my name?", nil, false, fakeClassConfig{})

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
			assert.Equal(t, "Bearer "+tt.wantKey, got[0].Header.Get("Authorization"))
		})
	}
}
