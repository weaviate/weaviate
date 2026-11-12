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

package voyageai

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/usecases/modulecomponents/credentialtest"
)

func TestServerKeyOnlyGoesToTheConfiguredHost(t *testing.T) {
	const (
		serverKey = "server-key"
		callerKey = "caller-key"
	)
	server := credentialtest.NewServer(t, embeddingsResponse{Data: []embeddingsDataResponse{{Embedding: []float32{0.1, 0.2}}}})

	calls := map[string]func(c *Client, ctx context.Context, settings Settings) error{
		"Vectorize": func(c *Client, ctx context.Context, settings Settings) error {
			_, _, _, err := c.Vectorize(ctx, []string{"text"}, settings)
			return err
		},
		"VectorizeQuery": func(c *Client, ctx context.Context, settings Settings) error {
			_, err := c.VectorizeQuery(ctx, []string{"text"}, settings)
			return err
		},
		"VectorizeMultiModal": func(c *Client, ctx context.Context, settings Settings) error {
			_, err := c.VectorizeMultiModal(ctx, []string{"text"}, nil, nil, settings)
			return err
		},
	}
	tests := []struct {
		name         string
		classBaseURL string
		headers      map[string]string
		wantErr      string
		wantAuth     string
	}{
		{
			name:         "baseURL header without a request key",
			classBaseURL: "https://api.voyageai.com/v1",
			headers:      map[string]string{"X-Voyageai-Baseurl": server.URL},
			wantErr:      "X-Voyageai-Baseurl needs X-Voyageai-Api-Key: the server's key is not sent to a host named by the request",
		},
		{
			name:         "baseURL header with a request key",
			classBaseURL: "https://api.voyageai.com/v1",
			headers:      map[string]string{"X-Voyageai-Baseurl": server.URL, "X-Voyageai-Api-Key": callerKey},
			wantAuth:     "Bearer " + callerKey,
		},
		{
			name:         "class baseURL gets the server key",
			classBaseURL: server.URL,
			wantAuth:     "Bearer " + serverKey,
		},
		{
			name:         "baseURL header equal to the class baseURL gets the server key",
			classBaseURL: server.URL,
			headers:      map[string]string{"X-Voyageai-Baseurl": server.URL},
			wantAuth:     "Bearer " + serverKey,
		},
	}
	for callName, call := range calls {
		for _, tt := range tests {
			t.Run(callName+"/"+tt.name, func(t *testing.T) {
				before := len(server.Requests())
				c := New(serverKey, time.Second, &fakeVoyageaiUrlBuilder{pathMask: "/embeddings"}, nullLogger())
				c.httpClient = server.Client()

				err := call(c, credentialtest.Context(tt.headers), Settings{BaseURL: tt.classBaseURL, Model: "voyage-3"})

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
			})
		}
	}
}
