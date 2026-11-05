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
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/usecases/build"
	"github.com/weaviate/weaviate/usecases/modulecomponents/credentialtest"
)

func TestServerCredentialsOnlyGoToTheConfiguredHost(t *testing.T) {
	const (
		serverKey  = "server-key"
		serverUUID = "server-uuid"
		callerKey  = "caller-key"
		model      = "qwen3-embedding-0.6b"
	)
	t.Setenv("VECTOR_DB_UUID", serverUUID)
	serverUserAgent := strings.TrimSpace("vector-db/weaviate/" + serverUUID + " " + build.Version)
	anonymousUserAgent := strings.TrimSpace("vector-db/weaviate/unknown " + build.Version)
	server := credentialtest.NewServer(t, embeddingsResponse{
		Object: "list",
		Data:   []embeddingData{{Object: "embedding", Index: 0, Embedding: []float32{0.1, 0.2}}},
	})

	calls := map[string]func(c *Client, ctx context.Context, cfg moduletools.ClassConfig) error{
		"Vectorize": func(c *Client, ctx context.Context, cfg moduletools.ClassConfig) error {
			_, _, _, err := c.Vectorize(ctx, []string{"text"}, cfg)
			return err
		},
		"VectorizeQuery": func(c *Client, ctx context.Context, cfg moduletools.ClassConfig) error {
			_, err := c.VectorizeQuery(ctx, []string{"text"}, cfg)
			return err
		},
	}
	tests := []struct {
		name          string
		classConfig   credentialtest.ClassConfig
		headers       map[string]string
		wantErr       string
		wantAuth      string
		wantUserAgent string
	}{
		{
			name:        "baseURL header without a request key",
			classConfig: credentialtest.ClassConfig{"model": model},
			headers:     map[string]string{"X-Digitalocean-Baseurl": server.URL},
			wantErr:     "X-Digitalocean-Baseurl needs X-Digitalocean-Api-Key: the server's key is not sent to a host named by the request",
		},
		{
			name:          "baseURL header with a request key",
			classConfig:   credentialtest.ClassConfig{"model": model},
			headers:       map[string]string{"X-Digitalocean-Baseurl": server.URL, "X-Digitalocean-Api-Key": callerKey},
			wantAuth:      "Bearer " + callerKey,
			wantUserAgent: anonymousUserAgent,
		},
		{
			name:          "class baseURL gets the server key and UUID",
			classConfig:   credentialtest.ClassConfig{"model": model, "baseURL": server.URL},
			wantAuth:      "Bearer " + serverKey,
			wantUserAgent: serverUserAgent,
		},
		{
			name:          "baseURL header equal to the class baseURL gets the server key and UUID",
			classConfig:   credentialtest.ClassConfig{"model": model, "baseURL": server.URL},
			headers:       map[string]string{"X-Digitalocean-Baseurl": server.URL},
			wantAuth:      "Bearer " + serverKey,
			wantUserAgent: serverUserAgent,
		},
		{
			name:          "request key to the class baseURL gets the server UUID",
			classConfig:   credentialtest.ClassConfig{"model": model, "baseURL": server.URL},
			headers:       map[string]string{"X-Digitalocean-Api-Key": callerKey},
			wantAuth:      "Bearer " + callerKey,
			wantUserAgent: serverUserAgent,
		},
	}
	for callName, call := range calls {
		for _, tt := range tests {
			t.Run(callName+"/"+tt.name, func(t *testing.T) {
				before := len(server.Requests())
				c := New(serverKey, 5*time.Second, logrus.New())
				c.httpClient = server.Client()

				err := call(c, credentialtest.Context(tt.headers), tt.classConfig)

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
				assert.Equal(t, tt.wantUserAgent, got[0].Header.Get("User-Agent"), "the server UUID only goes to the configured host")
			})
		}
	}
}
