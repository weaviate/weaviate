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
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/usecases/modulecomponents/credentialtest"
)

func TestServerKeyOnlyGoesToTheConfiguredHost(t *testing.T) {
	const (
		serverKey = "server-key"
		callerKey = "caller-key"
	)
	vector := &embedding{Segments: []segment{{Float: []float32{0.1, 0.2}}}}
	server := credentialtest.NewServer(t, embedResponse{TextEmbedding: vector, ImageEmbedding: vector})
	image := base64.StdEncoding.EncodeToString([]byte("image"))

	calls := map[string]func(v *vectorizer, ctx context.Context, cfg moduletools.ClassConfig) error{
		"Vectorize": func(v *vectorizer, ctx context.Context, cfg moduletools.ClassConfig) error {
			_, err := v.Vectorize(ctx, []string{"text"}, nil, cfg)
			return err
		},
		"VectorizeQuery": func(v *vectorizer, ctx context.Context, cfg moduletools.ClassConfig) error {
			_, err := v.VectorizeQuery(ctx, []string{"text"}, cfg)
			return err
		},
		"VectorizeImages": func(v *vectorizer, ctx context.Context, cfg moduletools.ClassConfig) error {
			_, err := v.VectorizeImages(ctx, []string{image}, cfg)
			return err
		},
	}
	tests := []struct {
		name        string
		classConfig credentialtest.ClassConfig
		headers     map[string]string
		wantErr     string
		wantKey     string
	}{
		{
			name:    "baseURL header without a request key",
			headers: map[string]string{"X-Twelvelabs-Baseurl": server.URL},
			wantErr: "X-Twelvelabs-Baseurl needs X-Twelvelabs-Api-Key: the server's key is not sent to a host named by the request",
		},
		{
			name:    "baseURL header with a request key",
			headers: map[string]string{"X-Twelvelabs-Baseurl": server.URL, "X-Twelvelabs-Api-Key": callerKey},
			wantKey: callerKey,
		},
		{
			name:        "class baseURL gets the server key",
			classConfig: credentialtest.ClassConfig{"baseURL": server.URL},
			wantKey:     serverKey,
		},
		{
			name:        "baseURL header equal to the class baseURL gets the server key",
			classConfig: credentialtest.ClassConfig{"baseURL": server.URL},
			headers:     map[string]string{"X-Twelvelabs-Baseurl": server.URL},
			wantKey:     serverKey,
		},
	}
	for callName, call := range calls {
		for _, tt := range tests {
			t.Run(callName+"/"+tt.name, func(t *testing.T) {
				before := len(server.Requests())
				v := New(serverKey, 0, nullLogger())
				v.httpClient = server.Client()

				err := call(v, credentialtest.Context(tt.headers), tt.classConfig)

				got := server.Requests()[before:]
				if tt.wantErr != "" {
					require.Error(t, err)
					assert.Contains(t, err.Error(), tt.wantErr)
					assert.Empty(t, got, "no request may leave Weaviate")
					return
				}
				require.NoError(t, err)
				require.Len(t, got, 1)
				assert.Equal(t, tt.wantKey, got[0].Header.Get("x-api-key"))
			})
		}
	}
}
