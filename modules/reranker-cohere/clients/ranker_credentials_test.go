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
		serverKey = "server-key"
		callerKey = "caller-key"
	)
	server := credentialtest.NewServer(t, RankResponse{Results: []Result{{Index: 0, RelevanceScore: 0.9}}})

	tests := []struct {
		name        string
		classConfig credentialtest.ClassConfig
		headers     map[string]string
		wantErr     string
		wantAuth    string
	}{
		{
			name:    "baseURL header without a request key",
			headers: map[string]string{"X-Cohere-Baseurl": server.URL},
			wantErr: "X-Cohere-Baseurl needs X-Cohere-Api-Key: the server's key is not sent to a host named by the request",
		},
		{
			name:     "baseURL header with a request key",
			headers:  map[string]string{"X-Cohere-Baseurl": server.URL, "X-Cohere-Api-Key": callerKey},
			wantAuth: "Bearer " + callerKey,
		},
		{
			name:        "class baseURL gets the server key",
			classConfig: credentialtest.ClassConfig{"baseURL": server.URL},
			wantAuth:    "Bearer " + serverKey,
		},
		{
			name:        "baseURL header equal to the class baseURL gets the server key",
			classConfig: credentialtest.ClassConfig{"baseURL": server.URL},
			headers:     map[string]string{"X-Cohere-Baseurl": server.URL},
			wantAuth:    "Bearer " + serverKey,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := len(server.Requests())
			c := New(serverKey, 0, nullLogger())
			c.httpClient = server.Client()

			_, err := c.Rank(credentialtest.Context(tt.headers), "Where do I work?", []string{"I work at Apple"}, tt.classConfig)

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
