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
	anthropicparams "github.com/weaviate/weaviate/modules/generative-anthropic/parameters"
	"github.com/weaviate/weaviate/usecases/modulecomponents/credentialtest"
)

func TestServerKeyOnlyGoesToTheConfiguredHost(t *testing.T) {
	const (
		serverKey = "server-key"
		callerKey = "caller-key"
	)
	server := credentialtest.NewServer(t, generateResponse{Content: []content{{Type: "text", Text: "ok"}}})
	props := &modulecapabilities.GenerateProperties{Text: map[string]string{"prop": "John"}}

	tests := []struct {
		name        string
		classConfig credentialtest.ClassConfig
		options     *anthropicparams.Params
		headers     map[string]string
		wantErr     string
		wantKey     string
	}{
		{
			name:    "query baseURL without a request key",
			options: &anthropicparams.Params{BaseURL: server.URL},
			wantErr: "baseURL needs X-Anthropic-Api-Key: the server's key is not sent to a host named by the request",
		},
		{
			name:    "baseURL header without a request key",
			headers: map[string]string{"X-Anthropic-Baseurl": server.URL},
			wantErr: "X-Anthropic-Baseurl needs X-Anthropic-Api-Key: the server's key is not sent to a host named by the request",
		},
		{
			name:    "query baseURL with a request key",
			options: &anthropicparams.Params{BaseURL: server.URL},
			headers: map[string]string{"X-Anthropic-Api-Key": callerKey},
			wantKey: callerKey,
		},
		{
			name:    "baseURL header with a request key",
			headers: map[string]string{"X-Anthropic-Baseurl": server.URL, "X-Anthropic-Api-Key": callerKey},
			wantKey: callerKey,
		},
		{
			name:        "class baseURL gets the server key",
			classConfig: credentialtest.ClassConfig{"baseURL": server.URL},
			wantKey:     serverKey,
		},
		{
			name:        "query baseURL equal to the class baseURL gets the server key",
			classConfig: credentialtest.ClassConfig{"baseURL": server.URL},
			options:     &anthropicparams.Params{BaseURL: server.URL},
			wantKey:     serverKey,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := len(server.Requests())
			c := New(serverKey, 0, nullLogger())
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
				assert.Empty(t, got, "no request may leave Weaviate")
				return
			}
			require.NoError(t, err)
			require.Len(t, got, 1)
			assert.Equal(t, tt.wantKey, got[0].Header.Get("x-api-key"))
		})
	}
}
